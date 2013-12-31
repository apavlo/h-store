package edu.brown.protorpc;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/** Wraps a Java NIO selector to dispatch events. */
public class NIOEventLoop implements EventLoop {
    private static final Logger LOG = Logger.getLogger(NIOEventLoop.class);
    
    public NIOEventLoop() {
        try {
            selector = Selector.open();
        } catch (IOException e) { throw new RuntimeException(e); }
    }

    public void setExitOnSigInt(boolean exitOnSigInt) {
        if (exitOnSigInt) {
            if (sigintHandler != null) {
                throw new IllegalStateException("SIGINT handler already enabled.");
            }
            // Install a handler to exit cleanly on sigint
            // TODO: This doesn't actually work if there are multiple EventLoops in an app.
            sigintHandler = new SigintHandler();
            sigintHandler.install();
        } else {
            if (sigintHandler == null) {
                throw new IllegalStateException("SIGINT handler not enabled.");
            }
            sigintHandler.remove();
            sigintHandler = null;
        }
    }

    private static final Signal SIGINT = new Signal("INT");
    private class SigintHandler implements SignalHandler {
        @Override
        public void handle(Signal signal) {
            System.out.println(signal);
            // mark that we should quit and interrupt the selector. unregister SIGINT
            setExitOnSigInt(false);
            exitLoop();
        }

        public void install() {
            assert oldHandler == null;
            oldHandler = Signal.handle(SIGINT, this);
        }

        public void remove() {
            if (oldHandler != null) {
                Signal.handle(SIGINT, oldHandler);
                oldHandler = null;
            }
        }

        private SignalHandler oldHandler = null;
    }

    @Override
    public void registerRead(SelectableChannel channel, Handler handler) {
        // Disallow both being registered for read events and connection events at the same time.
        // On Linux, when a connect fails, the socket is ready for both events, which causes
        // errors when reads are attempted on the closed socket.
        assert channel.keyFor(selector) == null ||
                (channel.keyFor(selector).interestOps() & SelectionKey.OP_CONNECT) == 0;
        addInterest(channel, SelectionKey.OP_READ, handler);
    }

    @Override
    public void registerAccept(ServerSocketChannel server, Handler handler) {
        assert server.keyFor(selector) == null;
        register(server, SelectionKey.OP_ACCEPT, handler);
    }

    @Override
    public void registerConnect(SocketChannel channel, Handler handler) {
        // Should not be registered
        assert channel.keyFor(selector) == null;
        register(channel, SelectionKey.OP_CONNECT, handler);
    }

    @Override
    public void registerWrite(SelectableChannel channel, Handler handler) {
        addInterest(channel, SelectionKey.OP_WRITE, handler);
    }

    @Override
    public void registerTimer(int timerMilliseconds, Handler handler) {
        assert timerMilliseconds >= 0;
        assert handler != null;
        long expirationMs = System.currentTimeMillis() + timerMilliseconds;
        timers.add(new Timer(expirationMs, handler));
    }

    @Override
    public void cancelTimer(Handler handler) {
        Iterator<Timer> timerIterator = timers.iterator();
        while (timerIterator.hasNext()) {
            Timer timer = timerIterator.next();
            if (timer.handler == handler) {
                timerIterator.remove();
                return;
            }
        }
        throw new IllegalArgumentException("Timer handler not found");
    }

    private void register(SelectableChannel channel, int ops, Handler callback) {
        try {
            channel.configureBlocking(false);
            /*SelectionKey serverKey =*/ channel.register(selector, ops, callback);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addInterest(SelectableChannel channel, int operation, Handler callback) {
        // TODO: Support multiple handlers?
        SelectionKey key = channel.keyFor(selector);
        if (key != null) {
            assert (key.interestOps() & operation) == 0;
            if (key.attachment() == null) {
                key.attach(callback);
            } else {
                assert callback == key.attachment();
            }
            key.interestOps(key.interestOps() | operation);
            // TODO: This fixes a synchronization issue where one thread changes the interest set
            // of a thread while another thread is blocked in select(), because the Selector
            // documentation states that it waits for events registered "as of the moment that the
            // selection operation began. Is there a better fix?
            selector.wakeup();
        } else {
            register(channel, operation, callback);
        }
    }

    public void run() {
        if (LOG.isDebugEnabled()) LOG.debug("Starting run() loop");
        while (!exitLoop) {
            runOnce();
        }
        exitLoop = false;
        if (LOG.isDebugEnabled()) LOG.debug("Completed run() loop");
    }

    public void runOnce() {
        long timeoutMs = 0;
        if (!timers.isEmpty()) {
            long now = System.currentTimeMillis();
            timeoutMs = triggerExpiredTimers(now);
        }

        try {
            int readyCount = selector.select(timeoutMs);
            handleSelectedKeys();
            if (readyCount == 0) {
                // TODO: Avoid checking this at both the top and the bottom of the loop.
                triggerExpiredTimers(System.currentTimeMillis());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** @return milliseconds until the next timer, or 0 if there are none. */
    private long triggerExpiredTimers(long now) {
        while (!timers.isEmpty()) {
            // hope that using an iterator to fetch and remove the least element is efficient?
            Timer least = timers.peek();
            if (least.expirationMs <= now) {
                timers.poll();

                least.handler.timerCallback();
            } else {
                // this timer has not expired yet: return its time
                long timeoutMs = least.expirationMs - now;
                assert timeoutMs > 0;
                return timeoutMs;
            }
        }

        return 0;
    }

//    public void close() {
//        try {
//            for (SelectionKey key : selector.keys()) {
//                if (key.attachment() != server) {
//                    MessageConnection connection = (MessageConnection) key.attachment();
//                    connection.close();
//                }
//            }
//            selector.close();
//            server.close();
//            eventQueue.clear();
//        } catch (IOException e) { throw new RuntimeException(e); }
//    }
//

    private void handleSelectedKeys() throws IOException {
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        for (Iterator<SelectionKey> it = selectedKeys.iterator(); it.hasNext(); ) {
            SelectionKey key = it.next();
            EventLoop.Handler callback = (EventLoop.Handler) key.attachment();

            // only handle one event per loop. more efficient: most times only one event is ready,
            // and it avoids canceled key exceptions
            if (key.isReadable()) {
                callback.readCallback(key.channel());
            } else if (key.isWritable()) {
                boolean stillNeedsWrite = callback.writeCallback(key.channel());
                if (!stillNeedsWrite) {
                    // Unregister write callbacks
                    assert key.interestOps() == (SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                    key.interestOps(SelectionKey.OP_READ);
                }
            } else if (key.isAcceptable()) {
                callback.acceptCallback(key.channel());
            } else if (key.isConnectable()) {
                assert key.interestOps() == SelectionKey.OP_CONNECT;
                key.interestOps(0);
                key.attach(null);
                callback.connectCallback((SocketChannel) key.channel());
            } else {
                // Mac OS X has a bug: when an async connect fails, this triggers with
                // key.readyOps == 0.
                assert key.readyOps() == 0;
                assert (key.interestOps() & SelectionKey.OP_CONNECT) != 0;
                System.out.println("Mac bug? no interest: connection failed?");
                callback.connectCallback((SocketChannel) key.channel());
            }
        }

        // Must remove the keys from the selected set
        selectedKeys.clear();

        // Handle any queued thread events
        Runnable callback = null;
        while ((callback = threadEvents.poll()) != null) {
            callback.run();
        }
    }

    public void runInEventThread(Runnable callback) {
        threadEvents.add(callback);
        selector.wakeup();
    }

    public void exitLoop() {
        if (LOG.isDebugEnabled()) LOG.debug("Stopping running loop");
        exitLoop = true;
        selector.wakeup();
    }

    private final Selector selector;
    private SigintHandler sigintHandler;
    // volatile because signal handlers run in other threads
    private volatile boolean exitLoop = false;
    private final ConcurrentLinkedQueue<Runnable> threadEvents =
            new ConcurrentLinkedQueue<Runnable>();

    private static final class Timer implements Comparable<Timer> {
        public final long expirationMs;
        public final Handler handler;

        public Timer(long expirationMs, Handler handler) {
            this.expirationMs = expirationMs;
            this.handler = handler;
        }

        @Override
        public int compareTo(Timer other) {
            if (this.expirationMs < other.expirationMs) return -1;
            if (this.expirationMs > other.expirationMs) return 1;
            return 0;
        }

        @Override
        public boolean equals(Object other) {
            throw new RuntimeException("TODO: implement");
        }

        @Override
        public int hashCode() {
            throw new RuntimeException("TODO: implement");
        }
    }
    private final PriorityQueue<Timer> timers = new PriorityQueue<Timer>();
}
