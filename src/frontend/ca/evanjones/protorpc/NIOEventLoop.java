package ca.evanjones.protorpc;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/** Wraps a Java NIO selector to dispatch events. */
public class NIOEventLoop implements EventLoop {
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
        register(channel, SelectionKey.OP_READ, handler);
    }

    @Override
    public void registerAccept(ServerSocketChannel server, Handler handler) {
        register(server, SelectionKey.OP_ACCEPT, handler);
    }

    @Override
    public void registerWrite(SelectableChannel channel, Handler handler) {
        // TODO: Support multiple handlers?
        SelectionKey serverKey = channel.keyFor(selector);
        if (serverKey != null) {
            assert (serverKey.interestOps() & SelectionKey.OP_WRITE) == 0;
            assert handler == serverKey.attachment();
            serverKey.interestOps(serverKey.interestOps() | SelectionKey.OP_WRITE);
        } else {
            try {
                channel.register(selector, SelectionKey.OP_WRITE, handler);
            } catch (ClosedChannelException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void register(SelectableChannel channel, int ops, Handler callback) {
        try {
            channel.configureBlocking(false);
            /*SelectionKey serverKey =*/ channel.register(selector, ops, callback);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void run() {
        while (!exitLoop) {
            runOnce();
        }
        exitLoop = false;
    }

    public void runOnce() {
        try {
            selector.select();
            handleSelectedKeys();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

            // only handle one event per loop. more efficient and avoids cancelled key exceptions
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
        exitLoop = true;
        selector.wakeup();
    }

    private final Selector selector;
    private SigintHandler sigintHandler;
    // volatile because signal handlers run in other threads
    private volatile boolean exitLoop = false;
    private final ConcurrentLinkedQueue<Runnable> threadEvents =
            new ConcurrentLinkedQueue<Runnable>();
}
