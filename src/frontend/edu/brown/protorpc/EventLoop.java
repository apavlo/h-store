package edu.brown.protorpc;

import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public interface EventLoop {
    public interface Handler {
        public void acceptCallback(SelectableChannel channel);
        public void readCallback(SelectableChannel channel);
        public void connectCallback(SocketChannel channel);

        /** @return true if this callback should remain enabled. */
        public boolean writeCallback(SelectableChannel channel);

        public void timerCallback();
    }

    /** Registers handler to receive read callbacks when channel is ready for reading. */
    void registerRead(SelectableChannel channel, Handler handler);

    /** Registers handler to receive accept callbacks when channel is ready for accepting. */
    void registerAccept(ServerSocketChannel channel, Handler handler);

    /** Registers handler to receive write callbacks when channel is ready for writing. */
    void registerWrite(SelectableChannel channel, Handler handler);

    /** Registers handler to receive connect callbacks when channel is connected. */
    void registerConnect(SocketChannel channel, Handler handler);

    /** Registers a one time callback to be triggered after timerMilliseconds. */
    void registerTimer(int timerMilliseconds, Handler handler);

    /** Cancel a timer that has not yet been called. */
    void cancelTimer(Handler handler);

    void runInEventThread(Runnable callback);

    void run();
    void runOnce();
}
