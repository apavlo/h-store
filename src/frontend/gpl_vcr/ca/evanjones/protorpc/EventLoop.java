package ca.evanjones.protorpc;

import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;

public interface EventLoop {
    public interface Handler {
        public void acceptCallback(SelectableChannel channel);
        public void readCallback(SelectableChannel channel);

        /** @return true if this callback should remain enabled. */
        public boolean writeCallback(SelectableChannel channel);
    }

    /** Registers handler to receive read callbacks when channel is ready for reading. */
    void registerRead(SelectableChannel channel, Handler handler);

    /** Registers handler to receive accept callbacks when channel is ready for accepting. */
    void registerAccept(ServerSocketChannel channel, Handler handler);

    /** Registers handler to receive write callbacks when channel is ready for writing. */
    void registerWrite(SelectableChannel channel, Handler handler);

    void runInEventThread(Runnable callback);

    void run();
    void runOnce();
}
