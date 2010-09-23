/**
 *
 */
package ca.evanjones.protorpc;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SelectableChannel;

public final class MockEventLoop implements EventLoop {
    @Override
    public void registerRead(SelectableChannel channel, Handler handler) {
        this.handler = handler;
    }

    @Override
    public void registerAccept(ServerSocketChannel channel, Handler handler) {}

    @Override
    public void registerWrite(SelectableChannel channel, Handler handler) {
        if (writeHandler != null) {
            throw new IllegalStateException("Each channel can only call registerWrite() once");
        }
        writeHandler = handler;
    }

    @Override
    public void run() {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public void runInEventThread(Runnable callback) {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public void runOnce() {
        throw new UnsupportedOperationException("TODO: implement");
    }

    public Handler handler;
    public Handler writeHandler;
}
