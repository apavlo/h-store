/**
 *
 */
package edu.brown.protorpc;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

public final class MockEventLoop implements EventLoop {
    @Override
    public void registerRead(SelectableChannel channel, Handler handler) {
        this.handler = handler;
    }

    @Override
    public void registerAccept(ServerSocketChannel channel, Handler handler) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void registerWrite(SelectableChannel channel, Handler handler) {
        if (writeHandler != null) {
            throw new IllegalStateException("Each channel can only call registerWrite() once");
        }
        writeHandler = handler;
    }

    @Override
    public void registerConnect(SocketChannel channel, Handler handler) {
        // This is just ignored: needed for ProtoRpcChannelTest
    }

    @Override
    public void registerTimer(int timerMilliseconds, Handler handler) {
        assert timerMilliseconds >= 0;
        assert handler != null;
        this.timerMilliseconds = timerMilliseconds;
        timerHandler = handler;
    }

    @Override
    public void cancelTimer(Handler handler) {
        throw new UnsupportedOperationException("TODO: implement");
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
    public int timerMilliseconds;
    public Handler timerHandler;
}
