package edu.brown.protorpc;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;

import edu.brown.protorpc.EventLoop.Handler;

/** Implements all EventLoop.Handler methods by throwing UnsupportedOperationException. */
public abstract class AbstractEventHandler implements Handler {
    @Override
    public void acceptCallback(SelectableChannel channel) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void readCallback(SelectableChannel channel) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void connectCallback(SocketChannel channel) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean writeCallback(SelectableChannel channel) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void timerCallback() {
        throw new UnsupportedOperationException("not implemented");
    }
}
