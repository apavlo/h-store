package edu.brown.protorpc;

import com.google.protobuf.RpcCallback;

/** A singleton RpcCallback that does nothing. */
public final class NullCallback<ParameterType> implements RpcCallback<ParameterType> {
    @Override
    public void run(ParameterType parameter) {
        // Do nothing!
    }

    @SuppressWarnings("unchecked")
    public static <Type> NullCallback<Type> getInstance() {
        return (NullCallback<Type>) instance;
    }

    private NullCallback() {}

    private final static NullCallback<?> instance = new NullCallback<Object>();
}
