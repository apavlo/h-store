package ca.evanjones.protorpc;

import com.google.protobuf.RpcCallback;

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

    @SuppressWarnings("unchecked")
    private static NullCallback instance = new NullCallback();
}
