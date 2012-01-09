package edu.brown.protorpc;

import com.google.protobuf.RpcCallback;

/** An RpcCallback implementation that just stores the parameter. Can be reused. */
public class StoreResultCallback<ParameterType> implements RpcCallback<ParameterType> {
    private ParameterType result = null;
    private boolean called = false;

    public void run(ParameterType rpcResult) {
        result = rpcResult;
        called = true;
    }

    public void reset() {
        result = null;
        called = false;
    }

    public ParameterType getResult() { return result; }
    public boolean wasCalled() { return called; }
}
