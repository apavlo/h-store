package ca.evanjones.protorpc;

import com.google.protobuf.RpcCallback;

/** An RpcCallback implementation that just stores the parameter. Can be reused. */
public class StoreResultCallback<ParameterType> implements RpcCallback<ParameterType> {
    public void run(ParameterType rpcResult) {
        result = rpcResult;
    }

    public void reset() { result = null; }
    public ParameterType getResult() { return result; }
    private ParameterType result = null;
}
