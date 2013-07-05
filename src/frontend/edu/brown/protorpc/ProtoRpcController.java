package edu.brown.protorpc;

import ca.evanjones.protorpc.Protocol;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.utils.StringUtil;

public class ProtoRpcController implements RpcController {
    // Call reset() to initialize everything
    public ProtoRpcController() { reset(); }

    // This is effectively the "constructor" for this class.
    @Override
    public void reset() {
        eventLoop = null;
        builder = null;
        callback = null;
        status = Protocol.Status.INVALID;
        errorText = null;
    }

    @Override
    public String errorText() {
        checkRpcState();
        if (status == Protocol.Status.OK) {
            throw new IllegalStateException("RPC has not failed");
        }
        return errorText;
    }

    @Override
    public boolean failed() {
        checkRpcState();
        return status != Protocol.Status.OK;
    }

    private void checkRpcState() {
        if (status == Protocol.Status.INVALID) {
            String message = "No RPC active.";
            if (callback != null) {
                message = "RPC has not completed.";
            }
            throw new IllegalStateException(message);
        }
    }

    @Override
    public boolean isCanceled() {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public void notifyOnCancel(RpcCallback<Object> callback) {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public void setFailed(String reason) {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public void startCancel() {
        throw new UnsupportedOperationException("unimplemented");
    }

    /** Wait for the current RPC to complete. */
    public void block() {
        if (status == Protocol.Status.OK) {
            // Assume mockFinishRpcForTest was called.
            return;
        }

        assert eventLoop != null;
        assert callback != null;

        // TODO: If we have a threaded implementation, this will need to be more complicated.
        while (callback != null) {
            eventLoop.runOnce();
        }
    }

    public void startRpc(EventLoop eventLoop, Message.Builder builder, RpcCallback<Message> callback) {
        if (this.callback != null) {
            throw new IllegalStateException(
                    "ProtoRpcController already in use by another RPC call; " +
                    "wait for callback before reusing.");
        }
        if (callback == null) {
            throw new NullPointerException("callback cannot be null");
        }
        assert this.eventLoop == null;
        assert eventLoop != null;
        assert this.builder == null;
        assert builder != null;

        this.eventLoop = eventLoop;
        this.builder = builder;
        this.callback = callback;
        status = Protocol.Status.INVALID;
    }

    /** Finish an RPC for unit tests. */
    // TODO: Create a factory to avoid needing this?
    public void mockFinishRpcForTest() {
        assert status == Protocol.Status.INVALID || status == Protocol.Status.OK;
        assert callback == null;
        status = Protocol.Status.OK; 
    }

    public void finishRpcSuccess(ByteString response) {
        assert response != null;
        finishRpc(Protocol.Status.OK, response, null);
    }

    public void finishRpcFailure(Protocol.Status status, String errorText) {
        finishRpc(status, null, errorText);
    }

    private void finishRpc(Protocol.Status status, ByteString response, String errorText) {
        assert this.status == Protocol.Status.INVALID :
            String.format("Trying to invoke finishRPC more than once [status=%s, errorText=%s]\n%s",
                          this.status, errorText, response);
        assert callback != null;

        assert status != Protocol.Status.INVALID;
        boolean success = status == Protocol.Status.OK;
        if (success) {
            assert response != null;
            assert errorText == null;
        } else {
            assert response == null;
            assert errorText != null;
        }

        // Set the status and reset state before we invoke the callback
        this.status = status;
        this.errorText = errorText;
        eventLoop = null;
        Message.Builder tempBuilder = builder;        
        builder = null;
        RpcCallback<Message> tempCallback = callback;
        callback = null;

        Message result = null;
        if (success) {
            try {
                tempBuilder.mergeFrom(response);
                result = tempBuilder.build();
            } catch (InvalidProtocolBufferException e) {
                System.err.println("RESPONSE: " + StringUtil.hexDump(response));
                System.err.println("BUILDER:  " + tempBuilder.toString());
                throw new RuntimeException(e);
            }
        }
        tempCallback.run(result);
    }

    private EventLoop eventLoop;
    private Message.Builder builder;
    private RpcCallback<Message> callback;
    private Protocol.Status status;
    private String errorText;
}
