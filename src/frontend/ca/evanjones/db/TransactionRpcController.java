package ca.evanjones.db;

import ca.evanjones.db.Transactions.Status;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class TransactionRpcController extends Transaction implements RpcController {
    public TransactionRpcController() {
        reset();
    }

    public void startTransaction(int id) {
        if (status != null) {
            throw new IllegalStateException(
                    "attempted to start an in progress transaction. (reset() may be needed?)");
        }
        this.id = id;
        status = Status.PREPARED;
    }

    @Override
    public void reset() {
        status = null;
        super.reset();
    }

    @Override
    public String errorText() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean failed() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean isCanceled() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void notifyOnCancel(RpcCallback<Object> callback) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void setFailed(String reason) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void startCancel() {
        throw new RuntimeException("unimplemented");
    }

    /** Makes this transaction abort with reason abortStatus. Typically used for ABORT_USER. */
    public void setAbort(Status abortStatus) {
        assert status == Status.PREPARED;
        assert abortStatus != Status.PREPARED;
        status = abortStatus;
    }

    public Status getStatus() { return status; }
    public int getId() { return id; }

    private int id;
    private Status status;
}
