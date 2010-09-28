package ca.evanjones.db;

import com.google.protobuf.Service;

/** Interface for services that support transactions, and being executed by TransactionServer. */
public interface TransactionalService extends Service {
    /** Called when transaction is committing or aborting. If it aborts, this implementation must
     * undo its effects. */
    public void finish(TransactionRpcController transaction, boolean commit);
}
