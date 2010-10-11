package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;

import edu.brown.markov.TransactionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.FinishRequest;
import edu.mit.hstore.HStoreCoordinatorNode;

/**
 * Base class used to perform the final operations of when a txn completes
 * @author pavlo
 */
public abstract class AbstractTxnCallback {
    private static final Logger LOG = Logger.getLogger(AbstractTxnCallback.class);
    
    protected final HStoreCoordinatorNode hstore_coordinator;
    protected final RpcCallback<byte[]> done;
    protected final long txn_id;
    protected final TransactionEstimator t_estimator;
 
    public AbstractTxnCallback(HStoreCoordinatorNode hstore_coordinator, long txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
        this.hstore_coordinator = hstore_coordinator;
        this.t_estimator = t_estimator;
        this.txn_id = txn_id;
        this.done = done;
        
        assert(this.hstore_coordinator != null) : "Null HStoreCoordinatorNode for txn #" + this.txn_id;
//        assert(this.t_estimator != null) : "Null TransactionEstimator for txn #" + this.txn_id;
    }
    
    public void prepareFinish(byte[] output, Dtxn.FragmentResponse.Status status) {
        boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
        final boolean trace = LOG.isTraceEnabled();
        if (trace) LOG.trace("Got callback for txn #" + this.txn_id + " [bytes=" + output.length + ", commit=" + commit + ", status=" + status + "]");
        
        // According to the where ever the VoltProcedure was running, our transaction is
        // now complete (either aborted or committed). So we need to tell Dtxn.Coordinator
        // to go fuck itself and send the final messages to everyone that was involved
        FinishRequest.Builder builder = FinishRequest.newBuilder()
                                            .setTransactionId((int)this.txn_id)
                                            .setCommit(commit);
        ClientCallback callback = new ClientCallback(this.hstore_coordinator, this.txn_id, output, commit, this.done);
        FinishRequest finish = builder.build();
        if (trace) LOG.debug("Calling Dtxn.Coordinator.finish() for txn #" + this.txn_id);
        
        this.hstore_coordinator.getDtxnCoordinator().finish(new ProtoRpcController(), finish, callback);
        
        // Then clean-up any extra information that we may have for the txn
        if (this.t_estimator != null) {
            if (commit) {
                if (trace) LOG.trace("Telling the TransactionEstimator to COMMIT txn #" + this.txn_id);
                this.t_estimator.commit(this.txn_id);
            } else {
                if (trace) LOG.trace("Telling the TransactionEstimator to ABORT txn #" + this.txn_id);
                this.t_estimator.abort(this.txn_id);
            }
        }
    }
} // END CLASS