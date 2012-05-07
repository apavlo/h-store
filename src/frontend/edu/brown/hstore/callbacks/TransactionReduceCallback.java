package edu.brown.hstore.callbacks;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse.ReduceResult;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

/**
 * This callback waits until all of the TransactionMapResponses have come
 * back from all other partitions in the cluster.
 * @author pavlo
 */
public class TransactionReduceCallback extends AbstractTransactionCallback<TransactionReduceResponse, TransactionReduceResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionReduceCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final VoltTable finalResults[];
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionReduceCallback(HStoreSite hstore_site) {
        super(hstore_site);
        this.finalResults = new VoltTable[hstore_site.getAllPartitionIds().size()];
    }

    public void init(MapReduceTransaction ts) {
        super.init(ts, ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.finishImpl();
        for (int i = 0; i < this.finalResults.length; i++) 
            this.finalResults[i] = null; 
    }
    
    /**
     * This gets invoked after all of the partitions have finished
     * executing the map phase for this txn
     */
    @Override
    protected boolean unblockTransactionCallback() {
        if (debug.get())
            LOG.debug(ts + " is ready to execute. Passing to HStoreSite");
        
        // Client gets the final result, and  txn  is about to finish
        
        // STEP 1
        // Send the final result from all the partitions for this MR job
        // back to the client.
        ClientResponseImpl cresponse = new ClientResponseImpl(); 
        cresponse.init(ts.getTransactionId().longValue(),
                       ts.getClientHandle(), 
                       ts.getBasePartition(), 
                       Status.OK, 
                       this.finalResults, 
                       "",
                       ts.getPendingError()); 
        hstore_site.sendClientResponse(ts, cresponse);

        // STEP 2
        // Initialize the FinishCallback and tell every partition in the cluster
        // to clean up this transaction because we're done with it!
        this.finishTransaction(Status.OK);
        return (false);
    }
    
    @Override
    protected boolean abortTransactionCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getTransactionId();
        return (true);
    }
    
    @Override
    protected int runImpl(TransactionReduceResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with status %s for %s [partitions=%s]",
                                    response.getClass().getSimpleName(),
                                    response.getStatus(),
                                    this.ts, 
                                    response.getResultsList()));
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), response.getStatus());
        // Otherwise, make sure it's legit
        assert(this.ts.getTransactionId().longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId(), this.getTransactionId());
        
        if (response.getStatus() != Status.OK || this.isAborted()) {
            this.abort(response.getStatus());
        } else {
            // Here we should receive the reduceOutput data
            for (ReduceResult pr : response.getResultsList()) {
                int partition = pr.getPartitionId();
                ByteBuffer bs = pr.getData().asReadOnlyByteBuffer();
                
                VoltTable vt = null;
                try {
                    vt = FastDeserializer.deserialize(bs, VoltTable.class);
                } catch (Exception ex) {
                    throw new RuntimeException("Unexpected error when deserializing VoltTable", ex);
                }
                assert(vt != null);
                if (debug.get()) {
                    byte bytes[] = pr.getData().toByteArray();
                    LOG.debug(String.format("Inbound Partition reduce result for Partition #%d: RowCount=%d / MD5=%s / Length=%d",
                                            partition, vt.getRowCount(),StringUtil.md5sum(bytes), bytes.length));
                }
                this.finalResults[partition] = vt;
            } // FOR
        }
        
        return (response.getResultsCount());
    }
}