package edu.brown.hstore.callbacks;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
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
public class TransactionReduceCallback extends BlockingCallback<TransactionReduceResponse, TransactionReduceResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionReduceCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private MapReduceTransaction ts;
    private TransactionFinishCallback finish_callback;
    private final VoltTable finalResults[];
    
    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionReduceCallback(HStoreSite hstore_site) {
        super(hstore_site, true);
        this.finalResults = new VoltTable[hstore_site.getAllPartitionIds().size()];
    }

    public void init(MapReduceTransaction ts) {
        if (debug.get())
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.finish_callback = null;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), null);
    }
    
    @Override
    protected void finishImpl() {
        this.ts = null;
        for (int i = 0; i < this.finalResults.length; i++) 
            this.finalResults[i] = null; 
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }
    
    /**
     * This gets invoked after all of the partitions have finished
     * executing the map phase for this txn
     */
    @Override
    protected void unblockCallback() {
        if (this.isAborted() == false) {
            if (debug.get())
                LOG.debug(ts + " is ready to execute. Passing to HStoreSite");
            
            // Client gets the final result, and txn is about to finish
            
            // STEP 1
            // Send the final result from all the partitions for this MR job
            // back to the client.
            ClientResponseImpl cresponse = new ClientResponseImpl(ts.getTransactionId(),
                                                                  ts.getClientHandle(),
                                                                  ts.getBasePartition(),
                                                                  Status.OK,
                                                                  this.finalResults,
                                                                  "");
           hstore_site.sendClientResponse(ts, cresponse);

           if (hstore_site.getHStoreConf().site.mr_map_blocking) {
               // STEP 2
               // Initialize the FinishCallback and tell every partition in the cluster
               // to clean up this transaction because we're done with it!
               this.finish_callback = this.ts.initTransactionFinishCallback(Hstoreservice.Status.OK);
               hstore_site.getCoordinator().transactionFinish(ts, Status.OK, this.finish_callback);
           }
            
        } else {
            assert(this.finish_callback != null);
            this.finish_callback.allowTransactionCleanup();
        }
    }
    
    @Override
    protected void abortCallback(Status status) {
        assert(this.isInitialized()) : "ORIG TXN: " + this.getOrigTransactionId();
        
        // If we abort, then we have to send out an ABORT to
        // all of the partitions that we originally sent INIT requests too
        // Note that we do this *even* if we haven't heard back from the remote
        // HStoreSite that they've acknowledged our transaction
        // We don't care when we get the response for this
        this.finish_callback = this.ts.initTransactionFinishCallback(status);
        this.finish_callback.disableTransactionCleanup();
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, this.finish_callback);
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
            String.format("Missing MapReduceTransaction handle for txn #%d", response.getTransactionId());
        assert(response.getResultsCount() > 0) :
            String.format("No partitions returned in %s for %s", response.getClass().getSimpleName(), this.ts);
        
        long orig_txn_id = this.getOrigTransactionId();
        long resp_txn_id = response.getTransactionId();
        long ts_txn_id = this.ts.getTransactionId();
        
        // If we get a response that matches our original txn but the LocalTransaction handle 
        // has changed, then we need to will just ignore it
        if (orig_txn_id == resp_txn_id && orig_txn_id != ts_txn_id) {
            if (debug.get()) LOG.debug(String.format("Ignoring %s for a different transaction #%d [origTxn=#%d]",
                                                     response.getClass().getSimpleName(), resp_txn_id, orig_txn_id));
            return (0);
        }
        // Otherwise, make sure it's legit
        assert(ts_txn_id == resp_txn_id) :
            String.format("Unexpected %s for a different transaction %s != #%d [expected=#%d]",
                          response.getClass().getSimpleName(), this.ts, resp_txn_id, ts_txn_id);
        
        if (response.getStatus() != Hstoreservice.Status.OK || this.isAborted()) {
            this.abort(response.getStatus());
            return (0);
        }
        // Here we should receive the reduceOutput data
        
        for (ReduceResult pr : response.getResultsList()) {
            int partition = pr.getPartitionId();
            ByteBuffer bs = pr.getData().asReadOnlyByteBuffer();
            
            VoltTable vt = null;
            try {
                vt = FastDeserializer.deserialize(bs, VoltTable.class);
                
            } catch (Exception ex) {
                LOG.warn("Unexpected error when deserializing VoltTable", ex);
            }
            assert(vt != null);
            if (debug.get()) {
                byte bytes[] = pr.getData().toByteArray();
                LOG.debug(String.format("Inbound Partition reduce result for Partition #%d: RowCount=%d / MD5=%s / Length=%d",
                                        partition, vt.getRowCount(),StringUtil.md5sum(bytes), bytes.length));
            }
            
            this.finalResults[partition] = vt;
        } // FOR
        
        return (response.getResultsCount());
    }
}