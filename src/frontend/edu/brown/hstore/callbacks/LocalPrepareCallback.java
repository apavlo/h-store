package edu.brown.hstore.callbacks;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * This callback is invoked at the base partition once the txn gets all of
 * the 2PC:PREPARE acknowledgments. This is where we will invoke the HStoreSite
 * to send the ClientResponse back to the client.
 * @author pavlo
 */
public class LocalPrepareCallback extends PartitionCountingCallback<LocalTransaction> implements RpcCallback<TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(LocalPrepareCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    private final List<TransactionPrepareResponse> responses = new ArrayList<TransactionPrepareResponse>();
    
    /**
     * Constructor
     * @param hstore_site
     */
    public LocalPrepareCallback(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    public void init(LocalTransaction ts, PartitionSet partitions) {
        this.responses.clear();
        super.init(ts, partitions);
    }

    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    protected void unblockCallback() {
        if (debug.val)
            LOG.debug(String.format("%s - Unblocking callback and sending back ClientResponse", this.ts));
        if (hstore_conf.site.txn_profiling && this.ts.profiler != null) {
            if (debug.val) LOG.debug(ts + " - TransactionProfiler.stopPostPrepare() / " + Status.OK);
            this.ts.profiler.stopPostPrepare();
            this.ts.profiler.startPostFinish();
        }

        // Everybody returned ok, so we'll tell them to all commit right now
        // so that they can start executing other things
        this.finishTransaction(Status.OK);
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        ClientResponseImpl cresponse = this.ts.getClientResponse();
        assert(cresponse.isInitialized()) :
            "Trying to send back ClientResponse for " + ts + " before it was set!";
        this.hstore_site.responseSend(this.ts, cresponse);
    }
    
    @Override
    protected void abortCallback(int partition, Status status) {
        if (debug.val)
            LOG.debug(String.format("%s - Aborting callback [status=%s]", this.ts, status));
        if (hstore_conf.site.txn_profiling && this.ts.profiler != null) {
            if (debug.val)
                LOG.debug(ts + " - TransactionProfiler.stopPostPrepare() / " + status);
            this.ts.profiler.stopPostPrepare();
            this.ts.profiler.startPostFinish();
        }
        
        // We don't care whether our transaction was rejected or not because we 
        // know that we still need to call TransactionFinish, which will delete
        // the final transaction state
        if (status == Status.ABORT_RESTART || status == Status.ABORT_SPECULATIVE) {
            hstore_site.getTransactionQueueManager().restartTransaction(this.ts, status);
        }
        // Change the response's status and send back the result to the client
        else {
            ClientResponseImpl cresponse = this.ts.getClientResponse();
            cresponse.setStatus(status);
            if (debug.val)
                LOG.debug(String.format("%s - Sending back %s %s",
                          this.ts, status, cresponse.getClass().getSimpleName()));
            this.hstore_site.responseSend(this.ts, cresponse);
        }
    }
    
    // ----------------------------------------------------------------------------
    // RPC CALLBACK
    // ----------------------------------------------------------------------------
    
    @Override
    public void run(TransactionPrepareResponse response) {
        if (debug.val)
            LOG.debug(String.format("Got %s with %d partitions for %s",
                      response.getClass().getSimpleName(),
                      response.getPartitionsCount(),
                      this.ts));
        assert(this.ts != null) :
            String.format("Missing LocalTransaction handle for txn #%d [status=%s]",
                          response.getTransactionId(), response.getStatus());
        assert(this.ts.getTransactionId().longValue() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId());
        
        // If any TransactionPrepareResponse comes back with anything but an OK,
        // then the we need to abort the transaction immediately
        // TODO: Instead of OK, we should have different status types for what the
        //       remote partition did. It should be PREPARE_OK or PREPARE_COMMIT
        //       If it's a PREPARE_COMMIT then we know that we don't need to send
        //       a COMMIT message to it in the next round.
        this.responses.add(response);
        if (response.getStatus() != Status.OK) {
            boolean first = true;
            for (int partition : response.getPartitionsList()) {
                if (first) {
                    this.abort(partition, response.getStatus());
                    first = false;
                } else {
                    this.decrementCounter(partition);
                }
            } // FOR
        } else {
            for (Integer partition : response.getPartitionsList()) {
                this.run(partition.intValue());
            } // FOR
        }
    }
    
} // END CLASS