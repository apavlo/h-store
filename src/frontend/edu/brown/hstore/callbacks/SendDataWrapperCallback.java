package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * This is callback is used on the remote side of a TransactionMapRequest
 * so that the network-outbound callback is not invoked until all of the partitions
 * at this HStoreSite is finished with the Map phase. 
 * @author pavlo
 */
public class SendDataWrapperCallback extends BlockingRpcCallback<SendDataResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(SendDataWrapperCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private SendDataResponse.Builder builder = null;
    private MapReduceTransaction ts = null;
    
    public SendDataResponse.Builder getBuilder() {
        return builder;
    }

    public SendDataWrapperCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(MapReduceTransaction ts, RpcCallback<SendDataResponse> orig_callback) {
        assert(this.isInitialized() == false) :
            String.format("Trying to initialize %s twice! [origTs=%s, newTs=%s]",
                          this.getClass().getSimpleName(), this.ts, ts);
        if (debug.val)
            LOG.debug("Starting new " + this.getClass().getSimpleName() + " for " + ts);
        this.ts = ts;
        this.builder = SendDataResponse.newBuilder()
                             .setTransactionId(ts.getTransactionId().longValue())
                             .setStatus(Hstoreservice.Status.OK);
        super.init(ts.getTransactionId(), hstore_site.getLocalPartitionIds().size(), orig_callback);
    }
    
    @Override
    protected void abortCallback(Status status) {
        if (debug.val)
            LOG.debug(String.format("Txn #%d - Aborting %s with status %s",
                                    this.getTransactionId(), this.getClass().getSimpleName(), status));
        this.builder.setStatus(status);
               
        this.unblockCallback();
    }

    @Override
    protected void finishImpl() {
        this.builder = null;
        this.ts = null;
    }
    
    @Override
    public boolean isInitialized() {
        return ( this.ts != null && this.builder != null && super.isInitialized());
    }

    @Override
    protected synchronized int runImpl(Integer partition) {
        assert(this.ts != null) :
            String.format("Missing MapReduceTransaction handle for txn #%d", this.ts.getTransactionId());
        
        return 1;
    }

    @Override
    protected void unblockCallback() {
        if (debug.val) {
            LOG.debug(String.format("Txn #%d - Sending %s to %s with status %s",
                                    this.getTransactionId(),
                                    TransactionInitResponse.class.getSimpleName(),
                                    this.getOrigCallback().getClass().getSimpleName(),
                                    this.builder.getStatus()));
        }
        
        assert(this.getOrigCallback() != null) :
            String.format("The original callback for txn #%d is null!", this.getTransactionId());
        this.getOrigCallback().run(this.builder.build());
    }
            
    

   

    
}
