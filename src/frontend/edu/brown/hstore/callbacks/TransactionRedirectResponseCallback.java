package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreObjectPools;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.utils.Poolable;

/**
 * This callback is used by the receiving HStoreSite during a transaction redirect.
 * It must be given the TransactionRedirectResponse callback so that we know how
 * pass the ClientResponse back to the other HStoreSite, which will then send the
 * results back to the client
 * @author pavlo
 */
public class TransactionRedirectResponseCallback implements RpcCallback<byte[]>, Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectResponseCallback.class);
    
    private RpcCallback<TransactionRedirectResponse> orig_callback;
    private int source_id = -1;
    private int dest_id = -1;

    /**
     * Default Constructor
     */
    public TransactionRedirectResponseCallback() {
        // Nothing to do...
    }
    
    public void init(int source_id, int dest_id, RpcCallback<TransactionRedirectResponse> orig_callback) {
        this.orig_callback = orig_callback;
        this.source_id = source_id;
        this.dest_id = dest_id;
    }

    @Override
    public boolean isInitialized() {
        return (this.orig_callback != null);
    }
    
    @Override
    public void finish() {
        this.orig_callback = null;
        this.source_id = -1;
        this.dest_id = -1;
    }
    
    @Override
    public void run(byte[] parameter) {
        final boolean trace = LOG.isTraceEnabled();
        if (trace) LOG.trace(String.format("Got ClientResponse callback! Sending back to %s [bytes=%d]",
                                           HStoreThreadManager.formatSiteName(this.dest_id), parameter.length));
        ByteString bs = ByteString.copyFrom(parameter);
        TransactionRedirectResponse response = TransactionRedirectResponse.newBuilder()
                                                              .setSenderSite(this.source_id)
                                                              .setOutput(bs)
                                                              .build();
        this.orig_callback.run(response);
        if (trace) LOG.trace("Sent our ClientResponse back. Returning to regularly scheduled program...");
        try {
            this.finish();
            HStoreObjectPools.CALLBACKS_TXN_REDIRECTRESPONSE.returnObject(this);
        } catch (Exception ex) {
            throw new RuntimeException("Funky failure", ex);
        }
    }
    
}
