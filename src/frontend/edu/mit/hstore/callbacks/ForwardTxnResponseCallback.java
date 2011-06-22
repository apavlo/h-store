package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.MessageAcknowledgement;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.Poolable;
import edu.mit.hstore.HStoreSite;

public class ForwardTxnResponseCallback implements RpcCallback<byte[]>, Poolable {
    private static final Logger LOG = Logger.getLogger(ForwardTxnResponseCallback.class);
    
    /**
     * Object Pool Factory
     */
    public static class Factory extends CountingPoolableObjectFactory<ForwardTxnResponseCallback> {
        
        public Factory(boolean enable_tracking) {
            super(enable_tracking);
        }
        @Override
        public ForwardTxnResponseCallback makeObjectImpl() throws Exception {
            return new ForwardTxnResponseCallback();
        }
    };
    
    private RpcCallback<MessageAcknowledgement> orig_callback;
    private int source_id = -1;
    private int dest_id = -1;

    /**
     * Default Constructor
     */
    private ForwardTxnResponseCallback() {
        // Nothing to do...
    }
    
    public void init(int source_id, int dest_id, RpcCallback<MessageAcknowledgement> orig_callback) {
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
                                           HStoreSite.getSiteName(this.dest_id), parameter.length));
        ByteString bs = ByteString.copyFrom(parameter);
        Hstore.MessageAcknowledgement response = Hstore.MessageAcknowledgement.newBuilder()
                                                                              .setSenderSiteId(this.source_id)
                                                                              .setDestSiteId(this.dest_id)
                                                                              .setData(bs)
                                                                              .build();
        this.orig_callback.run(response);
        if (trace) LOG.trace("Sent our ClientResponse back. Returning to regularly scheduled program...");
        try {
            this.finish();
            HStoreSite.POOL_FORWARDTXN_RESPONSE.returnObject(this);
        } catch (Exception ex) {
            throw new RuntimeException("Funky failure", ex);
        }
    }
    
}
