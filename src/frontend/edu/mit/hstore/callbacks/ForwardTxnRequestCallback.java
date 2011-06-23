package edu.mit.hstore.callbacks;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore.MessageAcknowledgement;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.Poolable;
import edu.mit.hstore.HStoreSite;

/**
 * 
 * @author pavlo
 */
public class ForwardTxnRequestCallback implements RpcCallback<MessageAcknowledgement>, Poolable {
    private static final Logger LOG = Logger.getLogger(ForwardTxnRequestCallback.class);
    
    /**
     * Object Pool Factory
     */
    public static class Factory extends CountingPoolableObjectFactory<ForwardTxnRequestCallback> {
        
        public Factory(boolean enable_tracking) {
            super(enable_tracking);
        }
        @Override
        public ForwardTxnRequestCallback makeObjectImpl() throws Exception {
            return new ForwardTxnRequestCallback();
        }
    };
    
    protected RpcCallback<byte[]> orig_callback;

    /**
     * Default Constructor
     */
    private ForwardTxnRequestCallback() {
        // Nothing to do...
    }
    
    public void init(RpcCallback<byte[]> orig_callback) {
        this.orig_callback = orig_callback;
    }

    @Override
    public boolean isInitialized() {
        return (this.orig_callback != null);
    }
    
    @Override
    public void finish() {
        this.orig_callback = null;
    }
    
    @Override
    public void run(MessageAcknowledgement parameter) {
        if (LOG.isTraceEnabled()) LOG.trace(String.format("Got back FORWARD_TXN response from %s. Sending response to client [bytes=%d]",
                                                          HStoreSite.formatSiteName(parameter.getSenderSiteId()), parameter.getData().size()));
        byte data[] = parameter.getData().toByteArray();
        try {
            this.orig_callback.run(data);
        } catch (AssertionError ex) {
            FastDeserializer fds = new FastDeserializer(data);
            ClientResponseImpl cresponse = null;
            long txn_id = -1;
            try {
                cresponse = fds.readObject(ClientResponseImpl.class);
                txn_id = cresponse.getTransactionId();
            } catch (IOException e) {
                LOG.fatal("We're really falling apart here!", e);
            }
            LOG.fatal("Failed to forward ClientResponse data back for txn #" + txn_id, ex);
            // throw ex;
        } finally {
            try {
                this.finish();
                HStoreSite.POOL_FORWARDTXN_REQUEST.returnObject(this);
            } catch (Exception ex) {
                throw new RuntimeException("Funky failure", ex);
            }
        }
        
    }
    
}
