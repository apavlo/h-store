package edu.brown.hstore.callbacks;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreObjectPools;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.utils.Poolable;

/**
 * This callback is used by the original HStoreSite that is sending out a transaction redirect
 * to another HStoreSite. We must be given the original callback that points back to the client. 
 * @author pavlo
 */
public class TransactionRedirectCallback implements RpcCallback<TransactionRedirectResponse>, Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectCallback.class);
    
    protected RpcCallback<byte[]> orig_callback;

    /**
     * Default Constructor
     */
    public TransactionRedirectCallback() {
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
    public void run(TransactionRedirectResponse parameter) {
        if (LOG.isTraceEnabled()) LOG.trace(String.format("Got back FORWARD_TXN response from %s. Sending response to client [bytes=%d]",
                                                          HStoreThreadManager.formatSiteName(parameter.getSenderSite()), parameter.getOutput().size()));
        byte data[] = parameter.getOutput().toByteArray();
        try {
            this.orig_callback.run(data);
        } catch (Throwable ex) {
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
                HStoreObjectPools.CALLBACKS_TXN_REDIRECT_REQUEST.returnObject(this);
            } catch (Exception ex) {
                throw new RuntimeException("Funky failure", ex);
            }
        }
        
    }
    
}
