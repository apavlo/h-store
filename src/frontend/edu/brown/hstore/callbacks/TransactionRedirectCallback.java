package edu.brown.hstore.callbacks;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.pools.Poolable;

/**
 * This callback is used by the original HStoreSite that is sending out a transaction redirect
 * to another HStoreSite. We must be given the original callback that points back to the client. 
 * @author pavlo
 */
public class TransactionRedirectCallback implements RpcCallback<TransactionRedirectResponse>, Poolable {
    private static final Logger LOG = Logger.getLogger(TransactionRedirectCallback.class);
    
    private HStoreSite hstore_site;
    protected RpcCallback<ClientResponseImpl> orig_callback;

    /**
     * Default Constructor
     */
    public TransactionRedirectCallback(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    public void init(RpcCallback<ClientResponseImpl> orig_callback) {
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
        
        // HACK: we h
        ByteBuffer data = parameter.getOutput().asReadOnlyByteBuffer();
        FastDeserializer fds = new FastDeserializer(data);
        ClientResponseImpl cresponse = null;
        try {
            cresponse = fds.readObject(ClientResponseImpl.class);
            LOG.info("Returning redirected ClientResponse to client:\n" + cresponse);
            this.orig_callback.run(cresponse);
        } catch (Throwable ex) {
            LOG.fatal("Failed to forward ClientResponse data back!", ex);
            throw new RuntimeException(ex);
        } finally {
            try {
                this.finish();
                hstore_site.getObjectPools().CALLBACKS_TXN_REDIRECT_REQUEST.returnObject(this);
            } catch (Exception ex) {
                throw new RuntimeException("Funky failure", ex);
            }
        }
        
    }
    
}
