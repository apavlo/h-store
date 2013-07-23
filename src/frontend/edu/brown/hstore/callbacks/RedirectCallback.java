package edu.brown.hstore.callbacks;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.exceptions.ClientConnectionLostException;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;

/**
 * This callback is used by the original HStoreSite that is sending out a transaction redirect
 * to another HStoreSite. We must be given the original callback that points back to the client. 
 * @author pavlo
 */
public class RedirectCallback implements RpcCallback<TransactionRedirectResponse>, Poolable {
    private static final Logger LOG = Logger.getLogger(RedirectCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    // private final HStoreSite hstore_site;
    private final FastDeserializer fds = new FastDeserializer();
    private RpcCallback<ClientResponseImpl> orig_callback;

    /**
     * Default Constructor
     */
    public RedirectCallback(HStoreSite hstore_site) {
        // this.hstore_site = hstore_site;
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
        if (debug.val)
            LOG.debug(String.format("Got back %s from %s. Sending response to client [bytes=%d]",
                      parameter.getClass().getSimpleName(),
                      HStoreThreadManager.formatSiteName(parameter.getSenderSite()),
                      parameter.getOutput().size()));
        
        try {
            // Get the embedded ClientResponse
            // TODO: We should really just send the raw bytes through the callback instead
            // of having to deserialize it first.
            ClientResponseImpl cresponse = null;
            this.fds.setBuffer(parameter.getOutput().asReadOnlyByteBuffer());
            try {
                cresponse = this.fds.readObject(ClientResponseImpl.class);
            } catch (IOException ex) {
                String msg = String.format("Failed to deserialize %s from %s",
                                           parameter.getClass().getSimpleName(),
                                           HStoreThreadManager.formatSiteName(parameter.getSenderSite()));
                throw new ServerFaultException(msg, ex);
            }
            
            assert(cresponse != null);
            if (debug.val) 
                LOG.debug("Returning redirected ClientResponse to client:\n" + cresponse);
            try {
                this.orig_callback.run(cresponse);
            } catch (ClientConnectionLostException ex) {
                if (debug.val) LOG.warn("Lost connection to client for txn #" + cresponse.getTransactionId());
            } catch (Throwable ex) {
                LOG.fatal("Failed to forward ClientResponse data back!", ex);
                throw new RuntimeException(ex);
            }
            
        // Always return ourselves to the HStoreObjectPool
        } finally {
            // this.hstore_site.getObjectPools().CALLBACKS_TXN_REDIRECT_REQUEST.returnObject(this);
        }
        
    }
    
}
