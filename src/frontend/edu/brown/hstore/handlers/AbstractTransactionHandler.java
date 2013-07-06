package edu.brown.hstore.handlers;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

/**
 * AbstractTransactionHandler is a wrapper around the invocation methods for some action
 * There is a local and remote method to send a message to the necessary HStoreSites
 * for a transaction. The sendMessages() method is the main entry point that the 
 * HStoreCoordinator's convenience methods will use. 
 * @param <T> The message that we will send out on the network
 * @param <U> The expected message that we will need to get back as a response 
 */
public abstract class AbstractTransactionHandler<T extends GeneratedMessage, U extends GeneratedMessage> {
    private static final Logger LOG = Logger.getLogger(AbstractTransactionHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    protected final HStoreConf hstore_conf;
    protected final HStoreCoordinator coordinator;
    protected final HStoreService handler;
    protected final CatalogContext catalogContext;
    
    /** The total number of sites in the cluster */
    protected final int num_sites;
    
    /** The site id where this handler is running at */
    protected final int local_site_id;
    
    public AbstractTransactionHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.catalogContext = hstore_site.getCatalogContext();
        this.coordinator = hstore_coord;
        this.handler = this.coordinator.getHandler();
        this.num_sites = this.catalogContext.numberOfSites;
        this.local_site_id = hstore_site.getSiteId();
    }
    
    /**
     * Send a copy of a single message request to the partitions given as input
     * If a partition is managed by the local HStoreSite, then we will invoke
     * the sendLocal() method. If it is on a remote HStoreSite, then we will
     * invoke sendRemote().
     * @param ts
     * @param request
     * @param callback
     * @param partitions
     */
    public void sendMessages(LocalTransaction ts, T request, RpcCallback<U> callback, PartitionSet partitions) {
        // If this flag is true, then we'll invoke the local method
        // We want to do this *after* we send out all the messages to the remote sites
        // so that we don't have to wait as long for the responses to come back over the network
        boolean send_local = false;
        boolean site_sent[] = new boolean[this.num_sites];
        
        if (debug.val)
            LOG.debug(String.format("Sending %s to %d partitions for %s",
                                    request.getClass().getSimpleName(),  partitions.size(), ts));
        
        for (int partition : partitions.values()) {
            int dest_site_id = this.catalogContext.getSiteIdForPartitionId(partition);

            // Skip this HStoreSite if we're already sent it a message 
            if (site_sent[dest_site_id]) continue;
            
            if (trace.val)
                LOG.trace(String.format("Sending %s message to %s for %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(dest_site_id), ts));
            
            // Local Partition
            if (this.local_site_id == dest_site_id) {
                send_local = true;
            }
            // Remote Partition
            else {
                HStoreService channel = this.coordinator.getChannel(dest_site_id);
                assert(channel != null) : "Invalid partition id '" + partition + "'";
                ProtoRpcController controller = this.getProtoRpcController(ts, dest_site_id);
                assert(controller != null) : "Invalid " + request.getClass().getSimpleName() + " ProtoRpcController for site #" + dest_site_id;
                this.sendRemote(channel, controller, request, callback);
            }
            site_sent[dest_site_id] = true;
        } // FOR
        // Optimization: We'll invoke sendLocal() after we have sent out
        // all of the messages to remote sites
        if (send_local) this.sendLocal(ts.getTransactionId(), request, partitions, callback);
    }
    
    /**
     * The processing method that is invoked if the outgoing message needs
     * to be sent to a partition that is on the same machine as where this
     * handler is executing.
     * @param txn_id
     * @param request
     * @param partitions
     * @param callback
     */
    public abstract void sendLocal(Long txn_id, T request, PartitionSet partitions, RpcCallback<U> callback);
    
    /**
     * The processing method that is invoked if the outgoing message needs
     * to be sent to a partition that is *not* managed by the same HStoreSite
     * as where this handler is executing. This is non-blocking and does not 
     * wait for the callback to be executed in response to the remote side.
     * @param channel
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void sendRemote(HStoreService channel, ProtoRpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * This is the method that is invoked on the remote HStoreSite for each incoming
     * message request. This will then determine whether the message should be queued up
     * for execution in a different thread, or whether it should invoke remoteHandler()
     * right away.
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void remoteQueue(RpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * The remoteHandler is the code that executes on the remote node to process
     * the request for a transaction that is executing on a different node.
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void remoteHandler(RpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * Return a cached ProtoRpcController handler from the LocalTransaction object
     * @param ts
     * @param site_id
     * @return
     */
    protected abstract ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id);
}