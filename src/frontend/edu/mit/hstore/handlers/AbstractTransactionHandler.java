package edu.mit.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * A MessageRouter is a wrapper around the invocation methods for some action
 * There is a local and remote method to send a message to the necessary HStoreSites
 * for a transaction. The sendMessages() method is the main entry point that the 
 * HStoreCoordinator's convenience methods will use. 
 * @param <T>
 * @param <U>
 */
public abstract class AbstractTransactionHandler<T extends GeneratedMessage, U extends GeneratedMessage> {
    private static final Logger LOG = Logger.getLogger(AbstractTransactionHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    protected final HStoreCoordinator hstore_coord;
    protected final HStoreService handler;
    protected final int num_sites;
    protected final int local_site_id;
    
    public AbstractTransactionHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        this.hstore_site = hstore_site;
        this.hstore_coord = hstore_coord;
        this.handler = this.hstore_coord.getHandler();
        this.num_sites = CatalogUtil.getNumberOfSites(hstore_site.getSite());
        this.local_site_id = hstore_site.getSiteId();
    }
    
    public void sendMessages(LocalTransaction ts, T request, RpcCallback<U> callback, Collection<Integer> partitions) {
        // If this flag is true, then we'll invoke the local method
        // We want to do this *after* we send out all the messages to the remote sites
        // so that we don't have to wait as long for the responses to come back over the network
        boolean send_local = false;
        boolean site_sent[] = new boolean[this.num_sites];
        int ctr = 0;
        for (Integer p : partitions) {
            int dest_site_id = hstore_site.getSiteIdForPartitionId(p).intValue();

            // Skip this HStoreSite if we're already sent it a message 
            if (site_sent[dest_site_id]) continue;
            
            if (trace.get())
                LOG.trace(String.format("Sending %s message to %s for %s",
                                        request.getClass().getSimpleName(), HStoreSite.formatSiteName(dest_site_id), ts));
            
            // Local Partition
            if (this.local_site_id == dest_site_id) {
                send_local = true;
            // Remote Partition
            } else {
                HStoreService channel = hstore_coord.getChannel(dest_site_id);
                assert(channel != null) : "Invalid partition id '" + p + "'";
                ProtoRpcController controller = this.getProtoRpcController(ts, dest_site_id);
                assert(controller != null) : "Invalid " + request.getClass().getSimpleName() + " ProtoRpcController for site #" + dest_site_id;
                this.sendRemote(channel, controller, request, callback);
            }
            site_sent[dest_site_id] = true;
            ctr++;
        } // FOR
        // Optimization: We'll invoke sendLocal() after we have sent out
        // all of the mesages to remote sites
        if (send_local) this.sendLocal(ts.getTransactionId(), request, partitions, callback);
        
        if (debug.get())
            LOG.debug(String.format("Sent %d %s to %d partitions for %s",
                                    ctr, request.getClass().getSimpleName(),  partitions.size(), ts));
    }
    
    public abstract void sendLocal(long txn_id, T request, Collection<Integer> partitions, RpcCallback<U> callback);
    public abstract void sendRemote(HStoreService channel, ProtoRpcController controller, T request, RpcCallback<U> callback);
    public abstract void remoteQueue(RpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * The remoteHandler is the code that executes on the remote node to process
     * the request for a transaction that is executing on a different node.
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void remoteHandler(RpcController controller, T request, RpcCallback<U> callback);
    
    protected abstract ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id);
}