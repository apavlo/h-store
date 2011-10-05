package edu.mit.hstore;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.Pair;
import org.voltdb.utils.DBBPool.BBContainer;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoRpcController;
import ca.evanjones.protorpc.ProtoServer;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.*;
import edu.brown.hstore.Hstore.TransactionWorkRequest.PartitionFragment;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.callbacks.TransactionPrepareCallback;
import edu.mit.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.mit.hstore.callbacks.TransactionWorkCallback;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.interfaces.Shutdownable;

/**
 * 
 * @author pavlo
 */
public class HStoreMessenger implements Shutdownable {
    public static final Logger LOG = Logger.getLogger(HStoreMessenger.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final DBBPool buffer_pool = new DBBPool(true, false);
    private static final ByteString ok = ByteString.copyFrom("OK".getBytes());
    
    private final HStoreSite hstore_site;
    private final Site catalog_site;
    private final int local_site_id;
    private final Set<Integer> local_partitions;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private final int num_sites;
    
    private final LinkedBlockingDeque<Pair<byte[], TransactionRedirectResponseCallback>> forwardQueue = new LinkedBlockingDeque<Pair<byte[], TransactionRedirectResponseCallback>>();  
    
    /** SiteId -> HStoreServer */
    private final Map<Integer, HStoreService> channels = new HashMap<Integer, HStoreService>();
    
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final Handler handler = new Handler();
    
    private final ForwardTxnDispatcher forwardDispatcher = new ForwardTxnDispatcher();
    private final Thread forward_thread;
    
    private boolean shutting_down = false;
    private Shutdownable.ShutdownState state = ShutdownState.INITIALIZED;
    

    /**
     * 
     */
    private class MessengerListener implements Runnable {
        @Override
        public void run() {
            if (hstore_site.getHStoreConf().site.cpu_affinity)
                hstore_site.getThreadManager().registerProcessingThread();
            Throwable error = null;
            try {
                HStoreMessenger.this.eventLoop.run();
            } catch (RuntimeException ex) {
                error = ex;
            } catch (AssertionError ex) {
                error = ex;
            } catch (Exception ex) {
                error = ex;
            }
            
            if (error != null) {
                if (hstore_site.isShuttingDown() == false) {
                    LOG.error("HStoreMessenger.Listener stopped!", error);
                }
                
                Throwable cause = null;
                if (error instanceof RuntimeException && error.getCause() != null) {
                    if (error.getCause().getMessage() != null && error.getCause().getMessage().isEmpty() == false) {
                        cause = error.getCause();
                    }
                }
                if (cause == null) cause = error;
                
                // These errors are ok if we're actually stopping...
                if (HStoreMessenger.this.state == ShutdownState.SHUTDOWN ||
                    HStoreMessenger.this.state == ShutdownState.PREPARE_SHUTDOWN ||
                    HStoreMessenger.this.hstore_site.isShuttingDown()) {
                    // IGNORE
                } else {
                    LOG.fatal("Unexpected error in messenger listener thread", cause);
                    HStoreMessenger.this.shutdownCluster(error);
                }
            }
            if (trace.get()) LOG.trace("Messenger Thread for Site #" + catalog_site.getId() + " has stopped!");
        }
    }
    
    /**
     * 
     */
    private class ForwardTxnDispatcher implements Runnable {
        final ProfileMeasurement idleTime = new ProfileMeasurement("IDLE");
        
        @Override
        public void run() {
            if (hstore_site.getHStoreConf().site.cpu_affinity)
                hstore_site.getThreadManager().registerProcessingThread();
            Pair<byte[], TransactionRedirectResponseCallback> p = null;
            while (HStoreMessenger.this.shutting_down == false) {
                try {
                    idleTime.start();
                    p = HStoreMessenger.this.forwardQueue.take();
                    idleTime.stop();
                } catch (InterruptedException ex) {
                    break;
                }
                try {
                    HStoreMessenger.this.hstore_site.procedureInvocation(p.getFirst(), p.getSecond());
                } catch (Throwable ex) {
                    LOG.warn("Failed to invoke forwarded transaction", ex);
                    continue;
                }
            } // WHILE
        }
    }
    
    /**
     * Constructor
     * @param site
     */
    public HStoreMessenger(HStoreSite site) {
        this.hstore_site = site;
        this.catalog_site = site.getSite();
        this.local_site_id = this.catalog_site.getId();
        this.num_sites = CatalogUtil.getNumberOfSites(this.catalog_site);
        
        Set<Integer> partitions = new HashSet<Integer>();
        for (Partition catalog_part : this.catalog_site.getPartitions()) {
            partitions.add(catalog_part.getId());
        } // FOR
        this.local_partitions = Collections.unmodifiableSet(partitions);
        if (debug.get()) LOG.debug("Local Partitions for Site #" + site.getSiteId() + ": " + this.local_partitions);

        // This listener thread will process incoming messages
        this.listener = new ProtoServer(this.eventLoop);
        
        // Special thread to handle forward requests
        if (this.hstore_site.getHStoreConf().site.messenger_redirect_thread) {
            this.forward_thread = new Thread(forwardDispatcher, HStoreSite.getThreadName(this.hstore_site, "frwd"));
            this.forward_thread.setDaemon(true);
        } else {
            this.forward_thread = null;
        }
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread(new MessengerListener(), HStoreSite.getThreadName(this.hstore_site, "msg"));
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
    }
    
    /**
     * Start the messenger. This is a blocking call that will initialize the connections
     * and start the listener thread!
     */
    public synchronized void start() {
        assert(this.state == ShutdownState.INITIALIZED) : "Invalid MessengerState " + this.state;
        
        this.state = ShutdownState.STARTED;
        
        if (debug.get()) LOG.debug("Initializing connections");
        this.initConnections();

        if (this.forward_thread != null) {
            if (debug.get()) LOG.debug("Starting ForwardTxn dispatcher thread");
            this.forward_thread.start();
        }
        
        if (debug.get()) LOG.debug("Starting listener thread");
        this.listener_thread.start();
    }

    /**
     * Returns true if the messenger has started
     * @return
     */
    public boolean isStarted() {
        return (this.state == ShutdownState.STARTED ||
                this.state == ShutdownState.PREPARE_SHUTDOWN);
    }
    
    /**
     * Internal call for testing to hide errors
     */
    @Override
    public void prepareShutdown() {
        if (this.state != ShutdownState.PREPARE_SHUTDOWN) {
            assert(this.state == ShutdownState.STARTED) : "Invalid MessengerState " + this.state;
            this.state = ShutdownState.PREPARE_SHUTDOWN;
        }
    }
    
    /**
     * Stop this messenger. This kills the ProtoRpc event loop
     */
    @Override
    public synchronized void shutdown() {
        assert(this.state == ShutdownState.STARTED || this.state == ShutdownState.PREPARE_SHUTDOWN) : "Invalid MessengerState " + this.state;
        
        this.state = ShutdownState.SHUTDOWN;
        
        try {
            if (trace.get()) LOG.trace("Stopping eventLoop for Site #" + this.getLocalSiteId());
            this.eventLoop.exitLoop();

            if (trace.get()) LOG.trace("Stopping listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.interrupt();
            
            if (trace.get()) LOG.trace("Joining on listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.join();
        } catch (InterruptedException ex) {
            // IGNORE
        } catch (Throwable ex) {
            LOG.error("Unexpected error when trying to stop messenger for Site #" + this.getLocalSiteId(), ex);
        } finally {
            if (trace.get()) LOG.trace("Closing listener socket for Site #" + this.getLocalSiteId());
            this.listener.close();
        }
        assert(this.isShuttingDown());
    }
    
    /**
     * Returns true if the messenger has stopped
     * @return
     */
    @Override
    public boolean isShuttingDown() {
        return (this.state == ShutdownState.SHUTDOWN);
    }
    
    protected int getLocalSiteId() {
        return (this.local_site_id);
    }
    protected int getLocalMessengerPort() {
        return (this.hstore_site.getSite().getMessenger_port());
    }
    protected final Thread getListenerThread() {
        return (this.listener_thread);
    }
    
    private VoltTable copyVoltTable(VoltTable vt) throws Exception {
        FastSerializer fs = new FastSerializer(buffer_pool);
        fs.writeObject(vt);
        BBContainer bc = fs.getBBContainer();
        assert(bc.b.hasArray());
        ByteString bs = ByteString.copyFrom(bc.b);
        
        FastDeserializer fds = new FastDeserializer(bs.asReadOnlyByteBuffer());
        VoltTable copy = fds.readObject(VoltTable.class);
        return (copy);
    }
    
    /**
     * Initialize all the network connections to remote 
     */
    private void initConnections() {
        Database catalog_db = CatalogUtil.getDatabase(this.catalog_site);
        
        // Find all the destinations we need to connect to
        if (debug.get()) LOG.debug("Configuring outbound network connections for Site #" + this.catalog_site.getId());
        Map<Host, Set<Site>> host_partitions = CatalogUtil.getSitesPerHost(catalog_db);
        Integer local_port = this.catalog_site.getMessenger_port();
        
        ArrayList<Integer> site_ids = new ArrayList<Integer>();
        ArrayList<InetSocketAddress> destinations = new ArrayList<InetSocketAddress>();
        for (Entry<Host, Set<Site>> e : host_partitions.entrySet()) {
            String host = e.getKey().getIpaddr();
            for (Site catalog_site : e.getValue()) {
                int site_id = catalog_site.getId();
                int port = catalog_site.getMessenger_port();
                if (site_id != this.catalog_site.getId()) {
                    if (debug.get()) LOG.debug("Creating RpcChannel to " + host + ":" + port + " for site #" + site_id);
                    destinations.add(new InetSocketAddress(host, port));
                    site_ids.add(site_id);
                } // FOR
            } // FOR 
        } // FOR
        
        // Initialize inbound channel
        assert(local_port != null);
        if (debug.get()) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.handler);
        this.listener.bind(local_port);

        // Make the outbound connections
        if (destinations.isEmpty()) {
            if (debug.get()) LOG.debug("There are no remote sites so we are skipping creating connections");
        } else {
            if (debug.get()) LOG.debug("Connecting to " + destinations.size() + " remote site messengers");
            ProtoRpcChannel[] channels = null;
            try {
                channels = ProtoRpcChannel.connectParallel(this.eventLoop, destinations.toArray(new InetSocketAddress[]{}), 15000);
            } catch (RuntimeException ex) {
                LOG.warn("Failed to connect to remote sites. Going to try again...");
                // Try again???
                try {
                    channels = ProtoRpcChannel.connectParallel(this.eventLoop, destinations.toArray(new InetSocketAddress[]{}));
                } catch (Exception ex2) {
                    LOG.fatal("Site #" + this.getLocalSiteId() + " failed to connect to remote sites");
                    this.listener.close();
                    throw ex;    
                }
            }
            assert channels.length == site_ids.size();
            for (int i = 0; i < channels.length; i++) {
                this.channels.put(site_ids.get(i), HStoreService.newStub(channels[i]));
            } // FOR

            
            if (debug.get()) LOG.debug("Site #" + this.getLocalSiteId() + " is fully connected to all sites");
        }
    }
    
    // ----------------------------------------------------------------------------
    // MESSAGE ROUTERS
    // ----------------------------------------------------------------------------

    private abstract class MessageRouter<T extends GeneratedMessage, U extends GeneratedMessage> {
        
        public void sendMessages(LocalTransaction ts, T msg, RpcCallback<U> callback, Collection<Integer> partitions) {
            // If this flag is true, then we'll invoke the local method
            // We want to do this *after* we send out all the messages to the remote sites
            // so that we don't have to wait as long for the responses to come back over the network
            Collection<Integer> local_partitions = null;
            
            boolean site_sent[] = new boolean[HStoreMessenger.this.num_sites];
            int ctr = 0;
            for (Integer p : partitions) {
                int dest_site_id = hstore_site.getSiteIdForPartitionId(p).intValue();

                // Skip this HStoreSite if we're already sent it a message 
                if (site_sent[dest_site_id]) continue;
                
                if (debug.get())
                    LOG.debug(String.format("Sending %s message to %s for %s",
                                            msg.getClass().getSimpleName(), HStoreSite.formatSiteName(dest_site_id), ts));
                
                // Local Partition
                if (HStoreMessenger.this.local_site_id == dest_site_id) {
                    if (local_partitions == null) local_partitions = new HashSet<Integer>(); // XXX: ObjectPool?
                    local_partitions.add(p);
                // Remote Partition
                } else {
                    HStoreService channel = HStoreMessenger.this.channels.get(dest_site_id);
                    assert(channel != null) : "Invalid partition id '" + p + "' at " + HStoreMessenger.this.hstore_site.getSiteName();
                    ProtoRpcController controller = this.getProtoRpcController(ts, dest_site_id);
                    assert(controller != null) : "Invalid " + msg.getClass().getSimpleName() + " ProtoRpcController for site #" + dest_site_id;
                    this.sendRemote(channel, controller, msg, callback);
                }
                site_sent[dest_site_id] = true;
                ctr++;
            } // FOR
            // Optimization: We'll invoke sendLocal() after we have sent out
            // all of the mesages to remote sites
            if (local_partitions.size() > 0) this.sendLocal(ts.getTransactionId(), msg, local_partitions);
            
            if (debug.get())
                LOG.debug(String.format("Sent %d %s to %d partitions for txn #%d ",
                                        ts.getTransactionId(), msg.getClass().getSimpleName(), partitions.size()));
        }
        
        /**
         * 
         * @param channel
         * @param msg
         * @param callback
         */
        protected abstract void sendRemote(HStoreService channel, ProtoRpcController controller, T msg, RpcCallback<U> callback);
        protected abstract void sendLocal(long txn_id, T msg, Collection<Integer> partitions);
        protected abstract ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id);
    }
    
    // TransactionInit
    private final MessageRouter<TransactionInitRequest, TransactionInitResponse> router_transactionInit = new MessageRouter<TransactionInitRequest, TransactionInitResponse>() {
        protected void sendLocal(long txn_id, TransactionInitRequest msg, Collection<Integer> partitions) {
            
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionInitRequest msg, RpcCallback<TransactionInitResponse> callback) {
            channel.transactionInit(controller, msg, callback);
        }
        protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
            return ts.getTransactionInitController(site_id);
        }
    };
    // TransactionPrepare
    private final MessageRouter<TransactionPrepareRequest, TransactionPrepareResponse> router_transactionPrepare = new MessageRouter<TransactionPrepareRequest, TransactionPrepareResponse>() {
        protected void sendLocal(long txn_id, TransactionPrepareRequest msg, Collection<Integer> partitions) {
            // We don't care whether we actually updated anybody locally, so we don't need to
            // pass in a set to get the partitions that were updated here.
            hstore_site.transactionPrepare(txn_id, partitions, null);
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionPrepareRequest msg, RpcCallback<TransactionPrepareResponse> callback) {
            channel.transactionPrepare(controller, msg, callback);
        }
        protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
            return ts.getTransactionPrepareController(site_id);
        }
    };
    // TransactionFinish
    private final MessageRouter<TransactionFinishRequest, TransactionFinishResponse> router_transactionFinish = new MessageRouter<TransactionFinishRequest, TransactionFinishResponse>() {
        protected void sendLocal(long txn_id, TransactionFinishRequest msg, Collection<Integer> partitions) {
            hstore_site.transactionFinish(txn_id, msg.getStatus(), partitions);
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionFinishRequest msg, RpcCallback<TransactionFinishResponse> callback) {
            channel.transactionFinish(controller, msg, callback);
        }
        protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
            return ts.getTransactionFinishController(site_id);
        }
    };
    // Shutdown
    private final MessageRouter<ShutdownRequest, ShutdownResponse> router_shutdown = new MessageRouter<ShutdownRequest, ShutdownResponse>() {
        protected void sendLocal(long txn_id, ShutdownRequest msg, Collection<Integer> partitions) {
            
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, ShutdownRequest msg, RpcCallback<ShutdownResponse> callback) {
            channel.shutdown(controller, msg, callback);
        }
        protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
            return new ProtoRpcController();
        }
    };

    
    // ----------------------------------------------------------------------------
    // HSTORE RPC SERVICE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * We want to make this a private inner class so that we do not expose
     * the RPC methods to other parts of the code.
     */
    private class Handler extends HStoreService {
    
        @Override
        public void transactionInit(RpcController controller, TransactionInitRequest request,
                RpcCallback<TransactionInitResponse> done) {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void transactionWork(RpcController controller, TransactionWorkRequest request,
                RpcCallback<TransactionWorkResponse> done) {
            assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
            long txn_id = request.getTransactionId();
            
            // This is work from a transaction executing at another node
            // Any other message can just be sent along to the ExecutionSite without sending
            // back anything right away. The ExecutionSite will use our wrapped callback handle
            // to send back whatever response it needs to, but we won't actually send it
            // until we get back results from all of the partitions
            TransactionWorkCallback callback = null;
            try {
                callback = (TransactionWorkCallback)HStoreObjectPools.POOL_TXNWORK.borrowObject();
                callback.init(txn_id, request.getFragmentsCount(), done);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            
            FragmentTaskMessage ftask = null;
            for (PartitionFragment partition_task : request.getFragmentsList()) {
                // Decode the inner VoltMessage
                try {
                    ftask = (FragmentTaskMessage)VoltMessage.createMessageFromBuffer(partition_task.getWork().asReadOnlyByteBuffer(), false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                assert(txn_id == ftask.getTxnId());
            
                hstore_site.transactionWork(txn_id, ftask, callback);
            } // FOR
            
            // We don't need to send back a response right here.
            // TransactionWorkCallback will wait until it has results from all of the partitions 
            // the tasks were sent to and then send back everything in a single response message
        }
        
        @Override
        public void transactionPrepare(RpcController controller, TransactionPrepareRequest request,
                RpcCallback<TransactionPrepareResponse> done) {
            assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
            long txn_id = request.getTransactionId();
            
            Collection<Integer> updated = new HashSet<Integer>();
            hstore_site.transactionPrepare(txn_id, request.getPartitionsList(), updated);
            assert(updated.isEmpty() == false);
            
            Hstore.TransactionPrepareResponse response = Hstore.TransactionPrepareResponse.newBuilder()
                                                                   .setTransactionId(txn_id)
                                                                   .addAllPartitions(updated)
                                                                   .setStatus(Hstore.Status.OK)
                                                                   .build();
            done.run(response);
        }
        
        @Override
        public void transactionFinish(RpcController controller, TransactionFinishRequest request,
                RpcCallback<TransactionFinishResponse> done) {
            assert(request.hasTransactionId()) : "Got Hstore." + request.getClass().getSimpleName() + " without a txn id!";
            long txn_id = request.getTransactionId();

            hstore_site.transactionFinish(txn_id, request.getStatus(), request.getPartitionsList());
            
            // Send back a FinishResponse to let them know we're cool with everything...
            Hstore.TransactionFinishResponse response = Hstore.TransactionFinishResponse.newBuilder()
                                                              .setTransactionId(txn_id)
                                                              .build();
            done.run(response);
            
            // Always tell the HStoreSite to clean-up any state for this txn
            hstore_site.completeTransaction(txn_id, request.getStatus());
        }
        
        @Override
        public void transactionRedirect(RpcController controller, TransactionRedirectRequest request,
                RpcCallback<TransactionRedirectResponse> done) {
            // We need to create a wrapper callback so that we can get the output that
            // HStoreSite wants to send to the client and forward 
            // it back to whomever told us about this txn
            if (trace.get()) 
                LOG.trace(String.format("Recieved redirected transaction request from HStoreSite %s", HStoreSite.formatSiteName(request.getSenderId())));
            byte serializedRequest[] = request.getWork().toByteArray(); // XXX Copy!
            TransactionRedirectResponseCallback callback = null;
            try {
                callback = (TransactionRedirectResponseCallback)HStoreObjectPools.POOL_FORWARDTXN_RESPONSE.borrowObject();
                callback.init(local_site_id, request.getSenderId(), done);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get ForwardTxnResponseCallback", ex);
            }
            
            if (HStoreMessenger.this.forward_thread != null) {
                HStoreMessenger.this.forwardQueue.add(Pair.of(serializedRequest, callback));
            } else {
                hstore_site.procedureInvocation(serializedRequest, callback);
            }
        }
        
        @Override
        public void shutdown(RpcController controller, ShutdownRequest request,
                RpcCallback<ShutdownResponse> done) {
            LOG.info(String.format("Got shutdown request from HStoreSite %s", HStoreSite.formatSiteName(request.getSenderId())));
            
            HStoreMessenger.this.shutting_down = true;
            
            // Tell the HStoreSite to shutdown
            hstore_site.shutdown();
            
            // Then send back the acknowledgment
            Hstore.ShutdownResponse response = Hstore.ShutdownResponse.newBuilder()
                                                   .setSenderId(local_site_id)
                                                   .build();
            // Send this now!
            done.run(response);
            LOG.info(String.format("Shutting down %s [status=%d]", hstore_site.getSiteName(), request.getExitStatus()));
            if (debug.get())
                LOG.info(String.format("ForwardDispatcher Queue Idle Time: %.2fms",
                                       forwardDispatcher.idleTime.getTotalThinkTimeMS()));
            ThreadUtil.sleep(1000); // HACK
            LogManager.shutdown();
            System.exit(request.getExitStatus());
            
        }
    } // END CLASS
    
    
    // ----------------------------------------------------------------------------
    // TRANSACTION METHODS
    // ----------------------------------------------------------------------------

    /**
     * 
     * @param ts
     * @param callback
     */
    public void transactionInit(LocalTransaction ts, RpcCallback<Hstore.TransactionInitResponse> callback) {
        Hstore.TransactionInitRequest request = Hstore.TransactionInitRequest.newBuilder()
                                                         .setTransactionId(ts.getTransactionId())
                                                         .addAllPartitions(ts.getPredictTouchedPartitions())
                                                         .build();
        this.router_transactionInit.sendMessages(ts, request, callback, request.getPartitionsList());
        
    }
    
    /**
     * 
     * @param builders
     * @param callback
     */
    public void transactionWork(LocalTransaction ts, Map<Integer, Hstore.TransactionWorkRequest.Builder> builders, RpcCallback<Hstore.TransactionWorkResponse> callback) {
        for (Entry<Integer, Hstore.TransactionWorkRequest.Builder> e : builders.entrySet()) {
            int site_id = e.getKey().intValue();
            assert(e.getValue().getFragmentsCount() > 0);
            
            // We should never get work for our local partitions
            assert(site_id != this.local_site_id);
            this.channels.get(site_id).transactionWork(ts.getTransactionWorkController(site_id), e.getValue().build(), callback);
        } // FOR
    }
    
    /**
     * Notify the given partitions that this transaction is finished with them
     * This can also be used for the "early prepare" optimization.
     * @param ts
     * @param callback
     * @param partitions
     */
    public void transactionPrepare(LocalTransaction ts, RpcCallback<Hstore.TransactionPrepareResponse> callback, Collection<Integer> partitions) {
        Hstore.TransactionPrepareRequest request = Hstore.TransactionPrepareRequest.newBuilder()
                                                        .setTransactionId(ts.getTransactionId())
                                                        .addAllPartitions(ts.getDonePartitions())
                                                        .build();
        this.router_transactionPrepare.sendMessages(ts, request, callback, partitions);
    }

    /**
     * Notify all remote HStoreSites that the distributed transaction is done with data
     * at the given partitions and that they need to commit/abort the results. 
     * @param ts
     * @param status
     * @param callback
     */
    public void transactionFinish(LocalTransaction ts, Hstore.Status status, RpcCallback<Hstore.TransactionFinishResponse> callback) {
        Hstore.TransactionFinishRequest request = Hstore.TransactionFinishRequest.newBuilder()
                                                        .setTransactionId(ts.getTransactionId())
                                                        .setStatus(status)
                                                        .addAllPartitions(ts.getDonePartitions())
                                                        .build();
        this.router_transactionFinish.sendMessages(ts,
                                                   request,
                                                   callback,
                                                   ts.getTouchedPartitions().values());
    }
    
    /**
     * Forward a StoredProcedureInvocation request to a remote site for execution
     * @param serializedRequest
     * @param callback
     * @param partition
     */
    public void transactionRedirect(byte[] serializedRequest, RpcCallback<Hstore.TransactionRedirectResponse> callback, int partition) {
        int dest_site_id = hstore_site.getSiteIdForPartitionId(partition);
        if (debug.get()) LOG.debug("Redirecting transaction request to partition #" + partition + " on " + HStoreSite.formatSiteName(dest_site_id));
        ByteString bs = ByteString.copyFrom(serializedRequest);
        Hstore.TransactionRedirectRequest mr = Hstore.TransactionRedirectRequest.newBuilder()
                                        .setSenderId(this.local_site_id)
                                        .setWork(bs)
                                        .build();
        this.channels.get(dest_site_id).transactionRedirect(new ProtoRpcController(), mr, callback);
    }
    
    // ----------------------------------------------------------------------------
    // SHUTDOWN METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Take down the cluster. If the blocking flag is true, then this call will never return
     * @param ex
     * @param blocking
     */
    public void shutdownCluster(final Throwable ex, final boolean blocking) {
        if (blocking) {
            this.shutdownCluster(null);
        } else {
            // Make this a thread so that we don't block and can continue cleaning up other things
            Thread shutdownThread = new Thread() {
                @Override
                public void run() {
                    HStoreMessenger.this.shutdownCluster(ex); // Never returns!
                }
            };
            shutdownThread.setDaemon(true);
            shutdownThread.start();
        }
        return;
    }
    
    /**
     * Tell all of the other sites to shutdown and then knock ourselves out...
     * This is will never return
     */
    public void shutdownCluster() {
        this.shutdownCluster(null);
    }
    
    /**
     * Shutdown the cluster. If the given Exception is not null, then all the nodes will
     * exit with a non-zero status. This is will never return
     * Will not return.
     * @param ex
     */
    public synchronized void shutdownCluster(Throwable ex) {
        final int num_sites = this.channels.size();
        if (this.shutting_down) return;
        this.shutting_down = true;
        this.hstore_site.prepareShutdown();
        LOG.info("Shutting down cluster" + (ex != null ? ": " + ex.getMessage() : ""));

        final int exit_status = (ex == null ? 0 : 1);
        final CountDownLatch latch = new CountDownLatch(num_sites);
        
        if (num_sites > 0) {
            RpcCallback<Hstore.ShutdownResponse> callback = new RpcCallback<Hstore.ShutdownResponse>() {
                private final Set<Integer> siteids = new HashSet<Integer>(); 
                
                @Override
                public void run(Hstore.ShutdownResponse parameter) {
                    int siteid = parameter.getSenderId();
                    assert(this.siteids.contains(siteid) == false) : "Duplicate response from " + hstore_site.getSiteName();
                    this.siteids.add(siteid);
                    if (trace.get()) LOG.trace("Received " + this.siteids.size() + "/" + num_sites + " shutdown acknowledgements");
                    latch.countDown();
                }
            };
            
            if (debug.get()) LOG.debug("Sending shutdown request to " + num_sites + " remote sites");
            for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
                Hstore.ShutdownRequest sm = Hstore.ShutdownRequest.newBuilder()
                                                .setSenderId(catalog_site.getId())
                                                .setExitStatus(exit_status)
                                                .build();
                e.getValue().shutdown(new ProtoRpcController(), sm, callback);
                if (trace.get()) LOG.trace("Sent SHUTDOWN to " + HStoreSite.formatSiteName(e.getKey()));
            } // FOR
        }
        
        // Tell ourselves to shutdown while we wait
        if (debug.get()) LOG.debug("Telling local site to shutdown");
        this.hstore_site.shutdown();
        
        // Block until the latch releases us
        if (num_sites > 0) {
            LOG.info(String.format("Waiting for %d sites to finish shutting down", latch.getCount()));
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (Exception ex2) {
                // IGNORE!
            }
        }
        LOG.info(String.format("Shutting down [site=%d, status=%d]", catalog_site.getId(), exit_status));
        LogManager.shutdown();
        System.exit(exit_status);
    }


    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns an HStoreService handle that is connected to the given site
     * @param catalog_site
     * @return
     */
    protected static HStoreService getHStoreService(Site catalog_site) {
        NIOEventLoop eventLoop = new NIOEventLoop();
        InetSocketAddress addresses[] = new InetSocketAddress[] {
            new InetSocketAddress(catalog_site.getHost().getIpaddr(), catalog_site.getMessenger_port()) 
        };
        ProtoRpcChannel[] channels = null;
        try {
            channels = ProtoRpcChannel.connectParallel(eventLoop, addresses);
        } catch (Exception ex) {
            
        }
        HStoreService channel = HStoreService.newStub(channels[0]);
        return (channel);
    }
}