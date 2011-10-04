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
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.callbacks.TransactionPrepareCallback;
import edu.mit.hstore.callbacks.TransactionRedirectResponseCallback;
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
    
    protected final HStoreService getSiteChannel(int site_id) {
        return (this.channels.get(site_id));
    }
    protected final Set<Integer> getLocalPartitionIds() {
        return (this.local_partitions);
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
        
        public void sendMessages(long txn_id, T msg, RpcCallback<U> callback, Collection<Integer> partitions) {
            // If this flag is true, then we'll invoke the local method
            // We want to do this *after* we send out all the messages to the remote sites
            // so that we don't have to wait as long for the responses to come back over the network
            boolean update_local = false;
            
            boolean site_sent[] = new boolean[HStoreMessenger.this.num_sites];
            int ctr = 0;
            for (Integer p : partitions) {
                int dest_site_id = hstore_site.getSiteIdForPartitionId(p).intValue();

                // Skip this HStoreSite if we're already sent it a message 
                if (site_sent[dest_site_id]) continue;
                
                if (debug.get())
                    LOG.debug(String.format("Sending %s message to %s for txn #%d",
                                            msg.getClass().getSimpleName(), HStoreSite.formatSiteName(dest_site_id), txn_id));
                
                // OPTIMIZATION: 
                if (HStoreMessenger.this.local_site_id == dest_site_id) {
                    update_local = true;
                } else {
                    HStoreService channel = HStoreMessenger.this.channels.get(dest_site_id);
                    assert(channel != null) : "Invalid partition id '" + p + "' at " + HStoreMessenger.this.hstore_site.getSiteName();
                    
                    // TODO: The callback needs to keep track of what partitions have acknowledged that they
                    // can prepare to commit the transaction, so that we don't send duplicate messages
                    // when it comes time to actually commit the transaction
                    this.sendRemote(channel, new ProtoRpcController(), msg, callback);
                }
                    
                site_sent[dest_site_id] = true;
                ctr++;
            } // FOR
            if (update_local) this.sendLocal(txn_id, msg, partitions);
        }
        
        /**
         * 
         * @param channel
         * @param msg
         * @param callback
         */
        protected abstract void sendRemote(HStoreService channel, ProtoRpcController controller, T msg, RpcCallback<U> callback);
        protected abstract void sendLocal(long txn_id, T msg, Collection<Integer> partitions);
    }
    
    // TransactionInit
    private final MessageRouter<TransactionInitRequest, TransactionInitResponse> router_transactionInit = new MessageRouter<TransactionInitRequest, TransactionInitResponse>() {
        protected void sendLocal(long txn_id, TransactionInitRequest msg, Collection<Integer> partitions) {
            
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionInitRequest msg, RpcCallback<TransactionInitResponse> callback) {
            channel.transactionInit(controller, msg, callback);
        }
    };
    // TransactionWork
    private final MessageRouter<TransactionWorkRequest, TransactionWorkResponse> router_transactionWork = new MessageRouter<TransactionWorkRequest, TransactionWorkResponse>() {
        protected void sendLocal(long txn_id, TransactionWorkRequest msg, Collection<Integer> partitions) {
            
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionWorkRequest msg, RpcCallback<TransactionWorkResponse> callback) {
            channel.transactionWork(controller, msg, callback);
        }
    };
    // TransactionPrepare
    private final MessageRouter<TransactionPrepareRequest, TransactionPrepareResponse> router_transactionPrepare = new MessageRouter<TransactionPrepareRequest, TransactionPrepareResponse>() {
        protected void sendLocal(long txn_id, TransactionPrepareRequest msg, Collection<Integer> partitions) {
            // FIXME
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionPrepareRequest msg, RpcCallback<TransactionPrepareResponse> callback) {
            channel.transactionPrepare(controller, msg, callback);
        }
    };
    // TransactionFinish
    private final MessageRouter<TransactionFinishRequest, TransactionFinishResponse> router_transactionFinish = new MessageRouter<TransactionFinishRequest, TransactionFinishResponse>() {
        protected void sendLocal(long txn_id, TransactionFinishRequest msg, Collection<Integer> partitions) {
            // FIXME
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, TransactionFinishRequest msg, RpcCallback<TransactionFinishResponse> callback) {
            channel.transactionFinish(controller, msg, callback);
        }
    };
    // Shutdown
    private final MessageRouter<ShutdownRequest, ShutdownResponse> router_shutdown = new MessageRouter<ShutdownRequest, ShutdownResponse>() {
        protected void sendLocal(long txn_id, ShutdownRequest msg, Collection<Integer> partitions) {
            
        }
        protected void sendRemote(HStoreService channel, ProtoRpcController controller, ShutdownRequest msg, RpcCallback<ShutdownResponse> callback) {
            channel.shutdown(controller, msg, callback);
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
            
            hstore_site.executeTransactionWork(request, done);
            
            /*
            long txn_id = request.getTxnId();
            int sender_partition_id = request.getSenderPartitionId();
            int dest_partition_id = request.getDestPartitionId();
            if (trace.get()) LOG.trace(String.format("Incoming data from partition #%d to partition #%d for txn #%d with %d dependencies",
                                           sender_partition_id, dest_partition_id, txn_id, request.getDependenciesCount()));
    
            for (Hstore.FragmentDependency fd : request.getDependenciesList()) {
                int dependency_id = fd.getDependencyId();
                VoltTable data = null;
                FastDeserializer fds = new FastDeserializer(fd.getData().asReadOnlyByteBuffer());
                try {
                    data = fds.readObject(VoltTable.class);
                } catch (IOException e) {
                    e.printStackTrace();
                    assert(false);
                }
                assert(data != null) : "Null data table from " + request;
                
                // Store the VoltTable in the ExecutionSite
                if (trace.get()) LOG.trace("Storing Depedency #" + dependency_id + " for Txn #" + txn_id + " at Partition #" + dest_partition_id);
                HStoreMessenger.this.hstore_site.getExecutionSite(dest_partition_id).storeDependency(txn_id, sender_partition_id, dependency_id, data);
            } // FOR
            
            // Send back a response
            if (trace.get()) LOG.trace("Sending back FragmentAcknowledgement to Partition #" + sender_partition_id + " for Txn #" + txn_id);
            Hstore.FragmentAcknowledgement ack = Hstore.FragmentAcknowledgement.newBuilder()
                                                        .setTxnId(txn_id)
                                                        .setSenderPartitionId(dest_partition_id)
                                                        .setDestPartitionId(sender_partition_id)
                                                        .build();
            done.run(ack);
            */
        }
        
        /**
         * This method is the first part of two phase commit for a transaction.
         * If speculative execution is enabled, then we'll notify each the ExecutionSites
         * for the listed partitions that it is done. This will cause all the 
         * that are blocked on this transaction to be released immediately and queued 
         * @param request
         */
        @Override
        public void transactionPrepare(RpcController controller, TransactionPrepareRequest request,
                RpcCallback<TransactionPrepareResponse> done) {
            assert(request.hasTransactionId()) : "Got Hstore.TransactionFinishRequest without a txn id!";
            long txn_id = request.getTransactionId();

            if (hstore_site.getHStoreConf().site.exec_speculative_execution) {
                int spec_cnt = 0;
                for (Integer p : request.getPartitionsList()) {
                    if (local_partitions.contains(p) == false) continue;
                        
                    // Make sure that we tell the ExecutionSite first before we allow txns to get fired off
                    boolean ret = hstore_site.getExecutionSite(p.intValue()).enableSpeculativeExecution(txn_id, false);
                    if (debug.get() && ret) {
                        spec_cnt++;
                        LOG.debug(String.format("Partition %d - Speculative Execution!", p));
                    }
                } // FOR
                if (debug.get())
                    LOG.debug(String.format("Enabled speculative execution at %d partitions because of waiting for txn #%d", spec_cnt, txn_id));
            }
            
            Hstore.TransactionPrepareResponse response = Hstore.TransactionPrepareResponse.newBuilder()
                                                                .setTransactionId(txn_id)
                                                                .setStatus(Hstore.Status.OK)
                                                                .build();
            done.run(response);
        }
        
        
        @Override
        public void transactionFinish(RpcController controller, TransactionFinishRequest request,
                RpcCallback<TransactionFinishResponse> done) {
            

            
            long txn_id = request.getTransactionId();
            
            if (debug.get()) LOG.debug(String.format("Processing %s message for txn #%d", request.getClass().getSimpleName(), txn_id));
    
            // FIXME        
            
            // Send back a FinishResponse to let them know we're cool with everything...
            if (done != null) {
                Hstore.TransactionFinishResponse response = Hstore.TransactionFinishResponse.newBuilder()
                                                                                        .setTransactionId(txn_id)
                                                                                        .build();
                done.run(response);
                if (trace.get()) LOG.trace("Sent back Dtxn.FinishResponse for txn #" + txn_id);
            } 
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
                callback = (TransactionRedirectResponseCallback)HStoreSite.POOL_FORWARDTXN_RESPONSE.borrowObject();
                callback.init(local_site_id, request.getSenderId(), done);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get ForwardTxnResponseCallback", ex);
            }
            
            if (HStoreMessenger.this.forward_thread != null) {
                HStoreMessenger.this.forwardQueue.add(Pair.of(serializedRequest, callback));
            } else {
                HStoreMessenger.this.hstore_site.procedureInvocation(serializedRequest, callback);
            }
        }
        
        @Override
        public void shutdown(RpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> done) {
            LOG.info(String.format("Got shutdown request from HStoreSite %s", HStoreSite.formatSiteName(request.getSenderId())));
            
            HStoreMessenger.this.shutting_down = true;
            
            // Tell the coordinator to shutdown
            HStoreMessenger.this.hstore_site.shutdown();
            
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
    
    public void transactionInit(Hstore.TransactionInitRequest request, RpcCallback<Hstore.TransactionInitRequest> callback) {
        
        // TODO: We need to make a general method that can send process requests and blast them out to all
        // of the HStoreSites as needed
        
    }
    
    /**
     * 
     * @param builders
     * @param callback
     */
    public void transactionWork(Map<Integer, Hstore.TransactionWorkRequest.Builder> builders, RpcCallback<Hstore.TransactionWorkResponse> callback) {
        for (Entry<Integer, Hstore.TransactionWorkRequest.Builder> e : builders.entrySet()) {
            assert(e.getValue().getFragmentsCount() > 0);
            // We should never get work for our local partitions
            assert(e.getKey() != this.local_site_id);
            this.router_transactionWork.sendRemote(this.channels.get(e.getKey()),
                                                   new ProtoRpcController(),
                                                   e.getValue().build(),
                                                   callback);
        } // FOR
    }
    
    /**
     * Notify the given partitions that this transaction is finished with them
     * This can also be used for the "early prepare" optimization.
     * @param ts
     * @param callback
     * @param partitions
     */
    public void transactionPrepare(LocalTransaction ts, Collection<Integer> partitions) {
        Hstore.TransactionPrepareRequest request = Hstore.TransactionPrepareRequest.newBuilder()
                                                        .setTransactionId(ts.getTransactionId())
                                                        .addAllPartitions(ts.getDonePartitions())
                                                        .build();
        this.router_transactionPrepare.sendMessages(request.getTransactionId(),
                                                    request,
                                                    ts.getPrepareCallback(),
                                                    partitions);
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
        this.router_transactionFinish.sendMessages(request.getTransactionId(),
                                                   request,
                                                   callback,
                                                   ts.getTouchedPartitions().values());
        if (debug.get())
            LOG.debug(String.format("Notified remote HStoreSites that txn #%d is done at %d partitions", request.getTransactionId(), request.getPartitionsList().size()));
    }
    
    /**
     * Forward a StoredProcedureInvocation request to a remote site for execution
     * @param serializedRequest
     * @param done
     * @param partition
     */
    public void transactionRedirect(byte[] serializedRequest, RpcCallback<Hstore.TransactionRedirectResponse> done, int partition) {
        int dest_site_id = hstore_site.getSiteIdForPartitionId(partition);
        if (debug.get()) LOG.debug("Redirecting transaction request to partition #" + partition + " on " + HStoreSite.formatSiteName(dest_site_id));
        ByteString bs = ByteString.copyFrom(serializedRequest);
        Hstore.TransactionRedirectRequest mr = Hstore.TransactionRedirectRequest.newBuilder()
                                        .setSenderId(this.local_site_id)
                                        .setWork(bs)
                                        .build();
        this.channels.get(dest_site_id).transactionRedirect(new ProtoRpcController(), mr, done);
    }
    
    
//    /**
//     * Send an individual dependency to a remote partition for a given transaction
//     * @param txn_id
//     * @param sender_partition_id TODO
//     * @param dest_partition_id
//     * @param dependency_id
//     * @param table
//     */
//    public void sendDependency(long txn_id, int sender_partition_id, int dest_partition_id, int dependency_id, VoltTable table) {
//        DependencySet dset = new DependencySet(new int[]{ dependency_id }, new VoltTable[]{ table });
//        this.sendDependencySet(txn_id, sender_partition_id, dest_partition_id, dset);
//    }
    
//    /**
//     * Send a DependencySet to a remote partition for a given transaction
//     * @param txn_id
//     * @param sender_partition_id TODO
//     * @param dest_partition_id
//     * @param dset
//     */
//    public void sendDependencySet(long txn_id, int sender_partition_id, int dest_partition_id, DependencySet dset) {
//        assert(dset != null);
//        
//        // Local Transfer
//        if (this.local_partitions.contains(dest_partition_id)) {
//            if (debug.get()) LOG.debug("Transfering " + dset.size() + " dependencies directly from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
//            
//            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
//                ExecutionSite executor = this.hstore_site.getExecutionSite(dest_partition_id);
//                assert(executor != null) : "Unexpected null ExecutionSite for Partition #" + dest_partition_id + " on " + this.hstore_site.getSiteName();
//                
//                // 2010-11-12: We have to copy each VoltTable, otherwise we get an error down in the EE when it tries
//                //             to read data from another EE.
//                VoltTable copy = null;
//                try {
////                    if (sender_partition_id == dest_partition_id) {
////                        copy = dset.dependencies[i];
////                    } else {
//                        copy = this.copyVoltTable(dset.dependencies[i]);
////                    }
//                } catch (Exception ex) {
//                    LOG.fatal("Failed to copy DependencyId #" + dset.depIds[i]);
//                    this.shutdownCluster(ex);
//                }
//                executor.storeDependency(txn_id, sender_partition_id, dset.depIds[i], copy);
//            } // FOR
//        // Remote Transfer
//        } else {
//            if (debug.get()) LOG.debug("Transfering " + dset.size() + " dependencies through network from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
//            ProtoRpcController rpc = new ProtoRpcController();
//            int site_id = this.partition_site_xref.get(dest_partition_id);
//            HStoreService channel = this.channels.get(site_id);
//            assert(channel != null) : "Invalid partition id '" + dest_partition_id + "'";
//            
//            // Serialize DependencySet
//            List<Hstore.FragmentDependency> dependencies = new ArrayList<Hstore.FragmentDependency>();
//            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
//                FastSerializer fs = new FastSerializer(buffer_pool);
//                try {
//                    fs.writeObject(dset.dependencies[i]);
//                } catch (Exception ex) {
//                    LOG.fatal("Failed to serialize DependencyId #" + dset.depIds[i], ex);
//                }
//                BBContainer bc = fs.getBBContainer();
//                assert(bc.b.hasArray());
//                ByteString bs = ByteString.copyFrom(bc.b);
//                
//                Hstore.FragmentDependency fd = Hstore.FragmentDependency.newBuilder()
//                                                        .setDependencyId(dset.depIds[i])
//                                                        .setData(bs)
//                                                        .build();
//                dependencies.add(fd);
//            } // FOR
//            
//            Hstore.FragmentTransfer ft = Hstore.FragmentTransfer.newBuilder()
//                                                    .setTxnId(txn_id)
//                                                    .setSenderPartitionId(sender_partition_id)
//                                                    .setDestPartitionId(dest_partition_id)
//                                                    .addAllDependencies(dependencies)
//                                                    .build();
//            channel.sendFragment(rpc, ft, this.fragment_callback);
//        }
//    }
    

    
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