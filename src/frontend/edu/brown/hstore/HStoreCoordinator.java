package edu.brown.hstore;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.clusterreorganizer.ClusterReorganizer;
import edu.brown.hstore.Hstoreservice.DataFragment;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.LiveMigrationSyncRequest;
import edu.brown.hstore.Hstoreservice.LiveMigrationSyncResponse;
import edu.brown.hstore.Hstoreservice.SendDataRequest;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.Hstoreservice.ShutdownRequest;
import edu.brown.hstore.Hstoreservice.ShutdownResponse;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TimeSyncRequest;
import edu.brown.hstore.Hstoreservice.TimeSyncResponse;
import edu.brown.hstore.Hstoreservice.TransactionFinishRequest;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.TransactionMapRequest;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.Hstoreservice.TransactionPrepareRequest;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.Hstoreservice.TransactionRedirectRequest;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.hstore.Hstoreservice.TransactionReduceRequest;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dispatchers.TransactionFinishDispatcher;
import edu.brown.hstore.dispatchers.TransactionInitDispatcher;
import edu.brown.hstore.dispatchers.TransactionRedirectDispatcher;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.handlers.SendDataHandler;
import edu.brown.hstore.handlers.TransactionFinishHandler;
import edu.brown.hstore.handlers.TransactionInitHandler;
import edu.brown.hstore.handlers.TransactionMapHandler;
import edu.brown.hstore.handlers.TransactionPrepareHandler;
import edu.brown.hstore.handlers.TransactionReduceHandler;
import edu.brown.hstore.handlers.TransactionWorkHandler;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.QueryPrefetcher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.NIOEventLoop;
import edu.brown.protorpc.ProtoRpcChannel;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.protorpc.ProtoServer;
import edu.brown.utils.EventObservable;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class HStoreCoordinator implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(HStoreCoordinator.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

//    private static final DBBPool buffer_pool = new DBBPool(true, false);
//    private static final ByteString ok = ByteString.copyFrom("OK".getBytes());
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final Site catalog_site;
    private int num_sites;
    private final int local_site_id;
    private final Collection<Integer> local_partitions;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    
    /** SiteId -> HStoreService */
    private final Map<Integer, HStoreService> channels = new HashMap<Integer, HStoreService>();
    
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final HStoreService remoteService;
    
    private final TransactionInitHandler transactionInit_handler;
    private final TransactionWorkHandler transactionWork_handler;
    private final TransactionMapHandler transactionMap_handler;
    private final TransactionReduceHandler transactionReduce_handler;
    private final TransactionPrepareHandler transactionPrepare_handler;
    private final TransactionFinishHandler transactionFinish_handler;
    private final SendDataHandler sendData_handler;
    
    private final TransactionInitDispatcher transactionInit_dispatcher;
    private final TransactionFinishDispatcher transactionFinish_dispatcher;
    private final TransactionRedirectDispatcher transactionRedirect_dispatcher;    
    
    private final List<Thread> dispatcherThreads = new ArrayList<Thread>();
    
    private Shutdownable.ShutdownState state = ShutdownState.INITIALIZED;
    
    private final EventObservable<HStoreCoordinator> ready_observable = new EventObservable<HStoreCoordinator>();
    
    private QueryPrefetcher prefetcher;

    private ClusterReorganizer cluster_reorganizer;
    
    /**
     * 
     */
    private class MessengerListener implements Runnable {
        @Override
        public void run() {
            if (hstore_conf.site.cpu_affinity)
                hstore_site.getThreadManager().registerProcessingThread();
            Throwable error = null;
            try {
                HStoreCoordinator.this.eventLoop.run();
            } catch (RuntimeException ex) {
                error = ex;
            } catch (AssertionError ex) {
                error = ex;
            } catch (Exception ex) {
                error = ex;
            }
            
            if (error != null) {
                if (hstore_site.isShuttingDown() == false) {
                    LOG.error(this.getClass().getSimpleName() + " has stopped!", error);
                }
                
                Throwable cause = null;
                if (error instanceof RuntimeException && error.getCause() != null) {
                    if (error.getCause().getMessage() != null && error.getCause().getMessage().isEmpty() == false) {
                        cause = error.getCause();
                    }
                }
                if (cause == null) cause = error;
                
                // These errors are ok if we're actually stopping...
                if (HStoreCoordinator.this.state == ShutdownState.SHUTDOWN ||
                    HStoreCoordinator.this.state == ShutdownState.PREPARE_SHUTDOWN ||
                    HStoreCoordinator.this.hstore_site.isShuttingDown()) {
                    // IGNORE
                } else {
                    LOG.fatal("Unexpected error in messenger listener thread", cause);
                    HStoreCoordinator.this.shutdownCluster(error);
                }
            }
            if (trace.get()) LOG.trace("Messenger Thread for Site #" + catalog_site.getId() + " has stopped!");
        }
    }
    
    /**
     * Constructor
     * @param hstore_site
     */
    public HStoreCoordinator(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.catalog_site = hstore_site.getSite();
        this.local_site_id = this.catalog_site.getId();
        this.local_partitions = hstore_site.getLocalPartitionIds();
        if (debug.get()) LOG.debug("Local Partitions for Site #" + hstore_site.getSiteId() + ": " + this.local_partitions);

        // Incoming RPC Handler
        this.remoteService = this.initHStoreService();
        
        // This listener thread will process incoming messages
        this.listener = new ProtoServer(this.eventLoop);
        
        // Special dispatcher threads to handle incoming requests
        // These are used so that we can process messages in a different thread than the main HStoreCoordinator thread
        
        // TransactionInitDispatcher
        if (hstore_conf.site.coordinator_init_thread) {
            this.transactionInit_dispatcher = new TransactionInitDispatcher(this);
            String name = HStoreThreadManager.getThreadName(this.hstore_site, "coord", "init");
            Thread t = new Thread(this.transactionInit_dispatcher, name);
            this.dispatcherThreads.add(t);
        } else {
            this.transactionInit_dispatcher = null;
        }
        
        // TransactionFinishDispatcher
        if (hstore_conf.site.coordinator_finish_thread) {
            this.transactionFinish_dispatcher = new TransactionFinishDispatcher(this);
            String name = HStoreThreadManager.getThreadName(this.hstore_site, "coord", "finish");
            Thread t = new Thread(this.transactionInit_dispatcher, name);
            this.dispatcherThreads.add(t);
        } else {
            this.transactionFinish_dispatcher = null;
        }

        // TransactionRedirectDispatcher
        if (hstore_conf.site.coordinator_redirect_thread) {
            this.transactionRedirect_dispatcher = new TransactionRedirectDispatcher(this);
            String name = HStoreThreadManager.getThreadName(this.hstore_site, "coord", "redirect");
            Thread t = new Thread(this.transactionInit_dispatcher, name);
            this.dispatcherThreads.add(t);
        } else {
            this.transactionRedirect_dispatcher = null;
        }

        this.transactionInit_handler = new TransactionInitHandler(hstore_site, this, this.transactionInit_dispatcher);
        this.transactionWork_handler = new TransactionWorkHandler(hstore_site, this);
        this.transactionMap_handler = new TransactionMapHandler(hstore_site, this);
        this.transactionReduce_handler = new TransactionReduceHandler(hstore_site,this);
        this.transactionPrepare_handler = new TransactionPrepareHandler(hstore_site, this);
        this.transactionFinish_handler = new TransactionFinishHandler(hstore_site, this, this.transactionFinish_dispatcher);
        this.sendData_handler = new SendDataHandler(hstore_site, this);
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread(new MessengerListener(), HStoreThreadManager.getThreadName(this.hstore_site, "coord"));
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
        
        //this.prefetcher = new QueryPrefetcher(catalog_db, p_estimator);
        
        //Live Migration: ClusterReorganizer --Yang
        this.cluster_reorganizer = null;
    }
    
    protected HStoreService initHStoreService() {
        return (new RemoteServiceHandler());
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
        this.num_sites = this.channels.size();

        for (Thread t : this.dispatcherThreads) {
            if (debug.get()) LOG.debug("Starting dispatcher thread: " + t.getName());
            t.setDaemon(true);
            t.start();
        } // FOR
        
        if (debug.get()) LOG.debug("Starting listener thread");
        this.listener_thread.start();
        
        if (hstore_conf.site.coordinator_sync_time) {
            syncClusterTimes();
        }
        
        // If we find a blank node from command line, we start the live migration process --Yang
        if (hstore_conf.site.isBlank){
            this.startClusterReorganizer();
            notifyClusterLiveMigrationAboutToStart(this.catalog_site.getCatalog());
        }
        
        this.ready_observable.notifyObservers(this);
    }

    /**
     * Returns true if the messenger has started
     * @return
     */
    public boolean isStarted() {
        return (this.state == ShutdownState.STARTED);
    }
    
    /**
     * Internal call for testing to hide errors
     */
    @Override
    public void prepareShutdown(boolean error) {
        if (this.state != ShutdownState.PREPARE_SHUTDOWN) {
            assert(this.state == ShutdownState.STARTED) : "Invalid HStoreCoordinator State " + this.state;
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
            // Kill all of our dispatchers
            for (Thread t : this.dispatcherThreads) {
                if (trace.get()) LOG.trace("Stopping dispatcher thread " + t.getName());
                t.interrupt();
            } // FOR
            
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
    }
    
    /**
     * Returns true if the messenger has stopped
     * @return
     */
    @Override
    public boolean isShuttingDown() {
        return (this.state == ShutdownState.PREPARE_SHUTDOWN);
    }
    
    public boolean isShutdownOrPrepareShutDown() {
        return (this.state == ShutdownState.PREPARE_SHUTDOWN || this.state == ShutdownState.SHUTDOWN);
    }
    
    public HStoreSite getHStoreSite() {
        return (this.hstore_site);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
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
    
    public HStoreService getChannel(int site_id) {
        return (this.channels.get(site_id));
    }
    public HStoreService getHandler() {
        return (this.remoteService);
    }
    public EventObservable<HStoreCoordinator> getReadyObservable() {
        return (this.ready_observable);
    }
    
    public TransactionInitHandler getTransactionInitHandler() {
        return (this.transactionInit_handler);
    }
    
    public TransactionFinishHandler getTransactionFinishHandler() {
        return (this.transactionFinish_handler);
    }
    
//    private int getNumLocalPartitions(Collection<Integer> partitions) {
//        int ctr = 0;
//        int size = partitions.size();
//        for (Integer p : this.local_partitions) {
//            if (partitions.contains(p)) {
//                ctr++;
//                if (size == ctr) break;
//            }
//        } // FOR
//        return (ctr);
//    }
//    
//    private VoltTable copyVoltTable(VoltTable vt) throws Exception {
//        FastSerializer fs = new FastSerializer(buffer_pool);
//        fs.writeObject(vt);
//        BBContainer bc = fs.getBBContainer();
//        assert(bc.b.hasArray());
//        ByteString bs = ByteString.copyFrom(bc.b);
//        
//        FastDeserializer fds = new FastDeserializer(bs.asReadOnlyByteBuffer());
//        VoltTable copy = fds.readObject(VoltTable.class);
//        return (copy);
//    }
    
    /**
     * Initialize all the network connections to remote
     *  
     */
    private void initConnections() {
        if (debug.get()) LOG.debug("Configuring outbound network connections for Site #" + this.catalog_site.getId());
        
        // Initialize inbound channel
        Integer local_port = this.catalog_site.getMessenger_port();
        assert(local_port != null);
        if (debug.get()) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.remoteService);
        this.listener.bind(local_port);

        // Find all the destinations we need to connect to
        // Make the outbound connections
        List<Pair<Integer, InetSocketAddress>> destinations = HStoreCoordinator.getRemoteCoordinators(this.catalog_site);
        
        if (destinations.isEmpty()) {
            if (debug.get()) LOG.debug("There are no remote sites so we are skipping creating connections");
        }
        else {
            if (debug.get()) LOG.debug("Connecting to " + destinations.size() + " remote site messengers");
            ProtoRpcChannel[] channels = null;
            InetSocketAddress arr[] = new InetSocketAddress[destinations.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = destinations.get(i).getSecond();
            } // FOR
                    
            try {
                channels = ProtoRpcChannel.connectParallel(this.eventLoop, arr, 15000);
            } catch (RuntimeException ex) {
                LOG.warn("Failed to connect to remote sites. Going to try again...");
                // Try again???
                try {
                    channels = ProtoRpcChannel.connectParallel(this.eventLoop, arr);
                } catch (Exception ex2) {
                    LOG.fatal("Site #" + this.getLocalSiteId() + " failed to connect to remote sites");
                    this.listener.close();
                    throw ex;    
                }
            }
            assert channels.length == destinations.size();
            for (int i = 0; i < channels.length; i++) {
                Pair<Integer, InetSocketAddress> p = destinations.get(i);
                this.channels.put(p.getFirst(), HStoreService.newStub(channels[i]));
            } // FOR
            
            if (debug.get()) LOG.debug("Site #" + this.getLocalSiteId() + " is fully connected to all sites");
        }
    }
    
    // ----------------------------------------------------------------------------
    // MESSAGE ROUTERS
    // ----------------------------------------------------------------------------
    
    // TransactionFinish
    
    // Shutdown
//    private final MessageRouter<ShutdownRequest, ShutdownResponse> router_shutdown = new MessageRouter<ShutdownRequest, ShutdownResponse>() {
//        protected void sendLocal(long txn_id, ShutdownRequest request, Collection<Integer> partitions, RpcCallback<ShutdownResponse> callback) {
//            
//        }
//        protected void sendRemote(HStoreService channel, ProtoRpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> callback) {
//            channel.shutdown(controller, request, callback);
//        }
//        protected ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id) {
//            return new ProtoRpcController();
//        }
//    };

    
    // ----------------------------------------------------------------------------
    // HSTORE RPC SERVICE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * We want to make this a private inner class so that we do not expose
     * the RPC methods to other parts of the code.
     */
    private class RemoteServiceHandler extends HStoreService {
    
        @Override
        public void transactionInit(RpcController controller, TransactionInitRequest request, RpcCallback<TransactionInitResponse> callback) {
             transactionInit_handler.remoteQueue(controller, request, callback);
        }
        
        @Override
        public void transactionWork(RpcController controller, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
            transactionWork_handler.remoteHandler(controller, request, callback);
        }
        
        @Override
        public void transactionMap(RpcController controller, TransactionMapRequest request, RpcCallback<TransactionMapResponse> callback) {
            transactionMap_handler.remoteQueue(controller, request, callback);
        }
        
        @Override
        public void transactionReduce(RpcController controller, TransactionReduceRequest request, RpcCallback<TransactionReduceResponse> callback) {
            transactionReduce_handler.remoteQueue(controller, request, callback);
        }
        
        @Override
        public void transactionPrepare(RpcController controller, TransactionPrepareRequest request, RpcCallback<TransactionPrepareResponse> callback) {
            transactionPrepare_handler.remoteQueue(controller, request, callback);
        }
        
        @Override
        public void transactionFinish(RpcController controller, TransactionFinishRequest request, RpcCallback<TransactionFinishResponse> callback) {
            transactionFinish_handler.remoteQueue(controller, request, callback);
        }
        
        @Override
        public void transactionRedirect(RpcController controller, TransactionRedirectRequest request, RpcCallback<TransactionRedirectResponse> done) {
            // We need to create a wrapper callback so that we can get the output that
            // HStoreSite wants to send to the client and forward 
            // it back to whomever told us about this txn
            if (debug.get()) 
                LOG.debug(String.format("Recieved redirected transaction request from HStoreSite %s", HStoreThreadManager.formatSiteName(request.getSenderId())));
            byte serializedRequest[] = request.getWork().toByteArray(); // XXX Copy!
            TransactionRedirectResponseCallback callback = null;
            try {
                callback = (TransactionRedirectResponseCallback)HStoreObjectPools.CALLBACKS_TXN_REDIRECTRESPONSE.borrowObject();
                callback.init(local_site_id, request.getSenderId(), done);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get ForwardTxnResponseCallback", ex);
            }
            
            if (transactionRedirect_dispatcher != null) {
                transactionRedirect_dispatcher.queue(Pair.of(serializedRequest, callback));
            } else {
                FastDeserializer fds = new FastDeserializer(serializedRequest);
                StoredProcedureInvocation invocation = null;
                try {
                    invocation = fds.readObject(StoredProcedureInvocation.class);
                } catch (Exception ex) {
                    LOG.fatal("Unexpected error when calling procedureInvocation!", ex);
                    throw new RuntimeException(ex);
                }
                hstore_site.procedureInvocation(invocation, serializedRequest, callback);
            }
        }
        
        @Override
        public void sendData(RpcController controller, SendDataRequest request, RpcCallback<SendDataResponse> done) {
            // Take the SendDataRequest and pass it to the sendData_handler, which
            // will deserialize the embedded VoltTable and wrap it in something that we can
            // then pass down into the underlying ExecutionEngine
            sendData_handler.remoteQueue(controller, request, done);
        }
        
        @Override
        public void shutdown(RpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> done) {
            String originName = HStoreThreadManager.formatSiteName(request.getSenderId());
            
            // See if they gave us the original error. If they did, then we'll
            // try to be helpful and print it out here
            SerializableException error = null;
            if (request.hasError()) {
                ByteString bytes = request.getError();
                error = SerializableException.deserializeFromBuffer(bytes.asReadOnlyByteBuffer());
//                LOG.fatal("Error that caused shutdown from HStoreSite " + originName, error);
            }
            
            LOG.warn(String.format("Got shutdown request from HStoreSite %s", originName, error));
            
            // Tell the HStoreSite to shutdown
            HStoreCoordinator.this.hstore_site.prepareShutdown(false);
            

            // Then send back the acknowledgment right away
            ShutdownResponse response = ShutdownResponse.newBuilder()
                                                   .setSenderId(HStoreCoordinator.this.local_site_id)
                                                   .build();
            done.run(response);
            
            // TODO: This should be moved into the HStoreSite.shutdown()
            HStoreCoordinator.this.hstore_site.shutdown();
            LOG.info(String.format("Shutting down %s [status=%d]", hstore_site.getSiteName(), request.getExitStatus()));
            if (debug.get())
                LOG.debug(String.format("ForwardDispatcher Queue Idle Time: %.2fms",
                                        transactionRedirect_dispatcher.getIdleTime().getTotalThinkTimeMS()));
            ThreadUtil.sleep(1000); // HACK
            LogManager.shutdown();
            System.exit(request.getExitStatus());
            
        }

        @Override
        public void timeSync(RpcController controller, TimeSyncRequest request, RpcCallback<TimeSyncResponse> done) {
            if (debug.get()) 
                LOG.debug(String.format("Recieved %s from HStoreSite %s",
                                                 request.getClass().getSimpleName(),
                                                 HStoreThreadManager.formatSiteName(request.getSenderId())));
            
            TimeSyncResponse response = TimeSyncResponse.newBuilder()
                                                    .setT0R(System.currentTimeMillis())
                                                    .setSenderId(local_site_id)
                                                    .setT0S(request.getT0S())
                                                    .setT1S(System.currentTimeMillis())
                                                    .build();
            done.run(response);
        }

        @Override
        public void liveMigrationSync(RpcController controller, LiveMigrationSyncRequest request, RpcCallback<LiveMigrationSyncResponse> done) {
            if (debug.get()) 
                LOG.debug("Recieved" + request.getClass().getSimpleName() +"from HStoreSite + "+request.getSenderId() +
                		            ", Hostinfo: " + request.getHostInfo().toString());

           
            ByteString host_info = request.getHostInfo();
            
            LiveMigrationSyncResponse response = LiveMigrationSyncResponse.newBuilder()
                                                    .setSenderId(local_site_id)
                                                    .build();
            
            done.run(response);
            
            //When receive a request from a remote site, starts its own clusterReorganizer
            hstore_site.getHStoreCoordinator().startClusterReorganizer(host_info.toString());
        }
    } // END CLASS
    
    
    // ----------------------------------------------------------------------------
    // TRANSACTION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Send a TransactionInitRequest message to all of the sites that have 
     * the partitions that this transaction will need during its execution
     * This must be guaranteed to only be invoked by one thread at a time
     * @param ts
     * @param callback
     */
    public void transactionInit(LocalTransaction ts, RpcCallback<TransactionInitResponse> callback) {
        if (debug.get()) LOG.debug(String.format("%s - Sending TransactionInitRequest to %d partitions %s",
                                   ts, ts.getPredictTouchedPartitions().size(), ts.getPredictTouchedPartitions()));

        
        // Look at the Procedure to see whether it has prefetchable queries. If it does, then
        // embed them in the TransactionInitRequest
        if (ts.getProcedure().getPrefetch()) {
            TransactionInitRequest[] requests = this.prefetcher.generateWorkFragments(ts);
            for (int site_id = 0; site_id < this.num_sites; site_id++) {
                if (site_id == this.local_site_id) {
                    this.transactionInit_handler.sendLocal(ts.getTransactionId(), requests[site_id], ts.getPredictTouchedPartitions(), callback);
                }
                if (requests[site_id] != null) {
                    ProtoRpcController controller = ts.getTransactionInitController(site_id);
                    this.channels.get(site_id).transactionInit(controller, requests[site_id], callback);
                }
            } // FOR
        }
        else {
            TransactionInitRequest request = TransactionInitRequest.newBuilder()
                    .setTransactionId(ts.getTransactionId())
                    .setProcedureId(ts.getProcedure().getId())
                    .addAllPartitions(ts.getPredictTouchedPartitions())
                    .build();
            assert(callback != null) :
                String.format("Trying to initialize %s with a null TransactionInitCallback", ts);
            this.transactionInit_handler.sendMessages(ts, request, callback, request.getPartitionsList());
        }
        
        // TODO(cjl6): In the later version, use a BatchPlanner to identify which WorkFragments need to
        //             go to which partitions and then generate unique InitRequest objects per partition
        
        
        // TODO(pavlo): Add boolean flag to Procedure catalog object that says whether 
        // it has pre-fetchable queries or not. Create a quick lookup mechanism to get those queries
        
        // TODO(pavlo): Add the ability to allow a partition that rejects a InitRequest to send notifications
        //              about the rejection to the other partitions that are included in the InitRequest.
    }
    
    /**
     * Send the TransactionWorkRequest to the target remote site
     * @param builders
     * @param callback
     */
    public void transactionWork(LocalTransaction ts, int site_id, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
        if (debug.get()) LOG.debug(String.format("%s - Sending TransactionWorkRequest to remote site %d [numFragments=%d]",
                                   ts, site_id, request.getFragmentsCount()));
        
        assert(request.getFragmentsCount() > 0) :
            String.format("No WorkFragments for Site %d in %s", site_id, ts);
        
        // We should never get work for our local partitions
        assert(site_id != this.local_site_id);
        assert(ts.getTransactionId().longValue() == request.getTransactionId()) :
            String.format("%s is for txn #%d but the %s has txn #%d",
                          ts.getClass().getSimpleName(), ts.getTransactionId(),
                          request.getClass().getSimpleName(), request.getTransactionId());
        
        this.channels.get(site_id).transactionWork(ts.getTransactionWorkController(site_id), request, callback);
    }
    
    /**
     * Notify the given partitions that this transaction is finished with them
     * This can also be used for the "early prepare" optimization.
     * @param ts
     * @param callback
     * @param partitions
     */
    public void transactionPrepare(LocalTransaction ts, TransactionPrepareCallback callback, Collection<Integer> partitions) {
        if (debug.get())
            LOG.debug(String.format("Notifying partitions %s that %s is preparing to commit", partitions, ts));
        
        TransactionPrepareRequest request = TransactionPrepareRequest.newBuilder()
                                                        .setTransactionId(ts.getTransactionId())
                                                        .addAllPartitions(partitions)
                                                        .build();
        this.transactionPrepare_handler.sendMessages(ts, request, callback, partitions);
    }

    /**
     * Notify all remote HStoreSites that the distributed transaction is done with data
     * at the given partitions and that they need to commit/abort the results.
     * IMPORTANT: Any data that you need from the LocalTransaction handle should be taken
     * care of before this is invoked, because it may clean-up that object before it returns
     * @param ts
     * @param status
     * @param callback
     */
    public void transactionFinish(LocalTransaction ts, Status status, TransactionFinishCallback callback) {
        Collection<Integer> partitions = ts.getPredictTouchedPartitions();
        if (debug.get())
            LOG.debug(String.format("Notifying partitions %s that %s is finished [status=%s]",
                                    partitions, ts, status));
        
        TransactionFinishRequest request = TransactionFinishRequest.newBuilder()
                                                        .setTransactionId(ts.getTransactionId())
                                                        .setStatus(status)
                                                        .addAllPartitions(partitions)
                                                        .build();
        this.transactionFinish_handler.sendMessages(ts, request, callback, partitions);
    }
    
    /**
     * Forward a StoredProcedureInvocation request to a remote site for execution
     * @param serializedRequest
     * @param callback
     * @param partition
     */
    public void transactionRedirect(byte[] serializedRequest, RpcCallback<TransactionRedirectResponse> callback, int partition) {
        int dest_site_id = hstore_site.getSiteIdForPartitionId(partition);
        if (debug.get()) LOG.debug("Redirecting transaction request to partition #" + partition + " on " + HStoreThreadManager.formatSiteName(dest_site_id));
        ByteString bs = ByteString.copyFrom(serializedRequest);
        TransactionRedirectRequest mr = TransactionRedirectRequest.newBuilder()
                                        .setSenderId(this.local_site_id)
                                        .setWork(bs)
                                        .build();
        this.channels.get(dest_site_id).transactionRedirect(new ProtoRpcController(), mr, callback);
    }
    
    // ----------------------------------------------------------------------------
    // MapReduce METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Tell all remote partitions to start the map phase for this txn
     * @param ts
     */
    public void transactionMap(LocalTransaction ts, RpcCallback<TransactionMapResponse> callback) {
        ByteString invocation = null;
        try {
            ByteBuffer b = ByteBuffer.wrap(FastSerializer.serialize(ts.getInvocation()));
            invocation = ByteString.copyFrom(b.array()); 
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error when serializing StoredProcedureInvocation", ex);
        }
        
        TransactionMapRequest request = TransactionMapRequest.newBuilder()
                                                     .setTransactionId(ts.getTransactionId())
                                                     .setBasePartition(ts.getBasePartition())
                                                     .setInvocation(invocation)
                                                     .build();
        
        Collection<Integer> partitions = ts.getPredictTouchedPartitions();
        if (debug.get())
             LOG.debug(String.format("Notifying partitions %s that %s is in Map Phase", partitions, ts));
        //assert(ts.mapreduce == true) : "MapReduce Transaction flag is not set, " + hstore_site.getSiteName();
        
        LOG.info("<HStoreCoordinator.TransactionMap> is executing to sendMessages to all partitions\n");
        this.transactionMap_handler.sendMessages(ts, request, callback, partitions);
    }
    
    /**
     * Tell all remote partitions to start the reduce phase for this txn
     * @param ts
     */
    public void transactionReduce(LocalTransaction ts, RpcCallback<TransactionReduceResponse> callback) {
        ByteString invocation = null;
        try {
            ByteBuffer b = ByteBuffer.wrap(FastSerializer.serialize(ts.getInvocation()));
            invocation = ByteString.copyFrom(b.array()); 
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error when serializing StoredProcedureInvocation", ex);
        }
        
        TransactionReduceRequest request = TransactionReduceRequest.newBuilder()
                                                     .setTransactionId(ts.getTransactionId())
                                                     .setBasePartition(ts.getBasePartition())
                                                     .setInvocation(invocation)
                                                     .build();
        
        Collection<Integer> partitions = ts.getPredictTouchedPartitions();
        if (debug.get())
             LOG.debug(String.format("Notifying partitions %s that %s is in Reduce Phase", partitions, ts));
               
        LOG.info("<HStoreCoordinator.TransactionReduce> is executing to sendMessages to all partitions\n");
        this.transactionReduce_handler.sendMessages(ts, request, callback, partitions);
    }
    
    // ----------------------------------------------------------------------------
    // SEND DATA METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * This is will be the main method used to send data from one partition to another.
     * We will probably to dispatch these messages and handle then on the remote 
     * side in a separate thread so that we don't block the ExecutionSite threads
     * or any networking thread. We also need to make sure that if have to send
     * data to a partition that's on our same machine, then we don't want to 
     * waste time serializing + deserializing the data when didn't have to.
     * @param ts
     */
    public void sendData(LocalTransaction ts, Map<Integer, VoltTable> data, RpcCallback<SendDataResponse> callback) {
        
        // TODO(xin): Loop through all of the remote HStoreSites and grab their partition data
        //            out of the map given as input. Create a single SendDataRequest for that
        //            HStoreSite and then use the direct channel to send the data. Be sure to skip
        //            the partitions at the local site
        //
        //            this.channels.get(dest_site_id).sendData(new ProtoRpcController(), request, callback);
        //
        //            Then go back and grab the local partition data and invoke sendData_handler.sendLocal
        
        
        long txn_id = ts.getTransactionId();
        Set<Integer> fake_responses = null;
        for (Site remote_site : CatalogUtil.getAllSites(this.catalog_site)) {
            int dest_site_id = remote_site.getId();
            if (debug.get())
                LOG.debug("Dest_site_id: " + dest_site_id + "  Local_site_id: " + this.local_site_id);
            if (dest_site_id == this.local_site_id) {
                // If there is no data for any partition at this remote HStoreSite, then we will fake a response
                // message to the callback and tell them that everything is ok
                if (fake_responses == null) fake_responses = new HashSet<Integer>();
                fake_responses.add(dest_site_id);
                if (debug.get()) 
                    LOG.debug("Did not send data to " + remote_site + ". Will send a fake response instead");
                
                continue;
            }

            SendDataRequest.Builder builder = SendDataRequest.newBuilder()
                    .setTransactionId(txn_id)
                    .setSenderId(local_site_id);

            // Loop through and get all the data for this site
            if (debug.get())
                LOG.debug(String.format("CatalogUtil.getAllSites : " + CatalogUtil.getAllSites(this.catalog_site).size() +
                        "     Remote_site partitions: " + remote_site.getPartitions().size()));
            for (Partition catalog_part : remote_site.getPartitions()) {
                VoltTable vt = data.get(catalog_part.getId());
                if (vt == null) {
                    LOG.warn("No data in " + ts + " for partition " + catalog_part.getId());
                    continue;
                }
                ByteString bs = null;
                byte bytes[] = null;
                try {
                    bytes = ByteBuffer.wrap(FastSerializer.serialize(vt)).array();
                    bs = ByteString.copyFrom(bytes); 
                    if (debug.get())
                        LOG.debug(String.format("Outbound data for Partition #%d: RowCount=%d / MD5=%s / Length=%d",
                                                catalog_part.getId(), vt.getRowCount(), StringUtil.md5sum(bytes), bytes.length));
                } catch (Exception ex) {
                    throw new RuntimeException(String.format("Unexpected error when serializing %s data for partition %d",
                                                             ts, catalog_part.getId()), ex);
                }
                if (debug.get()) 
                    LOG.debug("Constructing Dependency for " + catalog_part);
                builder.addFragments(DataFragment.newBuilder()
                             .setId(catalog_part.getId())
                             .addData(bs)
                             .build());
            } // FOR n partitions in remote_site
            
            if (builder.getFragmentsCount() > 0) {
                if (debug.get())
                    LOG.debug(String.format("Sending data to %d partitions at %s for %s",
                                                     builder.getFragmentsCount(), remote_site, ts));
                this.channels.get(dest_site_id).sendData(new ProtoRpcController(), builder.build(), callback);
            }
        } // FOR n sites in this catalog
                
        for (int partition : this.local_partitions) {
            VoltTable vt = data.get(partition);
            if (vt == null) {
                LOG.warn("No data in " + ts + " for partition " + partition);
                continue;
            }
            if (debug.get()) LOG.debug(String.format("Storing VoltTable directly at local partition %d for %s", partition, ts));
            ts.storeData(partition, vt);
        } // FOR
        
        if (fake_responses != null) {
            if (debug.get()) LOG.debug(String.format("Sending fake responses for %s for partitions %s", ts, fake_responses));
            for (int dest_site_id : fake_responses) {
                SendDataResponse.Builder builder = SendDataResponse.newBuilder()
                                                                                 .setTransactionId(txn_id)
                                                                                 .setStatus(Hstoreservice.Status.OK)
                                                                                 .setSenderId(dest_site_id);
                callback.run(builder.build());
            } // FOR
        }
    }
    // ----------------------------------------------------------------------------
    // Live Migration METHODS --Yang
    // ----------------------------------------------------------------------------
    /**
     * Tell all remote partitions to start their ClusterReorganizer for Live Migration
     */
    public void notifyClusterLiveMigrationAboutToStart(Catalog catalog){
        // We don't need to do this if there is only one site
        if (this.num_sites == 0) return;
        
        final CountDownLatch latch = new CountDownLatch(this.num_sites);
        
        RpcCallback<LiveMigrationSyncResponse> callback = new RpcCallback<LiveMigrationSyncResponse>() {
            @Override
            public void run(LiveMigrationSyncResponse request) {
                int res = request.getSenderId();
                if (debug.get()) 
                    LOG.debug("Received Live Migration ACK from partition " + res);
                latch.countDown();
            }
        };
        //extract additional host_info from catalog 
        String host_info = getNewHostInfoFromCatalog(catalog);
        ByteString host_info_byte = ByteString.copyFrom(host_info.getBytes());
        
        // Send out Live Migration request 
        for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
            if (e.getKey().intValue() == this.local_site_id) continue;
            LiveMigrationSyncRequest request = LiveMigrationSyncRequest.newBuilder()
                                            .setSenderId(this.local_site_id)
                                            .setHostInfo(host_info_byte)
                                            .build();
            
            e.getValue().liveMigrationSync(new ProtoRpcController(), request, callback);
            if (debug.get()) LOG.debug("Sent Live Migration SYNC to " + HStoreThreadManager.formatSiteName(e.getKey()));
        } // FOR
        
        if (debug.get()) LOG.debug("Sent out all Live Migration requests!");
        boolean success = false;
        try {
            success = latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            // nothing
        }
        
        if (success == false) {
            LOG.warn(String.format("Failed to recieve time Live Migration responses from %d remote HStoreSites", this.num_sites));
        } else if (debug.get()) LOG.trace("Received all Live Migration responses!");
        
    }
    
    private String getNewHostInfoFromCatalog(Catalog catalog){
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        CatalogMap<Host> hosts = catalog_clus.getHosts();
        CatalogMap<Site> sites = catalog_clus.getSites();
        Site[] sites_value = sites.values();
        Host[] hosts_value = hosts.values();
        
        String host_name = hosts_value[hosts_value.length - 1].getName();
        int site_id = sites_value[sites_value.length - 1].getId();
        
        CatalogMap<Partition> partitions = sites_value[sites_value.length -1].getPartitions();
        Partition[] partitions_value = partitions.values();
        int first_partition = partitions_value[0].getId();
        int last_partition = partitions_value[partitions_value.length - 1].getId();
        
        if(first_partition != last_partition){
            return host_name + ":" +site_id +":"+first_partition+"-"+(last_partition);
        }else{
            return host_name + ":" +site_id +":"+first_partition;
        }
        
    }
    
    public void startClusterReorganizer(){
        this.cluster_reorganizer = new ClusterReorganizer(this.hstore_site);
        Thread t = new Thread(this.cluster_reorganizer);
        t.setDaemon(true);
        t.start();
    }
    
    public void startClusterReorganizer(String host_info){
        this.cluster_reorganizer = new ClusterReorganizer(this.hstore_site, host_info);
        Thread t = new Thread(this.cluster_reorganizer);
        t.setDaemon(true);
        t.start();
    }
    
    public ClusterReorganizer getClusterReorganizer(){
        return this.cluster_reorganizer;
    }
    // ----------------------------------------------------------------------------
    // TIME SYNCHRONZIATION
    // ----------------------------------------------------------------------------
    
    /**
     * Approximate the time offsets of all the sites in the cluster so that we can offset
     * our TransactionIdManager's timestamps by the site with the clock the furthest ahead. 
     * This is a blocking call and only really needs to be performed once at start-up
     */
    public void syncClusterTimes() {
        // We don't need to do this if there is only one site
        if (this.num_sites == 0) return;
        
        final CountDownLatch latch = new CountDownLatch(this.num_sites);
        final Map<Integer, Integer> time_deltas = Collections.synchronizedMap(new HashMap<Integer, Integer>());
        
        RpcCallback<TimeSyncResponse> callback = new RpcCallback<TimeSyncResponse>() {
            @Override
            public void run(TimeSyncResponse request) {
                long t1_r = System.currentTimeMillis();
                int dt = (int)((request.getT1S() + request.getT0R()) - (t1_r + request.getT0S())) / 2;
                time_deltas.put(request.getSenderId(), dt);
                latch.countDown();
            }
        };
        
        // Send out TimeSync request 
        for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
            if (e.getKey().intValue() == this.local_site_id) continue;
            TimeSyncRequest request = TimeSyncRequest.newBuilder()
                                            .setSenderId(this.local_site_id)
                                            .setT0S(System.currentTimeMillis())
                                            .build();
            e.getValue().timeSync(new ProtoRpcController(), request, callback);
            if (trace.get()) LOG.trace("Sent TIMESYNC to " + HStoreThreadManager.formatSiteName(e.getKey()));
        } // FOR
        
        if (trace.get()) LOG.trace("Sent out all TIMESYNC requests!");
        boolean success = false;
        try {
            success = latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            // nothing
        }
        if (success == false) {
            LOG.warn(String.format("Failed to recieve time synchronization responses from %d remote HStoreSites", this.num_sites));
        } else if (trace.get()) LOG.trace("Received all TIMESYNC responses!");
        
        // Then do the time calculation
        long max_dt = 0L;
        int culprit = this.local_site_id;
        for (Entry<Integer, Integer> e : time_deltas.entrySet()) {
            if (debug.get()) LOG.debug(String.format("Time delta to HStoreSite %d is %d ms", e.getKey(), e.getValue()));
            if (e.getValue() > max_dt) {
                max_dt = e.getValue();
                culprit = e.getKey();
            }
        }
        this.hstore_site.setTransactionIdManagerTimeDelta(max_dt);
        if (debug.get()) {
            LOG.debug("Setting time delta to " + max_dt + "ms");
            LOG.debug("I think the killer is site " + culprit + "!");
        }
    }
    
    // ----------------------------------------------------------------------------
    // SHUTDOWN METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Take down the cluster. This is a non-blocking call. It will return right away
     * @param error
     * @param blocking
     */
    public void shutdownCluster(final Throwable error) {
        if (debug.get())
            LOG.debug(String.format("Invoking shutdown protocol [non-blocking / error=%s]", error));
        // Make this a thread so that we don't block and can continue cleaning up other things
        Thread shutdownThread = new Thread() {
            @Override
            public void run() {
                HStoreCoordinator.this.shutdownClusterBlocking(error); // Never returns!
            }
        };
        shutdownThread.setName(HStoreThreadManager.getThreadName(this.hstore_site, "shutdown"));
        shutdownThread.setDaemon(true);
        shutdownThread.start();
        return;
    }
    
    /**
     * Tell all of the other sites to shutdown and then knock ourselves out...
     * This is a non-blocking call.
     */
    public void shutdownCluster() {
        this.shutdownCluster(null);
    }
    
    /**
     * Shutdown the cluster. If the given Exception is not null, then all the nodes will
     * exit with a non-zero status. This is will never return
     * TODO: Move into HStoreSite
     * @param error
     */
    protected synchronized void shutdownClusterBlocking(final Throwable error) {
        if (this.state == ShutdownState.SHUTDOWN) return;
        this.hstore_site.prepareShutdown(error != null);
        LOG.info("Shutting down cluster", error);

        final int exit_status = (error == null ? 0 : 1);
        final CountDownLatch latch = new CountDownLatch(this.num_sites);
        
        try {
            if (this.num_sites > 0) {
                RpcCallback<ShutdownResponse> callback = new RpcCallback<ShutdownResponse>() {
                    private final Set<Integer> siteids = new HashSet<Integer>(); 
                    
                    @Override
                    public void run(ShutdownResponse parameter) {
                        int siteId = parameter.getSenderId();
                        assert(this.siteids.contains(siteId) == false) :
                            "Duplicate response from remote HStoreSite " + HStoreThreadManager.formatSiteName(siteId);
                        this.siteids.add(siteId);
                        if (trace.get()) LOG.trace("Received " + this.siteids.size() + "/" + num_sites + " shutdown acknowledgements");
                        latch.countDown();
                    }
                };
                
                ShutdownRequest.Builder builder = ShutdownRequest.newBuilder()
                                                        .setSenderId(catalog_site.getId())
                                                        .setExitStatus(exit_status);
                // Pack the error into a SerializableException
                if (error != null) {
                    SerializableException sError = new SerializableException(error);
                    ByteBuffer buffer = sError.serializeToBuffer();
                    builder.setError(ByteString.copyFrom(buffer));
                    LOG.info("Serializing error message in shutdown request");
                }
                
                ShutdownRequest request = builder.build();
                if (debug.get()) LOG.debug(String.format("Sending %s to %d remote sites",
                                                         request.getClass().getSimpleName(), this.num_sites));
                for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
                    e.getValue().shutdown(new ProtoRpcController(), request, callback);
                    if (trace.get()) LOG.trace(String.format("Sent %s to %s",
                                                             request.getClass().getSimpleName(),
                                                             HStoreThreadManager.formatSiteName(e.getKey())));
                } // FOR
            }
        
            // Tell ourselves to shutdown while we wait
            if (debug.get()) LOG.debug("Telling local site to shutdown");
            this.hstore_site.shutdown();
            
            // Block until the latch releases us
            if (this.num_sites > 0) {
                LOG.info(String.format("Waiting for %d sites to finish shutting down", latch.getCount()));
                latch.await(5, TimeUnit.SECONDS);
            }
        } catch (Throwable ex2) {
            // IGNORE
        } finally {
            LOG.info(String.format("Shutting down [site=%d, status=%d]", catalog_site.getId(), exit_status));
            if (error != null) {
                LOG.fatal("A fatal error caused this shutdown", error);
            }
            LogManager.shutdown();
            System.exit(exit_status);
        }
    }


    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public static List<Pair<Integer, InetSocketAddress>> getRemoteCoordinators(Site catalog_site) {
        List<Pair<Integer, InetSocketAddress>> m = new ArrayList<Pair<Integer,InetSocketAddress>>();
        
        Database catalog_db = CatalogUtil.getDatabase(catalog_site);
        Map<Host, Set<Site>> host_partitions = CatalogUtil.getSitesPerHost(catalog_db);
        for (Entry<Host, Set<Site>> e : host_partitions.entrySet()) {
            String host = e.getKey().getIpaddr();
            for (Site remote_site : e.getValue()) {
                if (remote_site.getId() != catalog_site.getId()) {
                    InetSocketAddress address = new InetSocketAddress(host, remote_site.getMessenger_port()); 
                    m.add(Pair.of(remote_site.getId(), address));
                    if (debug.get()) LOG.debug(String.format("Creating RpcChannel to %s for site %s",
                                               address, HStoreThreadManager.formatSiteName(remote_site.getId())));
                } // FOR
            } // FOR 
        } // FOR
        return (m);
    }

    /**
     * Returns an HStoreService handle that is connected to the given site
     * This should not be called directly.
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