package edu.brown.hstore;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.HeartbeatRequest;
import edu.brown.hstore.Hstoreservice.HeartbeatResponse;
import edu.brown.hstore.Hstoreservice.InitializeRequest;
import edu.brown.hstore.Hstoreservice.InitializeResponse;
import edu.brown.hstore.Hstoreservice.SendDataRequest;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareRequest;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareResponse;
import edu.brown.hstore.Hstoreservice.ShutdownRequest;
import edu.brown.hstore.Hstoreservice.ShutdownResponse;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TimeSyncRequest;
import edu.brown.hstore.Hstoreservice.TimeSyncResponse;
import edu.brown.hstore.Hstoreservice.TransactionDebugRequest;
import edu.brown.hstore.Hstoreservice.TransactionDebugResponse;
import edu.brown.hstore.Hstoreservice.TransactionFinishRequest;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.TransactionMapRequest;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchAcknowledgement;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.Hstoreservice.TransactionPrepareRequest;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.Hstoreservice.TransactionRedirectRequest;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.hstore.Hstoreservice.TransactionReduceRequest;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.Hstoreservice.UnevictDataRequest;
import edu.brown.hstore.Hstoreservice.UnevictDataRequest.Builder;
import edu.brown.hstore.Hstoreservice.UnevictDataResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.LocalInitQueueCallback;
import edu.brown.hstore.callbacks.ShutdownPrepareCallback;
import edu.brown.hstore.callbacks.LocalFinishCallback;
import edu.brown.hstore.callbacks.TransactionPrefetchCallback;
import edu.brown.hstore.callbacks.LocalPrepareCallback;
import edu.brown.hstore.callbacks.TransactionRedirectResponseCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dispatchers.TransactionFinishDispatcher;
import edu.brown.hstore.dispatchers.TransactionInitDispatcher;
import edu.brown.hstore.dispatchers.TransactionRedirectDispatcher;
import edu.brown.hstore.handlers.SendDataHandler;
import edu.brown.hstore.handlers.TransactionFinishHandler;
import edu.brown.hstore.handlers.TransactionInitHandler;
import edu.brown.hstore.handlers.TransactionMapHandler;
import edu.brown.hstore.handlers.TransactionPrefetchHandler;
import edu.brown.hstore.handlers.TransactionPrepareHandler;
import edu.brown.hstore.handlers.TransactionReduceHandler;
import edu.brown.hstore.handlers.TransactionWorkHandler;
import edu.brown.hstore.specexec.PrefetchQueryPlanner;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.DependencyTracker;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.hstore.txns.TransactionUtil;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.NIOEventLoop;
import edu.brown.protorpc.ProtoRpcChannel;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.protorpc.ProtoServer;
import edu.brown.utils.EventObservable;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class HStoreCoordinator implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(HStoreCoordinator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL STATE
    // ----------------------------------------------------------------------------
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final CatalogContext catalogContext;
    private final Site catalog_site;
    private final int num_sites;
    private final int local_site_id;
    
    /** SiteId -> HStoreService */
    private final HStoreService channels[];
    
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final HStoreService remoteService;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    
    private Shutdownable.ShutdownState state = ShutdownState.INITIALIZED;
    
    private final ThreadLocal<FastSerializer> serializers = new ThreadLocal<FastSerializer>() {
        protected FastSerializer initialValue() {
            return new FastSerializer(); // TODO: Use pooled memory
        };
    };
    
    /**
     * Special observable that is invoked when this HStoreCoordinator is on-line
     * and ready to communicating with other nodes in the cluster.
     */
    private final EventObservable<HStoreCoordinator> ready_observable = new EventObservable<HStoreCoordinator>();
    
    // ----------------------------------------------------------------------------
    // HANDLERS
    // ----------------------------------------------------------------------------
    
    private final TransactionInitHandler transactionInit_handler;
    private final TransactionWorkHandler transactionWork_handler;
    private final TransactionPrefetchHandler transactionPrefetch_handler;
    private final TransactionMapHandler transactionMap_handler;
    private final TransactionReduceHandler transactionReduce_handler;
    private final TransactionPrepareHandler transactionPrepare_handler;
    private final TransactionFinishHandler transactionFinish_handler;
    private final SendDataHandler sendData_handler;
    
    // ----------------------------------------------------------------------------
    // DISPATCHERS
    // ----------------------------------------------------------------------------
    
    private final TransactionInitDispatcher transactionInit_dispatcher;
    private final TransactionFinishDispatcher transactionFinish_dispatcher;
    private final TransactionRedirectDispatcher transactionRedirect_dispatcher;    
    private final List<Thread> dispatcherThreads = new ArrayList<Thread>();

    // ----------------------------------------------------------------------------
    // QUERY PREFETCHING
    // ----------------------------------------------------------------------------
    
    private final TransactionPrefetchCallback transactionPrefetch_callback;
    private final PrefetchQueryPlanner prefetchPlanner;
    
    // ----------------------------------------------------------------------------
    // MESSENGER LISTENER THREAD
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    private class MessengerListener implements Runnable {
        @Override
        public void run() {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_COORDINATOR));
            hstore_site.getThreadManager().registerProcessingThread();
            
            Throwable error = null;
            try {
                HStoreCoordinator.this.eventLoop.run();
            } catch (Throwable ex) {
                error = ex;
            }
            
            if (error != null) {
                if (hstore_site.isShuttingDown() == false) {
                    LOG.error(this.getClass().getSimpleName() + " has stopped!", error);
                }
                
                Throwable cause = null;
                if (error instanceof ServerFaultException && error.getCause() != null) {
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
            if (trace.val)
                LOG.trace("Messenger Thread for Site #" + catalog_site.getId() + " has stopped!");
        }
    }
    
    // ----------------------------------------------------------------------------
    // HEARTBEAT CALLBACK
    // ----------------------------------------------------------------------------
    
    private final RpcCallback<HeartbeatResponse> heartbeatCallback = new RpcCallback<HeartbeatResponse>() {
        @Override
        public void run(HeartbeatResponse response) {
            if (response.getStatus() == Status.OK) {
                if (trace.val)
                    LOG.trace(String.format("%s %s -> %s [%s]",
                              response.getClass().getSimpleName(),
                              HStoreThreadManager.formatSiteName(response.getSenderSite()),
                              HStoreThreadManager.formatSiteName(local_site_id),
                              response.getStatus()));
                // FIXME: We need to actually store the heartbeat updates somewhere...
                assert(response.getSenderSite() != local_site_id);
            }
        }
    };

    // ----------------------------------------------------------------------------
    // UNEVICT CALLBACK
    // ----------------------------------------------------------------------------
    
    private RpcCallback<UnevictDataResponse> unevictCallback = new RpcCallback<UnevictDataResponse>() {
        @Override
        public void run(UnevictDataResponse response) {
            if (response.getStatus() == Status.OK) {
                if (trace.val)
                    LOG.trace(String.format("%s %s -> %s [%s]",
                              response.getClass().getSimpleName(),
                              HStoreThreadManager.formatSiteName(response.getSenderSite()),
                              HStoreThreadManager.formatSiteName(local_site_id),
                              response.getStatus()));
                long oldTxnId = response.getTransactionId();
                // int partition = response.getPartitionId();

                LocalTransaction ts = hstore_site.getTransaction(oldTxnId);
                
                assert(response.getSenderSite() != local_site_id);
                hstore_site.getTransactionInitializer().resetTransactionId(ts, ts.getBasePartition());
                if (debug.val)
                    LOG.debug(String.format("transaction %d is being restarted", ts.getTransactionId()));
            	LocalInitQueueCallback initCallback = (LocalInitQueueCallback)ts.getInitCallback();
                hstore_site.getCoordinator().transactionInit(ts, initCallback);
            }
        }
    };

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * @param hstore_site
     */
    public HStoreCoordinator(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = this.hstore_site.getHStoreConf();
        this.catalogContext = this.hstore_site.getCatalogContext();
        this.catalog_site = this.hstore_site.getSite();
        this.local_site_id = this.catalog_site.getId();
        this.num_sites = this.hstore_site.getCatalogContext().numberOfSites;
        this.channels = new HStoreService[this.num_sites];

        if (debug.val)
            LOG.debug(String.format("Local Partitions for Site #%d: %s",
                      hstore_site.getSiteId(), hstore_site.getLocalPartitionIds()));

        // Incoming RPC Handler
        this.remoteService = this.initHStoreService();
        
        // This listener thread will process incoming messages
        this.listener = new ProtoServer(this.eventLoop);
        
        // Special dispatcher threads to handle incoming requests
        // These are used so that we can process messages in a different thread than the main HStoreCoordinator thread
        
        // TransactionInitDispatcher
        if (hstore_conf.site.coordinator_init_thread) {
            this.transactionInit_dispatcher = new TransactionInitDispatcher(this.hstore_site, this);
            String name = HStoreThreadManager.getThreadName(this.hstore_site, "coord", "init");
            Thread t = new Thread(this.transactionInit_dispatcher, name);
            this.dispatcherThreads.add(t);
        } else {
            this.transactionInit_dispatcher = null;
        }
        
        // TransactionFinishDispatcher
        if (hstore_conf.site.coordinator_finish_thread) {
            this.transactionFinish_dispatcher = new TransactionFinishDispatcher(this.hstore_site, this);
            String name = HStoreThreadManager.getThreadName(this.hstore_site, "coord", "finish");
            Thread t = new Thread(this.transactionInit_dispatcher, name);
            this.dispatcherThreads.add(t);
        } else {
            this.transactionFinish_dispatcher = null;
        }

        // TransactionRedirectDispatcher
        if (hstore_conf.site.coordinator_redirect_thread) {
            this.transactionRedirect_dispatcher = new TransactionRedirectDispatcher(this.hstore_site, this);
            String name = HStoreThreadManager.getThreadName(this.hstore_site, "coord", "redirect");
            Thread t = new Thread(this.transactionInit_dispatcher, name);
            this.dispatcherThreads.add(t);
        } else {
            this.transactionRedirect_dispatcher = null;
        }

        this.transactionInit_handler = new TransactionInitHandler(hstore_site, this, this.transactionInit_dispatcher);
        this.transactionWork_handler = new TransactionWorkHandler(hstore_site, this);
        this.transactionPrefetch_handler = new TransactionPrefetchHandler(hstore_site, this);
        this.transactionMap_handler = new TransactionMapHandler(hstore_site, this);
        this.transactionReduce_handler = new TransactionReduceHandler(hstore_site,this);
        this.transactionPrepare_handler = new TransactionPrepareHandler(hstore_site, this);
        this.transactionFinish_handler = new TransactionFinishHandler(hstore_site, this, this.transactionFinish_dispatcher);
        this.sendData_handler = new SendDataHandler(hstore_site, this);
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread(new MessengerListener());
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
        
        // Initialize the PrefetchQueryPlanner if we're allowed to execute
        // speculative queries and we actually have some in the catalog 
        PrefetchQueryPlanner tmpPlanner = null;
        if (hstore_conf.site.exec_prefetch_queries) {
            boolean has_prefetch = false;
            for (Procedure catalog_proc : this.catalogContext.procedures.values()) {
                if (catalog_proc.getPrefetchable()) {
                    has_prefetch = true;
                    break;
                }
            }
            if (has_prefetch) {
                tmpPlanner = new PrefetchQueryPlanner(this.catalogContext,
                                                      hstore_site.getPartitionEstimator());
            }
        }
        this.prefetchPlanner = tmpPlanner;
        this.transactionPrefetch_callback = (this.prefetchPlanner != null ? new TransactionPrefetchCallback() : null);
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
        
        if (debug.val) LOG.debug("Initializing connections");
        this.initConnections();

        for (Thread t : this.dispatcherThreads) {
            if (debug.val) LOG.debug("Starting dispatcher thread: " + t.getName());
            t.setDaemon(true);
            t.start();
        } // FOR
        
        if (debug.val) LOG.debug("Starting listener thread");
        this.listener_thread.start();
        
        // If we're at site zero, then we'll announce our instanceId
        // to everyone in the cluster
        if (this.local_site_id == 0) {
            this.initCluster();
        }
        
        if (hstore_conf.site.coordinator_sync_time) {
            syncClusterTimes();
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
     * Stop this HStoreCoordinator. This kills the ProtoRPC messenger event loop
     */
    @Override
    public synchronized void shutdown() {
        assert(this.state == ShutdownState.STARTED || this.state == ShutdownState.PREPARE_SHUTDOWN) :
            "Invalid MessengerState " + this.state;
        
        this.state = ShutdownState.SHUTDOWN;
        
        try {
            // Kill all of our dispatchers
            for (Thread thread : this.dispatcherThreads) {
                if (trace.val) LOG.trace("Stopping dispatcher thread " + thread.getName());
                thread.interrupt();
            } // FOR
            
            if (trace.val) LOG.trace("Stopping eventLoop for Site #" + this.getLocalSiteId());
            this.eventLoop.exitLoop();

            if (trace.val) LOG.trace("Stopping listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.interrupt();
            
            if (trace.val) LOG.trace("Joining on listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.join();
        } catch (InterruptedException ex) {
            // IGNORE
        } catch (Throwable ex) {
            LOG.error("Unexpected error when trying to stop messenger for Site #" + this.getLocalSiteId(), ex);
        } finally {
            if (trace.val) LOG.trace("Closing listener socket for Site #" + this.getLocalSiteId());
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
        return (this.channels[site_id]);
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
    
    public void setUnevictCallback(RpcCallback<UnevictDataResponse> callback){
    	this.unevictCallback = callback;
    }
    /**
     * Initialize all the network connections to remote
     *  
     */
    private void initConnections() {
        if (debug.val) LOG.debug("Configuring outbound network connections for Site #" + this.catalog_site.getId());
        
        // Initialize inbound channel
        Integer local_port = this.catalog_site.getMessenger_port();
        assert(local_port != null);
        if (debug.val) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.remoteService);
        this.listener.bind(local_port);

        // Find all the destinations we need to connect to
        // Make the outbound connections
        List<Pair<Integer, InetSocketAddress>> destinations = HStoreCoordinator.getRemoteCoordinators(this.catalog_site);
        
        if (destinations.isEmpty()) {
            if (debug.val) LOG.debug("There are no remote sites so we are skipping creating connections");
        }
        else {
            if (debug.val) LOG.debug("Connecting to " + destinations.size() + " remote site messengers");
            ProtoRpcChannel[] channels = null;
            InetSocketAddress arr[] = new InetSocketAddress[destinations.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = destinations.get(i).getSecond();
                if (debug.val) LOG.debug("Attemping to connect to " + arr[i]);
            } // FOR
                    
            int tries = hstore_conf.site.network_startup_retries;
            boolean success = false;
            Throwable error = null;
            while (tries-- > 0 && success == false) {
                try {
                    channels = ProtoRpcChannel.connectParallel(this.eventLoop,
                                                               arr,
                                                               hstore_conf.site.network_startup_wait);
                    success = true;
                } catch (Throwable ex) {
                    if (tries > 0) {
                        LOG.warn("Failed to connect to remote sites. Going to try again...");
                        continue;
                    }
                }
            } // WHILE
            if (success == false) {
                LOG.fatal("Site #" + this.getLocalSiteId() + " failed to connect to remote sites");
                this.listener.close();
                throw new RuntimeException(error);
            }
            assert channels.length == destinations.size();
            for (int i = 0; i < channels.length; i++) {
                Pair<Integer, InetSocketAddress> p = destinations.get(i);
                this.channels[p.getFirst()] = HStoreService.newStub(channels[i]);
            } // FOR
            
            if (debug.val) LOG.debug("Site #" + this.getLocalSiteId() + " is fully connected to all sites");
        }
    }
    
    protected void initCluster() {
        long instanceId = EstTime.currentTimeMillis();
        hstore_site.setInstanceId(instanceId);
        InitializeRequest request = InitializeRequest.newBuilder()
                                            .setSenderSite(0)
                                            .setInstanceId(instanceId)
                                            .build();
        final CountDownLatch latch = new CountDownLatch(this.num_sites-1); 
        RpcCallback<InitializeResponse> callback = new RpcCallback<InitializeResponse>() {
            @Override
            public void run(InitializeResponse parameter) {
                if (debug.val)
                    LOG.debug(String.format("Initialization Response: %s / %s",
                              HStoreThreadManager.formatSiteName(parameter.getSenderSite()),
                              parameter.getStatus()));
                latch.countDown();
            }
        };
        for (int site_id = 0; site_id < this.num_sites; site_id++) {
            if (site_id == this.local_site_id) continue;
            ProtoRpcController controller = new ProtoRpcController();
            this.channels[site_id].initialize(controller, request, callback);
        } // FOR
        
        if (latch.getCount() > 0) {
            if (debug.val)
                LOG.debug(String.format("Waiting for %s initialization responses", latch.getCount()));
            boolean finished = false;
            try {
                finished = latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                throw new ServerFaultException("Unexpected interruption", ex);
            }
            assert(finished);
        }
    }
    
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
            try {
                transactionInit_handler.remoteQueue(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void transactionWork(RpcController controller, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
            try {
                transactionWork_handler.remoteHandler(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }

        @Override
        public void transactionPrefetch(RpcController controller, TransactionPrefetchResult request, RpcCallback<TransactionPrefetchAcknowledgement> callback) {
            try {
                transactionPrefetch_handler.remoteHandler(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void transactionMap(RpcController controller, TransactionMapRequest request, RpcCallback<TransactionMapResponse> callback) {
            try {
                transactionMap_handler.remoteQueue(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void transactionReduce(RpcController controller, TransactionReduceRequest request, RpcCallback<TransactionReduceResponse> callback) {
            try {
                transactionReduce_handler.remoteQueue(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void transactionPrepare(RpcController controller, TransactionPrepareRequest request, RpcCallback<TransactionPrepareResponse> callback) {
            try {
                transactionPrepare_handler.remoteQueue(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void transactionFinish(RpcController controller, TransactionFinishRequest request, RpcCallback<TransactionFinishResponse> callback) {
            try {
                transactionFinish_handler.remoteQueue(controller, request, callback);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void transactionRedirect(RpcController controller, TransactionRedirectRequest request, RpcCallback<TransactionRedirectResponse> done) {
            // We need to create a wrapper callback so that we can get the output that
            // HStoreSite wants to send to the client and forward 
            // it back to whomever told us about this txn
            if (debug.val)
                LOG.debug(String.format("Received redirected transaction request from HStoreSite %s",
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
            ByteBuffer serializedRequest = request.getWork().asReadOnlyByteBuffer();
            TransactionRedirectResponseCallback callback = null;
            try {
                // callback = hstore_site.getObjectPools().CALLBACKS_TXN_REDIRECT_RESPONSE.borrowObject();
                callback = new TransactionRedirectResponseCallback(hstore_site);
                callback.init(local_site_id, request.getSenderSite(), done);
            } catch (Exception ex) {
                String msg = "Failed to get " + TransactionRedirectResponseCallback.class.getSimpleName();
                throw new RuntimeException(msg, ex);
            }
            
            try {
                if (transactionRedirect_dispatcher != null) {
                    transactionRedirect_dispatcher.queue(Pair.of(serializedRequest, callback));
                } else {
                    hstore_site.invocationProcess(serializedRequest, callback);
                }
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void sendData(RpcController controller, SendDataRequest request, RpcCallback<SendDataResponse> done) {
            // Take the SendDataRequest and pass it to the sendData_handler, which
            // will deserialize the embedded VoltTable and wrap it in something that we can
            // then pass down into the underlying ExecutionEngine
            try {
                sendData_handler.remoteQueue(controller, request, done);
            } catch (Throwable ex) {
                shutdownCluster(ex);
            }
        }
        
        @Override
        public void initialize(RpcController controller, InitializeRequest request, RpcCallback<InitializeResponse> done) {
            if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s [instanceId=%d]",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite()),
                          request.getInstanceId()));
            
            hstore_site.setInstanceId(request.getInstanceId());
            InitializeResponse response = InitializeResponse.newBuilder()
                                                .setSenderSite(local_site_id)
                                                .setStatus(Status.OK)
                                                .build();
            done.run(response);
        }
        
        @Override
        public void shutdownPrepare(RpcController controller, ShutdownPrepareRequest request, RpcCallback<ShutdownPrepareResponse> done) {
            String originName = HStoreThreadManager.formatSiteName(request.getSenderSite());
            
            // See if they gave us the original error. If they did, then we'll
            // try to be helpful and print it out here
            SerializableException error = null;
            if (request.hasError() && request.getError().isEmpty() == false) {
                error = SerializableException.deserializeFromBuffer(request.getError().asReadOnlyByteBuffer());
            }
            LOG.warn(String.format("Got %s from %s [hasError=%s]%s",
                     request.getClass().getSimpleName(), originName, (error != null),
                     (error != null ? "\n" + error : "")));
            
            // Tell the HStoreSite to prepare to shutdown
            HStoreCoordinator.this.hstore_site.prepareShutdown(request.hasError());
            
            ThreadUtil.sleep(5000);
            
            // Then send back the acknowledgment that we're good to go
            ShutdownPrepareResponse response = ShutdownPrepareResponse.newBuilder()
                                                   .setSenderSite(HStoreCoordinator.this.local_site_id)
                                                   .build();
            done.run(response);
            LOG.warn(String.format("Sent %s back to %s",
                    response.getClass().getSimpleName(), originName));
        }
        
        @Override
        public void shutdown(RpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> done) {
            String originName = HStoreThreadManager.formatSiteName(request.getSenderSite());
            if (debug.val)
                LOG.warn(String.format("Got %s from %s", request.getClass().getSimpleName(), originName));
            LOG.warn(String.format("Shutting down %s [status=%d]",
                     hstore_site.getSiteName(), request.getExitStatus()));

            // Then send back the acknowledgment right away
            ShutdownResponse response = ShutdownResponse.newBuilder()
                                                   .setSenderSite(HStoreCoordinator.this.local_site_id)
                                                   .build();
            done.run(response);
            HStoreCoordinator.this.hstore_site.shutdown();
            if (debug.val) LOG.debug(String.format("ForwardDispatcher Queue Idle Time: %.2fms",
                             transactionRedirect_dispatcher.getIdleTime().getTotalThinkTimeMS()));
        }
        
        @Override
        public void heartbeat(RpcController controller, HeartbeatRequest request, RpcCallback<HeartbeatResponse> done) {
            if (debug.val)
                LOG.debug(String.format("heartbeat from %d at %d^^^^^^^^^^",
                          request.getSenderSite(), local_site_id));
        	HeartbeatResponse.Builder builder = HeartbeatResponse.newBuilder()
                                                    .setSenderSite(local_site_id)
                                                    .setStatus(Status.OK);
            done.run(builder.build());            
        }

        @Override
        public void timeSync(RpcController controller, TimeSyncRequest request, RpcCallback<TimeSyncResponse> done) {
            if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
            TimeSyncResponse.Builder builder = TimeSyncResponse.newBuilder()
                                                    .setT0R(System.currentTimeMillis())
                                                    .setT0S(request.getT0S())
                                                    .setSenderSite(local_site_id);
            ThreadUtil.sleep(10);
            done.run(builder.setT1S(System.currentTimeMillis()).build());
        }

        @Override
        public void transactionDebug(RpcController controller, TransactionDebugRequest request, RpcCallback<TransactionDebugResponse> done) {
            if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
            
            Long txnId = request.getTransactionId();
            AbstractTransaction ts = hstore_site.getTransaction(txnId);
            String debug;
            Status status;
            if (ts != null) {
                debug = ts.debug();
                status = Status.OK;
            } else {
                debug = "";
                LOG.info("Found the abort!!!");
                status = Status.ABORT_UNEXPECTED;
            }
            TransactionDebugResponse response = TransactionDebugResponse.newBuilder()
                                                  .setSenderSite(local_site_id)
                                                  .setStatus(status)
                                                  .setDebug(debug)
                                                  .build();
            done.run(response);
        }

		@Override
		public void unevictData(RpcController controller,
				UnevictDataRequest request,
				RpcCallback<UnevictDataResponse> done) {
			LOG.info(String.format("Received %s from HStoreSite %s at HStoreSite %s",
                    request.getClass().getSimpleName(),
                    HStoreThreadManager.formatSiteName(request.getSenderSite()),
                    HStoreThreadManager.formatSiteName(local_site_id)));
			
			AbstractTransaction ts = hstore_site.getTransaction(request.getTransactionId());
			System.out.println(hstore_site.getInflightTxns().size());
			System.out.println(request.getTransactionId());
			assert(ts!=null);
			ts.setUnevictCallback(done);
			
			
			ts.setNewTransactionId(request.getNewTransactionId());
			int partition = request.getPartitionId();
			Table catalog_tbl = hstore_site.getCatalogContext().getTableById(request.getTableId());
			int[] block_ids = new int[request.getBlockIdsList().size()];
			for(int i = 0; i < request.getBlockIdsList().size(); i++) block_ids[i] = (int) request.getBlockIds(i);

			int [] tuple_offsets = new int[request.getTupleOffsetsList().size()];
			for(int i = 0; i < request.getTupleOffsetsList().size(); i++) tuple_offsets[i] = request.getTupleOffsets(i);

			hstore_site.getAntiCacheManager().queue(ts, partition, catalog_tbl, block_ids, tuple_offsets);
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
        if (debug.val)
            LOG.debug(String.format("%s - Sending %s to %d partitions %s",
                      ts, TransactionInitRequest.class.getSimpleName(),
                      ts.getPredictTouchedPartitions().size(), ts.getPredictTouchedPartitions()));
        assert(callback != null) :
            String.format("Trying to initialize %s with a null TransactionInitCallback", ts);
        
        ParameterSet procParams = ts.getProcedureParameters();
        FastSerializer fs = this.serializers.get();
        
        // Look at the Procedure to see whether it has prefetchable queries. If it does, 
        // then embed them in the TransactionInitRequest. We will need to generate a separate
        // request for each site that we want to execute different queries on.
        // TODO: We probably don't want to bother prefetching for txns that only touch
        //       partitions that are in its same local HStoreSite
        if (hstore_conf.site.exec_prefetch_queries && ts.getProcedure().getPrefetchable() && ts.getEstimatorState() != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Generating %s with prefetchable queries",
                          ts, TransactionInitRequest.class.getSimpleName()));
            
            // We also need to add our boy to its base partition's DependencyTracker
            // This is so that we can store the prefetch results when they come back
            DependencyTracker depTracker = hstore_site.getDependencyTracker(ts.getBasePartition());
            TransactionInitRequest.Builder[] builders = this.prefetchPlanner.plan(ts, procParams,
                                                                                  depTracker, fs);
            
            // If the PrefetchQueryPlanner returns a null array, then there is nothing
            // that we can actually prefetch, so we'll just send the normal txn init requests
            if (builders == null) {
                TransactionInitRequest.Builder builder = TransactionUtil.createTransactionInitBuilder(ts, fs); 
                this.transactionInit_handler.sendMessages(ts, builder.build(), callback, ts.getPredictTouchedPartitions());
                return;
            }
            
            TransactionCounter.PREFETCH.inc(ts.getProcedure());
            int sent_ctr = 0;
            int prefetch_ctr = 0;
            assert(builders.length == this.num_sites) :
                String.format("Expected %d %s but we got %d",
                              this.num_sites, TransactionInitRequest.class.getSimpleName(), builders.length);
            
            // Send out all of the prefetch requests first
            for (int site_id = 0; site_id < this.num_sites; site_id++) {
                // Blast out this mofo. Tell them that Rico sent you...
                if (builders[site_id] != null && builders[site_id].getPrefetchFragmentsCount() > 0) {
                    TransactionInitRequest request = builders[site_id].build();
                    if (site_id == this.local_site_id) {
                        this.transactionInit_handler.remoteHandler(null, request, null);    
                    } else {
                        ProtoRpcController controller = ts.getTransactionInitController(site_id);
                        this.channels[site_id].transactionInit(controller, request, callback);
                    }
                    prefetch_ctr += request.getPrefetchFragmentsCount();
                    sent_ctr++;
                    builders[site_id] = null;
                }
            } // FOR
            
            // Then send out the ones without prefetching. These should all be the same
            // builder so we have to make sure that we only build it once.
            TransactionInitRequest request = null;
            for (int site_id = 0; site_id < this.num_sites; site_id++) {
                if (builders[site_id] != null) {
                    if (request == null) request = builders[site_id].build();
                    if (site_id == this.local_site_id) {
                        this.transactionInit_handler.remoteHandler(null, request, null);    
                    } else {
                        ProtoRpcController controller = ts.getTransactionInitController(site_id);
                        this.channels[site_id].transactionInit(controller, request, callback);
                    }
                    sent_ctr++;
                }
            } // FOR
            assert(sent_ctr > 0) : 
                String.format("No %s available for %s", TransactionInitRequest.class.getSimpleName(), ts);
            if (debug.val)
                LOG.debug(String.format("%s - Sent %d %s with %d prefetch %s",
                          ts, sent_ctr, TransactionInitRequest.class.getSimpleName(),
                          prefetch_ctr, WorkFragment.class.getSimpleName()));
        }
        // Otherwise we will send the same TransactionInitRequest to all of the remote sites 
        else {
            TransactionInitRequest.Builder builder = TransactionUtil.createTransactionInitBuilder(ts, fs); 
            this.transactionInit_handler.sendMessages(ts, builder.build(), callback, ts.getPredictTouchedPartitions());
        }
        
        // TODO(pavlo): Add the ability to allow a partition that rejects a InitRequest to send notifications
        //              about the rejection to the other partitions that are included in the InitRequest.
    }
    
    /**
     * Send the TransactionWorkRequest to the target remote site
     * @param builders
     * @param callback
     */
    public void transactionWork(LocalTransaction ts, int site_id, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
        if (debug.val)
            LOG.debug(String.format("%s - Sending TransactionWorkRequest to remote site %d " +
                      "[numFragments=%d, txnId=%d]",
                      ts, site_id, request.getFragmentsCount(), request.getTransactionId()));
        
        assert(request.getFragmentsCount() > 0) :
            String.format("No WorkFragments for Site %d in %s", site_id, ts);
        assert(site_id != this.local_site_id) :
            String.format("Trying to send %s for %s to local site %d",
                          request.getClass().getSimpleName(), ts, site_id); 
        assert(ts.getTransactionId().longValue() == request.getTransactionId()) :
            String.format("%s is for txn #%d but the %s has txn #%d",
                          ts.getClass().getSimpleName(), ts.getTransactionId(),
                          request.getClass().getSimpleName(), request.getTransactionId());
        
        this.channels[site_id].transactionWork(ts.getTransactionWorkController(site_id), request, callback);
    }
    
    /**
     * Send the result of a prefetched query back to the txn's base partition.
     * @param ts
     * @param request
     */
    public void transactionPrefetchResult(RemoteTransaction ts, TransactionPrefetchResult request) {
        if (debug.val)
            LOG.debug(String.format("%s - Sending %s back to base partition %d",
                      ts, request.getClass().getSimpleName(),
                      ts.getBasePartition()));
        assert(request.hasResult()) :
            String.format("No WorkResults in %s for %s", request.getClass().getSimpleName(), ts);
        int site_id = catalogContext.getSiteIdForPartitionId(ts.getBasePartition());
        assert(site_id != this.local_site_id);
        
        ProtoRpcController controller = ts.getTransactionPrefetchController(request.getSourcePartition());
        this.channels[site_id].transactionPrefetch(controller,
                                                   request,
                                                   this.transactionPrefetch_callback);
    }
    
    /**
     * Notify the given partitions that this transaction is finished with them
     * <B>Note:</B> This can also be used for the "early prepare" optimization.
     * @param ts
     * @param callback
     * @param partitions
     */
    public void transactionPrepare(LocalTransaction ts, LocalPrepareCallback callback, PartitionSet partitions) {
        if (debug.val)
            LOG.debug(String.format("Notifying partitions %s that %s is preparing to commit",
                      partitions, ts));
        
        // Remove any partitions that we have notified previously *and* we have
        // already gotten a response from.
        PartitionSet receivedPartitions = callback.getReceivedPartitions(); 
        if (receivedPartitions.isEmpty() == false) {
            if (debug.val)
                LOG.debug(String.format("Removed partitions %s from %s for %s [origPartitions=%s]",
                          receivedPartitions, TransactionPrepareRequest.class.getSimpleName(),
                          ts, partitions));
            partitions = new PartitionSet(partitions);
            partitions.removeAll(receivedPartitions);
        }
        
        // FAST PATH: If all of the partitions that this txn needs are on this
        // HStoreSite, then we don't need to bother with making this request
        if (hstore_site.allLocalPartitions(partitions)) {
            hstore_site.transactionPrepare(ts, partitions, callback);
        }
        // SLOW PATH: Since we have to go over the network, we have to use our trusty ol'
        // TransactionPrepareHandler to route the request to proper sites.
        else {
            TransactionPrepareRequest request = TransactionPrepareRequest.newBuilder()
                                                            .setTransactionId(ts.getTransactionId())
                                                            .addAllPartitions(partitions)
                                                            .build();
            this.transactionPrepare_handler.sendMessages(ts, request, callback, partitions);
        }
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
    public void transactionFinish(LocalTransaction ts, Status status, LocalFinishCallback callback) {
        // Check whether we have already begun the finish process for this txn
        if (ts.shouldInvokeFinish() == false) {
            return;
        }
        
        PartitionSet partitions = ts.getPredictTouchedPartitions();
        if (debug.val)
            LOG.debug(String.format("Notifying partitions %s that %s is finished [status=%s]",
                      partitions, ts, status));
        
        // FAST PATH: If all of the partitions that this txn needs are on this
        // HStoreSite, then we don't need to bother with making this request
        if (ts.isPredictAllLocal()) {
            hstore_site.transactionFinish(ts.getTransactionId(), status, partitions);
        }
        // SLOW PATH: Since we have to go over the network, we have to use our trusty ol'
        // TransactionFinishHandler to route the request to proper sites.
        else {
            TransactionFinishRequest request = TransactionFinishRequest.newBuilder()
                                                            .setTransactionId(ts.getTransactionId())
                                                            .setStatus(status)
                                                            .addAllPartitions(partitions)
                                                            .build();
            this.transactionFinish_handler.sendMessages(ts, request, callback, partitions);
        }
    }
    
    /**
     * Forward a StoredProcedureInvocation request to a remote site for execution
     * @param serializedRequest
     * @param callback
     * @param partition
     */
    public void transactionRedirect(byte[] serializedRequest, RpcCallback<TransactionRedirectResponse> callback, int partition) {
        int dest_site_id = catalogContext.getSiteIdForPartitionId(partition);
        if (debug.val)
            LOG.debug(String.format("Redirecting transaction request to partition #%d on %s",
                      partition, HStoreThreadManager.formatSiteName(dest_site_id)));
        
        ByteString bs = ByteString.copyFrom(serializedRequest);
        TransactionRedirectRequest mr = TransactionRedirectRequest.newBuilder()
                                        .setSenderSite(this.local_site_id)
                                        .setWork(bs)
                                        .build();
        this.channels[dest_site_id].transactionRedirect(new ProtoRpcController(), mr, callback);
    }
    
    // ----------------------------------------------------------------------------
    // MapReduce METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Tell all remote partitions to start the map phase for this txn
     * @param ts
     */
    public void transactionMap(LocalTransaction ts, RpcCallback<TransactionMapResponse> callback) {
        ByteString paramBytes = null;
        try {
            ByteBuffer b = ByteBuffer.wrap(FastSerializer.serialize(ts.getProcedureParameters()));
            paramBytes = ByteString.copyFrom(b.array()); 
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error when serializing StoredProcedureInvocation", ex);
        }
        
        TransactionMapRequest request = TransactionMapRequest.newBuilder()
                                                     .setTransactionId(ts.getTransactionId())
                                                     .setClientHandle(ts.getClientHandle())
                                                     .setBasePartition(ts.getBasePartition())
                                                     .setProcedureId(ts.getProcedure().getId())
                                                     .setParams(paramBytes)
                                                     .build();
        
        PartitionSet partitions = ts.getPredictTouchedPartitions();
        if (debug.val){
            LOG.debug(String.format("Notifying partitions %s that %s is in Map Phase", partitions, ts));
            if (trace.val) LOG.trace("<HStoreCoordinator.TransactionMap> is executing to sendMessages to all partitions");
        }
        this.transactionMap_handler.sendMessages(ts, request, callback, partitions);
    }
    
    /**
     * Tell all remote partitions to start the reduce phase for this txn
     * @param ts
     */
    public void transactionReduce(LocalTransaction ts, RpcCallback<TransactionReduceResponse> callback) {
        // We only need to send over the transaction. The remote side should 
        // already have all the information that it needs about this txn
        TransactionReduceRequest request = TransactionReduceRequest.newBuilder()
                                                     .setTransactionId(ts.getTransactionId())
                                                     .build();
        
        PartitionSet partitions = ts.getPredictTouchedPartitions();
        if (debug.val) {
            LOG.debug(String.format("Notifying partitions %s that %s is in Reduce Phase", partitions, ts));
            if (trace.val) LOG.trace("<HStoreCoordinator.TransactionReduce> is executing to sendMessages to all partitions");
        }
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
        
        
        Long txn_id = ts.getTransactionId();
        Set<Integer> fake_responses = null;
        for (Site remote_site : this.catalogContext.sites.values()) {
            int dest_site_id = remote_site.getId();
            if (debug.val)
                LOG.debug("Dest_site_id: " + dest_site_id + "  Local_site_id: " + this.local_site_id);
            if (dest_site_id == this.local_site_id) {
                // If there is no data for any partition at this remote HStoreSite, then we will fake a response
                // message to the callback and tell them that everything is ok
                if (fake_responses == null) fake_responses = new HashSet<Integer>();
                fake_responses.add(dest_site_id);
                if (debug.val)
                    LOG.debug("Did not send data to " + remote_site + ". Will send a fake response instead");
                continue;
            }

            SendDataRequest.Builder builder = SendDataRequest.newBuilder()
                                                .setTransactionId(txn_id.longValue())
                                                .setSenderSite(local_site_id);
            // Loop through and get all the data for this site
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
                    if (debug.val)
                        LOG.debug(String.format("%s - Outbound data for partition #%d " +
                        		  "[RowCount=%d / MD5=%s / Length=%d]",
                                  ts, catalog_part.getId(),
                                  vt.getRowCount(), StringUtil.md5sum(bytes), bytes.length));
                } catch (Exception ex) {
                    String msg = String.format("Unexpected error when serializing %s data for partition %d",
                                               ts, catalog_part.getId());
                    throw new ServerFaultException(msg, ex, ts.getTransactionId());
                }
                if (trace.val)
                    LOG.trace("Constructing Dependency for " + catalog_part);
                builder.addDepId(catalog_part.getId())
                       .addData(bs);
            } // FOR n partitions in remote_site
            
            if (builder.getDataCount() > 0) {
                if (debug.val)
                    LOG.debug(String.format("%s - Sending data to %d partitions at %s for %s",
                              ts, builder.getDataCount(), remote_site, ts));
                this.channels[dest_site_id].sendData(new ProtoRpcController(), builder.build(), callback);
            }
        } // FOR n sites in this catalog
                
        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            VoltTable vt = data.get(Integer.valueOf(partition));
            if (vt == null) {
                LOG.warn("No data in " + ts + " for partition " + partition);
                continue;
            }
            if (debug.val) LOG.debug(String.format("Storing VoltTable directly at local partition %d for %s", partition, ts));
            ts.storeData(partition, vt);
        } // FOR
        
        if (fake_responses != null) {
            if (debug.val) LOG.debug(String.format("Sending fake responses for %s for partitions %s", ts, fake_responses));
            for (int dest_site_id : fake_responses) {
                SendDataResponse.Builder builder = SendDataResponse.newBuilder()
                                                           .setTransactionId(txn_id.longValue())
                                                           .setStatus(Hstoreservice.Status.OK)
                                                           .setSenderSite(dest_site_id);
                callback.run(builder.build());
            } // FOR
        }
    }
    
    public Map<Integer, String> transactionDebug(Long txn_id) {
        assert(txn_id != null);
        
        final CountDownLatch latch = new CountDownLatch(this.num_sites-1);
        final Map<Integer, String> responses = new TreeMap<Integer, String>();
        
        RpcCallback<TransactionDebugResponse> callback = new RpcCallback<TransactionDebugResponse>() {
            @Override
            public void run(TransactionDebugResponse response) {
                if (response.getStatus() == Status.OK) {
                    int site_id = response.getSenderSite();
                    assert(responses.containsKey(site_id) == false);
                    responses.put(site_id, response.getDebug());
                }
                latch.countDown();
            }
        };
        
        TransactionDebugRequest request = TransactionDebugRequest.newBuilder()
                                           .setSenderSite(this.local_site_id)
                                           .setTransactionId(txn_id)
                                           .build();
        for (int site_id = 0; site_id < this.num_sites; site_id++) {
            if (site_id == this.local_site_id) continue;
            this.channels[site_id].transactionDebug(new ProtoRpcController(), request, callback);
            if (trace.val)
                LOG.trace(String.format("Sent %s to %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(site_id)));
        } // FOR
        
        // Added our own debug info
        AbstractTransaction ts = this.hstore_site.getTransaction(txn_id);
        if (ts != null) {
            responses.put(this.local_site_id, ts.debug());
        }

        // Then wait for all of our responses
        boolean success = false;
        try {
            success = latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            // nothing
        }
        if (success == false) {
            LOG.warn(String.format("Failed to recieve debug responses from %d remote HStoreSites",
                     this.num_sites-1));
        }
        return (responses);
    }
    
    // ----------------------------------------------------------------------------
    // HEARTBEAT METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Send a heartbeat notification message to all the other sites in the cluster.
     */
    public void sendHeartbeat() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                                    .setSenderSite(this.local_site_id)
                                    .setLastTransactionId(-1) // FIXME
                                    .build();
        for (int site_id = 0; site_id < this.num_sites; site_id++) {
            if (site_id == this.local_site_id) continue;
            if (this.isShuttingDown()) break;
            try {
                this.channels[site_id].heartbeat(new ProtoRpcController(), request, this.heartbeatCallback);
                if (trace.val)
                    LOG.trace(String.format("Sent %s to %s",
                              request.getClass().getSimpleName(),
                              HStoreThreadManager.formatSiteName(site_id)));
            } catch (RuntimeException ex) {
                // Silently ignore these errors...
            }
        } // FOR
    }

    // ----------------------------------------------------------------------------
    // UNEVICT DATA
    // ----------------------------------------------------------------------------
    
    /**
     * Send a message to a remote site to unevict data
     * @param tuple_offsets 
     * @param block_ids 
     * @param catalog_tbl 
     * @param partition_id 
     * @param txn 
     * @return 
     */
    public void sendUnevictDataMessage(int remote_site_id, LocalTransaction txn, int partition_id, Table catalog_tbl, int[] block_ids, int[] tuple_offsets) {
    	 Builder builder = UnevictDataRequest.newBuilder()
                                    .setSenderSite(this.local_site_id)
                                    .setTransactionId(txn.getOldTransactionId())
                                    .setNewTransactionId(txn.getTransactionId())
                                    .setPartitionId(partition_id)
                                    .setTableId(catalog_tbl.getRelativeIndex());
                          
    	 for (int i = 0; i< block_ids.length; i++){
		builder = builder.addBlockIds(block_ids[i]);
    	 }
    	 for (int i=0; i< tuple_offsets.length; i++){
		builder = builder.addTupleOffsets(tuple_offsets[i]);
    	 }
    	 UnevictDataRequest request = builder.build();            
            try {
				this.channels[remote_site_id].unevictData(new ProtoRpcController(), request, this.unevictCallback);
                if (trace.val) {
                    LOG.trace(String.format("Sent unevict message request to remote hstore site %d from base site %d",
                              remote_site_id, this.hstore_site.getSiteId()));
                    LOG.trace(String.format("Sent %s to %s",
                              request.getClass().getSimpleName(),
                              HStoreThreadManager.formatSiteName(remote_site_id)));
                }
            } catch (RuntimeException ex) {
                // Silently ignore these errors...
            	ex.printStackTrace();
            }

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
        if (this.num_sites == 1) return;
        
        final CountDownLatch latch = new CountDownLatch(this.num_sites-1);
        final Map<Integer, Integer> time_deltas = new HashMap<Integer, Integer>();
        
        RpcCallback<TimeSyncResponse> callback = new RpcCallback<TimeSyncResponse>() {
            @Override
            public void run(TimeSyncResponse request) {
                long t1_r = System.currentTimeMillis();
                int dt = (int)((request.getT1S() + request.getT0R()) - (t1_r + request.getT0S())) / 2;
                time_deltas.put(request.getSenderSite(), dt);
                latch.countDown();
            }
        };
        
        // Send out TimeSync request 
        for (int site_id = 0; site_id < this.num_sites; site_id++) {
            if (site_id == this.local_site_id) continue;
            ProtoRpcController controller = new ProtoRpcController();
            TimeSyncRequest request = TimeSyncRequest.newBuilder()
                                            .setSenderSite(this.local_site_id)
                                            .setT0S(System.currentTimeMillis())
                                            .build();
            this.channels[site_id].timeSync(controller, request, callback);
            if (trace.val) LOG.trace("Sent TIMESYNC to " + HStoreThreadManager.formatSiteName(site_id));
        } // FOR
        
        if (trace.val) LOG.trace("Sent out all TIMESYNC requests!");
        boolean success = false;
        try {
            success = latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            // nothing
        }
        if (success == false) {
            LOG.warn(String.format("Failed to recieve time synchronization responses " +
            		 "from %d remote sites", this.num_sites-1));
        } else if (trace.val) LOG.trace("Received all TIMESYNC responses!");
        
        // Then do the time calculation
        long max_dt = 0L;
        int culprit = this.local_site_id;
        for (Entry<Integer, Integer> e : time_deltas.entrySet()) {
            if (debug.val)
                LOG.debug(String.format("Time delta to site %s is %d ms",
                         HStoreThreadManager.formatSiteName(e.getKey()), e.getValue()));
            if (e.getValue() > max_dt) {
                max_dt = e.getValue();
                culprit = e.getKey();
            }
        } // FOR
        this.hstore_site.setTransactionIdManagerTimeDelta(max_dt);
        if (debug.val)
            LOG.debug(String.format("Setting time delta to %d ms [culprit=%s]",
                      max_dt, HStoreThreadManager.formatSiteName(culprit)));
    }
    
    // ----------------------------------------------------------------------------
    // SHUTDOWN METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Take down the cluster. This is a non-blocking call. It will return right away
     * @param error
     */
    public void shutdownCluster(final Throwable error) {
        if (debug.val) 
            LOG.debug(String.format("Invoking non-blocking shutdown protocol [hasError=%s]",
                      error!=null), error);
        
        // Make this a thread so that we don't block and can continue cleaning up other things
        Runnable shutdownRunnable = new Runnable() {
            @Override
            public void run() {
                LOG.debug("Shutting down cluster " + (error != null ? " - " + error : ""));
                try {
                    HStoreCoordinator.this.shutdownClusterBlocking(error); // Never returns!
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        };
        hstore_site.getThreadManager().scheduleWork(shutdownRunnable, 2500, TimeUnit.MILLISECONDS);
        return;
    }
    
    /**
     * Tell all of the other sites to shutdown and then knock ourselves out...
     * This is a non-blocking call.
     */
    public void shutdownCluster() {
        this.shutdownCluster(null);
    }
    
    protected void prepareShutdownCluster(final Throwable error) throws Exception {
        final CountDownLatch latch = new CountDownLatch(this.num_sites-1);
        
        if (this.num_sites > 1) {
            RpcCallback<ShutdownPrepareResponse> callback = new ShutdownPrepareCallback(this.num_sites, latch);
            ShutdownPrepareRequest.Builder builder = ShutdownPrepareRequest.newBuilder()
                                                        .setSenderSite(this.catalog_site.getId());
            // Pack the error into a SerializableException
            if (error != null) {
                SerializableException sError = new SerializableException(error);
                ByteBuffer buffer = sError.serializeToBuffer();
                buffer.rewind();
                builder.setError(ByteString.copyFrom(buffer));
                if (debug.val)
                    LOG.debug("Serializing error message in shutdown request");
            }
            ShutdownPrepareRequest request = builder.build();
            
            if (debug.val)
                LOG.debug(String.format("Sending %s to %d remote sites",
                          request.getClass().getSimpleName(), this.num_sites-1));
            for (int site_id = 0; site_id < this.num_sites; site_id++) {
                if (site_id == this.local_site_id) continue;
                
                if (this.channels[site_id] == null) {
                    LOG.error(String.format("Trying to send %s to %s before the connection was established",
                              request.getClass().getSimpleName(),
                              HStoreThreadManager.formatSiteName(site_id)));
                } else {
                    this.channels[site_id].shutdownPrepare(new ProtoRpcController(), request, callback);
                    if (trace.val)
                        LOG.trace(String.format("Sent %s to %s",
                                  request.getClass().getSimpleName(),
                                  HStoreThreadManager.formatSiteName(site_id)));
                }
            } // FOR
        }
        
        // Tell ourselves to get ready
        this.hstore_site.prepareShutdown(error != null);
        
        // Block until the latch releases us
        if (this.num_sites > 1) {
            LOG.info(String.format("Waiting for %d sites to finish shutting down", latch.getCount()));
            boolean result = latch.await(10, TimeUnit.SECONDS);
            if (result == false) {
                LOG.warn("Failed to recieve all shutdown responses");
            }
        }
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
        
        if (error != null) {
            LOG.warn("Shutting down cluster with " + error.getClass().getSimpleName(), error);
        } else {
            LOG.warn("Shutting down cluster");
        }

        final int exit_status = (error == null ? 0 : 1);
        
        try {
            // Tell everyone that we're getting ready to stop the party
            this.prepareShutdownCluster(error);
            
            // Now send the final shutdown request
            if (this.num_sites > 1) {
                ThreadUtil.sleep(5000); // XXX
                LOG.info(String.format("Sending final shutdown message to %d remote sites", this.num_sites-1));
                RpcCallback<ShutdownResponse> callback = new RpcCallback<ShutdownResponse>() {
                    @Override
                    public void run(ShutdownResponse parameter) {
                        // Nothing to do...
                    }
                };
                ShutdownRequest request = ShutdownRequest.newBuilder()
                                                            .setSenderSite(this.catalog_site.getId())
                                                            .setExitStatus(exit_status)
                                                            .build();
                
                if (debug.val)
                    LOG.debug(String.format("Sending %s to %d remote sites",
                              request.getClass().getSimpleName(), this.num_sites));
                for (int site_id = 0; site_id < this.num_sites; site_id++) {
                    if (site_id == this.local_site_id) continue;
                    this.channels[site_id].shutdown(new ProtoRpcController(), request, callback);
                    if (debug.val)
                        LOG.debug(String.format("Sent %s to %s",
                                  request.getClass().getSimpleName(),
                                  HStoreThreadManager.formatSiteName(site_id)));
                } // FOR
                
                ThreadUtil.sleep(2000);
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            // IGNORE
        } finally {
            LOG.info(String.format("Shutting down [site=%d / exitCode=%d]",
                     this.catalog_site.getId(), exit_status));
            if (error != null) {
                LOG.fatal("A fatal error caused this shutdown", error);
            }
            this.hstore_site.shutdown();
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
                    if (debug.val)
                        LOG.debug(String.format("Creating RpcChannel to %s for site %s",
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
