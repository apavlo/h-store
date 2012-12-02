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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
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
import edu.brown.hstore.callbacks.ShutdownPrepareCallback;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionPrefetchCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.hstore.callbacks.TransactionPrepareWrapperCallback;
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
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.hstore.util.PrefetchQueryPlanner;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.interfaces.Loggable;
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
public class HStoreCoordinator implements Shutdownable, Loggable {
    private static final Logger LOG = Logger.getLogger(HStoreCoordinator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private static boolean d;
    private static boolean t;
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
        d = debug.get();
        t = trace.get();
    }

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
    private final TransactionPrefetchCallback transactionPrefetch_callback;
    
    private final TransactionInitHandler transactionInit_handler;
    private final TransactionWorkHandler transactionWork_handler;
    private final TransactionPrefetchHandler transactionPrefetch_handler;
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
    
    private final PrefetchQueryPlanner queryPrefetchPlanner;

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
            if (t) LOG.trace("Messenger Thread for Site #" + catalog_site.getId() + " has stopped!");
        }
    }
    
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
        
        if (d) LOG.debug("Local Partitions for Site #" + hstore_site.getSiteId() + ": " + hstore_site.getLocalPartitionIds());

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
        
        // Initialized QueryPrefetchPlanner if we're allowed to execute
        // prefetch queries and we actually have some in the catalog 
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
        this.queryPrefetchPlanner = tmpPlanner;
        this.transactionPrefetch_callback = (this.queryPrefetchPlanner != null ? new TransactionPrefetchCallback() : null);
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
        
        if (d) LOG.debug("Initializing connections");
        this.initConnections();

        for (Thread t : this.dispatcherThreads) {
            if (d) LOG.debug("Starting dispatcher thread: " + t.getName());
            t.setDaemon(true);
            t.start();
        } // FOR
        
        if (d) LOG.debug("Starting listener thread");
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
                if (t) LOG.trace("Stopping dispatcher thread " + thread.getName());
                thread.interrupt();
            } // FOR
            
            if (t) LOG.trace("Stopping eventLoop for Site #" + this.getLocalSiteId());
            this.eventLoop.exitLoop();

            if (t) LOG.trace("Stopping listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.interrupt();
            
            if (t) LOG.trace("Joining on listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.join();
        } catch (InterruptedException ex) {
            // IGNORE
        } catch (Throwable ex) {
            LOG.error("Unexpected error when trying to stop messenger for Site #" + this.getLocalSiteId(), ex);
        } finally {
            if (t) LOG.trace("Closing listener socket for Site #" + this.getLocalSiteId());
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
    
    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
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
    
//    private int getNumLocalPartitions(PartitionSet partitions) {
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
        if (d) LOG.debug("Configuring outbound network connections for Site #" + this.catalog_site.getId());
        
        // Initialize inbound channel
        Integer local_port = this.catalog_site.getMessenger_port();
        assert(local_port != null);
        if (d) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.remoteService);
        this.listener.bind(local_port);

        // Find all the destinations we need to connect to
        // Make the outbound connections
        List<Pair<Integer, InetSocketAddress>> destinations = HStoreCoordinator.getRemoteCoordinators(this.catalog_site);
        
        if (destinations.isEmpty()) {
            if (d) LOG.debug("There are no remote sites so we are skipping creating connections");
        }
        else {
            if (d) LOG.debug("Connecting to " + destinations.size() + " remote site messengers");
            ProtoRpcChannel[] channels = null;
            InetSocketAddress arr[] = new InetSocketAddress[destinations.size()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = destinations.get(i).getSecond();
                if (d) LOG.debug("Attemping to connect to " + arr[i]);
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
            
            if (d) LOG.debug("Site #" + this.getLocalSiteId() + " is fully connected to all sites");
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
                if (d) LOG.debug(String.format("Initialization Response: %s / %s",
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
        
        if (d)
            LOG.debug(String.format("Waiting for %s initialization responses", latch.getCount()));
        boolean finished = false;
        try {
            finished = latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new ServerFaultException("Unexpected interruption", ex);
        }
        assert(finished);
    }
    
    // ----------------------------------------------------------------------------
    // MESSAGE ROUTERS
    // ----------------------------------------------------------------------------
    
    // TransactionFinish
    
    // Shutdown
//    private final MessageRouter<ShutdownRequest, ShutdownResponse> router_shutdown = new MessageRouter<ShutdownRequest, ShutdownResponse>() {
//        protected void sendLocal(long txn_id, ShutdownRequest request, PartitionSet partitions, RpcCallback<ShutdownResponse> callback) {
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
            if (d) LOG.debug(String.format("Received redirected transaction request from HStoreSite %s",
                             HStoreThreadManager.formatSiteName(request.getSenderSite())));
            ByteBuffer serializedRequest = request.getWork().asReadOnlyByteBuffer();
            TransactionRedirectResponseCallback callback = null;
            try {
                callback = hstore_site.getObjectPools().CALLBACKS_TXN_REDIRECT_RESPONSE.borrowObject();
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
            if (d) LOG.debug(String.format("Received %s from HStoreSite %s [instanceId=%d]",
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
                     (error != null ? " - " + error : "")));
            
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
            if (d) LOG.warn(String.format("Got %s from %s", request.getClass().getSimpleName(), originName));
            LOG.warn(String.format("Shutting down %s [status=%d]",
                     hstore_site.getSiteName(), request.getExitStatus()));

            // Then send back the acknowledgment right away
            ShutdownResponse response = ShutdownResponse.newBuilder()
                                                   .setSenderSite(HStoreCoordinator.this.local_site_id)
                                                   .build();
            done.run(response);
            HStoreCoordinator.this.hstore_site.shutdown();
            if (d) LOG.debug(String.format("ForwardDispatcher Queue Idle Time: %.2fms",
                             transactionRedirect_dispatcher.getIdleTime().getTotalThinkTimeMS()));
        }

        @Override
        public void timeSync(RpcController controller, TimeSyncRequest request, RpcCallback<TimeSyncResponse> done) {
            if (d) LOG.debug(String.format("Received %s from HStoreSite %s",
                             request.getClass().getSimpleName(),
                             HStoreThreadManager.formatSiteName(request.getSenderSite())));
            
            TimeSyncResponse response = TimeSyncResponse.newBuilder()
                                                    .setT0R(System.currentTimeMillis())
                                                    .setSenderSite(local_site_id)
                                                    .setT0S(request.getT0S())
                                                    .setT1S(System.currentTimeMillis())
                                                    .build();
            done.run(response);
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
        if (d) LOG.debug(String.format("%s - Sending TransactionInitRequest to %d partitions %s",
                         ts, ts.getPredictTouchedPartitions().size(), ts.getPredictTouchedPartitions()));
        assert(callback != null) :
            String.format("Trying to initialize %s with a null TransactionInitCallback", ts);
        
        // Look at the Procedure to see whether it has prefetchable queries. If it does, 
        // then embed them in the TransactionInitRequest. We will need to generate a separate
        // request for each site that we want to execute different queries on.
        // TODO: We probably don't want to bother prefetching for txns that only touch
        //       partitions that are in its same local HStoreSite
//        if (ts.getProcedure().getPrefetchable()) {
//        	TransactionCounter.PREFETCH_LOCAL.inc(ts.getProcedure());
//        }
//        if (ts.getEstimatorState() != null) {
//        	TransactionCounter.PREFETCH_REMOTE.inc(ts.getProcedure());
//        }
        if (ts.getProcedure().getPrefetchable() && ts.getEstimatorState() != null && hstore_conf.site.exec_prefetch_queries) {
            if (d) LOG.debug(String.format("%s - Generating TransactionInitRequests with prefetchable queries", ts));
            
            // Make sure that we initialize our internal PrefetchState for this txn
            ts.initializePrefetch();
            TransactionCounter.PREFETCH_LOCAL.inc(ts.getProcedure());
            
            TransactionInitRequest[] requests = this.queryPrefetchPlanner.generateWorkFragments(ts);
            if (requests == null) {
                sendDefaultTransactionInitRequests(ts, callback);
                return;
            }
            int sent_ctr = 0;
            int prefetch_ctr = 0;
            assert(requests.length == this.num_sites) :
                String.format("Expected %d TransactionInitRequests but we got %d", this.num_sites, requests.length); 
            for (int site_id = 0; site_id < this.num_sites; site_id++) {
                if (requests[site_id] == null) continue;
                
                if (site_id == this.local_site_id) {
                    this.transactionInit_handler.sendLocal(ts.getTransactionId(),
                                                           requests[site_id],
                                                           ts.getPredictTouchedPartitions(),
                                                           callback);
                }
                else {
                    ProtoRpcController controller = ts.getTransactionInitController(site_id);
                    this.channels[site_id].transactionInit(controller,
                                                           requests[site_id],
                                                           callback);
                }
                
                sent_ctr++;
                prefetch_ctr += requests[site_id].getPrefetchFragmentsCount();
            } // FOR
            assert(sent_ctr > 0) : "No TransactionInitRequests available for " + ts;
            if (d) LOG.debug(String.format("%s - Sent %d TransactionInitRequests with %d prefetch WorkFragments",
                             ts, sent_ctr, prefetch_ctr));
            
        }
        // Otherwise we will send the same TransactionInitRequest to all of the remote sites 
        else {
            sendDefaultTransactionInitRequests(ts, callback);
        }
        
        // TODO(pavlo): Add the ability to allow a partition that rejects a InitRequest to send notifications
        //              about the rejection to the other partitions that are included in the InitRequest.
    }
    
    private void sendDefaultTransactionInitRequests(LocalTransaction ts, RpcCallback<TransactionInitResponse> callback) {
        TransactionInitRequest request = TransactionInitRequest.newBuilder()
                .setTransactionId(ts.getTransactionId())
                .setProcedureId(ts.getProcedure().getId())
                .setBasePartition(ts.getBasePartition())
                .addAllPartitions(ts.getPredictTouchedPartitions())
                .build();
        this.transactionInit_handler.sendMessages(ts, request, callback, ts.getPredictTouchedPartitions());
    }
    
    /**
     * Send the TransactionWorkRequest to the target remote site
     * @param builders
     * @param callback
     */
    public void transactionWork(LocalTransaction ts, int site_id, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> callback) {
        if (d) LOG.debug(String.format("%s - Sending TransactionWorkRequest to remote site %d " +
        		         "[numFragments=%d]",
                         ts, site_id, request.getFragmentsCount()));
        
        assert(request.getFragmentsCount() > 0) :
            String.format("No WorkFragments for Site %d in %s", site_id, ts);
        
        // We should never get work for our local partitions
        assert(site_id != this.local_site_id);
        assert(ts.getTransactionId().longValue() == request.getTransactionId()) :
            String.format("%s is for txn #%d but the %s has txn #%d",
                          ts.getClass().getSimpleName(), ts.getTransactionId(),
                          request.getClass().getSimpleName(), request.getTransactionId());
        
        this.channels[site_id].transactionWork(ts.getTransactionWorkController(site_id), request, callback);
    }
    
    public void transactionPrefetchResult(RemoteTransaction ts, TransactionPrefetchResult request) {
        if (d) LOG.debug(String.format("%s - Sending %s back to base partition %d",
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
    public void transactionPrepare(LocalTransaction ts, TransactionPrepareCallback callback, PartitionSet partitions) {
        if (d) LOG.debug(String.format("Notifying partitions %s that %s is preparing to commit", partitions, ts));
        
        // FAST PATH: If all of the partitions that this txn needs are on this
        // HStoreSite, then we don't need to bother with making this request
        if (hstore_site.isLocalPartitions(partitions)) {
            TransactionPrepareWrapperCallback wrapper = ts.getPrepareWrapperCallback();
            if (wrapper.isInitialized()) wrapper.finish();
            wrapper.init(ts, partitions, callback);
            hstore_site.transactionPrepare(ts, partitions);
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
    public void transactionFinish(LocalTransaction ts, Status status, TransactionFinishCallback callback) {
        PartitionSet partitions = ts.getPredictTouchedPartitions();
        if (d) LOG.debug(String.format("Notifying partitions %s that %s is finished [status=%s]",
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
        if (d) LOG.debug(String.format("Redirecting transaction request to partition #%d on %s",
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
        if (d) LOG.debug(String.format("Notifying partitions %s that %s is in Map Phase", partitions, ts));
        if (d) LOG.debug("<HStoreCoordinator.TransactionMap> is executing to sendMessages to all partitions\n");
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
        if (d) LOG.debug(String.format("Notifying partitions %s that %s is in Reduce Phase", partitions, ts));
        if (d) LOG.debug("<HStoreCoordinator.TransactionReduce> is executing to sendMessages to all partitions\n");
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
            if (d) LOG.debug("Dest_site_id: " + dest_site_id + "  Local_site_id: " + this.local_site_id);
            if (dest_site_id == this.local_site_id) {
                // If there is no data for any partition at this remote HStoreSite, then we will fake a response
                // message to the callback and tell them that everything is ok
                if (fake_responses == null) fake_responses = new HashSet<Integer>();
                fake_responses.add(dest_site_id);
                if (d) LOG.debug("Did not send data to " + remote_site + ". Will send a fake response instead");
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
                    if (d) LOG.debug(String.format("Outbound data for Partition #%d: RowCount=%d / MD5=%s / Length=%d",
                                     catalog_part.getId(), vt.getRowCount(), StringUtil.md5sum(bytes), bytes.length));
                } catch (Exception ex) {
                    String msg = String.format("Unexpected error when serializing %s data for partition %d",
                                               ts, catalog_part.getId());
                    throw new ServerFaultException(msg, ex, ts.getTransactionId());
                }
                if (d) LOG.debug("Constructing Dependency for " + catalog_part);
                builder.addDepId(catalog_part.getId())
                       .addData(bs);
            } // FOR n partitions in remote_site
            
            if (builder.getDataCount() > 0) {
                if (d) LOG.debug(String.format("Sending data to %d partitions at %s for %s",
                                 builder.getDataCount(), remote_site, ts));
                this.channels[dest_site_id].sendData(new ProtoRpcController(), builder.build(), callback);
            }
        } // FOR n sites in this catalog
                
        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            VoltTable vt = data.get(Integer.valueOf(partition));
            if (vt == null) {
                LOG.warn("No data in " + ts + " for partition " + partition);
                continue;
            }
            if (d) LOG.debug(String.format("Storing VoltTable directly at local partition %d for %s", partition, ts));
            ts.storeData(partition, vt);
        } // FOR
        
        if (fake_responses != null) {
            if (d) LOG.debug(String.format("Sending fake responses for %s for partitions %s", ts, fake_responses));
            for (int dest_site_id : fake_responses) {
                SendDataResponse.Builder builder = SendDataResponse.newBuilder()
                                                           .setTransactionId(txn_id.longValue())
                                                           .setStatus(Hstoreservice.Status.OK)
                                                           .setSenderSite(dest_site_id);
                callback.run(builder.build());
            } // FOR
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
            TimeSyncRequest request = TimeSyncRequest.newBuilder()
                                            .setSenderSite(this.local_site_id)
                                            .setT0S(System.currentTimeMillis())
                                            .build();
            this.channels[site_id].timeSync(new ProtoRpcController(), request, callback);
            if (t) LOG.trace("Sent TIMESYNC to " + HStoreThreadManager.formatSiteName(site_id));
        } // FOR
        
        if (t) LOG.trace("Sent out all TIMESYNC requests!");
        boolean success = false;
        try {
            success = latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            // nothing
        }
        if (success == false) {
            LOG.warn(String.format("Failed to recieve time synchronization responses from %d remote HStoreSites", this.num_sites-1));
        } else if (t) LOG.trace("Received all TIMESYNC responses!");
        
        // Then do the time calculation
        long max_dt = 0L;
        int culprit = this.local_site_id;
        for (Entry<Integer, Integer> e : time_deltas.entrySet()) {
            if (d) LOG.debug(String.format("Time delta to HStoreSite %d is %d ms", e.getKey(), e.getValue()));
            if (e.getValue() > max_dt) {
                max_dt = e.getValue();
                culprit = e.getKey();
            }
        }
        this.hstore_site.setTransactionIdManagerTimeDelta(max_dt);
        if (d) {
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
     */
    public void shutdownCluster(final Throwable error) {
        if (d) 
            LOG.debug(String.format("Invoking non-blocking shutdown protocol [hasError=%s]", error!=null), error);
        
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
            LOG.warn("Shutting down cluster with " + error.getClass().getSimpleName());
        } else {
            LOG.warn("Shutting down cluster");
        }

        final int exit_status = (error == null ? 0 : 1);
        final CountDownLatch latch = new CountDownLatch(this.num_sites-1);
        
        try {
            if (this.num_sites > 1) {
                RpcCallback<ShutdownPrepareResponse> callback = new ShutdownPrepareCallback(this.num_sites, latch);
                ShutdownPrepareRequest.Builder builder = ShutdownPrepareRequest.newBuilder()
                                                            .setSenderSite(this.catalog_site.getId());
                // Pack the error into a SerializableException
                if (error != null) {
                    SerializableException sError = new SerializableException(error);
                    ByteBuffer buffer = sError.serializeToBuffer();
                    builder.setError(ByteString.copyFrom(buffer));
                    LOG.info("Serializing error message in shutdown request");
                }
                ShutdownPrepareRequest request = builder.build();
                
                if (d) LOG.debug(String.format("Sending %s to %d remote sites",
                                 request.getClass().getSimpleName(), this.num_sites-1));
                for (int site_id = 0; site_id < this.num_sites; site_id++) {
                    if (site_id == this.local_site_id) continue;
                    this.channels[site_id].shutdownPrepare(new ProtoRpcController(), request, callback);
                    if (t) LOG.trace(String.format("Sent %s to %s",
                                     request.getClass().getSimpleName(),
                                     HStoreThreadManager.formatSiteName(site_id)));
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
                
                if (d) LOG.debug(String.format("Sending %s to %d remote sites",
                                 request.getClass().getSimpleName(), this.num_sites));
                for (int site_id = 0; site_id < this.num_sites; site_id++) {
                    if (site_id == this.local_site_id) continue;
                    this.channels[site_id].shutdown(new ProtoRpcController(), request, callback);
                    if (d) LOG.debug(String.format("Sent %s to %s",
                                     request.getClass().getSimpleName(),
                                     HStoreThreadManager.formatSiteName(site_id)));
                } // FOR
                
                ThreadUtil.sleep(2000);
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            // IGNORE
        } finally {
            LOG.info(String.format("Shutting down [site=%d, status=%d]", catalog_site.getId(), exit_status));
            this.hstore_site.shutdown();
            if (error != null) {
                LOG.fatal("A fatal error caused this shutdown", error);
            }
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
                    if (d) LOG.debug(String.format("Creating RpcChannel to %s for site %s",
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