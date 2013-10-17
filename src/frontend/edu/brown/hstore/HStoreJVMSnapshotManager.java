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
import org.voltdb.benchmark.tpcc.procedures.neworder;
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
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Jvmsnapshots.HStoreJVMSnapshots;
import edu.brown.hstore.Jvmsnapshots.TransactionRequest;
import edu.brown.hstore.Jvmsnapshots.TransactionResponse;
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

public class HStoreJVMSnapshotManager {
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
    private final int local_site_id;
    
    private boolean parent;
    private boolean refresh;
    
    // for parent
    private HStoreJVMSnapshots channel;
    
    // for child snapshots
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final HStoreJVMSnapshots jvmSnapshots;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    
    /**
     * Special observable that is invoked when this HStoreCoordinator is on-line
     * and ready to communicating with other nodes in the cluster.
     */
    private final EventObservable<HStoreJVMSnapshotManager> ready_observable = new EventObservable<HStoreJVMSnapshotManager>();
    
    // ----------------------------------------------------------------------------
    // HANDLERS
    // ----------------------------------------------------------------------------
    
    
    // ----------------------------------------------------------------------------
    // MESSENGER LISTENER THREAD
    // ----------------------------------------------------------------------------
    
    private class SnapshotsListener implements Runnable {
        @Override
        public void run() {
            hstore_site.getThreadManager().registerProcessingThread();
            
            Throwable error = null;
            try {
            	HStoreJVMSnapshotManager.this.eventLoop.run();
            } catch (Throwable ex) {
                error = ex;
            }
            
            if (trace.val) LOG.trace("SnapshotsListener Thread for Site #" + catalog_site.getId() + " has stopped!");
        }
    }

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Constructor
     * @param hstore_site
     */
    public HStoreJVMSnapshotManager(HStoreSite hstore_site, boolean parent) {
        this.hstore_site = hstore_site;
        this.hstore_conf = this.hstore_site.getHStoreConf();
        this.catalogContext = this.hstore_site.getCatalogContext();
        this.catalog_site = this.hstore_site.getSite();
        this.local_site_id = this.catalog_site.getId();
        this.channel = null;
        this.parent = true;
        
        if (debug.val)
            LOG.debug(String.format("Local Partitions for Site #%d: %s",
                      hstore_site.getSiteId(), hstore_site.getLocalPartitionIds()));

        // Incoming RPC Handler
        this.jvmSnapshots = this.initJVMSnapshots();
        
        // This listener thread will process incoming messages
        this.listener = new ProtoServer(this.eventLoop);
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread(new SnapshotsListener());
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
    }
    
    protected HStoreJVMSnapshots initJVMSnapshots() {
        return (new SnapshotHandler());
    }
    
    /**
     * Start the messenger. This is a blocking call that will initialize the connections
     * and start the listener thread!
     */
    public synchronized void start() {
        
        if (debug.val) LOG.debug("Initializing connections");
        this.initConnections();
        
        if (debug.val) LOG.debug("Starting listener thread");
        this.listener_thread.start();
        
        this.ready_observable.notifyObservers(this);
    }
    
    protected int getLocalSiteId() {
        return (this.local_site_id);
    }
    protected int getJVMSnapshotPort() {
        return (this.hstore_site.getSite().getJVMSnapshot_port());
    }
    protected final Thread getListenerThread() {
        return (this.listener_thread);
    }
    
    public HStoreJVMSnapshots getChannel() {
        return (this.channel);
    }
    public HStoreJVMSnapshots getHandler() {
        return (this.jvmSnapshots);
    }
    public EventObservable<HStoreJVMSnapshotManager> getReadyObservable() {
        return (this.ready_observable);
    }
    
    /**
     * Initialize all the network connections to remote
     *  
     */
    private void initConnections() {
    	
        if (debug.val) LOG.debug("Configuring JVM snapshot connections for Site #" + this.catalog_site.getId());
        
        // Initialize inbound channel
        Integer local_port = this.catalog_site.getJVMSnapshot_port();
        assert(local_port != null);
        if (debug.val) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.jvmSnapshots);
        this.listener.bind(local_port);

        // Find all the destinations we need to connect to
        // Make the outbound connections
        InetSocketAddress destinationAddress = new InetSocketAddress(this.catalog_site.getHost().getIpaddr(), this.catalog_site.getJVMSnapshot_port());
        if (debug.val) LOG.debug("Connecting to parent address");
        ProtoRpcChannel channel = new ProtoRpcChannel(eventLoop, destinationAddress);

        this.channel = HStoreJVMSnapshots.newStub(channel);
        
        if (debug.val) LOG.debug("Site #" + this.getLocalSiteId() + " is fully connected to all sites");
    }
    
    // ----------------------------------------------------------------------------
    // HSTORE RPC SERVICE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * We want to make this a private inner class so that we do not expose
     * the RPC methods to other parts of the code.
     */
    private class SnapshotHandler extends HStoreJVMSnapshots {

		@Override
		public void execTransactionRequest(RpcController controller,
				TransactionRequest request,
				RpcCallback<TransactionResponse> done) {
			// TODO Auto-generated method stub
			
		}
    	
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

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
