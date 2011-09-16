package edu.mit.hstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ExecutionSite;
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

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoRpcController;
import ca.evanjones.protorpc.ProtoServer;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.FragmentAcknowledgement;
import edu.brown.hstore.Hstore.FragmentTransfer;
import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.MessageRequest;
import edu.brown.hstore.Hstore.MessageAcknowledgement;
import edu.brown.hstore.Hstore.MessageType;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.callbacks.ForwardTxnResponseCallback;
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
    
    private final HStoreSite hstore_site;
    private final Site catalog_site;
    private final int local_site_id;
    private final Set<Integer> local_partitions;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private final int num_sites;
    
    private final LinkedBlockingDeque<Pair<byte[], ForwardTxnResponseCallback>> forwardQueue = new LinkedBlockingDeque<Pair<byte[], ForwardTxnResponseCallback>>();  

    /** PartitionId -> SiteId */
    private final Map<Integer, Integer> partition_site_xref = new HashMap<Integer, Integer>();
    
    /** SiteId -> HStoreServer */
    private final Map<Integer, HStoreService> channels = new HashMap<Integer, HStoreService>();
    
    private final Thread listener_thread;
    private final ProtoServer listener;
    
    private final ForwardTxnDispatcher forwardDispatcher = new ForwardTxnDispatcher();
    private final Thread forward_thread;
    
    private final Handler handler;
    
    private final FragmentCallback fragment_callback;
    private final MessageCallback message_callback;
    
    private boolean shutting_down = false;
    private Shutdownable.ShutdownState state = ShutdownState.INITIALIZED;
    
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
    
    private class ForwardTxnDispatcher implements Runnable {
        final ProfileMeasurement idleTime = new ProfileMeasurement("IDLE");
        
        @Override
        public void run() {
            if (hstore_site.getHStoreConf().site.cpu_affinity)
                hstore_site.getThreadManager().registerProcessingThread();
            Pair<byte[], ForwardTxnResponseCallback> p = null;
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

        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_site)) {
            this.partition_site_xref.put(catalog_part.getId(), ((Site)catalog_part.getParent()).getId());
        } // FOR
        
        this.listener = new ProtoServer(eventLoop);
        this.handler = new Handler();
        this.fragment_callback = new FragmentCallback();
        this.message_callback = new MessageCallback();
        
        // Special thread to handle forward requests
        if (this.hstore_site.getHStoreConf().site.messenger_redirect_thread) {
            this.forward_thread = new Thread(forwardDispatcher, this.hstore_site.getThreadName("frwd"));
            this.forward_thread.setDaemon(true);
        } else {
            this.forward_thread = null;
        }
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread(new MessengerListener(), this.hstore_site.getThreadName("msg"));
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
    
    /**
     * Messenger Handler
     * This takes in a new FragmentTransfer message and stores it in the ExecutionSite
     */
    private class Handler extends HStoreService {
        
        final ByteString ok = ByteString.copyFrom("OK".getBytes()); 
        
        @Override
        public void sendMessage(RpcController controller, MessageRequest request, RpcCallback<MessageAcknowledgement> done) {
            int sender_site_id = request.getSenderSiteId();
            int dest_site_id = request.getDestSiteId();
            MessageType type = request.getType();
            if (debug.get()) LOG.debug("Received " + type.name() + " request from " + HStoreSite.formatSiteName(sender_site_id));
            
            Hstore.MessageAcknowledgement response = null;
            switch (type) {
                // -----------------------------------------------------------------
                // MARK A TXN AS DONE FROM A REMOTE SITE
                // -----------------------------------------------------------------
                case DONE_PARTITIONS: {
                    // Tell our HStoreSite that this txn is done at the given partitions
                    assert(request.hasTxnId());
                    long txn_id = request.getTxnId();
                    if (debug.get()) LOG.debug(String.format("Processing %s message for txn #%d", type, txn_id));
                    HStoreMessenger.this.hstore_site.doneAtPartitions(txn_id, request.getPartitionsList());

                    Hstore.MessageAcknowledgement done_ack = Hstore.MessageAcknowledgement.newBuilder()
                                                                    .setDestSiteId(sender_site_id)
                                                                    .setSenderSiteId(dest_site_id)
                                                                    .setTxnId(txn_id)
                                                                    .build();
                    done.run(done_ack);
                    break;
                }
                // -----------------------------------------------------------------
                // STATUS REQUEST
                // ----------------------------------------------------------------- 
                case STATUS: {
                    response = Hstore.MessageAcknowledgement.newBuilder()
                                                            .setDestSiteId(sender_site_id)
                                                            .setSenderSiteId(dest_site_id)
                                                            .setData(ok)
                                                            .build();
                    done.run(response);
                    break;
                }
                // -----------------------------------------------------------------
                // SHUTDOWN REQUEST
                // -----------------------------------------------------------------
                case SHUTDOWN: {
                    LOG.info(String.format("Got shutdown request from HStoreSite %s", HStoreSite.formatSiteName(sender_site_id)));
                    
                    HStoreMessenger.this.shutting_down = true;
                    
                    // Tell the coordinator to shutdown
                    HStoreMessenger.this.hstore_site.shutdown();
                    
                    // Get exit status code
                    byte exit_status = request.getData().byteAt(0);
                    
                    // Then send back the acknowledgment
                    response = Hstore.MessageAcknowledgement.newBuilder()
                                                           .setDestSiteId(sender_site_id)
                                                           .setSenderSiteId(local_site_id)
                                                           .setData(ok)
                                                           .build();
                    // Send this now!
                    done.run(response);
                    LOG.info(String.format("Shutting down %s [status=%d]", hstore_site.getSiteName(), exit_status));
                    if (debug.get())
                        LOG.info(String.format("ForwardDispatcher Queue Idle Time: %.2fms",
                                               forwardDispatcher.idleTime.getTotalThinkTimeMS()));
                    ThreadUtil.sleep(1000); // HACK
                    LogManager.shutdown();
                    System.exit(exit_status);
                    break;
                }
                // -----------------------------------------------------------------
                // FORWARD A TXN FROM A REMOTE SITE TO EXECUTE ON THIS SITE
                // -----------------------------------------------------------------
                case FORWARD_TXN: {
                    // We need to create a wrapper callback so that we can get the output that
                    // HStoreSite wants to send to the client and forward 
                    // it back to whomever told us about this txn
                    if (trace.get()) LOG.trace("Passing " + type.name() + " information to HStoreSite");
                    byte serializedRequest[] = request.getData().toByteArray();
                    ForwardTxnResponseCallback callback = null;
                    try {
                        callback = (ForwardTxnResponseCallback)HStoreSite.POOL_FORWARDTXN_RESPONSE.borrowObject();
                        callback.init(dest_site_id, sender_site_id, done);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to get ForwardTxnResponseCallback", ex);
                    }
                    
                    if (HStoreMessenger.this.forward_thread != null) {
                        HStoreMessenger.this.forwardQueue.add(Pair.of(serializedRequest, callback));
                    } else {
                        HStoreMessenger.this.hstore_site.procedureInvocation(serializedRequest, callback);
                    }
                    break;
                }
                // -----------------------------------------------------------------
                // UNKNOWN
                // -----------------------------------------------------------------
                default:
                    throw new RuntimeException("Unexpected MessageType " + type);
            } // SWITCH
        }
        
        /**
         * A remote site is sending us a Frag
         */
        @Override
        public void sendFragment(RpcController controller, FragmentTransfer request, RpcCallback<FragmentAcknowledgement> done) {
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
        }
    };
    
    /**
     * FragmentCallback
     * This is invoked with a successful acknowledgment that we stored the dependency at the remote partition
     */
    private class FragmentCallback implements RpcCallback<FragmentAcknowledgement> {
        @Override
        public void run(FragmentAcknowledgement parameter) {
            if (trace.get())
                LOG.trace("Received sendFragment callback from remote Partition #" + parameter.getSenderPartitionId() +
                          " for Txn #" + parameter.getTxnId());
        }
    }
    
    /**
     * MessageCallback
     */
    private class MessageCallback implements RpcCallback<MessageAcknowledgement> {
        @Override
        public void run(MessageAcknowledgement parameter) {
            if (trace.get()) LOG.trace("Received sendMessage callback from remote HStoreSite " + HStoreSite.formatSiteName(parameter.getSenderSiteId()));
        }
    }
 

    // ----------------------------------------------------------------------------
    // CONVIENCE METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Send an individual dependency to a remote partition for a given transaction
     * @param txn_id
     * @param sender_partition_id TODO
     * @param dest_partition_id
     * @param dependency_id
     * @param table
     */
    public void sendDependency(long txn_id, int sender_partition_id, int dest_partition_id, int dependency_id, VoltTable table) {
        DependencySet dset = new DependencySet(new int[]{ dependency_id }, new VoltTable[]{ table });
        this.sendDependencySet(txn_id, sender_partition_id, dest_partition_id, dset);
    }
    
    /**
     * Send a DependencySet to a remote partition for a given transaction
     * @param txn_id
     * @param sender_partition_id TODO
     * @param dest_partition_id
     * @param dset
     */
    public void sendDependencySet(long txn_id, int sender_partition_id, int dest_partition_id, DependencySet dset) {
        assert(dset != null);
        
        // Local Transfer
        if (this.local_partitions.contains(dest_partition_id)) {
            if (debug.get()) LOG.debug("Transfering " + dset.size() + " dependencies directly from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
            
            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
                ExecutionSite executor = this.hstore_site.getExecutionSite(dest_partition_id);
                assert(executor != null) : "Unexpected null ExecutionSite for Partition #" + dest_partition_id + " on " + this.hstore_site.getSiteName();
                
                // 2010-11-12: We have to copy each VoltTable, otherwise we get an error down in the EE when it tries
                //             to read data from another EE.
                VoltTable copy = null;
                try {
//                    if (sender_partition_id == dest_partition_id) {
//                        copy = dset.dependencies[i];
//                    } else {
                        copy = this.copyVoltTable(dset.dependencies[i]);
//                    }
                } catch (Exception ex) {
                    LOG.fatal("Failed to copy DependencyId #" + dset.depIds[i]);
                    this.shutdownCluster(ex);
                }
                executor.storeDependency(txn_id, sender_partition_id, dset.depIds[i], copy);
            } // FOR
        // Remote Transfer
        } else {
            if (debug.get()) LOG.debug("Transfering " + dset.size() + " dependencies through network from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
            ProtoRpcController rpc = new ProtoRpcController();
            int site_id = this.partition_site_xref.get(dest_partition_id);
            HStoreService channel = this.channels.get(site_id);
            assert(channel != null) : "Invalid partition id '" + dest_partition_id + "'";
            
            // Serialize DependencySet
            List<Hstore.FragmentDependency> dependencies = new ArrayList<Hstore.FragmentDependency>();
            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
                FastSerializer fs = new FastSerializer(buffer_pool);
                try {
                    fs.writeObject(dset.dependencies[i]);
                } catch (Exception ex) {
                    LOG.fatal("Failed to serialize DependencyId #" + dset.depIds[i], ex);
                }
                BBContainer bc = fs.getBBContainer();
                assert(bc.b.hasArray());
                ByteString bs = ByteString.copyFrom(bc.b);
                
                Hstore.FragmentDependency fd = Hstore.FragmentDependency.newBuilder()
                                                        .setDependencyId(dset.depIds[i])
                                                        .setData(bs)
                                                        .build();
                dependencies.add(fd);
            } // FOR
            
            Hstore.FragmentTransfer ft = Hstore.FragmentTransfer.newBuilder()
                                                    .setTxnId(txn_id)
                                                    .setSenderPartitionId(sender_partition_id)
                                                    .setDestPartitionId(dest_partition_id)
                                                    .addAllDependencies(dependencies)
                                                    .build();
            channel.sendFragment(rpc, ft, this.fragment_callback);
        }
    }
    
    /**
     * Notify remote HStoreSites that the multi-partition transaction is done with data
     * at the given partitions 
     * @param txn_id
     * @param partitions
     */
    public void sendDoneAtPartitions(long txn_id, Collection<Integer> partitions) {
        // Try to combine the messages per destination site
        Hstore.MessageRequest request = Hstore.MessageRequest.newBuilder()
                                                             .setType(MessageType.DONE_PARTITIONS)
                                                             .setSenderSiteId(this.local_site_id)
                                                             .setDestSiteId(-1) // doesn't matter
                                                             .setTxnId(txn_id)
                                                             .addAllPartitions(partitions)
                                                             .build();
        assert(request.hasTxnId());
        boolean site_sent[] = new boolean[this.num_sites];
        int cnt = 0;
        for (Integer p : partitions) {
            int dest_site_id = this.partition_site_xref.get(p).intValue();
            if (site_sent[dest_site_id]) continue;
            
            if (debug.get()) LOG.debug(String.format("Sending DoneAtPartitions message to %s for txn #%d", HStoreSite.formatSiteName(dest_site_id), txn_id));
            
            if (this.local_site_id == dest_site_id) {
                this.hstore_site.doneAtPartitions(txn_id, partitions);
            } else {
                HStoreService channel = this.channels.get(dest_site_id);
                assert(channel != null) : "Invalid partition id '" + p + "' at " + this.hstore_site.getSiteName();
                channel.sendMessage(new ProtoRpcController(), request, this.message_callback);
            }
                
            site_sent[dest_site_id] = true;
            cnt++;
        } // FOR
        if (debug.get()) LOG.debug(String.format("Notified %d sites that txn #%d is done at %d partitions", cnt, txn_id, partitions.size()));
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

        final ByteString exit_status = ByteString.copyFrom(new byte[] { (byte)(ex == null ? 0 : 1) });
        final CountDownLatch latch = new CountDownLatch(num_sites);
        
        if (num_sites > 0) {
            RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
                private final Set<Integer> siteids = new HashSet<Integer>(); 
                
                @Override
                public void run(MessageAcknowledgement parameter) {
                    int siteid = parameter.getSenderSiteId();
                    assert(this.siteids.contains(siteid) == false) : "Duplicate response from " + hstore_site.getSiteName();
                    this.siteids.add(siteid);
                    if (trace.get()) LOG.trace("Received " + this.siteids.size() + "/" + num_sites + " shutdown acknowledgements");
                    latch.countDown();
                }
            };
            
            if (debug.get()) LOG.debug("Sending shutdown request to " + num_sites + " remote sites");
            for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
                Hstore.MessageRequest sm = Hstore.MessageRequest.newBuilder()
                                                .setSenderSiteId(catalog_site.getId())
                                                .setDestSiteId(e.getKey())
                                                .setData(exit_status)
                                                .setType(MessageType.SHUTDOWN)
                                                .build();
                e.getValue().sendMessage(new ProtoRpcController(), sm, callback);
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
        LOG.info(String.format("Shutting down [site=%d, status=%d]", catalog_site.getId(), exit_status.byteAt(0)));
        LogManager.shutdown();
        System.exit(exit_status.byteAt(0));
    }
    
    // ----------------------------------------------------------------------------
    // FORWARD TXN REQUEST METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Forward a StoredProcedureInvocation request to a remote site for execution
     * @param serializedRequest
     * @param done
     * @param partition
     */
    public void forwardTransaction(byte[] serializedRequest, RpcCallback<MessageAcknowledgement> done, int partition) {
        int dest_site_id = this.partition_site_xref.get(partition);
        if (debug.get()) LOG.debug("Forwarding a transaction request to partition #" + partition + " on " + HStoreSite.formatSiteName(dest_site_id));
        ByteString bs = ByteString.copyFrom(serializedRequest);
        Hstore.MessageRequest mr = Hstore.MessageRequest.newBuilder()
                                        .setSenderSiteId(this.local_site_id)
                                        .setDestSiteId(dest_site_id)
                                        .setType(MessageType.FORWARD_TXN)
                                        .setData(bs)
                                        .build();
        this.channels.get(dest_site_id).sendMessage(new ProtoRpcController(), mr, done);
        if (trace.get()) LOG.trace("Sent " + MessageType.FORWARD_TXN.name() + " to " + HStoreSite.formatSiteName(dest_site_id));
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Returns an HStoreService handle that is connected to the given site
     * @param catalog_site
     * @return
     */
    public static HStoreService getHStoreService(Site catalog_site) {
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