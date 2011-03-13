package edu.mit.hstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import edu.brown.utils.ThreadUtil;
import edu.mit.hstore.callbacks.ForwardTxnResponseCallback;

/**
 * 
 * @author pavlo
 */
public class HStoreMessenger {
    public static final Logger LOG = Logger.getLogger(HStoreMessenger.class);
    private final static AtomicBoolean debug = new AtomicBoolean(LOG.isDebugEnabled());
    private final static AtomicBoolean trace = new AtomicBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    enum MessengerState {
        INITIALIZED,
        STARTED,
        PREPARE_STOP,
        STOPPED,
    };
    
    private final HStoreSite hstore_site;
    private final Site catalog_site;
    private final Set<Integer> local_partitions;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private static final DBBPool buffer_pool = new DBBPool(true, false);

    /**
     * PartitionId -> SiteId
     */
    private final Map<Integer, Integer> partition_site_xref = new HashMap<Integer, Integer>();
    
    /**
     * SiteId -> HStoreServer
     */
    private final Map<Integer, HStoreService> channels = new HashMap<Integer, HStoreService>();
    
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final Handler handler;
    private final Callback callback;
    private boolean shutting_down = false;
    private MessengerState state = MessengerState.INITIALIZED;
    
    /**
     * Constructor
     * @param site
     */
    public HStoreMessenger(HStoreSite site) {
        this.hstore_site = site;
        this.catalog_site = site.getSite();
        
        Set<Integer> partitions = new HashSet<Integer>();
        for (Partition catalog_part : this.catalog_site.getPartitions()) {
            partitions.add(catalog_part.getId());
        } // FOR
        this.local_partitions = Collections.unmodifiableSet(partitions);
        if (LOG.isDebugEnabled()) LOG.debug("Local Partitions for Site #" + site.getSiteId() + ": " + this.local_partitions);
        
        this.listener = new ProtoServer(eventLoop);
        this.handler = new Handler();
        this.callback = new Callback();
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread() {
            @Override
            public void run() {
                Thread.currentThread().setName(HStoreMessenger.this.hstore_site.getThreadName("msg"));
                try {
                    HStoreMessenger.this.eventLoop.run();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    
                    Throwable cause = null;
                    if (ex instanceof RuntimeException && ex.getCause() != null) {
                        if (ex.getCause().getMessage() != null && ex.getCause().getMessage().isEmpty() == false) {
                            cause = ex.getCause();
                        }
                    }
                    if (cause == null) cause = ex;
                    
                    // These errors are ok if we're actually stopping...
                    if (HStoreMessenger.this.state == MessengerState.STOPPED ||
                        HStoreMessenger.this.state == MessengerState.PREPARE_STOP) {
                        // IGNORE
                    } else {
                        LOG.fatal("Unexpected error in messenger listener thread", cause);
                        HStoreMessenger.this.shutdownCluster(ex);
                    }
                } finally {
                    // if (HStoreMessenger.this.state != MessengerState.STOPPED) HStoreMessenger.this.stop();
                }
                if (trace.get()) {
                    LOG.trace("Messenger Thread for Site #" + catalog_site.getId() + " has stopped!");
                }
            }
        };
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
    }
    
    /**
     * Start the messenger. This is a blocking call that will initialize the connections
     * and start the listener thread!
     */
    public synchronized void start() {
        assert(this.state == MessengerState.INITIALIZED) : "Invalid MessengerState " + this.state;
        
        this.state = MessengerState.STARTED;
        
        LOG.debug("Initializing connections");
        this.initConnections();

        LOG.debug("Starting listener thread");
        this.listener_thread.start();
    }

    /**
     * Returns true if the messenger has started
     * @return
     */
    public boolean isStarted() {
        return (this.state == MessengerState.STARTED ||
                this.state == MessengerState.PREPARE_STOP);
    }
    
    /**
     * Internal call for testing to hide errors
     */
    protected void prepareToStop() {
        assert(this.state == MessengerState.STARTED) : "Invalid MessengerState " + this.state;
        this.state = MessengerState.PREPARE_STOP;
    }
    
    /**
     * Stop this messenger. This kills the ProtoRpc event loop
     */
    public synchronized void stop() {
        assert(this.state == MessengerState.STARTED || this.state == MessengerState.PREPARE_STOP) : "Invalid MessengerState " + this.state;
        
        this.state = MessengerState.STOPPED;
        
        try {
            if (trace.get()) LOG.trace("Stopping eventLoop for Site #" + this.getLocalSiteId());
            this.eventLoop.exitLoop();

            if (trace.get()) LOG.trace("Stopping listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.interrupt();
            
            if (trace.get()) LOG.trace("Joining on listener thread for Site #" + this.getLocalSiteId());
            this.listener_thread.join();
        } catch (InterruptedException ex) {
            // IGNORE
        } catch (Exception ex) {
            LOG.error("Unexpected error when trying to stop messenger for Site #" + this.getLocalSiteId(), ex);
        } finally {
            if (trace.get()) LOG.trace("Closing listener socket for Site #" + this.getLocalSiteId());
            this.listener.close();
        }
        assert(this.isStopped());
    }
    
    /**
     * Returns true if the messenger has stopped
     * @return
     */
    public boolean isStopped() {
        return (this.state == MessengerState.STOPPED);
    }
    
    protected int getLocalSiteId() {
        return (this.hstore_site.getSiteId());
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
                    
                    for (Partition catalog_part : catalog_site.getPartitions()) {
                        this.partition_site_xref.put(catalog_part.getId(), site_id);
                    } // FOR
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
            for (int i = 0; i < site_ids.size(); i++) {
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

        @Override
        public void sendMessage(RpcController controller, MessageRequest request, RpcCallback<MessageAcknowledgement> done) {
            int sender = request.getSenderId();
            int dest = request.getDestId();
            MessageType type = request.getType();
            if (debug.get()) LOG.debug("Received " + type.name() + " request from Site #" + sender);
            
            Hstore.MessageAcknowledgement response = null;
            switch (type) {
                // -----------------------------------------------------------------
                // STATUS REQUEST
                // ----------------------------------------------------------------- 
                case STATUS: {
                    response = Hstore.MessageAcknowledgement.newBuilder()
                                                            .setDestId(sender)
                                                            .setSenderId(dest)
                                                            .setData(ByteString.copyFrom("OK".getBytes())) // TODO
                                                            .build();
                    done.run(response);
                    break;
                }
                // -----------------------------------------------------------------
                // SHUTDOWN REQUEST
                // -----------------------------------------------------------------
                case SHUTDOWN: {
                    HStoreMessenger.this.shutting_down = true;
                    
                    // Tell the coordinator to shutdown
                    HStoreMessenger.this.hstore_site.shutdown();
                    
                    // Get exit status code
                    byte exit_status = request.getData().byteAt(0);
                    
                    // Then send back the acknowledgment
                    response = Hstore.MessageAcknowledgement.newBuilder()
                                                           .setDestId(sender)
                                                           .setSenderId(catalog_site.getId())
                                                           .setData(ByteString.copyFrom("OK".getBytes())) // TODO
                                                           .build();
                    // Send this now!
                    done.run(response);
                    LOG.info(String.format("Shutting down [site=%d, status=%d]", catalog_site.getId(), exit_status));
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
                    // HStoreCoordinatorNode wants to send to the client and forward 
                    // it back to whomever told us about this txn
                    if (trace.get()) LOG.trace("Passing " + type.name() + " information to HStoreCoordinatorNode");
                    byte serializedRequest[] = request.getData().toByteArray();
                    ForwardTxnResponseCallback callback = new ForwardTxnResponseCallback(dest, sender, done);
                    HStoreMessenger.this.hstore_site.procedureInvocation(serializedRequest, callback);
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
            if (trace.get()) LOG.trace("Incoming data from Partition #" + sender_partition_id + " to Partition #" + dest_partition_id +
                                 " for Txn #" + txn_id + " with " + request.getDependenciesCount() + " dependencies");

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
                HStoreMessenger.this.hstore_site.getExecutors().get(dest_partition_id).storeDependency(txn_id, sender_partition_id, dependency_id, data);
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
     * Messenger Callback
     * This is invoked with a successful acknowledgment that we stored the dependency at the remote partition
     */
    private class Callback implements RpcCallback<FragmentAcknowledgement> {
        
        @Override
        public void run(FragmentAcknowledgement parameter) {
            if (trace.get())
                LOG.trace("Received sendFragment callback from remote Partition #" + parameter.getSenderPartitionId() +
                          " for Txn #" + parameter.getTxnId());
        }
    }
 
    
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
     * Send a DependencySet to a remote partition for a given transaction
     * @param txn_id
     * @param sender_partition_id TODO
     * @param dest_partition_id
     * @param dset
     */
    public void sendDependencySet(long txn_id, int sender_partition_id, int dest_partition_id, DependencySet dset) {
        assert(dset != null);
        final boolean d = debug.get();
        
        // Local Transfer
        if (this.local_partitions.contains(dest_partition_id)) {
            if (d) LOG.debug("Transfering " + dset.size() + " dependencies directly from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
                ExecutionSite executor = this.hstore_site.getExecutors().get(dest_partition_id);
                assert(executor != null) : "Unexpected null ExecutionSite for Partition #" + dest_partition_id + " on Site #" + catalog_site.getId();
                
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
            if (d) LOG.debug("Transfering " + dset.size() + " dependencies through network from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
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
            channel.sendFragment(rpc, ft, this.callback);
        }
    }

    /**
     * Take down the cluster. If the blocking flag is true, then this call will never return
     * @param blocking
     * @param ex
     */
    public void shutdownCluster(final boolean blocking, final Throwable ex) {
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
     */
    public void shutdownCluster() {
        this.shutdownCluster(null);
    }
    
    /**
     * Shutdown the cluster. If the given Exception is not null, then all the nodes will
     * exit with a non-zero status.
     * @param ex
     */
    public synchronized void shutdownCluster(Throwable ex) {
        final boolean t = trace.get();
        final boolean d = debug.get();
        
        final int num_sites = this.channels.size();
        if (this.shutting_down) return;
        this.shutting_down = true;
        LOG.info("Shutting down cluster" + (ex != null ? ": " + ex.getMessage() : ""));

        final ByteString exit_status = ByteString.copyFrom(new byte[] { (byte)(ex == null ? 0 : 1) });
        final CountDownLatch latch = new CountDownLatch(num_sites);
        
        if (num_sites > 0) {
            RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
                private final Set<Integer> siteids = new HashSet<Integer>(); 
                
                @Override
                public void run(MessageAcknowledgement parameter) {
                    int siteid = parameter.getSenderId();
                    assert(this.siteids.contains(siteid) == false) : "Duplicate response from Site #" + siteid;
                    this.siteids.add(siteid);
                    if (t) LOG.trace("Received " + this.siteids.size() + "/" + num_sites + " shutdown acknowledgements");
                    latch.countDown();
                }
            };
            
            if (d) LOG.debug("Sending shutdown request to " + num_sites + " remote sites");
            for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
                Hstore.MessageRequest sm = Hstore.MessageRequest.newBuilder()
                                                .setSenderId(catalog_site.getId())
                                                .setDestId(e.getKey())
                                                .setData(exit_status)
                                                .setType(MessageType.SHUTDOWN)
                                                .build();
                e.getValue().sendMessage(new ProtoRpcController(), sm, callback);
                if (t) LOG.trace("Sent SHUTDOWN to Site #" + e.getKey());
            } // FOR
        }
        
        // Tell ourselves to shutdown while we wait
        if (d) LOG.debug("Telling local site to shutdown");
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
    
    /**
     * Forward a StoredProcedureInvocation request to a remote site for execution
     * @param serializedRequest
     * @param done
     * @param partition
     */
    public void forwardTransaction(byte[] serializedRequest, RpcCallback<MessageAcknowledgement> done, int partition) {
        int dest_site_id = this.partition_site_xref.get(partition);
        if (debug.get()) LOG.debug("Forwarding a transaction request to Partition #" + partition + " on Site #" + dest_site_id);
        Hstore.MessageRequest mr = Hstore.MessageRequest.newBuilder()
                                        .setSenderId(this.catalog_site.getId())
                                        .setDestId(dest_site_id)
                                        .setType(MessageType.FORWARD_TXN)
                                        .setData(ByteString.copyFrom(serializedRequest))
                                        .build();
        this.channels.get(dest_site_id).sendMessage(new ProtoRpcController(), mr, done);
        if (trace.get()) LOG.debug("Sent " + MessageType.FORWARD_TXN.name() + " to Site #" + dest_site_id);
    }

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