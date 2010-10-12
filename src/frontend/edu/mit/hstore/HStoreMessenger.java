package edu.mit.hstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

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
import edu.brown.utils.CollectionUtil;

/**
 * 
 * @author pavlo
 */
public class HStoreMessenger {
    public static final Logger LOG = Logger.getLogger(HStoreMessenger.class);
    
    private final Map<Integer, ExecutionSite> executors;
    private final Site catalog_site;
    private final Set<Integer> local_partitions = new HashSet<Integer>();
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private final DBBPool buffer_pool = new DBBPool(true, true);
    
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
    
    public HStoreMessenger(Map<Integer, ExecutionSite> executors, Site catalog_site) {
        this.executors = executors;
        this.catalog_site = catalog_site;
        
        for (Partition catalog_part : this.catalog_site.getPartitions()) {
            this.local_partitions.add(catalog_part.getId());
        } // FOR
        LOG.info("Local Partitions: " + this.local_partitions);
        
        this.listener = new ProtoServer(eventLoop);
        this.handler = new Handler();
        this.callback = new Callback();
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread() {
//            {
//                Thread.currentThread().setName(String.format("H%03d-msg", HStoreMessenger.this.catalog_site.getId()));
//            }
            @Override
            public void run() {
                Thread.currentThread().setName(String.format("H%03d-msg", HStoreMessenger.this.catalog_site.getId()));
                eventLoop.run();
            }
        };
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
    }
    
    public void start() {
        LOG.debug("Initializing connections");
        this.initConnections();
        LOG.debug("Starting listener thread");
        this.listener_thread.start();
//        LOG.debug("Testing connections");
//        this.testConnections();
    }
    
    public void stop() {
        this.eventLoop.exitLoop();
    }
    
    /**
     * Initialize all the network connections to remote 
     */
    protected void initConnections() {
        final boolean debug = LOG.isDebugEnabled(); 
        Database catalog_db = CatalogUtil.getDatabase(this.catalog_site);
        
        // Find all the destinations we need to connect to
        if (debug) LOG.debug("Configuring outbound network connections for Site #" + this.catalog_site.getId());
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
                    LOG.debug("Creating RpcChannel to " + host + ":" + port + " for site #" + site_id);
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
        if (debug) LOG.debug("Binding listener to port " + local_port + " for Site #" + this.catalog_site.getId());
        this.listener.register(this.handler);
        this.listener.bind(local_port);

        // Make the outbound connections
        if (destinations.isEmpty()) {
            if (debug) LOG.debug("There are no remote sites so we are skipping creating connections");
        } else {
            if (debug) LOG.debug("Connecting to " + destinations.size() + " remote sites");
            ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(
                    this.eventLoop, destinations.toArray(new InetSocketAddress[]{}));
            assert channels.length == site_ids.size();
            for (int i = 0; i < site_ids.size(); i++) {
                this.channels.put(site_ids.get(i), HStoreService.newStub(channels[i]));
            } // FOR
        }
    }
    
    protected void testConnections() {
        // Go through and connect to all of our remote partitions
        final Map<Integer, String> responses = new HashMap<Integer, String>();
        final Set<Integer> waiting = new HashSet<Integer>();
        
        RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
            @Override
            public void run(MessageAcknowledgement parameter) {
                int sender = parameter.getSenderId();
                String status = parameter.getMessage();
                responses.put(sender, status);
                waiting.remove(sender);
                
                if (waiting.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("TestConnection Responses:\n");
                    for (Entry<Integer, String> e : responses.entrySet()) {
                        sb.append(String.format("  Partition %03d: %s\n", e.getKey(), e.getValue()));
                    } // FOR
                    LOG.info(sb.toString());
                }
            }
        };
        
        // The sender partition can just be our first partition that we have
        Partition catalog_part = CollectionUtil.getFirst(this.catalog_site.getPartitions());
        
        ProtoRpcController rpc = new ProtoRpcController();
        for (Entry<Integer, HStoreService> e : this.channels.entrySet()) {
            if (this.local_partitions.contains(e.getKey())) {
                responses.put(e.getKey(), "LOCAL");
            } else {
                Hstore.MessageRequest sm = Hstore.MessageRequest.newBuilder()
                                                    .setSenderId(catalog_part.getId())
                                                    .setDestId(e.getKey())
                                                    .setType(MessageType.STATUS)
                                                    .build();
                e.getValue().sendMessage(rpc, sm, callback);
                waiting.add(e.getKey());
            }
        } // FOR
        
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
            
            Hstore.MessageAcknowledgement response = null;
            
            switch (type) {
                case STATUS: {
                    response = Hstore.MessageAcknowledgement.newBuilder()
                                                            .setDestId(sender)
                                                            .setSenderId(dest)
                                                            .setMessage("OK") // TODO
                                                            .build();
                    done.run(response);
                    break;
                }
                case SHUTDOWN: {
                    // First tell all of our ExecutionSites to shutdown
                    for (ExecutionSite executor : HStoreMessenger.this.executors.values()) {
                        executor.shutdown();
                    } // FOR
                    
                    // Get exit status code
                    byte exit_status = request.getData().byteAt(0);
                    
                    // Then send back the acknowledgment
                    response = Hstore.MessageAcknowledgement.newBuilder()
                                                           .setDestId(sender)
                                                           .setSenderId(catalog_site.getId())
                                                           .setMessage("OK") // TODO
                                                           .build();
                    // Send this now!
                    done.run(response);
                    LOG.info("Shutting down [site=" + catalog_site.getId() + ", status=" + exit_status + "]");
                    LogManager.shutdown();
                    System.exit(exit_status);

                    break;
                }
                case FORWARD_TXN: {
                    
                }
                default:
                    throw new RuntimeException("Unexpected MessageType " + type);
            } // SWITCH
        }
        
        
        @Override
        public void sendFragment(RpcController controller, FragmentTransfer request, RpcCallback<FragmentAcknowledgement> done) {
            final boolean trace = LOG.isTraceEnabled();
            long txn_id = request.getTxnId();
            int sender_partition_id = request.getSenderPartitionId();
            int dest_partition_id = request.getDestPartitionId();
            if (trace) LOG.trace("Incoming data from Partition #" + sender_partition_id + " to Partition #" + dest_partition_id +
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
                if (trace) LOG.trace("Storing Depedency #" + dependency_id + " for Txn #" + txn_id + " at Partition #" + dest_partition_id);
                HStoreMessenger.this.executors.get(dest_partition_id).storeDependency(txn_id, sender_partition_id, dependency_id, data);
            } // FOR
            
            // Send back a response
            if (trace) LOG.trace("Sending back FragmentAcknowledgement to Partition #" + sender_partition_id + " for Txn #" + txn_id);
            Hstore.FragmentAcknowledgement fa = Hstore.FragmentAcknowledgement.newBuilder()
                                                        .setTxnId(txn_id)
                                                        .setSenderPartitionId(dest_partition_id)
                                                        .setDestPartitionId(sender_partition_id)
                                                        .build();
            done.run(fa);
        }
    };
    
    /**
     * Messenger Callback
     * This is invoked with a successful acknowledgement that we stored the dependency at the remote partition
     */
    private class Callback implements RpcCallback<FragmentAcknowledgement> {
        
        @Override
        public void run(FragmentAcknowledgement parameter) {
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
            LOG.debug("Transfering " + dset.size() + " dependencies directly from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
                ExecutionSite executor = this.executors.get(dest_partition_id);
                assert(executor != null) : "Unexpected null ExecutionSite for Partition #" + dest_partition_id + " on Site #" + catalog_site.getId();
                executor.storeDependency(txn_id, dest_partition_id, dset.depIds[i], dset.dependencies[i]);
            } // FOR
        // Remote Transfer
        } else {
            LOG.debug("Transfering " + dset.size() + " dependencies through network from partition #" + sender_partition_id + " to partition #" + dest_partition_id);
            ProtoRpcController rpc = new ProtoRpcController();
            int site_id = this.partition_site_xref.get(dest_partition_id);
            HStoreService channel = this.channels.get(site_id);
            assert(channel != null) : "Invalid partition id '" + dest_partition_id + "'";
            
            // Serialize DependencySet
            List<Hstore.FragmentDependency> dependencies = new ArrayList<Hstore.FragmentDependency>();
            for (int i = 0, cnt = dset.size(); i < cnt; i++) {
                FastSerializer fs = new FastSerializer(this.buffer_pool);
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
    public synchronized void shutdownCluster(Exception ex) {
        final int num_sites = this.channels.size();
        if (LOG.isDebugEnabled()) LOG.debug("Sending shutdown request to " + num_sites + " remote sites");
        
        final CountDownLatch latch = new CountDownLatch(num_sites);
        RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
            private final Set<Integer> siteids = new HashSet<Integer>(); 
            
            @Override
            public void run(MessageAcknowledgement parameter) {
                int siteid = parameter.getSenderId();
                assert(this.siteids.contains(siteid) == false) : "Duplicate response from Site #" + siteid;
                this.siteids.add(siteid);
                LOG.debug("Received " + this.siteids.size() + "/" + num_sites + " shutdown acknowledgements");
                latch.countDown();
            }
        };
        
        
        ByteString exit_status = ByteString.copyFrom(new byte[] { (byte)(ex == null ? 0 : 1) });
        for (Entry<Integer, HStoreService> e: this.channels.entrySet()) {
            Hstore.MessageRequest sm = Hstore.MessageRequest.newBuilder()
                                            .setSenderId(catalog_site.getId())
                                            .setDestId(e.getKey())
                                            .setData(exit_status)
                                            .setType(MessageType.SHUTDOWN)
                                            .build();
            e.getValue().sendMessage(new ProtoRpcController(), sm, callback);
            if (LOG.isTraceEnabled()) LOG.debug("Sent SHUTDOWN to Site #" + e.getKey());
        } // FOR
        
        // Tell our local boys to go down too
        for (ExecutionSite executor : this.executors.values()) {
            executor.shutdown();
        } // FOR
        
        // Block until the latch releases us
        try {
            latch.await();
        } catch (Exception ex2) {
            // IGNORE!
        }
        LOG.info("Shutting down [site=" + catalog_site.getId() + ", status=" + exit_status.byteAt(0) + "]");
        LogManager.shutdown();
        System.exit(exit_status.byteAt(0));
    }
}