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
import edu.brown.hstore.Hstore.StatusAcknowledgement;
import edu.brown.hstore.Hstore.StatusRequest;

/**
 * 
 * @author pavlo
 */
public class HStoreMessenger {
    public static final Logger LOG = Logger.getLogger(HStoreMessenger.class);
    
    private final ExecutionSite executor;
    private final int local_partition;
    private final NIOEventLoop eventLoop = new NIOEventLoop();
    
    private final Map<Integer, HStoreService> channels = new HashMap<Integer, HStoreService>();
    private final Thread listener_thread;
    private final ProtoServer listener;
    private final Handler handler;
    private final Callback callback;
    
    public HStoreMessenger(ExecutionSite executor, int local_partition) {
        this.executor = executor;
        this.local_partition = local_partition;
        this.listener = new ProtoServer(eventLoop);
        this.handler = new Handler();
        this.callback = new Callback();
        
        // Wrap the listener in a daemon thread
        this.listener_thread = new Thread() {
            @Override
            public void run() {
                eventLoop.run();
            }
        };
        this.listener_thread.setDaemon(true);
        this.eventLoop.setExitOnSigInt(true);
        
        this.initConnections();
    }
    
    public void start() {
        this.listener_thread.start();
        this.testConnections();
    }
    
    public void stop() {
        this.eventLoop.exitLoop();
    }
    
    protected void initConnections() {
        final boolean debug = LOG.isDebugEnabled(); 
        Database catalog_db = CatalogUtil.getDatabase(this.executor.getCatalogSite());
        
        // Find all the destinations we need to connect to
        if (debug) LOG.debug("Configuring outbound network connections");
        Map<Host, Set<Site>> host_partitions = CatalogUtil.getHostPartitions(catalog_db);
        Integer local_port = null;
        ArrayList<Integer> partition_ids = new ArrayList<Integer>();
        ArrayList<InetSocketAddress> destinations = new ArrayList<InetSocketAddress>();
        for (Entry<Host, Set<Site>> e : host_partitions.entrySet()) {
            String host = e.getKey().getIpaddr();
            for (Site catalog_site : e.getValue()) {
                Partition catalog_part = catalog_site.getPartition();
                int port = catalog_site.getMessenger_port();
                int partition_id = catalog_part.getId();
                if (partition_id == this.local_partition) {
                    local_port = port;
                    continue;
                }
                
                LOG.debug("Creating RpcChannel to " + host + ":" + port);
                partition_ids.add(partition_id);
                destinations.add(new InetSocketAddress(host, port));
            } // FOR 
        } // FOR
        
        // Initialize inbound channel
        assert(local_port != null);
        if (debug) LOG.debug("Binding listener to port " + local_port + " for Partition #" + this.local_partition);
        this.listener.register(this.handler);
        this.listener.bind(local_port);

        // Make the outbound connections
        if (debug) LOG.debug("Connecting to remote nodes");
        ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(
                this.eventLoop, destinations.toArray(new InetSocketAddress[]{}));
        assert channels.length == partition_ids.size();
        for (int i = 0; i < partition_ids.size(); i++) {
            this.channels.put(partition_ids.get(i), HStoreService.newStub(channels[i]));
        }
    }
    
    protected void testConnections() {
        // Go through and connect to all of our remote partitions
        final Map<Integer, String> responses = new HashMap<Integer, String>();
        final Set<Integer> waiting = new HashSet<Integer>();
        
        RpcCallback<StatusAcknowledgement> callback = new RpcCallback<StatusAcknowledgement>() {
            @Override
            public void run(StatusAcknowledgement parameter) {
                int sender = parameter.getSenderPartitionId();
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
        
        ProtoRpcController rpc = new ProtoRpcController();
        for (Entry<Integer, HStoreService> e : this.channels.entrySet()) {
            Hstore.StatusRequest sm = Hstore.StatusRequest.newBuilder()
                                                .setSenderPartitionId(local_partition)
                                                .setDestPartitionId(e.getKey())
                                                .build();
            e.getValue().getStatus(rpc, sm, callback);
        }
        
    }
    
    /**
     * Messenger Handler
     * This takes in a new FragmentTransfer message and stores it in the ExecutionSite
     */
    private class Handler extends HStoreService {

        @Override
        public void getStatus(RpcController controller, StatusRequest request, RpcCallback<StatusAcknowledgement> done) {
            int sender = request.getSenderPartitionId();
            int dest = request.getDestPartitionId();
            
            Hstore.StatusAcknowledgement sa = Hstore.StatusAcknowledgement.newBuilder()
                                                        .setDestPartitionId(sender)
                                                        .setSenderPartitionId(dest)
                                                        .setMessage("OK") // TODO
                                                        .build();
            
            done.run(sa);
        }
        
        @Override
        public void sendFragment(RpcController controller, FragmentTransfer request, RpcCallback<FragmentAcknowledgement> done) {
            long txn_id = request.getTxnId();
            int sender_partition_id = request.getSenderPartitionId();

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
                HStoreMessenger.this.executor.storeDependency(txn_id, sender_partition_id, dependency_id, data);
            }
            
            // Send back a response
            Hstore.FragmentAcknowledgement fa = Hstore.FragmentAcknowledgement.newBuilder()
                                                        .setTxnId(txn_id)
                                                        .setSenderPartitionId(sender_partition_id)
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
            // TODO Auto-generated method stub
            
        }
    }
    
    /**
     * Send an individual dependency to a remote partition for a given transaction
     * @param txn_id
     * @param partition_id
     * @param dependency_id
     * @param table
     */
    public void sendDependency(long txn_id, int partition_id, int dependency_id, VoltTable table) {
        DependencySet dset = new DependencySet(new int[]{ dependency_id }, new VoltTable[]{ table });
        this.sendDependencySet(txn_id, partition_id, dset);
    }
    
    /**
     * Send a DependencySet to a remote partition for a given transaction
     * @param txn_id
     * @param partition_id
     * @param dset
     */
    public void sendDependencySet(long txn_id, int partition_id, DependencySet dset) {
        ProtoRpcController rpc = new ProtoRpcController();
        HStoreService channel = this.channels.get(partition_id);
        assert(channel != null) : "Invalid partition id '" + partition_id + "'";
        
        // Serialize DependencySet
        List<Hstore.FragmentDependency> dependencies = new ArrayList<Hstore.FragmentDependency>();
        for (int i = 0, cnt = dset.size(); i < cnt; i++) {
            FastSerializer fs = new FastSerializer();
            try {
                fs.writeObject(dset.dependencies[i]);
            } catch (Exception ex) {
                LOG.fatal("Failed to serialize DependencyId #" + dset.depIds[i], ex);
            }
            ByteString bs = ByteString.copyFrom(fs.getBuffer().array());
            
            Hstore.FragmentDependency fd = Hstore.FragmentDependency.newBuilder()
                                                    .setDependencyId(dset.depIds[i])
                                                    .setData(bs)
                                                    .build();
            dependencies.add(fd);
        } // FOR
        
        Hstore.FragmentTransfer ft = Hstore.FragmentTransfer.newBuilder()
                                                .setTxnId(txn_id)
                                                .setSenderPartitionId(this.local_partition)
                                                .addAllDependencies(dependencies)
                                                .build();
        channel.sendFragment(rpc, ft, this.callback);        
    }

}