package edu.mit.hstore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ExecutionSite;
import org.voltdb.ProcedureProfiler;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoRpcController;
import ca.evanjones.protorpc.ProtoServer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.ExecutionEngine;
import edu.mit.dtxn.Dtxn.FinishRequest;
import edu.mit.dtxn.Dtxn.FinishResponse;

/**
 * 
 * @author pavlo
 */
public class HStoreCoordinatorNode extends ExecutionEngine implements VoltProcedureListener.Handler {
    private static final Logger LOG = Logger.getLogger(HStoreCoordinatorNode.class.getName());
    private static final AtomicLong NEXT_TXN_ID = new AtomicLong(1); 
    
    private final DBBPool buffer_pool = new DBBPool(true, true);
    private Dtxn.Coordinator coordinator;
    
    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final ExecutionSite executor;
    private final TransactionEstimator t_estimator;
    private EstimationThresholds thresholds;
    private final Thread executor_thread;
    
    /**
     * Status Monitor
     */
    private Thread status_monitor;
    
    /**
     * Count the number of multi-partition txns and single-partitions that we have seen
     */
    private final AtomicInteger singlepart_ctr = new AtomicInteger(0);
    private final AtomicInteger multipart_ctr = new AtomicInteger(0);
    
    /**
     * Keep track of which txns that we have in-flight right now
     */
    private final ConcurrentHashMap<Long, Integer> inflight_txns = new ConcurrentHashMap<Long, Integer>();
    private final AtomicInteger completed_txns = new AtomicInteger(0);
    
    /**
     * Simple Status Printer
     */
    protected class StatusMonitorThread implements Runnable {
        private final int interval; // seconds
        private final String prefix = ">>>>> ";
        private final TreeMap<Integer, TreeSet<Long>> partition_txns = new TreeMap<Integer, TreeSet<Long>>();
        
        private Integer last_completed = null;
        
        private Integer inflight_min = null;
        private Integer inflight_max = null;
        
        public StatusMonitorThread(int interval) {
            this.interval = interval;
            for (Integer partition : CatalogUtil.getAllPartitionIds(HStoreCoordinatorNode.this.catalog_db)) {
                this.partition_txns.put(partition, new TreeSet<Long>());
            } // FOR
            // Throw in -1 for local txns
            this.partition_txns.put(-1, new TreeSet<Long>());
        }
        
        @Override
        public void run() {
            LOG.debug("Starting HStoreCoordinator status monitor thread [interval=" + interval + " secs]");
            Thread self = Thread.currentThread();
            while (!self.isInterrupted()) {
                try {
                    Thread.sleep(interval * 1000);
                } catch (InterruptedException ex) {
                    return;
                }
                int inflight_cur = inflight_txns.size();
                if (inflight_min == null || inflight_cur < inflight_min) inflight_min = inflight_cur;
                if (inflight_max == null || inflight_cur > inflight_max) inflight_max = inflight_cur;
                
                for (Integer partition : this.partition_txns.keySet()) {
                    this.partition_txns.get(partition).clear();
                } // FOR
                for (Entry<Long, Integer> e : inflight_txns.entrySet()) {
                    this.partition_txns.get(e.getValue()).add(e.getKey());
                } // FOR
                
                int singlep_txns = singlepart_ctr.get();
                int multip_txns = multipart_ctr.get();
                int total_txns = singlep_txns + multip_txns;
                int completed = HStoreCoordinatorNode.this.completed_txns.get();

                StringBuilder sb = new StringBuilder();
                sb.append(prefix).append(StringUtil.DOUBLE_LINE)
                  .append(prefix).append("HstoreCoordinator Status\n")
                  .append(prefix).append("InFlight Txn Ids: ").append(String.format("%-4d", inflight_cur)).append(" [")
                                 .append("min=").append(inflight_min).append(", ")
                                 .append("max=").append(inflight_max).append("]\n");
                for (Integer partition : this.partition_txns.keySet()) {
                    int cnt = this.partition_txns.get(partition).size();
                    if (cnt > 0) {
                        sb.append(prefix).append("  Partition[").append(String.format("%02d", partition)).append("]: ")
                          .append(this.partition_txns.get(partition)).append("\n");
                    }
                } // FOR
                
                sb.append(prefix).append("Total Txns:       ").append(String.format("%-4d", total_txns)).append(" [")
                                 .append("singleP=").append(singlep_txns).append(", ")
                                 .append("multiP=").append(multip_txns).append("]\n")
                  .append(prefix).append("Completed Txns: ").append(String.format("%-4d\n", completed))
                  .append(prefix).append(StringUtil.DOUBLE_LINE);
                System.err.print(sb.toString());
                
                
                if (this.last_completed != null) {
                    // If we're not making progress, bring the whole thing down!
                    if (this.last_completed == completed) {
                        LOG.fatal("System is stuck! We're going down so that we can investigate!");
                        System.exit(1);
                    }
                }
                this.last_completed = completed;
            } // WHILE
        }
    } // END CLASS
    
    /**
     * Shutdown Hook Thread
     */
    protected class ShutdownThread implements Runnable {
        @Override
        public void run() {
            // Dump out our status
            int num_inflight = inflight_txns.size();
            if (num_inflight > 0) {
                System.err.println("FAIL: InFlight Txns Ids => " + inflight_txns.keySet());
            }
        }
    } // END CLASS
    
    /**
     * Constructor
     * @param coordinator
     * @param p_estimator
     */
    public HStoreCoordinatorNode(ExecutionSite executor, PartitionEstimator p_estimator, TransactionEstimator t_estimator) {
        // General Stuff
        this.catalog_db = p_estimator.getDatabase();
        this.p_estimator = p_estimator;
        this.t_estimator = t_estimator;
        this.thresholds = new EstimationThresholds(); // default values
        
        // ExecutionEngine Stuff
        this.executor = executor;
        this.executor_thread = new Thread(this.executor);
        this.executor_thread.start();
        
        assert(this.coordinator != null);
        assert(this.catalog_db != null);
        assert(this.p_estimator != null);
        
        // Add in our shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownThread()));
        
        LOG.debug("Starting HStoreCoordinatorNode...");
    }
    
    /**
     * Enable the HStoreCoordinator's status monitor
     * @param interval
     */
    public void enableStatusMonitor(int interval) {
        if (interval > 0) this.status_monitor = new Thread(new StatusMonitorThread(interval)); 
    }
    
    public void setDtxnCoordinator(Dtxn.Coordinator coordinator) {
        this.coordinator = coordinator;
    }
    
    public Dtxn.Coordinator getDtxnCoordinator() {
        return (this.coordinator);
    }
    
    private void setEstimationThresholds(EstimationThresholds thresholds) {
         this.thresholds = thresholds;
    }
    
    public int getLocalPartition() {
        return (this.executor.getPartitionId());
    }

    @Override
    public void procedureInvocation(byte[] serializedRequest, RpcCallback<byte[]> done) {
        if (this.status_monitor != null && !this.status_monitor.isAlive()) this.status_monitor.start();
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();
        
        // The serializedRequest is a ProcedureInvocation object
        StoredProcedureInvocation request = null;
        FastDeserializer fds = new FastDeserializer(serializedRequest);
        try {
            request = fds.readObject(StoredProcedureInvocation.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        if (request == null) {
            throw new RuntimeException("Failed to get ProcedureInvocation object from request bytes");
        }
        
        // TODO(evanj): Set to a real value
        long txn_id = NEXT_TXN_ID.getAndIncrement();
        long client_handle = request.getClientHandle();
        request.buildParameterSet();
        assert(request.getParams() != null) : "The parameters object is null for new txn from client #" + client_handle;
        Object args[] = request.getParams().toArray(); 
        // LOG.info("Parameter Size = " + request.getParameters())
        
        Procedure catalog_proc = this.catalog_db.getProcedures().get(request.getProcName());
        if (catalog_proc == null) throw new RuntimeException("Unknown procedure '" + request.getProcName() + "'");
        if (trace) LOG.trace("Executing new stored procedure invocation request for " + catalog_proc.getName() + " as Txn #" + txn_id);
        
        // Setup "Magic Evan" RPC stuff
        ProtoRpcController rpc = new ProtoRpcController();
        Dtxn.CoordinatorFragment.Builder requestBuilder = Dtxn.CoordinatorFragment.newBuilder();
        requestBuilder.setTransactionId((int)txn_id);
        requestBuilder.setLastFragment(true);
        
        // First figure out where this sucker needs to go
        Integer dest_partition;
        try {
            dest_partition = this.p_estimator.getPartition(catalog_proc, args);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        // assert(dest_partition >= 0);
        if (trace) {
            LOG.trace("Client Handle = " + client_handle);
            LOG.trace("Destination Partition = " + dest_partition);
        }
        
        // Grab the TransactionEstimator for the destination partition and figure out whether
        // this mofo is likely to be single-partition or not. Anything that we can't estimate
        // will just have to be multi-partitioned. This includes sysprocs
        Boolean single_partition = null;
        if (!t_estimator.canEstimate(catalog_proc)) {
            single_partition = false;
        } else {
            if (trace) LOG.trace("Using TransactionEstimator to check whether txn #" + txn_id + " is single-partition");
            TransactionEstimator.Estimate estimate = t_estimator.startTransaction(txn_id, catalog_proc, args);
            single_partition = estimate.isSinglePartition(this.thresholds);
        }
        assert (single_partition != null);
        //LOG.debug("Single-Partition = " + single_partition);
        InitiateTaskMessage wrapper = new InitiateTaskMessage(txn_id, client_handle, request);
        
        // ----------------------------------------------------------------------------
        // SINGLE-PARTITION
        // Everything gets shipped off to HStoreNode 
        // ----------------------------------------------------------------------------
        if (debug) LOG.debug("Sending " + catalog_proc.getName() + " as single-partition txn #" + txn_id + " on partition " + dest_partition);
        this.singlepart_ctr.incrementAndGet();
        this.inflight_txns.put(txn_id, dest_partition);
        
        requestBuilder.addFragment(Dtxn.CoordinatorFragment.PartitionFragment.newBuilder()
                .setPartitionId(dest_partition)
                // TODO: Use copyFrom(ByteBuffer) in the newer version of protobuf
                .setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array())))
                // 2010-06-18: We need to set this for single-partition txns
                .setLastFragment(single_partition);
        CoordinatorResponsePassThroughCallback rpcDone = new CoordinatorResponsePassThroughCallback(txn_id, t_estimator, done);
        coordinator.execute(rpc, requestBuilder.build(), rpcDone);
    }

    /**
     * Base class used to perform the final operations of when a txn completes
     */
    private abstract class AbstractTxnCallback {
        protected final RpcCallback<byte[]> done;
        protected final long txn_id;
        protected final TransactionEstimator t_estimator;
     
        public AbstractTxnCallback(long txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
            this.t_estimator = t_estimator;
            this.txn_id = txn_id;
            this.done = done;
        }
        
        public void prepareFinish(byte[] output, Dtxn.FragmentResponse.Status status) {
            boolean commit = (status == Dtxn.FragmentResponse.Status.OK);
            final boolean trace = LOG.isTraceEnabled();
            if (trace) LOG.trace("Got callback for txn #" + this.txn_id + " [bytes=" + output.length + ", commit=" + commit + ", status=" + status + "]");
            
            // According to the where ever the VoltProcedure was running, our transaction is
            // now complete (either aborted or committed). So we need to tell Dtxn.Coordinator
            // to go fuck itself and send the final messages to everyone that was involved
            FinishRequest.Builder builder = FinishRequest.newBuilder()
                                                .setTransactionId((int)this.txn_id)
                                                .setCommit(commit);
            ClientCallback callback = new ClientCallback(this.txn_id, output, commit, this.done);
            FinishRequest finish = builder.build();
            if (trace) LOG.debug("Calling Dtxn.Coordinator.finish() for txn #" + this.txn_id);
            
            HStoreCoordinatorNode.this.coordinator.finish(new ProtoRpcController(), finish, callback);
            
            // Then clean-up any extra information that we may have for the txn
            if (this.t_estimator != null) {
                if (commit) {
                    if (trace) LOG.trace("Telling the ExecutionSite to COMMIT txn #" + this.txn_id);
                    this.t_estimator.commit(this.txn_id);
                } else {
                    if (trace) LOG.trace("Telling the ExecutionSite to ABORT txn #" + this.txn_id);
                    this.t_estimator.abort(this.txn_id);
                }
            }
        }
    } // END CLASS
    
    /**
     * Finally send the output bytes to the client
     */
    private class ClientCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FinishResponse> {
        private final byte output[];
        private final boolean commit;
        
        public ClientCallback(long txn_id, byte output[], boolean commit, RpcCallback<byte[]> done) {
            super(txn_id, null, done);
            this.output = output;
            this.commit = commit;
        }
        
        /**
         * We can finally send out the final answer to the client
         */
        @Override
        public void run(FinishResponse parameter) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending final response to client for txn #" + this.txn_id + " [" +
                           "status=" + (this.commit ? "COMMIT" : "ABORT") + ", " + 
                           "bytes=" + this.output.length + "]");
            }
            HStoreCoordinatorNode.this.inflight_txns.remove(this.txn_id);
            HStoreCoordinatorNode.this.completed_txns.incrementAndGet();
            assert(!HStoreCoordinatorNode.this.inflight_txns.containsKey(this.txn_id)) :
                "Failed to remove InFlight entry for txn #" + this.txn_id + "\n" + inflight_txns;
            this.done.run(this.output);
        }
    } // END CLASS
    
    /**
     * Unpack a FragmentResponse and send the bytes to the client
     */
    private final class FragmentResponsePassThroughCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.FragmentResponse> {
        public FragmentResponsePassThroughCallback(long txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
            super(txn_id, t_estimator, done);
        }
        
        @Override
        public void run(Dtxn.FragmentResponse response) {
            this.prepareFinish(response.getOutput().toByteArray(), response.getStatus());
            
        }
    } // END CLASS

    /**
     * Unpack a CoordinatorResponse and send the bytes to the client
     */
    private final class CoordinatorResponsePassThroughCallback extends AbstractTxnCallback implements RpcCallback<Dtxn.CoordinatorResponse> {
        public CoordinatorResponsePassThroughCallback(long txn_id, TransactionEstimator t_estimator, RpcCallback<byte[]> done) {
            super(txn_id, t_estimator, done);
        }

        @Override
        public void run(Dtxn.CoordinatorResponse response) {
            assert response.getResponseCount() == 1;
            // FIXME(evanj): This callback should call AbstractTxnCallback.run() once we get
            // generate txn working. For now we'll just forward the request back to the client
            // this.done.run(response.getResponse(0).getOutput().toByteArray());
            this.prepareFinish(response.getResponse(0).getOutput().toByteArray(), response.getStatus());
        }
    } // END CLASS

    /**
     * Is this always going to be a StoredProcedureInvocation??
     */
    @Override
    public void execute(RpcController controller, Dtxn.Fragment request, RpcCallback<Dtxn.FragmentResponse> done) {
        // Decode the procedure request
        TransactionInfoBaseMessage msg = null;
        try {
            msg = (TransactionInfoBaseMessage)VoltMessage.createMessageFromBuffer(request.getWork().asReadOnlyByteBuffer(), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        long txn_id = msg.getTxnId(); // request.getTransactionId();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Got " + msg.getClass().getSimpleName() + " message for txn #" + txn_id + ". Forwarding to ExecutionSite");
            LOG.trace("CONTENTS:\n" + msg);
        }
        this.executor.doWork(msg, done);
    }

    @Override
    public void finish(RpcController controller, Dtxn.FinishRequest request, RpcCallback<Dtxn.FinishResponse> done) {
        long txn_id = request.getTransactionId();
        if (LOG.isTraceEnabled()) LOG.trace("Got " + request.getClass().getSimpleName() + " for txn #" + txn_id + " [commit=" + request.getCommit() + "]");
        
        // Tell our node to either commit or abort the txn in the FinishRequest
        if (request.getCommit()) {
            this.executor.commitWork(txn_id);
        } else {
            this.executor.abortWork(txn_id);
        }
        
        // Send back a FinishResponse to let them know we're cool with everything...
        Dtxn.FinishResponse.Builder builder = Dtxn.FinishResponse.newBuilder();
        done.run(builder.build());
    }
    
    
    /**
     * 
     * @param hstore_node
     * @param hstore_conf_path
     * @param dtxnengine_path
     * @param dtxncoordinator_path
     * @param dtxncoord_path
     * @param execHost
     * @param execPort
     * @param coordinatorHost
     * @param coordinatorPort
     * @throws Exception
     */
    public static void launch(final HStoreCoordinatorNode hstore_node,
                              final String hstore_conf_path, final String dtxnengine_path, final String dtxncoordinator_path,
                              final String execHost, final int execPort,
                              final String coordinatorHost, final int coordinatorPort) throws Exception {
        List<Thread> threads = new ArrayList<Thread>();
        final int partition = hstore_node.getLocalPartition();
        
        // ----------------------------------------------------------------------------
        // (1) ExecutionExecution Thread
        // ----------------------------------------------------------------------------
        LOG.debug(String.format("Launching ProtoServer [partition=%d, port=%d]", partition, execPort));
        final NIOEventLoop execEventLoop = new NIOEventLoop();
        final CountDownLatch execLatch = new CountDownLatch(1);
        execEventLoop.setExitOnSigInt(true);
        threads.add(new Thread() {
            public void run() {
                ProtoServer execServer = new ProtoServer(execEventLoop);
                execServer.register(hstore_node);
                execServer.bind(execPort);
                execLatch.countDown();
                execEventLoop.run();
            };
        });
        
        // ----------------------------------------------------------------------------
        // (2) DTXN Engine Thread
        // ----------------------------------------------------------------------------
        LOG.debug(String.format("Launching DTXN Engine [partition=%d]", partition));
        final CountDownLatch dtxnExecLatch = new CountDownLatch(1);
        threads.add(new Thread() {
            public void run() {
                try {
                    execLatch.await();
                } catch (InterruptedException ex) {
                    // Silently ignore...
                    return;
                }
                String[] command = new String[]{
                    dtxnengine_path,                // protodtxnengine
                    execHost + ":" + execPort,      // host:port
                    hstore_conf_path,               // hstore.conf
                    Integer.toString(partition),    // partition #
                    "0"                             // ??
                };
                String ready_msg = "[ready]";
                ThreadUtil.forkLatch(command, ready_msg, dtxnExecLatch);
            }
        });
        
        // ----------------------------------------------------------------------------
        // (4) Procedure Request Listener Thread
        // ----------------------------------------------------------------------------
        threads.add(new Thread() {
            public void run() {
                final NIOEventLoop coordinatorEventLoop = new NIOEventLoop();
                coordinatorEventLoop.setExitOnSigInt(true);
                ProtoRpcChannel channel = new ProtoRpcChannel(coordinatorEventLoop, new InetSocketAddress(coordinatorHost, coordinatorPort));
                Dtxn.Coordinator stub = Dtxn.Coordinator.newStub(channel);
                hstore_node.setDtxnCoordinator(stub);
                VoltProcedureListener voltListener = new VoltProcedureListener(coordinatorEventLoop, hstore_node);
                voltListener.bind();
                LOG.info("HStoreCoordinatorNode is ready for action [partition=" + hstore_node.getLocalPartition() + "]");
                coordinatorEventLoop.run();
            };
        });

        // Blocks!
        ThreadUtil.run(threads);
    }
    
    /**
     * Required Arguments
     * catalog.jar=<path/to/catalog.jar>
     * coordinator.host=<hostname>
     * coordinator.port=<#>
     * 
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG,
                     ArgumentsParser.PARAM_COORDINATOR_HOST,
                     ArgumentsParser.PARAM_COORDINATOR_PORT,
                     ArgumentsParser.PARAM_NODE_HOST,
                     ArgumentsParser.PARAM_NODE_PORT,
                     ArgumentsParser.PARAM_NODE_PARTITION,
                     ArgumentsParser.PARAM_DTXN_CONF,
                     ArgumentsParser.PARAM_DTXN_COORDINATOR,
                     ArgumentsParser.PARAM_DTXN_ENGINE
        );

        // HStoreNode Stuff
        final int local_partition = args.getIntParam(ArgumentsParser.PARAM_NODE_PARTITION);
        final String nodeHost = args.getParam(ArgumentsParser.PARAM_NODE_HOST);
        final int nodePort = args.getIntParam(ArgumentsParser.PARAM_NODE_PORT);

        // HStoreCoordinator Stuff
        final String coordinatorHost = args.getParam(ArgumentsParser.PARAM_COORDINATOR_HOST);
        final int coordinatorPort = args.getIntParam(ArgumentsParser.PARAM_COORDINATOR_PORT);
        
        // Initialize TransactionEstimator stuff
        // Load the Markov models if we were given an input path and pass them to t_estimator
        // HACK: For now we have to create a TransactionEstimator for all partitions, since
        // it is written under the assumption that it was going to be running at just a single partition
        // I'm not proud of this...
        // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
        // stick them into the HStoreCoordinator
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        Map<Integer, Map<Procedure, MarkovGraph>> markovs = null;
        TransactionEstimator t_estimator = new TransactionEstimator(local_partition, p_estimator, args.param_correlations);

        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            File path = new File(args.getParam(ArgumentsParser.PARAM_MARKOV));
            if (path.exists()) {
                markovs = MarkovUtil.load(args.catalog_db, path.getAbsolutePath(), CatalogUtil.getAllPartitionIds(args.catalog_db));
                t_estimator.addMarkovGraphs(markovs.get(local_partition));
            } else {
                LOG.warn("The Markov Graphs file '" + path + "' does not exist");
            }
        }

        // Workload Trace
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT)) {
            ProcedureProfiler.profilingLevel = ProcedureProfiler.Level.INTRUSIVE;
            String traceClass = WorkloadTraceFileOutput.class.getName();
            String tracePath = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT) + ".hstorenode-" + local_partition;
            String traceIgnore = args.getParam(ArgumentsParser.PARAM_WORKLOAD_PROC_EXCLUDE);
            ProcedureProfiler.initializeWorkloadTrace(args.catalog, traceClass, tracePath, traceIgnore);
            
            // For each HStoreNode, we need to make sure that the trace ids start at our offset
            // This will allow us to merge multiple traces together for a benchmark single-run
            long start_id = (local_partition + 1) * 100000l;
            AbstractTraceElement.setStartingId(start_id);
            
            LOG.info("Enabled workload logging '" + tracePath + "' with trace id offset " + start_id);
        }
        
        // setup the EE
        ExecutionSite executor = new ExecutionSite(
                local_partition,
                args.catalog,
                BackendTarget.NATIVE_EE_JNI, // BackendTarget.NULL,
                false,
                p_estimator,
                t_estimator);
        HStoreCoordinatorNode node = new HStoreCoordinatorNode(executor, p_estimator, t_estimator);
        node.setEstimationThresholds(args.thresholds); // may be null...
        
        // Status Monitor
        if (args.hasParam(ArgumentsParser.PARAM_COORDINATOR_STATUS_INTERVAL)) {
            int interval = args.getIntParam(ArgumentsParser.PARAM_COORDINATOR_STATUS_INTERVAL);
            // assert(interval > 0) : "Invalid value '" + interval + "' for parameter " + ArgumentsParser.PARAM_COORDINATOR_STATUS_INTERVAL; 
            node.enableStatusMonitor(interval);
        }
        
        // Bombs Away!
        HStoreCoordinatorNode.launch(node,
                args.getParam(ArgumentsParser.PARAM_DTXN_CONF), 
                args.getParam(ArgumentsParser.PARAM_DTXN_ENGINE),
                args.getParam(ArgumentsParser.PARAM_DTXN_COORDINATOR),
                nodeHost, nodePort,
                coordinatorHost, coordinatorPort);

    }
}
