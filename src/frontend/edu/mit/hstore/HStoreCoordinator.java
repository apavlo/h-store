package edu.mit.hstore;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ExecutionSite;
import org.voltdb.ProcedureProfiler;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.*;
import org.voltdb.utils.DBBPool;
import org.voltdb.catalog.*;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.FinishRequest;
import edu.mit.dtxn.Dtxn.FinishResponse;

public class HStoreCoordinator implements VoltProcedureListener.Handler {
    private static final Logger LOG = Logger.getLogger(HStoreCoordinator.class.getName());
    private static final AtomicLong NEXT_TXN_ID = new AtomicLong(1); 
    
    private final DBBPool buffer_pool = new DBBPool(true, true);
    private final Dtxn.Coordinator coordinator;
    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final Map<Integer, TransactionEstimator> t_estimators = new HashMap<Integer, TransactionEstimator>();
    private final Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
    private final Map<Integer, Thread> executor_threads = new HashMap<Integer, Thread>();
    // private EstimationThresholds thresholds;
    
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
            for (Integer partition : CatalogUtil.getAllPartitionIds(HStoreCoordinator.this.catalog_db)) {
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
                int completed = HStoreCoordinator.this.completed_txns.get();

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
    public HStoreCoordinator(Dtxn.Coordinator coordinator, PartitionEstimator p_estimator) {
        this.coordinator = coordinator;
        this.catalog_db = p_estimator.getDatabase();
        this.p_estimator = p_estimator;
        // this.thresholds = new EstimationThresholds(); // default values
        
        assert(this.coordinator != null);
        assert(this.catalog_db != null);
        assert(this.p_estimator != null);
        
        // Add in our shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownThread()));
        
        LOG.debug("Starting HStoreCoordinator...");
    }
    
    /**
     * Enable the HStoreCoordinator's status monitor
     * @param interval
     */
    public void enableStatusMonitor(int interval) {
        if (interval > 0) this.status_monitor = new Thread(new StatusMonitorThread(interval)); 
    }
    
    public Dtxn.Coordinator getDtxnCoordinator() {
        return (this.coordinator);
    }
    
    private void setEstimationThresholds(EstimationThresholds thresholds) {
        // this.thresholds = thresholds;
    }

    private void addExecutionSites(Map<Integer, ExecutionSite> executors) {
        this.executors.putAll(executors);
        
        // Start each ExecutionSite thread
        for (int partition : executors.keySet()) {
            ExecutionSite executor = executors.get(partition);
            assert(executor.isCoordinator());
            Thread thread = new Thread(executor);
            thread.start();
            this.executor_threads.put(partition, thread);
        } // FOR
    }
    
    private void addTransactionEstimators(Map<Integer, TransactionEstimator> t_estimators) {
        this.t_estimators.putAll(t_estimators);
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
        
        //
        // Grab the TransactionEstimator for the destination partition and figure out whether
        // this mofo is likely to be single-partition or not
        // If no TransactionEstimator is available, then we'll just say screw it and fire it off
        // as singles-partition
        //
        boolean single_partition = (dest_partition != null);
        TransactionEstimator t_estimator = this.t_estimators.get(dest_partition);
        /* TODO(pavlo)
        if (t_estimator != null) {
            LOG.debug("Using TransactionEstimator to check whether txn #" + txn_id + " is single-partition");
            TransactionEstimator.Estimate estimate = t_estimator.startTransaction(txn_id, catalog_proc, args);
            single_partition = estimate.isSinglePartition(this.thresholds);
        }*/
        //LOG.debug("Single-Partition = " + single_partition);
        InitiateTaskMessage wrapper = new InitiateTaskMessage(txn_id, client_handle, request);
        
        // ----------------------------------------------------------------------------
        // SINGLE-PARTITION
        // Our txn is single-partition, so ship it off to the proper HStoreNode 
        // ----------------------------------------------------------------------------
        if (single_partition && !catalog_proc.getSystemproc()) {
            if (debug) LOG.debug("Sending " + catalog_proc.getName() + " as single-partition txn #" + txn_id + " on partition " + dest_partition);
            this.singlepart_ctr.incrementAndGet();
            this.inflight_txns.put(txn_id, dest_partition);
            
            requestBuilder.addFragment(Dtxn.CoordinatorFragment.PartitionFragment.newBuilder()
                    .setPartitionId(dest_partition)
                    // TODO: Use copyFrom(ByteBuffer) in the newer version of protobuf
                    .setWork(ByteString.copyFrom(wrapper.getBufferForMessaging(this.buffer_pool).b.array())))
                    // 2010-06-18: We need to set this for single-partition txns
                    .setLastFragment(true);
            CoordinatorResponsePassThroughCallback rpcDone = new CoordinatorResponsePassThroughCallback(txn_id, t_estimator, done);
            coordinator.execute(rpc, requestBuilder.build(), rpcDone);

        // ----------------------------------------------------------------------------
        // MULTI-PARTITION
        // Unfortunately, just like getting crabs right before prom, if we're multi-partition then
        // we are going to play with ourselves here at home. Grab the fake ExecutionSite for the
        // destination partition and start executing the control code
        // ----------------------------------------------------------------------------
        } else {
            if (debug) LOG.debug("Executing " + catalog_proc.getName() + " as local/coordinator txn #" + txn_id);
            this.multipart_ctr.incrementAndGet();
            this.inflight_txns.put(txn_id, -1);
            
            ExecutionSite executor = this.executors.get(dest_partition != null ? dest_partition : 0);
            assert(executor != null) : "No ExecutionSite is available for partition " + dest_partition;
            
            // Here's how this all works:
            // (1) We start executing the VoltProcedure locally in the same JVM as this HStoreCoordinator
            // (2) When the VoltProcedure needs to execute some SQL statements, it will call out to this coordinator
            // (3) The VoltProcedure blocks while waiting for a response while the coordinator automatically
            //     passes the FragmentRequest objects to the proper nodes. These are then executed directly in the EE
            //     by the ExecutionSite (VoltProcedure is not needed) 
            // (4) When the FragmentResponse comes back, the RequestWorkCallback will get the response and push them
            //     to the local ExecutionSite. Once all the responses are there, the VoltProcedure is unblocked
            //     and continues to execute like nothing ever happened.
            // (5) When the local VoltProcedure is finished, its ExecutionSite will send the ClientResponse back to
            //     this HStoreCoordinator through FragmentResponsePassThroughCallback, which then forwards the
            //     response inside of the FragmentResponse to the client
            FragmentResponsePassThroughCallback rpcDone = new FragmentResponsePassThroughCallback(txn_id, t_estimator, done);
            executor.doWork(wrapper, rpcDone);
        }
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
            
            HStoreCoordinator.this.coordinator.finish(new ProtoRpcController(), finish, callback);
            
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
            HStoreCoordinator.this.inflight_txns.remove(this.txn_id);
            HStoreCoordinator.this.completed_txns.incrementAndGet();
            assert(!HStoreCoordinator.this.inflight_txns.containsKey(this.txn_id)) :
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
                     ArgumentsParser.PARAM_COORDINATOR_PORT);
        
        String host = args.getParam(ArgumentsParser.PARAM_COORDINATOR_HOST);
        int port = args.getIntParam(ArgumentsParser.PARAM_COORDINATOR_PORT);
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);

        NIOEventLoop eventLoop = new NIOEventLoop();
        eventLoop.setExitOnSigInt(true);

        // Connect to the server with retries
        ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(eventLoop,
                new InetSocketAddress[] { new InetSocketAddress(host, port) });
        assert channels.length == 1;
        assert channels[0] != null;
        Dtxn.Coordinator stub = Dtxn.Coordinator.newStub(channels[0]);
        HStoreCoordinator coordinator = new HStoreCoordinator(stub, p_estimator);
        
        // Initialize TransactionEstimator stuff
        // Load the Markov models if we were given an input path and pass them to t_estimator
        // HACK: For now we have to create a TransactionEstimator for all partitions, since
        // it is written under the assumption that it was going to be running at just a single partition
        // I'm not proud of this...
        // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
        // stick them into the HStoreCoordinator
        Map<Integer, Map<Procedure, MarkovGraph>> markovs = null;
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            File path = new File(args.getParam(ArgumentsParser.PARAM_MARKOV));
            if (path.exists()) {
                markovs = MarkovUtil.load(args.catalog_db, path.getAbsolutePath(), CatalogUtil.getAllPartitionIds(args.catalog_db));
            } else {
                LOG.warn("The Markov Graphs file '" + path + "' does not exist");
            }
        }
        
        Map<Integer, TransactionEstimator> t_estimators = new HashMap<Integer, TransactionEstimator>();
        Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
        for (int partition : CatalogUtil.getAllPartitionIds(args.catalog_db)) {
            TransactionEstimator t_estimator = new TransactionEstimator(partition, p_estimator, args.param_correlations);
            if (markovs != null) {
                t_estimator.addMarkovGraphs(markovs.get(partition));
            }
            t_estimators.put(partition, t_estimator);
            
            ExecutionSite executor = new ExecutionSite(
                                            partition,
                                            args.catalog,
                                            BackendTarget.NATIVE_EE_JNI, // BackendTarget.NULL,
                                            true,
                                            p_estimator,
                                            t_estimator);
            executor.setHStoreCoordinator(coordinator);
            executors.put(partition, executor);
        } // FOR                                
        coordinator.setEstimationThresholds(args.thresholds); // may be null...
        coordinator.addTransactionEstimators(t_estimators);
        coordinator.addExecutionSites(executors);
        
        // Workload Trace
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT)) {
            ProcedureProfiler.profilingLevel = ProcedureProfiler.Level.INTRUSIVE;
            String traceClass = WorkloadTraceFileOutput.class.getName();
            String tracePath = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT) + ".hstorecoordinator";
            String traceIgnore = args.getParam(ArgumentsParser.PARAM_WORKLOAD_PROC_EXCLUDE);
            ProcedureProfiler.initializeWorkloadTrace(args.catalog, traceClass, tracePath, traceIgnore);
            LOG.info("Enabled workload trace logging '" + tracePath + "'");
        }
        
        // Status Monitor
        if (args.hasParam(ArgumentsParser.PARAM_COORDINATOR_STATUS_INTERVAL)) {
            int interval = args.getIntParam(ArgumentsParser.PARAM_COORDINATOR_STATUS_INTERVAL);
            // assert(interval > 0) : "Invalid value '" + interval + "' for parameter " + ArgumentsParser.PARAM_COORDINATOR_STATUS_INTERVAL; 
            coordinator.enableStatusMonitor(interval);
        }

        // Listen for procedure requests
        VoltProcedureListener voltListener = new VoltProcedureListener(eventLoop, coordinator);
        voltListener.bind();
        LOG.info("Setup complete. Waiting for procedure requests from client");
        eventLoop.run();
    }
}
