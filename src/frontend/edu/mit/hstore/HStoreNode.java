package edu.mit.hstore;

import java.io.File;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ExecutionSite;
import org.voltdb.ProcedureProfiler;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;

import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoServer;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.mit.dtxn.Dtxn;

public class HStoreNode extends Dtxn.ExecutionEngine {
    private static final Logger LOG = Logger.getLogger(HStoreNode.class.getName());
    
    private final ExecutionSite executor;
    private final Thread executor_thread;

    public HStoreNode(ExecutionSite executor) {
        this.executor = executor;
        this.executor_thread = new Thread(this.executor);
        this.executor_thread.start();
        LOG.debug("Starting new HStoreNode for partition " + this.executor.getPartitionId());
    }

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

    public static void main(String[] arguments) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(arguments);
        args.require(
                ArgumentsParser.PARAM_CATALOG,
                ArgumentsParser.PARAM_NODE_PORT,
                ArgumentsParser.PARAM_NODE_PARTITION
        );
        Integer local_partition = args.getIntParam(ArgumentsParser.PARAM_NODE_PARTITION);
        assert local_partition != null : "Must pass in the local partition id";
        Integer listenPort = args.getIntParam(ArgumentsParser.PARAM_NODE_PORT);
        assert listenPort != null : "Must pass a listenPort";

        // Partition Estimator
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db, args.hasher);
        
        // Transaction Estimator
        TransactionEstimator t_estimator = null;
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            File path = new File(args.getParam(ArgumentsParser.PARAM_MARKOV));
            if (path.exists()) {
                t_estimator = new TransactionEstimator(local_partition, p_estimator, args.param_correlations);
                t_estimator.addMarkovGraphs(MarkovUtil.load(args.catalog_db, path.getAbsolutePath(), local_partition));
            } else {
                if (LOG.isDebugEnabled()) LOG.warn("The Markov Graphs file '" + path + "' does not exist");
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
        ExecutionSite executor = new ExecutionSite(local_partition, args.catalog, BackendTarget.NATIVE_EE_JNI, p_estimator, t_estimator);
        HStoreNode node = new HStoreNode(executor);

        // Serve RPCs
        NIOEventLoop eventLoop = new NIOEventLoop();
        eventLoop.setExitOnSigInt(true);
        ProtoServer server = new ProtoServer(eventLoop);
        server.register(node);
        server.bind(listenPort);
        LOG.info("HStoreNode is now listening on port " + listenPort);
        eventLoop.run();
    }
}