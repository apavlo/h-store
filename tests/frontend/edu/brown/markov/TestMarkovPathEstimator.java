package edu.brown.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractWorkload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovPathEstimator extends BaseTestCase {
    private static final int WORKLOAD_XACT_LIMIT = 10000;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 10;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    
    private static AbstractWorkload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterCorrelations correlations;
    private static TransactionTrace singlep_trace;
    private static TransactionTrace multip_trace;
    private static final Set<Integer> multip_partitions = new HashSet<Integer>();
    private static final List<Vertex> multip_path = new ArrayList<Vertex>();
    
    private TransactionEstimator t_estimator;
    private Procedure catalog_proc;
    private MarkovGraph graph;
    
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (markovs == null) {
            File file = this.getCorrelationsFile(ProjectType.TPCC);
            correlations = new ParameterCorrelations();
            correlations.load(file.getAbsolutePath(), catalog_db);
            
            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new WorkloadTraceFileOutput(catalog);
            ProcedureNameFilter filter = new ProcedureNameFilter();
            filter.include(TARGET_PROCEDURE.getSimpleName(), WORKLOAD_XACT_LIMIT);
            
            // Custom filter that only grabs neworders that go to our BASE_PARTITION
            filter.attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION));
            ((WorkloadTraceFileOutput) workload).load(file.getAbsolutePath(), catalog_db, filter);
            
            // Generate MarkovGraphs
            markovs = MarkovUtil.createGraphs(catalog_db, workload, p_estimator);
            assertNotNull(markovs);
            
            // Find a single-partition and multi-partition trace
            multip_partitions.add(BASE_PARTITION);
            for (TransactionTrace xact : workload.getTransactions()) {
                Object ol_supply_w_ids[] = (Object[])xact.getParam(5);
                assert(ol_supply_w_ids != null);
                boolean same_partition = true;
                for (Object i : ol_supply_w_ids) {
                    Integer partition = p_estimator.getHasher().hash(Integer.valueOf(i.toString()));
                    same_partition = same_partition && (partition == BASE_PARTITION);
                    if (same_partition == false && multip_trace == null) {
                        multip_partitions.add(partition);
                    }
                } // FOR
                if (same_partition && singlep_trace == null) singlep_trace = xact;
                if (same_partition == false && multip_trace == null) {
                    multip_trace = xact;
                    multip_path.addAll(markovs.get(BASE_PARTITION, this.catalog_proc).processTransaction(xact, p_estimator));
                }
                if (singlep_trace != null && multip_trace != null) break;
            } // FOR
        }
        assertNotNull(singlep_trace);
        assertNotNull(multip_trace);
        assert(multip_partitions.size() > 1);
        assertFalse(multip_path.isEmpty());

        // Setup
        this.graph = markovs.get(BASE_PARTITION, this.catalog_proc);
        assertNotNull("No graph exists for " + this.catalog_proc + " on Partition #" + BASE_PARTITION, this.graph);
        this.t_estimator = new TransactionEstimator(BASE_PARTITION, p_estimator, correlations);
        this.t_estimator.addMarkovGraphs(markovs.get(BASE_PARTITION));
    }
    
    /**
     * testSinglePartition
     */
    public void testSinglePartition() throws Exception {
        Vertex start = this.graph.getStartVertex();
        Vertex commit = this.graph.getCommitVertex();
        Vertex abort = this.graph.getAbortVertex();
        
        MarkovPathEstimator estimator = new MarkovPathEstimator(this.graph, this.t_estimator, singlep_trace.getParams());
        estimator.traverse(this.graph.getStartVertex());
        Vector<Vertex> path = new Vector<Vertex>(estimator.getVisitPath());
        
        assertEquals(start, path.firstElement());
        assertEquals(commit, path.lastElement());
        assertFalse(path.contains(abort));
        
        // All of the vertices should only have the base partition in their partition set
        for (int i = 1, cnt = path.size() - 1; i < cnt; i++) {
            Vertex v = path.get(i);
            assertEquals(1, v.getPartitions().size());
            assert(v.getPartitions().contains(BASE_PARTITION));
        } // FOR
    }
    
    
    /**
     * testMultiPartition
     */
    public void testMultiPartition() throws Exception {
//        System.err.println("MULTI-PARTITION: " + multip_trace);

        Vertex start = this.graph.getStartVertex();
        Vertex commit = this.graph.getCommitVertex();
        Vertex abort = this.graph.getAbortVertex();
        
        MarkovPathEstimator estimator = new MarkovPathEstimator(this.graph, this.t_estimator, multip_trace.getParams());
        estimator.traverse(this.graph.getStartVertex());
        Vector<Vertex> path = new Vector<Vertex>(estimator.getVisitPath());
        
//        System.err.println("INITIAL PATH:\n" + StringUtil.join("\n", path));
        
        assertEquals(start, path.firstElement());
        assertEquals(commit, path.lastElement());
        assertFalse(path.contains(abort));
        
        // All of the vertices should only have the base partition in their partition set
        Set<Integer> touched_partitions = new HashSet<Integer>();
        for (Vertex v : path) {
            touched_partitions.addAll(v.getPartitions());
        } // FOR
//        System.err.println("Expected Partitions: " + multip_partitions);
//        System.err.println("Touched Partitions:  " + touched_partitions);
//        System.err.println("MULTI-PARTITION PATH: " + path);

//        this.writeGraphviz(multip_path);
//        this.writeGraphviz(path);
//        assertEquals(multip_partitions, touched_partitions);
    }
}
