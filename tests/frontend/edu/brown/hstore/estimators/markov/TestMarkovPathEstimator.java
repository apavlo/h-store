package edu.brown.hstore.estimators.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.estimators.markov.MarkovEstimate;
import edu.brown.hstore.estimators.markov.MarkovPathEstimator;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcParameterArraySizeFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * @author pavlo
 */
public class TestMarkovPathEstimator extends BaseTestCase {
    private static final int WORKLOAD_XACT_LIMIT = 100;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 10;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    
    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterMappingsSet correlations;
    private static TransactionTrace singlep_trace;
    private static TransactionTrace multip_trace;
    private static final Set<Integer> multip_partitions = new HashSet<Integer>();
    private static final List<MarkovVertex> multip_path = new ArrayList<MarkovVertex>();
    
    private MarkovPathEstimator pathEstimator;
    private MarkovEstimate estimate; 
    private Procedure catalog_proc;
    private MarkovGraph graph;
    
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (markovs == null) {
            File file = this.getParameterMappingsFile(ProjectType.TPCC);
            correlations = new ParameterMappingsSet();
            correlations.load(file, catalog_db);
            
            // Workload Filter:
            //  (1) Only include TARGET_PROCEDURE traces
            //  (2) Only include traces with 10 orderline items
            //  (3) Only include traces that execute on the BASE_PARTITION
            //  (4) Limit the total number of traces to WORKLOAD_XACT_LIMIT
            List<ProcParameter> array_params = CatalogUtil.getArrayProcParameters(this.catalog_proc);
            Filter filter = new ProcedureNameFilter(false)
                  .include(TARGET_PROCEDURE.getSimpleName())
                  .attach(new NoAbortFilter())
                  .attach(new ProcParameterArraySizeFilter(array_params.get(0), 10, ExpressionType.COMPARE_EQUAL))
                  .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                  .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            
            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            ((Workload) workload).load(file, catalog_db, filter);
//             for (TransactionTrace xact : workload.getTransactions()) {
//                 System.err.println(xact.debug(catalog_db));
//                 System.err.println(StringUtil.repeat("+", 100));
//             }
            
            // Generate MarkovGraphs
            markovs = MarkovGraphContainersUtil.createBasePartitionMarkovGraphsContainer(catalog_db, workload, p_estimator);
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
        this.pathEstimator = new MarkovPathEstimator(catalogContext, p_estimator);
        this.graph = markovs.get(BASE_PARTITION, this.catalog_proc);
        assertNotNull("No graph exists for " + this.catalog_proc + " on Partition #" + BASE_PARTITION, this.graph);
        this.estimate = new MarkovEstimate(catalogContext);
        this.estimate.init(this.graph.getStartVertex(), 0);
    }
    
    /**
     * testFinish
     */
    public void testFinish() throws Exception {
        pathEstimator.init(this.graph, this.estimate, BASE_PARTITION, singlep_trace.getParams());
        assertTrue(pathEstimator.isInitialized());
        pathEstimator.enableForceTraversal(true);
        
        assertEquals(1.0f, estimate.getConfidenceCoefficient(), MarkovGraph.PROBABILITY_EPSILON);
        pathEstimator.traverse(this.graph.getStartVertex());
        assertTrue(pathEstimator.isInitialized());
        pathEstimator.finish();
        assertFalse(pathEstimator.isInitialized());
    }
    
    /**
     * testMarkovEstimate
     */
    public void testMarkovEstimate() throws Exception {
        pathEstimator.init(this.graph, this.estimate, BASE_PARTITION, singlep_trace.getParams());
        assert(pathEstimator.isInitialized());
        pathEstimator.enableForceTraversal(true);
        pathEstimator.traverse(this.graph.getStartVertex());
        
        List<MarkovVertex> visitPath = pathEstimator.getVisitPath();
        System.err.println(StringUtil.columns(StringUtil.join("\n", visitPath), this.estimate.toString()));
        
        assertFalse(singlep_trace.isAborted());
        assertFalse(visitPath.contains(this.graph.getAbortVertex()));
        
//        System.err.println(singlep_trace.debug(catalog_db));
        System.err.println("Base Partition = " + p_estimator.getBasePartition(singlep_trace));
//        for (QueryTrace qtrace : singlep_trace.getQueries()) {
//            System.err.println(qtrace.debug(catalog_db) + " => " + p_estimator.getAllPartitions(qtrace, BASE_PARTITION));
//        }
        
        for (int p : catalogContext.getAllPartitionIdArray()) {
            assertTrue(estimate.toString(), estimate.isReadOnlyProbabilitySet(p));
            assertTrue(estimate.toString(), estimate.isWriteProbabilitySet(p));
            assertTrue(estimate.toString(), estimate.isFinishProbabilitySet(p));
            
            if (estimate.getFinishProbability(p) < 0.9f) {
                assert(estimate.getTouchedCounter(p) > 0) : String.format("TOUCHED[%d]: %d", p, estimate.getTouchedCounter(p)); 
                assert(MathUtil.greaterThan(estimate.getWriteProbability(p), 0.0f, 0.01f)) : String.format("WRITE[%d]: %f", p, estimate.getWriteProbability(p));
            } else if (MathUtil.equals(estimate.getFinishProbability(p), 0.01f, 0.03f)) {
                assertEquals(0, estimate.getTouchedCounter(p));
                assertEquals(0.0f, estimate.getWriteProbability(p), MarkovGraph.PROBABILITY_EPSILON);
            }
        } // FOR
        assert(estimate.isSinglePartitionProbabilitySet());
        assert(estimate.isAbortProbabilitySet());
        assert(estimate.getSinglePartitionProbability() < 1.0f);
        
        assertTrue(estimate.toString(), estimate.isConfidenceCoefficientSet());
        assert(estimate.getConfidenceCoefficient() >= 0f);
        assert(estimate.getConfidenceCoefficient() <= 1f);
    }
    
    /**
     * testSinglePartition
     */
    public void testSinglePartition() throws Exception {
        MarkovVertex start = this.graph.getStartVertex();
        MarkovVertex commit = this.graph.getCommitVertex();
        MarkovVertex abort = this.graph.getAbortVertex();
        
//        MarkovPathEstimator.LOG.setLevel(Level.DEBUG);
        pathEstimator.init(this.graph, this.estimate, BASE_PARTITION, singlep_trace.getParams());
        pathEstimator.enableForceTraversal(true);
        pathEstimator.traverse(this.graph.getStartVertex());
        ArrayList<MarkovVertex> path = new ArrayList<MarkovVertex>(pathEstimator.getVisitPath());
        assertTrue(estimate.isConfidenceCoefficientSet());
        float confidence = this.estimate.getConfidenceCoefficient();
        
//        System.err.println("INITIAL PATH:\n" + StringUtil.join("\n", path));
//        System.err.println("CONFIDENCE: " + confidence);
//        System.err.println("DUMPED FILE: " + MarkovUtil.exportGraphviz(this.graph, false, this.graph.getPath(path)).writeToTempFile());
        
        assertEquals(start, CollectionUtil.first(path));
        assertEquals(commit, CollectionUtil.last(path));
        assertFalse(path.contains(abort));
        assert(confidence > 0.0f);
        
        // All of the vertices should only have the base partition in their partition set
        for (int i = 1, cnt = path.size() - 1; i < cnt; i++) {
            MarkovVertex v = path.get(i);
            assertEquals(1, v.getPartitions().size());
            assert(v.getPartitions().contains(BASE_PARTITION));
        } // FOR
//        MarkovPathEstimator.LOG.setLevel(Level.INFO);
        
//        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(this.graph, true, this.graph.getPath(path));
//        FileUtil.writeStringToFile("/tmp/dump.dot", gv.export(this.graph.getProcedure().getName()));
    }
    
    /**
     * testMultiPartition
     */
    public void testMultiPartition() throws Exception {
//        System.err.println("MULTI-PARTITION: " + multip_trace);

        MarkovVertex start = this.graph.getStartVertex();
        MarkovVertex commit = this.graph.getCommitVertex();
        MarkovVertex abort = this.graph.getAbortVertex();
        
        pathEstimator.init(this.graph, this.estimate, BASE_PARTITION, multip_trace.getParams());
        pathEstimator.enableForceTraversal(true);
        pathEstimator.traverse(this.graph.getStartVertex());
        ArrayList<MarkovVertex> path = new ArrayList<MarkovVertex>(pathEstimator.getVisitPath());
        
//        System.err.println("INITIAL PATH:\n" + StringUtil.join("\n", path));
        
        assertEquals(start, CollectionUtil.first(path));
        assertEquals(commit, CollectionUtil.last(path));
        assertFalse(path.contains(abort));
        
        // All of the vertices should only have the base partition in their partition set
        Set<Integer> touched_partitions = new HashSet<Integer>();
        for (MarkovVertex v : path) {
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
