package edu.brown.markov;

import java.io.File;

import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovGraphProbabilities extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = UpdateSubscriberData.class;
    private static final int WORKLOAD_XACT_LIMIT = 10;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 4;

    private static Workload workload;
    private static ParameterMappingsSet correlations;
    private static MarkovGraph markov;
    
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);

        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_stmt = CollectionUtil.first(this.catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        
        if (isFirstSetup()) {
            
            File file = this.getParameterMappingsFile(ProjectType.TM1);
            correlations = new ParameterMappingsSet();
            correlations.load(file, catalogContext.database);

            file = this.getWorkloadFile(ProjectType.TM1);
            workload = new Workload(catalogContext.catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            Filter filter = new ProcedureNameFilter(false)
                    .include(TARGET_PROCEDURE.getSimpleName())
                    .attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalogContext.database, filter);
            assert(workload.getTransactionCount() > 0);
            
            markov = new MarkovGraph(catalog_proc).initialize();
            for (TransactionTrace txn_trace : workload.getTransactions()) {
                markov.processTransaction(txn_trace, p_estimator);
            } // FOR
            markov.calculateProbabilities(catalogContext.getAllPartitionIds());
        }
    }
    
    /**
     * testVertexProbabilities
     */
    public void testVertexProbabilities() {
        // There should only be one query vertex and it should be marked as single-partitioned
        MarkovVertex v = markov.getVertex(catalog_stmt);
        assertNotNull(v);
        
        System.err.println(v.debug());
        
//        assertEquals(1.0f, v.getSinglePartitionProbability());
        assert(v.getAbortProbability() > 0.0) : v.getAbortProbability(); 
        
        for (int p : catalogContext.getAllPartitionIds()) {
            if (p == BASE_PARTITION) {
//                assertEquals("ReadOnly("+p+")", 0.0f, v.getReadOnlyProbability(p), MarkovGraph.PROBABILITY_EPSILON);
                assertEquals("Write("+p+")", 1.0f, v.getWriteProbability(p), MarkovGraph.PROBABILITY_EPSILON);
                assertEquals("Done("+p+")", 0.0f, v.getDoneProbability(p), MarkovGraph.PROBABILITY_EPSILON);
            } else {
//                assertEquals("ReadOnly("+p+")", 1.0f, v.getReadOnlyProbability(p), MarkovGraph.PROBABILITY_EPSILON);
                assertEquals("Write("+p+")", 0.0f, v.getWriteProbability(p), MarkovGraph.PROBABILITY_EPSILON);
                assertEquals("Done("+p+")", 1.0f, v.getDoneProbability(p), MarkovGraph.PROBABILITY_EPSILON);
            }
        } // FOR
    }
    
}
