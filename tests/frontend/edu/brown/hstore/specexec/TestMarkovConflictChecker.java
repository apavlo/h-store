package edu.brown.hstore.specexec;

import java.io.File;

import org.junit.Before;
import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.BaseTestCase;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParametersUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovConflictChecker extends BaseTestCase {

    private static final int NUM_PARTITIONS = 10;
    private static final int WORKLOAD_XACT_LIMIT = 10;
    private static final int BASE_PARTITIONS[] = { 1, 2 };
    @SuppressWarnings("unchecked")
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[]{
        neworder.class,
        delivery.class,
        ostatByCustomerId.class,
        slev.class,
    };
    
    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    
    private final EstimationThresholds thresholds = new EstimationThresholds();
    private MarkovConflictChecker checker;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        if (isFirstSetup()) {
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            Filter filter = new BasePartitionTxnFilter(p_estimator, BASE_PARTITIONS);
            ProcedureNameFilter procFilter = new ProcedureNameFilter(false);
            for (Class<? extends VoltProcedure> procClass : TARGET_PROCEDURES) {
                procFilter.include(procClass.getSimpleName(), WORKLOAD_XACT_LIMIT);
            }
            workload.load(file, catalogContext.database, filter.attach(procFilter));

            // Generate MarkovGraphs per base partition
            markovs = MarkovGraphContainersUtil.createBasePartitionMarkovGraphsContainer(catalogContext.database,
                                                                                         workload, p_estimator);
        }
        assertNotNull(markovs);
        
        this.checker = new MarkovConflictChecker(catalogContext, thresholds);
    }
    
    /**
     * testEqualParameters
     */
    public void testEqualParameters() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        StmtParameter catalog_stmt_param = CollectionUtil.first(catalog_stmt.getParameters());
        assertNotNull(catalog_stmt_param);
        
        TransactionTrace txn_trace = CollectionUtil.first(workload.getTraces(catalog_proc));
        assertNotNull(txn_trace);
        
        ParameterSet params = new ParameterSet(txn_trace.getParams());
        for (ProcParameter catalog_param : catalog_proc.getParameters()) {
            if (catalog_param.getIsarray()) {
                Object inner[] = (Object[])params.toArray()[catalog_param.getIndex()];
                for (int i = 0; i < inner.length; i++) {
                    ParameterMapping pm = new ParameterMapping(catalog_stmt, 0, catalog_stmt_param, catalog_param, i, 1.0d); 
                    assertTrue(this.checker.equalParameters(params, pm, params, pm));
                } // FOR
            } else {
                ParameterMapping pm = new ParameterMapping(catalog_stmt, 0, catalog_stmt_param, catalog_param, ParametersUtil.NULL_PROC_PARAMETER_OFFSET, 1.0d); 
                assertTrue(this.checker.equalParameters(params, pm, params, pm));
            }
        } // FOR
        
        
        
    }
}
