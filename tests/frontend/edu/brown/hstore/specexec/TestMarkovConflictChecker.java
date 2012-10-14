package edu.brown.hstore.specexec;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.BaseTestCase;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.QueryEstimate;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParametersUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcParameterValueFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovConflictChecker extends BaseTestCase {

    private static final int NUM_PARTITIONS = 10;
    private static final int WORKLOAD_XACT_LIMIT = 20;
    private static final int TARGET_WAREHOUSES[] = { 1, 2 };
    private static final Integer TARGET_DISTRICT_ID = 5;
    @SuppressWarnings("unchecked")
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[]{
        neworder.class,
        ostatByCustomerId.class,
        slev.class,
    };
    
    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    
    private final EstimationThresholds thresholds = new EstimationThresholds();
    private MarkovConflictChecker checker;
    private HStoreSite hstore_site;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        
        if (isFirstSetup()) {
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalog);
            ProcParameterValueFilter filter = new ProcParameterValueFilter().include(1, TARGET_DISTRICT_ID);
            for (int w_id : TARGET_WAREHOUSES) {
                filter.include(0, w_id);
            } // FOR
            ProcedureNameFilter procFilter = new ProcedureNameFilter(false);
            for (Class<? extends VoltProcedure> procClass : TARGET_PROCEDURES) {
                procFilter.include(procClass.getSimpleName(), WORKLOAD_XACT_LIMIT);
            } // FOR
            workload.load(file, catalogContext.database, filter.attach(procFilter));

            // Generate MarkovGraphs per base partition
            markovs = MarkovGraphContainersUtil.createBasePartitionMarkovGraphsContainer(catalogContext.database,
                                                                                         workload, p_estimator);
        }
        assertNotNull(markovs);
        
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton());
        this.checker = new MarkovConflictChecker(catalogContext, thresholds);
    }
    
    // ----------------------------------------------------------------------------------
    // HELPER METHODS
    // ----------------------------------------------------------------------------------
    
    private TransactionTrace getTransactionTrace(Procedure proc, int w_id) throws Exception {
        TransactionTrace txn_trace = null;
        int partition = p_estimator.getHasher().hash(w_id);
        for (TransactionTrace tt : workload.getTraces(proc)) {
            System.err.println(tt + " :: " + w_id + " / " + tt.getParam(0));
            if (partition == p_estimator.getBasePartition(tt)) {
                txn_trace = tt;
                break;
            }
        } // FOR
        return (txn_trace);
    }

    private AbstractTransaction createTransaction(TransactionTrace txn_trace) throws Exception {
        PartitionSet partitions = new PartitionSet(); 
        int base_partition = p_estimator.getBasePartition(txn_trace);
        p_estimator.getAllPartitions(partitions, txn_trace);
        LocalTransaction ts = new LocalTransaction(this.hstore_site);
        ts.testInit(txn_trace.getTransactionId(),
                    base_partition, partitions,
                    txn_trace.getCatalogItem(catalogContext.database));
        return (ts);
    }
    
    private QueryEstimate createQueryEstimate(TransactionTrace txn_trace) {
        List<Statement> stmts = new ArrayList<Statement>();
        List<Long> stmtCtrs = new ArrayList<Long>();
        Histogram<Statement> stmtHistogram = new Histogram<Statement>();  
        for (QueryTrace q : txn_trace.getQueries()) {
            Statement stmt = q.getCatalogItem(catalogContext.database); 
            stmts.add(stmt);
            stmtCtrs.add(stmtHistogram.put(stmt));
        } // FOR
        QueryEstimate est = new QueryEstimate(stmts.toArray(new Statement[0]),
                                              CollectionUtil.toIntArray(stmtCtrs));
        return (est);
    }
    
    // ----------------------------------------------------------------------------------
    // TESTS
    // ----------------------------------------------------------------------------------
    
    
    /**
     * testCanExecuteConflicting
     */
    public void testCanExecuteConflicting() throws Exception {
        Procedure procs[] = {
            this.getProcedure(neworder.class),
            this.getProcedure(ostatByCustomerId.class),
        };
        TransactionTrace traces[] = new TransactionTrace[procs.length];
        
        // We need need at least a TransactionTrace for all of the procedures
        // such that they have the same warehouse ids
        int w_id = -1;
        for (int w : TARGET_WAREHOUSES) {
            boolean foundAll = true;
            for (int i = 0; i < procs.length; i++) {
                traces[i] = this.getTransactionTrace(procs[i], w);
                foundAll = foundAll && (traces[i] != null); 
            } // FOR
            if (foundAll) {
                w_id = w;
                break;
            }
        } // FOR
        assert(w_id >= 0);
        
        
        AbstractTransaction txns[] = new AbstractTransaction[procs.length];
        QueryEstimate queries[] = new QueryEstimate[procs.length];
        for (int i = 0; i < procs.length; i++) {
            txns[i] = this.createTransaction(traces[i]);
            queries[i] = this.createQueryEstimate(traces[i]);
            assert(queries[i].size() > 0);
        }
        
        // Both txns should be going after the same warehouse + district id, so 
        // that means they will be conflicting
//        System.err.println(StringUtil.columns(trace0.debug(catalog_db), trace1.debug(catalog_db)));
        System.err.println(StringUtil.columns(
                Arrays.toString(traces[0].getParams()),
                Arrays.toString(traces[1].getParams())
        ));
        boolean result = this.checker.canExecute(txns[0], queries[0], txns[1], queries[1]);
        assertFalse(result);
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
