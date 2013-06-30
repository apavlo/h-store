package edu.brown.hstore.specexec.checkers;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.UpdateNewOrder;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.specexec.checkers.MarkovConflictChecker.StatementCache;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParametersUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
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
    
    private final TPCCProjectBuilder builder = new TPCCProjectBuilder() {
        {
            this.addAllDefaults();
            this.addProcedure(UpdateNewOrder.class);
        }
    };
    
    @Before
    public void setUp() throws Exception {
        this.reset(ProjectType.TPCC);
        super.setUp(this.builder);
        this.addPartitions(NUM_PARTITIONS);
        
        if (isFirstSetup()) {
            
            File file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalogContext.catalog);
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
            markovs = MarkovGraphsContainerUtil.createBasePartitionMarkovGraphsContainer(catalogContext,
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
            // System.err.println(tt + " :: " + w_id + " / " + tt.getParam(0));
            if (partition == p_estimator.getBasePartition(tt)) {
                txn_trace = tt;
                break;
            }
        } // FOR
        return (txn_trace);
    }
    
    private TransactionTrace[] getTransactionTraces(Procedure procs[], boolean differentWarehouse) throws Exception {
        TransactionTrace traces[] = new TransactionTrace[procs.length];
        int last = -1;
        
        // Different W_ID
        if (differentWarehouse) {
            for (int i = 0; i < procs.length; i++) {
                for (int w_id : TARGET_WAREHOUSES) {
                    if (w_id == last) continue;
                    traces[i] = this.getTransactionTrace(procs[i], w_id);
                    if (traces[i] != null) {
                        last = w_id;
                        break;
                    }
                } // FOR
                assertNotNull(traces[i]);
                assert(last >= 0);
            } // FOR
        }
        // Same W_ID
        else {
            for (int w_id : TARGET_WAREHOUSES) {
                boolean foundAll = true;
                for (int i = 0; i < procs.length; i++) {
                    traces[i] = this.getTransactionTrace(procs[i], w_id);
                    foundAll = foundAll && (traces[i] != null); 
                } // FOR
                if (foundAll) {
                    last = w_id;
                    break;
                }
            } // FOR
        }
        assert(last >= 0);
        
        return (traces);
    }

    private AbstractTransaction createTransaction(TransactionTrace txn_trace) throws Exception {
        PartitionSet partitions = new PartitionSet(); 
        int base_partition = p_estimator.getBasePartition(txn_trace);
        p_estimator.getAllPartitions(partitions, txn_trace);
        LocalTransaction ts = new LocalTransaction(this.hstore_site);
        ts.testInit(txn_trace.getTransactionId(),
                    base_partition,
                    partitions,
                    txn_trace.getCatalogItem(catalogContext.database),
                    txn_trace.getParams());
        return (ts);
    }
    
    private List<CountedStatement> createQueryEstimate(TransactionTrace txn_trace) {
        return this.createQueryEstimate(txn_trace, null);
    }
    
    private List<CountedStatement> createQueryEstimate(TransactionTrace txn_trace, Statement start) {
        Histogram<Statement> stmtHistogram = new ObjectHistogram<Statement>();
        List<CountedStatement> queries = new ArrayList<CountedStatement>();
        boolean include = (start == null);
        for (QueryTrace q : txn_trace.getQueries()) {
            Statement stmt = q.getCatalogItem(catalogContext.database);
            if (include == false && stmt.equals(start) == false) continue;
            include = true;
            queries.add(new CountedStatement(stmt, (int)stmtHistogram.get(stmt, 0l)));
            stmtHistogram.put(stmt);
        } // FOR
        return (queries);
    }
    
    // ----------------------------------------------------------------------------------
    // TESTS
    // ----------------------------------------------------------------------------------
    
    /**
     * testNonConflictingReadOnly
     */
    public void testNonConflictingReadOnly() throws Exception {
        // Quickly check that two read-only txns are non-conflicting 
        // even without a txn estimate
        Procedure proc = this.getProcedure(slev.class);
        int basePartition = 0;
        PartitionSet partitions = catalogContext.getAllPartitionIds(); 
        
        LocalTransaction ts0 = new LocalTransaction(this.hstore_site);
        Object params0[] = new Object[]{ 0, 1, 2 };
        ts0.testInit(10000l, basePartition, partitions, proc, params0);
        
        LocalTransaction ts1 = new LocalTransaction(this.hstore_site);
        Object params1[] = new Object[]{ 0, 1, 2 };
        ts1.testInit(10001l, basePartition, partitions, proc, params1);
        
        boolean ret = this.checker.hasConflictBefore(ts0, ts1, basePartition);
        assertFalse(ret);
    }
    
    /**
     * testNonConflictingDisparateTables
     */
    public void testNonConflictingDisparateTables() throws Exception {
        // Quickly check that two read-only txns are non-conflicting 
        // even without a txn estimate
        int basePartition = 0;
        PartitionSet partitions = catalogContext.getAllPartitionIds(); 

        Procedure proc0 = this.getProcedure(paymentByCustomerId.class);
        LocalTransaction ts0 = new LocalTransaction(this.hstore_site);
        Object params0[] = new Object[]{ 0, 1, 2 };
        ts0.testInit(10000l, basePartition, partitions, proc0, params0);
        
        Procedure proc1 = this.getProcedure(UpdateNewOrder.class);
        LocalTransaction ts1 = new LocalTransaction(this.hstore_site);
        Object params1[] = new Object[]{ 0, 0 };
        ts1.testInit(10001l, basePartition, partitions, proc1, params1);
        
        boolean ret = this.checker.hasConflictBefore(ts0, ts1, basePartition);
        assertFalse(ret);
    }
    
    /**
     * testColumnStmtParameters
     */
    public void testColumnStmtParameters() throws Exception {
        Procedure proc = this.getProcedure(neworder.class);
        Statement stmt = this.getStatement(proc, "getDistrict");
        StatementCache cache = this.checker.stmtCache.get(stmt);
        assertNotNull(stmt.fullName(), cache);
        
        Collection<Column> cols = CatalogUtil.getReferencedColumns(stmt);
        assertFalse(cols.isEmpty());
        // System.err.println(stmt.fullName() + " -> " + cols + "\n" + StringUtil.formatMaps(cache.colParams));
        
        Set<StmtParameter> seenParams = new HashSet<StmtParameter>();
        for (Column col : cols) {
            StmtParameter param = cache.colParams.get(col);
            assertNotNull(col.fullName(), param);
            assertFalse(param.fullName(), seenParams.contains(param));
            seenParams.add(param);
        } // FOR
        assertEquals(cols.size(), seenParams.size());
    }
    
    /**
     * testStatementCache
     */
    public void testStatementCache() throws Exception {
        Procedure proc0 = this.getProcedure(slev.class);
        Statement stmt0 = this.getStatement(proc0, "GetStockCount");
        Procedure proc1 = this.getProcedure(neworder.class);
        Statement stmt1 = this.getStatement(proc1, "createOrderLine");
        
        // STMT0 is going to try to read to a table that STMT1 will write to
        // So we should be able to see that conflict
        StatementCache cache = this.checker.stmtCache.get(stmt0);
        assertNotNull(stmt0.fullName(), cache);
        
        ConflictPair cp = cache.conflicts.get(stmt1);
        assertNotNull(stmt0.fullName()+"->"+stmt1.fullName(), cp);
        assertTrue(cp.getAlwaysconflicting());
    }
    
    /**
     * testCanExecuteNonConflicting
     */
    public void testCanExecuteNonConflicting() throws Exception {
        Procedure procs[] = {
            this.getProcedure(neworder.class),
            this.getProcedure(ostatByCustomerId.class),
        };
        Statement startStmts[] = {
            this.getStatement(procs[0], "updateStock"),
            null,
        };
        TransactionTrace traces[] = this.getTransactionTraces(procs, true);
        
        AbstractTransaction txns[] = new AbstractTransaction[procs.length];
        @SuppressWarnings("unchecked")
        List<CountedStatement> queries[] = (List<CountedStatement>[])(new ArrayList<?>[procs.length]);
        for (int i = 0; i < procs.length; i++) {
            txns[i] = this.createTransaction(traces[i]);
            queries[i] = this.createQueryEstimate(traces[i], startStmts[i]);
            assert(queries[i].size() > 0);
        } // FOR
        
        // Both txns should be going after the same warehouse + district id, so 
        // that means they will be conflicting
//        System.err.println(StringUtil.columns(trace0.debug(catalog_db), trace1.debug(catalog_db)));
//        System.err.println(StringUtil.columns(
//                Arrays.toString(traces[0].getParams()),
//                Arrays.toString(traces[1].getParams())
//        ));
//        System.err.println(StringUtil.columns(
//                StringUtil.join("\n", queries[0].getFirst()),
//                StringUtil.join("\n", queries[1].getFirst())
//        ));
        boolean result = this.checker.canExecute(txns[0], queries[0], txns[1], queries[1]);
        assertTrue(result);
    }
    
    /**
     * testCanExecuteNonConflictingUniqueIndex
     */
    public void testCanExecuteNonConflictingUniqueIndex() throws Exception {
        Procedure procs[] = {
            this.getProcedure(ostatByCustomerId.class),
            this.getProcedure(neworder.class),
        };
        TransactionTrace traces[] = this.getTransactionTraces(procs, true);
        
        AbstractTransaction txns[] = new AbstractTransaction[procs.length];
        @SuppressWarnings("unchecked")
        List<CountedStatement> queries[] = (List<CountedStatement>[])(new ArrayList<?>[procs.length]);
        for (int i = 0; i < procs.length; i++) {
            txns[i] = this.createTransaction(traces[i]);
            queries[i] = this.createQueryEstimate(traces[i]);
            assert(queries[i].size() > 0);
        } // FOR
        
        // Both txns should be going after the same warehouse + district id, so 
        // that means they will be conflicting
//        System.err.println(StringUtil.columns(trace0.debug(catalog_db), trace1.debug(catalog_db)));
//        System.err.println(StringUtil.columns(
//            Arrays.toString(traces[0].getParams()),
//            Arrays.toString(traces[1].getParams())
//        ));
//        System.err.println(StringUtil.columns(
//            queries[0].debug(),
//            queries[1].debug()
//        ));
        boolean result = this.checker.canExecute(txns[0], queries[0], txns[1], queries[1]);
        assertTrue(result);
    }
    
    /**
     * testCanExecuteConflictingUniqueIndex
     */
    public void testCanExecuteConflictingUniqueIndex() throws Exception {
        Procedure procs[] = {
            this.getProcedure(ostatByCustomerId.class),
            this.getProcedure(neworder.class),
        };
        TransactionTrace traces[] = this.getTransactionTraces(procs, false);
        
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
        @SuppressWarnings("unchecked")
        List<CountedStatement> queries[] = (List<CountedStatement>[])(new ArrayList<?>[procs.length]);
        for (int i = 0; i < procs.length; i++) {
            txns[i] = this.createTransaction(traces[i]);
            queries[i] = this.createQueryEstimate(traces[i]);
            assert(queries[i].size() > 0);
        } // FOR
        
        // Both txns should be going after the same warehouse + district id, so 
        // that means they will be conflicting
//        System.err.println(StringUtil.columns(trace0.debug(catalog_db), trace1.debug(catalog_db)));
//        System.err.println(StringUtil.columns(
//            Arrays.toString(traces[0].getParams()),
//            Arrays.toString(traces[1].getParams())
//        ));
//        System.err.println(StringUtil.columns(
//            queries[0].debug(),
//            queries[1].debug()
//        ));
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
