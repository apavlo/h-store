package edu.brown.hstore;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.ClientResponseDebug;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.regressionsuites.specexecprocs.BlockableSendPayment;
import org.voltdb.types.SpeculationType;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.smallbank.SmallBankProjectBuilder;
import edu.brown.benchmark.smallbank.procedures.SendPayment;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.hstore.specexec.checkers.AbstractConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.mappings.ParameterMapping;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * PartitionExecutor Tests for Early 2PC Optimization
 * @author pavlo
 */
public class TestPartitionExecutorEarly2PC extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestPartitionExecutorEarly2PC.class);
    
    private static final int NUM_PARTITIONS = 2;
    private static final int BASE_PARTITION = 0;
    private static final int NOTIFY_TIMEOUT = 1000; // ms
    private static final int NUM_SPECEXEC_TXNS = 5;
    private static final int WORKLOAD_XACT_LIMIT = 5000;

    // We want to make sure that the PartitionExecutor only spec execs 
    // at the 2PC stall points.
    private static final Collection<SpeculationType> IGNORED_STALLPOINTS = new HashSet<SpeculationType>();
    static {
        CollectionUtil.addAll(IGNORED_STALLPOINTS, SpeculationType.values());
        IGNORED_STALLPOINTS.remove(SpeculationType.SP3_REMOTE);
    } // STATIC
    
    private static Workload workload;
    private static File markovsFile;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    private Procedure spProc;
    private Procedure blockingProc;
    
    private PartitionExecutor executors[];
    private PartitionExecutor baseExecutor;
    private PartitionExecutor remoteExecutor;
    
    private final SmallBankProjectBuilder builder = new SmallBankProjectBuilder() {
        {
            this.addAllDefaults();
            this.addProcedure(BlockableSendPayment.class);
        }
    };
    
    /**
     * Simple conflict checker that allows anything to be executed
     */
    private final AbstractConflictChecker checker = new AbstractConflictChecker(null) {
        @Override
        public boolean shouldIgnoreProcedure(Procedure proc) {
            return (false);
        }
        @Override
        public boolean canExecute(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
            return (true);
        }
    };
    
    // --------------------------------------------------------------------------------------------
    // SETUP
    // --------------------------------------------------------------------------------------------
    
    @Before
    public void setUp() throws Exception {
        super.setUp(this.builder);
        initializeCatalog(1, 1, NUM_PARTITIONS);

        this.spProc = this.getProcedure(SendPayment.class);
        this.blockingProc = this.getProcedure(BlockableSendPayment.class);
        
        if (isFirstSetup()) {
            
            // LOAD SAMPLE WORKLOAD
            Filter filter =  new ProcedureNameFilter(false)
                    .include(this.spProc.getName())
                    .attach(new NoAbortFilter())
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            File workloadFile = this.getWorkloadFile(ProjectType.SMALLBANK);
            Workload workload0 = new Workload(catalogContext.catalog).load(workloadFile, catalogContext.database, filter);
            
            // DUPLICATE ALL SENDPAYMENTS TO BE BLOCKABLE SENDPAYMENTS
            File tempFile = FileUtil.getTempFile("workload", true);
            workload0.save(tempFile, catalogContext.database);
            assertTrue(tempFile.exists());
            String dump = FileUtil.readFile(tempFile);
            assertFalse(dump.isEmpty());
            FileUtil.writeStringToFile(tempFile, dump.replace(this.spProc.getName(), this.blockingProc.getName()));
            Workload workload1 = new Workload(catalogContext.catalog).load(tempFile, catalogContext.database);
            assertEquals(workload0.getTransactionCount(), workload1.getTransactionCount());
            assertEquals(workload0.getQueryCount(), workload1.getQueryCount());
            // Make sure we change their txn ids
            for (TransactionTrace tt : workload1) {
                tt.setTransactionId(tt.getTransactionId() + 1000000);
            } // FOR
            
            // DUPLICATE PARAMETER MAPPINGS
            for (ParameterMapping pm : catalogContext.paramMappings.get(this.spProc)) {
                ParameterMapping clone = pm.clone();
                clone.procedure_parameter = this.blockingProc.getParameters().get(pm.procedure_parameter.getIndex());
                clone.statement = this.blockingProc.getStatements().get(pm.statement.getName());
                clone.statement_parameter = clone.statement.getParameters().get(pm.statement_parameter.getIndex());
                catalogContext.paramMappings.add(clone);
            } // FOR

            // COMBINE INTO A SINGLE WORKLOAD HANDLE
            workload = new Workload(catalogContext.catalog, workload0, workload1);
            assertEquals(workload.getTransactionCount(), workload1.getTransactionCount() * 2);
            assertEquals(workload.getQueryCount(), workload1.getQueryCount() * 2);
            
            // GENERATE MARKOV GRAPHS
            Map<Integer, MarkovGraphsContainer> markovs = MarkovGraphsContainerUtil.createMarkovGraphsContainers(
                                                                catalogContext,
                                                                workload,
                                                                p_estimator,
                                                                MarkovGraphsContainer.class);
            assertNotNull(markovs);
            markovsFile = FileUtil.getTempFile("markovs");
            MarkovGraphsContainerUtil.save(markovs, markovsFile);
        }
        assert(markovsFile.exists());
        
        for (TransactionCounter tc : TransactionCounter.values()) {
            tc.clear();
        } // FOR
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.specexec_enable = true;
        this.hstore_conf.site.specexec_ignore_stallpoints = StringUtil.join(",", IGNORED_STALLPOINTS);
        this.hstore_conf.site.txn_client_debug = true;
        this.hstore_conf.site.txn_counters = true;
        this.hstore_conf.site.exec_force_singlepartitioned = true;
        this.hstore_conf.site.pool_profiling = true;
        this.hstore_conf.site.markov_enable = true;
        this.hstore_conf.site.markov_path = markovsFile.getAbsolutePath();
        
        this.hstore_site = this.createHStoreSite(catalog_site, hstore_conf);
        this.client = createClient();
        
        this.baseExecutor = this.hstore_site.getPartitionExecutor(BASE_PARTITION);
        assertNotNull(this.baseExecutor);
        this.remoteExecutor = this.hstore_site.getPartitionExecutor(BASE_PARTITION+1);
        assertNotNull(this.remoteExecutor);
        assertNotSame(this.baseExecutor.getPartitionId(), this.remoteExecutor.getPartitionId());
        
        // Make sure the HStoreSite initializes all of its PartitionExecutors with
        // a MarkovEstimator.
        this.executors = new PartitionExecutor[]{ this.baseExecutor, this.remoteExecutor };
        for (PartitionExecutor executor : this.executors) {
            TransactionEstimator t_estimator = executor.getTransactionEstimator();
            assertNotNull(t_estimator);
            assertEquals(MarkovEstimator.class, t_estimator.getClass());
        } // FOR
        
        // Make sure that we replace the conflict checker on the remote partition
        // so that it can schedule our speculative txns
        PartitionExecutor.Debug remoteDebug = this.remoteExecutor.getDebugContext();
        remoteDebug.getSpecExecScheduler().setConflictChecker(this.checker);
        
//        // We want to always insert one SUBSCRIBER record per partition so 
//        // that we can play with them. Set VLR_LOCATION to zero so that 
//        // can check whether it has been modified
//        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
//        Column catalog_col = this.getColumn(catalog_tbl, "VLR_LOCATION");
//        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
//        for (int i = 0; i < NUM_PARTITIONS; i++) {
//            Object row[] = VoltTableUtil.getRandomRow(catalog_tbl);
//            row[0] = new Long(i);
//            row[catalog_col.getIndex()] = 0l;
//            vt.addRow(row);
//        } // FOR
//        String procName = VoltSystemProcedure.procCallName(LoadMultipartitionTable.class);
//        ClientResponse cr = this.client.callProcedure(procName, catalog_tbl.getName(), vt);
//        assertEquals(cr.toString(), Status.OK, cr.getStatus());
    }
    
    @Override
    protected void tearDown() throws Exception {
        if (this.client != null) this.client.close();
        if (this.hstore_site != null) this.hstore_site.shutdown();
        // HACK: Delete JAR
        if (catalogContext.jarPath != null && catalogContext.jarPath.exists()) {
            // System.err.println("DELETE: " + catalogContext.jarPath);
            catalogContext.jarPath.delete();
        }
    }

    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void checkCurrentDtxn() {
        // Make sure that this txn is the current dtxn at each of the partitions
        AbstractTransaction dtxn = null;
        for (PartitionExecutor executor : this.executors) {
            AbstractTransaction ts = null;
            int tries = 3;
            while (tries-- > 0) {
                ts = executor.getDebugContext().getCurrentDtxn();
                if (ts != null) break;
                ThreadUtil.sleep(NOTIFY_TIMEOUT);
            } // WHILE
            assertNotNull("No dtxn at " + executor.getPartition(), ts);
            if (dtxn == null) {
                dtxn = ts;
            } else {
                assertEquals(dtxn, ts);
            }
        } // FOR
        assertNotNull(dtxn);
    }
    
    private void checkQueuedTxns(PartitionExecutor executor, int expected) {
        // Wait until they have all been executed but make sure that nobody actually returned yet
        int tries = 3;
        int blocked = -1;
        PartitionLockQueue queue = hstore_site.getTransactionQueueManager().getLockQueue(executor.getPartitionId()); 
        while (tries-- > 0) {
            blocked = queue.size();
            if (blocked == expected) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertEquals(executor.toString(), expected, blocked);
    }
    
    @SuppressWarnings("unchecked")
    private <T extends VoltProcedure> T getCurrentVoltProcedure(PartitionExecutor executor, Class<T> expectedType) {
        int tries = 3;
        VoltProcedure voltProc = null;
        while (tries-- > 0) {
            voltProc = executor.getDebugContext().getCurrentVoltProcedure();
            if (voltProc != null) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertNotNull(String.format("Failed to get %s from %s", expectedType, executor), voltProc);
        assertEquals(expectedType, voltProc.getClass());
        return ((T)voltProc);
    }
    
//    private void checkClientResponses(Collection<ClientResponse> responses, Status status, boolean speculative, Integer restarts) {
//        for (ClientResponse cr : responses) {
//            assertNotNull(cr);
//            assertEquals(cr.toString(), status, cr.getStatus());
//            assertTrue(cr.toString(), cr.isSinglePartition());
//            assertEquals(cr.getTransactionId() + " - SPECULATIVE", speculative, cr.isSpeculative());
//            assertTrue(cr.toString(), cr.hasDebug());
//            
//            ClientResponseDebug crDebug = cr.getDebug();
//            assertNotNull(crDebug);
//            if (restarts != null) {
//                assertEquals(cr.getTransactionId() + " - RESTARTS", restarts.intValue(), cr.getRestartCounter());
//            }
//        } // FOR
//    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testEarly2PCWithQuery
     */
    @Test
    public void testEarly2PCWithQuery() throws Throwable {
        // Check that the base PartitionExecutor recognizes when a txn is 
        // finished with a partition at the moment that it sends a query request. The
        // remote PartitionExecutor should process the query and then immediately send
        // back the 2PC acknowledgment.

        // Fire off a distributed a txn that will block.
        Object dtxnParams[] = new Object[]{ BASE_PARTITION, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints dtxnHints = new StoredProcedureInvocationHints();
        dtxnHints.basePartition = BASE_PARTITION;
        LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        this.client.callProcedure(dtxnCallback, this.blockingProc.getName(), dtxnHints, dtxnParams);
        
        // Block until we know that the txn has started running
        BlockableSendPayment dtxnVoltProc = this.getCurrentVoltProcedure(this.baseExecutor, BlockableSendPayment.class); 
        assertNotNull(dtxnVoltProc);
        boolean result = dtxnVoltProc.NOTIFY_BEFORE.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Fire off a single-partition txn that will not get executed right away
        Object spParams[] = new Object[]{ BASE_PARTITION+1, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints spHints = new StoredProcedureInvocationHints();
        spHints.basePartition = BASE_PARTITION+1;
        LatchableProcedureCallback spCallback0 = new LatchableProcedureCallback(1);
        this.client.callProcedure(spCallback0, this.blockingProc.getName(), spHints, spParams);
        this.checkQueuedTxns(this.remoteExecutor, 1);
        
        // Ok now we're going to release our txn. It will execute a bunch of stuff.
        // It should be able to identify that it is finished with the remote partition without
        // having to be explicitly told in the code.
        dtxnVoltProc.LOCK_BEFORE.release();
        result = dtxnVoltProc.NOTIFY_AFTER.tryAcquire(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        LocalTransaction dtxn = (LocalTransaction)this.baseExecutor.getDebugContext().getCurrentDtxn();
        assertEquals(dtxnVoltProc.getTransactionId(), dtxn.getTransactionId());
        EstimatorState t_state = dtxn.getEstimatorState(); 
        if (t_state instanceof MarkovEstimatorState) {
            LOG.warn("WROTE MARKOVGRAPH: " + ((MarkovEstimatorState)t_state).dumpMarkovGraph());
        }
        PartitionSet donePartitions = dtxn.getDonePartitions();
        assertEquals(donePartitions.toString(), 1, donePartitions.size());
        assertEquals(this.remoteExecutor.getPartitionId(), donePartitions.get());
        
        // We're looking good!
        // Check to make sure that the dtxn succeeded
        dtxnVoltProc.LOCK_AFTER.release();
        result = dtxnCallback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+dtxnCallback.latch, result);
        assertEquals(Status.OK, CollectionUtil.first(dtxnCallback.responses).getStatus());
        
        BlockableSendPayment spVoltProc = this.getCurrentVoltProcedure(this.remoteExecutor, BlockableSendPayment.class); 
        assertNotNull(spVoltProc);
        spVoltProc.LOCK_BEFORE.release();
        spVoltProc.LOCK_AFTER.release();
        result = spCallback0.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SP LATCH"+spCallback0.latch, result);
        assertEquals(Status.OK, CollectionUtil.first(spCallback0.responses).getStatus());
        
    }
    

}
