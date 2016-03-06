package edu.brown.hstore;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.regressionsuites.specexecprocs.BlockingSendPayment;
import org.voltdb.regressionsuites.specexecprocs.NonBlockingSendPayment;
import org.voltdb.types.SpeculationType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.BaseTestCase;
import edu.brown.HStoreSiteTestUtil;
import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.smallbank.SmallBankProjectBuilder;
import edu.brown.benchmark.smallbank.procedures.DepositChecking;
import edu.brown.benchmark.smallbank.procedures.SendPayment;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
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
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.NoAbortFilter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * PartitionExecutor Tests for 2PC
 * @author pavlo
 */
public class TestPartitionExecutor2PC extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 2;
    private static final int BASE_PARTITION = 0;
    private static final int WORKLOAD_XACT_LIMIT = 500;

    private static Workload workload;
    private static File markovsFile;
    
    private HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private Client client;
    private Procedure origProc;
    private Procedure nonblockingProc;
    private Procedure blockingProc;
    
    private PartitionExecutor executors[];
    private PartitionExecutor.Debug executorDbgs[];
    private PartitionExecutor baseExecutor;
    private PartitionExecutor remoteExecutor;
    
    private final SmallBankProjectBuilder builder = new SmallBankProjectBuilder() {
        {
            this.addAllDefaults();
            this.addProcedure(BlockingSendPayment.class);
            this.addProcedure(NonBlockingSendPayment.class);
        }
    };
    
    // --------------------------------------------------------------------------------------------
    // SETUP
    // --------------------------------------------------------------------------------------------
    
    @Before
    public void setUp() throws Exception {
        super.setUp(this.builder);
        initializeCatalog(1, 1, NUM_PARTITIONS);

        this.origProc = this.getProcedure(SendPayment.class);
        this.nonblockingProc = this.getProcedure(NonBlockingSendPayment.class);
        this.blockingProc = this.getProcedure(BlockingSendPayment.class);
        
        if (isFirstSetup()) {

            // DUPLICATE ALL SENDPAYMENTS TO BE NON-BLOCKING AND BLOCKABLE SENDPAYMENTS
            Procedure procs[] = { this.origProc, this.blockingProc, this.nonblockingProc };
            Workload workloads[] = new Workload[procs.length];
            
            // LOAD SAMPLE WORKLOAD
            Filter filter =  new ProcedureNameFilter(false)
                    .include(this.origProc.getName())
                    .attach(new NoAbortFilter())
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            File workloadFile = this.getWorkloadFile(ProjectType.SMALLBANK);
            workloads[0] = new Workload(catalogContext.catalog).load(workloadFile, catalogContext.database, filter);
            File tempFile = FileUtil.getTempFile("workload", true);
            workloads[0].save(tempFile, catalogContext.database);
            assertTrue(tempFile.exists());
            String dump = FileUtil.readFile(tempFile);
            assertFalse(dump.isEmpty());
            
            for (int i = 1; i < procs.length; i++) {
                FileUtil.writeStringToFile(tempFile, dump.replace(this.origProc.getName(), procs[i].getName()));
                workloads[i] = new Workload(catalogContext.catalog).load(tempFile, catalogContext.database);
                assertEquals(workloads[0].getTransactionCount(), workloads[i].getTransactionCount());
                assertEquals(workloads[0].getQueryCount(), workloads[i].getQueryCount());
                // Make sure we change their txn ids
                for (TransactionTrace tt : workloads[i]) {
                    tt.setTransactionId(tt.getTransactionId() + (1000000 * i));
                } // FOR
            
                // DUPLICATE PARAMETER MAPPINGS
                for (ParameterMapping pm : catalogContext.paramMappings.get(this.origProc)) {
                    ParameterMapping clone = pm.clone();
                    clone.procedure_parameter = procs[i].getParameters().get(pm.procedure_parameter.getIndex());
                    clone.statement = procs[i].getStatements().get(pm.statement.getName());
                    clone.statement_parameter = clone.statement.getParameters().get(pm.statement_parameter.getIndex());
                    catalogContext.paramMappings.add(clone);
                } // FOR
                assert(workloads[i] != null) : i;
            } // FOR

            // COMBINE INTO A SINGLE WORKLOAD HANDLE
            workload = new Workload(catalogContext.catalog, workloads);
            assertEquals(workload.getTransactionCount(), workloads[0].getTransactionCount() * procs.length);
            assertEquals(workload.getQueryCount(), workloads[0].getQueryCount() * procs.length);
            
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
        this.hstore_conf.site.txn_client_debug = true;
        this.hstore_conf.site.txn_counters = true;
        this.hstore_conf.site.exec_force_singlepartitioned = true;
        this.hstore_conf.site.exec_force_allpartitions = false;
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
        this.executors = new PartitionExecutor[]{
                this.baseExecutor,
                this.remoteExecutor
        };
        this.executorDbgs = new PartitionExecutor.Debug[this.executors.length];
        for (int i = 0; i < this.executors.length; i++) {
            this.executorDbgs[i] = this.executors[i].getDebugContext();
            TransactionEstimator t_estimator = this.executors[i].getTransactionEstimator();
            assertNotNull(t_estimator);
            assertEquals(MarkovEstimator.class, t_estimator.getClass());
        } // FOR
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
                ThreadUtil.sleep(HStoreSiteTestUtil.NOTIFY_TIMEOUT);
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
     * testPrepareAbort
     */
    public void testPrepareAbort() throws Exception {
        // Run a distributed partition txn where one of the remote 
        // partitions will decide to abort the txn in the 2PC prepare phase
        final int remotePartition = BASE_PARTITION+1;
        final PartitionExecutor remoteExecutor = hstore_site.getPartitionExecutor(remotePartition);
        final AtomicBoolean remotePartitionCheck = new AtomicBoolean(false);
        remoteExecutor.getDebugContext().getSpecExecScheduler().ignoreSpeculationType(SpeculationType.SP3_REMOTE_BEFORE);
        hstore_conf.site.exec_early_prepare = false;
        
        // Load in some test data so the txn doesn't abort
        for (Table tbl : catalogContext.database.getTables()) {
            VoltTable vt = CatalogUtil.getVoltTable(tbl);
            Object row[] = VoltTableUtil.getRandomRow(tbl);
            row[0] = remotePartition;
            vt.addRow(row);
            this.remoteExecutor.loadTable(100l, tbl, vt, false);
        } // FOR
        
        AbstractConflictChecker remoteChecker = new AbstractConflictChecker(catalogContext) {
            @Override
            public boolean shouldIgnoreTransaction(AbstractTransaction ts) {
                return false;
            }
            @Override
            public boolean hasConflictBefore(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
                return false;
            }
            @Override
            public boolean hasConflictAfter(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
                System.err.printf("PARTITION:%d -- %s<-->%s\n", partitionId, ts0, ts1);
                if (partitionId == remotePartition) {
                    remotePartitionCheck.set(true);
                    return (true);
                }
                return (false);
            }
        };
        for (PartitionExecutor.Debug dbg : this.executorDbgs) {
            dbg.setConflictChecker(remoteChecker);
        } // FOR
        
        // Fire off a distributed a txn that will block.
        Object dtxnParams[] = new Object[]{ BASE_PARTITION, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints dtxnHints = new StoredProcedureInvocationHints();
        dtxnHints.basePartition = BASE_PARTITION;
        LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        this.client.callProcedure(dtxnCallback, this.blockingProc.getName(), dtxnHints, dtxnParams);
        
        // Block until we know that the txn has started running
        BlockingSendPayment dtxnVoltProc = HStoreSiteTestUtil.getCurrentVoltProcedure(this.baseExecutor, BlockingSendPayment.class); 
        assertNotNull(dtxnVoltProc);
        boolean result = dtxnVoltProc.NOTIFY_BEFORE.tryAcquire(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Let the dtxn execute some queries that modify the remote partition
        dtxnVoltProc.LOCK_BEFORE.release();
        result = dtxnVoltProc.NOTIFY_AFTER.tryAcquire(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        // Fire off a single-partition txn that will get executed right away but 
        // have its ClientResponse held until it learns whether the dtxn
        // will commit successfully
        Procedure spProc = this.getProcedure(DepositChecking.class);
        Object spParams[] = new Object[]{ remotePartition, 1.0 };
        StoredProcedureInvocationHints spHints = new StoredProcedureInvocationHints();
        spHints.basePartition = remotePartition;
        LatchableProcedureCallback spCallback0 = new LatchableProcedureCallback(1);
        this.client.callProcedure(spCallback0, spProc.getName(), spHints, spParams);
        HStoreSiteTestUtil.checkBlockedSpeculativeTxns(remoteExecutor, 1);

        // Now release the dtxn. When it goes to commit with 2PC it will
        // find that there is a conflict with the single-partition txn.
        // Everyone should get restarted.
        dtxnVoltProc.LOCK_AFTER.release();
        result = dtxnCallback.latch.await(HStoreSiteTestUtil.NOTIFY_TIMEOUT*3, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH: "+dtxnCallback.latch, result);
        ClientResponse dtxnResponse = CollectionUtil.first(dtxnCallback.responses);
        assertEquals(dtxnResponse.toString(), Status.OK, dtxnResponse.getStatus());
        assertTrue(dtxnResponse.toString(), dtxnResponse.getRestartCounter() >= 1);
        
        // The other transaction should be executed now on the remote partition
        result = spCallback0.latch.await(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SP LATCH: "+spCallback0.latch, result);
        ClientResponse spResponse = CollectionUtil.first(spCallback0.responses);
        assertEquals(Status.OK, spResponse.getStatus());
        assertTrue(spResponse.isSpeculative());
        assertTrue(spResponse.toString(), spResponse.getRestartCounter() >= 1);
        
        assertTrue(remotePartitionCheck.get());
    }
    
    /**
     * testEarly2PCWithQuery
     */
    @Test
    public void testEarly2PCWithQuery() throws Throwable {
        // Check that the base PartitionExecutor recognizes when a txn is 
        // finished with a partition at the moment that it sends a query request. The
        // remote PartitionExecutor should process the query and then immediately send
        // back the 2PC acknowledgment.
        hstore_conf.site.exec_early_prepare = true;

        // We want to make sure that the PartitionExecutor only spec execs 
        // at the 2PC stall points.
        for (SpeculationType specType : SpeculationType.values()) {
            if (specType == SpeculationType.SP4_REMOTE) continue;
            for (int i = 0; i < this.executors.length; i++) {
                this.executorDbgs[i].getSpecExecScheduler().ignoreSpeculationType(specType);
            }
        }
        
        AbstractConflictChecker checker = new AbstractConflictChecker(null) {
            @Override
            public boolean shouldIgnoreTransaction(AbstractTransaction ts) {
                return (false);
            }
            @Override
            public boolean hasConflictBefore(AbstractTransaction dtxn, LocalTransaction candidate, int partitionId) {
                return (false);
            }
            @Override
            public boolean hasConflictAfter(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
                return (false);
            }
        };
        // Make sure that we replace the conflict checker on the remote partition
        // so that it can schedule our speculative txns
        PartitionExecutor.Debug remoteDebug = this.remoteExecutor.getDebugContext();
        remoteDebug.getSpecExecScheduler().getDebugContext().setConflictChecker(checker);
        
        // Fire off a distributed a txn that will block.
        Object dtxnParams[] = new Object[]{ BASE_PARTITION, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints dtxnHints = new StoredProcedureInvocationHints();
        dtxnHints.basePartition = BASE_PARTITION;
        LatchableProcedureCallback dtxnCallback = new LatchableProcedureCallback(1);
        this.client.callProcedure(dtxnCallback, this.blockingProc.getName(), dtxnHints, dtxnParams);
        
        // Block until we know that the txn has started running
        BlockingSendPayment dtxnVoltProc = HStoreSiteTestUtil.getCurrentVoltProcedure(this.baseExecutor, BlockingSendPayment.class); 
        assertNotNull(dtxnVoltProc);
        boolean result = dtxnVoltProc.NOTIFY_BEFORE.tryAcquire(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        this.checkCurrentDtxn();
        
        // Fire off a single-partition txn that will not get executed right away
        // We have to use the blocking version because there needs to be data in tables first
        Object spParams[] = new Object[]{ BASE_PARTITION+1, BASE_PARTITION+1, 1.0 };
        StoredProcedureInvocationHints spHints = new StoredProcedureInvocationHints();
        spHints.basePartition = BASE_PARTITION+1;
        LatchableProcedureCallback spCallback0 = new LatchableProcedureCallback(1);
        this.client.callProcedure(spCallback0, this.nonblockingProc.getName(), spHints, spParams);
        HStoreSiteTestUtil.checkQueuedTxns(this.remoteExecutor, 1);
        
        // Ok now we're going to release our txn. It will execute a bunch of stuff.
        // It should be able to identify that it is finished with the remote partition without
        // having to be explicitly told in the code.
        dtxnVoltProc.LOCK_BEFORE.release();
        result = dtxnVoltProc.NOTIFY_AFTER.tryAcquire(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(result);
        
        LocalTransaction dtxn = (LocalTransaction)this.baseExecutor.getDebugContext().getCurrentDtxn();
        assertEquals(dtxnVoltProc.getTransactionId(), dtxn.getTransactionId());
//        EstimatorState t_state = dtxn.getEstimatorState(); 
//        if (t_state instanceof MarkovEstimatorState) {
//            LOG.warn("WROTE MARKOVGRAPH: " + ((MarkovEstimatorState)t_state).dumpMarkovGraph());
//        }
        PartitionSet donePartitions = dtxn.getDonePartitions();
        assertEquals(donePartitions.toString(), 1, donePartitions.size());
        assertEquals(this.remoteExecutor.getPartitionId(), donePartitions.get());
        
        // ThreadUtil.sleep(10000000);
        
        // We're looking good!
        // Check to make sure that the dtxn succeeded
        dtxnVoltProc.LOCK_AFTER.release();
        result = dtxnCallback.latch.await(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("DTXN LATCH"+dtxnCallback.latch, result);
        assertEquals(Status.OK, CollectionUtil.first(dtxnCallback.responses).getStatus());

        // The other transaction should be executed now on the remote partition
        result = spCallback0.latch.await(HStoreSiteTestUtil.NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue("SP LATCH"+spCallback0.latch, result);
        ClientResponse spResponse = CollectionUtil.first(spCallback0.responses);
        assertEquals(Status.OK, spResponse.getStatus());
        assertTrue(spResponse.isSpeculative());
        
    }

}
