package org.voltdb;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.client.ClientResponse;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransaction;

/**
 * @author pavlo
 */
public class TestNewVoltProcedure extends BaseTestCase {

    private static final int NUM_PARTITONS = 10;
    private static final int LOCAL_PARTITION = 1;
    private static long CLIENT_HANDLE = 1; 
    private static final BackendTarget BACKEND_TARGET = BackendTarget.HSQLDB_BACKEND;

    private static final AtomicLong NEXT_TXN_ID = new AtomicLong(0);
    private static final String TARGET_PROCEDURE = "GetAccessData";
    private static final Object TARGET_PARAMS[] = new Object[] { new Long(1), new Long(1) };
    
    private static HStoreSite hstore_site;
    private static ExecutionSite site;
    private static PartitionEstimator p_estimator;
    
    private VoltProcedure volt_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITONS);
        
        if (site == null) {
            // Figure out whether we are on a machine that has the native lib
            // we can use right now
            // BACKEND_TARGET = (this.hasVoltLib() ? BackendTarget.NATIVE_EE_JNI : BackendTarget.HSQLDB_BACKEND);
            
            p_estimator = new PartitionEstimator(catalog_db);
            site = new MockExecutionSite(LOCAL_PARTITION, catalog, p_estimator);
            
            Partition catalog_part = CatalogUtil.getPartitionById(catalog_db, LOCAL_PARTITION);
            Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
            executors.put(LOCAL_PARTITION, site);
            hstore_site = new HStoreSite((Site)catalog_part.getParent(), executors, p_estimator);
        }
        volt_proc = site.getVoltProcedure(TARGET_PROCEDURE);
        assertNotNull(volt_proc);
    }
        
    /**
     * testCall
     */
    public void testCall() throws Exception {
        // Use this callback to attach to the VoltProcedure and get the ClientResponse
        final LinkedBlockingDeque<ClientResponse> lock = new LinkedBlockingDeque<ClientResponse>(1);
        EventObserver<ClientResponse> observer = new EventObserver<ClientResponse>() {
            @Override
            public void update(EventObservable<ClientResponse> o, ClientResponse arg) {
                assert(arg != null);
                lock.offer((ClientResponse)arg);
            }
        };
        volt_proc.registerCallback(observer);

        Long xact_id = NEXT_TXN_ID.getAndIncrement();
        Collection<Integer> partitions = Collections.singleton(LOCAL_PARTITION);
        LocalTransaction ts = new LocalTransaction(hstore_site).init(xact_id, CLIENT_HANDLE++, LOCAL_PARTITION, partitions, false, true);
        // FIXME site.txn_states.put(xact_id, ts);
        
        // 2010-11-12: call() no longer immediately updates the internal state of the VoltProcedure
        //             so there is no way for us to check whether things look legit until we get
        //             back the results
        volt_proc.call(ts, TARGET_PARAMS);
        
        // Now check whether we got the ClientResponse
        ClientResponse response = null;
        response = lock.poll(10, TimeUnit.SECONDS);
        assertNotNull("Timed out before receiving response", response);
        assertEquals(1, response.getResults().length);
    }
    
    /**
     * testCallAndBlock
     */
    public void testCallAndBlock() throws Exception {
        // ClientResponse response = volt_proc.callAndBlock(rand.nextLong(), CLIENT_HANDLE++, TARGET_PARAMS); 
        // assertNotNull(response);
    }
    
    /**
     * testExecuteLocalBatch
     */
    public void testExecuteLocalBatch() throws Exception {
        long txn_id = NEXT_TXN_ID.incrementAndGet();
        
        // We have to slap some queries into a BatchPlan
        Procedure catalog_proc = volt_proc.getProcedure();
        assertNotNull(catalog_proc);
        SQLStmt batchStmts[] = new SQLStmt[catalog_proc.getStatements().size()];
        ParameterSet args[] = new ParameterSet[batchStmts.length];
        
        Statement catalog_stmts[] = catalog_proc.getStatements().values();
        for (int i = 0; i < batchStmts.length; i++) {
            assertNotNull(catalog_stmts[i]);
            batchStmts[i] = new SQLStmt(catalog_stmts[i], catalog_stmts[i].getFragments());
            args[i] = VoltProcedure.getCleanParams(batchStmts[i], TARGET_PARAMS);
        } // FOR
        
        BatchPlanner planner = new BatchPlanner(batchStmts, catalog_proc, p_estimator);
        BatchPlanner.BatchPlan plan = planner.plan(txn_id, CLIENT_HANDLE, LOCAL_PARTITION, Collections.singleton(LOCAL_PARTITION), args, true);
        assertNotNull(plan);
        
        // Only try to execute a BatchPlan if we have the real EE
        // The HSQL Backend doesn't take plan fragments
        if (BACKEND_TARGET == BackendTarget.NATIVE_EE_JNI) {
//            VoltTable results[] = volt_proc.executeLocalBatch(plan);
//            assertNotNull(results);
//            assertEquals(batchStmts.length, results.length);
//            for (VoltTable result : results) {
//                assertNotNull(result);
//            } // FOR
        }
    }
}