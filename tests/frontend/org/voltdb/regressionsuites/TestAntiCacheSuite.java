package org.voltdb.regressionsuites;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.sysprocs.EvictHistory;
import org.voltdb.sysprocs.EvictTuples;
import org.voltdb.sysprocs.EvictedAccessHistory;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.benchmark.voter.VoterProjectBuilder;
import edu.brown.benchmark.voter.procedures.Initialize;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.StringUtil;

/**
 * Anti-Caching Test Suite
 * @author pavlo
 */
public class TestAntiCacheSuite extends RegressionSuite {

    private static final String PREFIX = "anticache";
    private static final int NOTIFY_TIMEOUT = 2000; // ms
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestAntiCacheSuite(String name) {
        super(name);
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private void initializeDatabase(Client client) throws Exception {
        System.err.println("Loading data...");
        Object params[] = {
            VoterConstants.NUM_CONTESTANTS,
            VoterConstants.CONTESTANT_NAMES_CSV
        };
        
        ClientResponse cresponse = client.callProcedure(Initialize.class.getSimpleName(), params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }
    
    private void loadVotes(Client client, int num_txns) throws Exception {
        LatchableProcedureCallback callback = new LatchableProcedureCallback(num_txns);
        for (int i = 0; i < num_txns; i++) {
            Object params[] = { new Long(i),
                                TestVoterSuite.phoneNumber+i,
                                TestVoterSuite.contestantNumber,
                                num_txns+1 };  
            client.callProcedure(callback, Vote.class.getSimpleName(), params);
        } // FOR

        // Wait until they all finish
        boolean result = callback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(callback.toString(), result);
        for (ClientResponse cr : callback.responses) {
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
        }
        
        // Make sure that our vote is actually in the real table and materialized views
        String query = "SELECT COUNT(*) FROM votes";
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        ClientResponse cresponse = client.callProcedure(procName, query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(num_txns, results[0].asScalarLong());
    }
    
    private Map<Integer, VoltTable> evictData(Client client) throws Exception {
//        System.err.printf("Evicting data..."); 
        String procName = VoltSystemProcedure.procCallName(EvictTuples.class);
        CatalogContext catalogContext = this.getCatalogContext();
        String tableNames[] = { VoterConstants.TABLENAME_VOTES };
        LatchableProcedureCallback callback = new LatchableProcedureCallback(catalogContext.numberOfPartitions);
//        long evictBytes[] = { Integer.MAX_VALUE };
        long evictBytes[] = {10000};
        int numBlocks[] = { 1 };
        for (int partition : catalogContext.getAllPartitionIds()) {
//            System.err.printf("Evicting data at partition %d...\n", partition);
            Object params[] = { partition, tableNames, evictBytes, numBlocks };
            boolean result = client.callProcedure(callback, procName, params);
            assertTrue(result);
        } // FOR
        
        // Wait until they all finish
        boolean result = callback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(callback.toString(), result);
        
        // Construct a mapping BasePartition->VoltTable
        Map<Integer, VoltTable> m = new TreeMap<Integer, VoltTable>();
        for (ClientResponse cr : callback.responses) {
            assertEquals(cr.toString(), Status.OK, cr.getStatus());
            assertEquals(cr.toString(), 1, cr.getResults().length);
            m.put(cr.getBasePartition(), cr.getResults()[0]);
        } // FOR
        assertEquals(catalogContext.numberOfPartitions, m.size());
//        System.err.printf("Finished evicting data.");
        return (m);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    
    /**
     * testEvictEmptyTable
     */
    public void testEvictEmptyTable() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
                
        // Force an eviction on a table before putting anything in it
        Map<Integer, VoltTable> evictResults = this.evictData(client);
        System.err.println(StringUtil.formatMaps(evictResults));
        System.err.println("-------------------------------");
    }
    
    /**
     * testProfiling
     */
    public void testProfiling() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        this.loadVotes(client, 100);

        // Force an eviction
        Map<Integer, VoltTable> evictResults = this.evictData(client);
        for (int partition : evictResults.keySet()) {
            System.err.println("Partition " + partition);
            System.err.println(StringUtil.prefix("  ", VoltTableUtil.format(evictResults.get(partition))));
        }
        System.err.println("-------------------------------");

        // Our stats should now come back with one eviction executed
        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params[] = { SysProcSelector.ANTICACHE.name(), 0 };
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
        VoltTable statsResult = cresponse.getResults()[0];

        System.err.println(VoltTableUtil.format(statsResult));

        // We need this just to get the name of the column
        AntiCacheManagerProfiler profiler = new AntiCacheManagerProfiler();
        String colName = profiler.eviction_time.getName().toUpperCase()+"_CNT";
        while (statsResult.advanceRow()) {
            System.err.println("colName: " + colName);
            int partition = (int)statsResult.getLong("PARTITION");
            VoltTable vt = evictResults.get(partition);
            boolean adv = vt.advanceRow();
            assert(adv);
            long expected = vt.getLong("ANTICACHE_BLOCKS_EVICTED");
            assertEquals("Partition "+partition, expected, statsResult.getLong(colName));
        } // WHILE
    }

//    /**
//     * testEvictHistory
//     */
//    public void testEvictHistory() throws Exception {
//        CatalogContext catalogContext = this.getCatalogContext();
//        Client client = this.getClient();
//        this.initializeDatabase(client);
//        this.loadVotes(client, 100);
//        int num_evicts = 5;
//        for (int i = 0; i < num_evicts; i++) {
//            this.evictData(client);
//        } // FOR
//        
//        // Our stats should now come back with one eviction executed
//        String procName = VoltSystemProcedure.procCallName(EvictHistory.class);
//        ClientResponse cresponse = client.callProcedure(procName);
//        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
//        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
//        VoltTable result = cresponse.getResults()[0];
//        assertEquals(num_evicts * catalogContext.numberOfPartitions, result.getRowCount());
//        System.err.println(VoltTableUtil.format(result));
//        
//        while (result.advanceRow()) {
//            long start = result.getLong("START");
//            long stop = result.getLong("STOP");
//            assert(start <= stop) : start + " <= " + stop;
//        } // WHILE
//    }
//    
//    /**
//     * testEvictedAccessHistory
//     */
//    public void testEvictedAccessHistory() throws Exception {
//        Client client = this.getClient();
//        this.initializeDatabase(client);
//        this.loadVotes(client, 100);
//        int num_evicts = 5;
//        for (int i = 0; i < num_evicts; i++) {
//            this.evictData(client);
//        } // FOR
//        
//        // Now force the system to fetch the block back in
//        long expected = 1;
//        String procName = "GetVote";
//        Object params[] = { expected };
//        ClientResponse cresponse = client.callProcedure(procName, params);
//        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
//        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
//        assertEquals(cresponse.toString(), expected, cresponse.getResults()[0].getLong(0));
//        
//        // Our stats should now come back with one evicted access
//        procName = VoltSystemProcedure.procCallName(EvictedAccessHistory.class);
//        cresponse = client.callProcedure(procName);
//        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
//        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
//        VoltTable result = cresponse.getResults()[0];
//        assertEquals(1, result.getRowCount());
//        System.err.println(VoltTableUtil.format(result));
//        
//        while (result.advanceRow()) {
//            assertEquals(procName, result.getString("PROCEDURE"));
//        } // WHILE
//    }
    

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAntiCacheSuite.class);
        builder.setGlobalConfParameter("site.exec_voltdb_procinfo", true);
        builder.setGlobalConfParameter("site.anticache_enable", true);
        builder.setGlobalConfParameter("site.anticache_profiling", true);
        builder.setGlobalConfParameter("site.anticache_reset", true);
        builder.setGlobalConfParameter("site.anticache_check_interval", Integer.MAX_VALUE);

        // build up a project builder for the TPC-C app
        VoterProjectBuilder project = new VoterProjectBuilder();
        project.addAllDefaults();
        project.markTableEvictable(VoterConstants.TABLENAME_VOTES);
        project.addStmtProcedure("GetVote",
                                 "SELECT * FROM " + VoterConstants.TABLENAME_VOTES + " WHERE vote_id = ?");
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX+"-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
