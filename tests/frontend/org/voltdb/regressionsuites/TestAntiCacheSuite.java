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
import org.voltdb.sysprocs.EvictTuples;
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

    private static final int NOTIFY_TIMEOUT = 2000; // ms
    
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestAntiCacheSuite(String name) {
        super(name);
    }
    
    private void initializeDatabase(Client client) throws Exception {
        Object params[] = {
            VoterConstants.NUM_CONTESTANTS,
            VoterConstants.CONTESTANT_NAMES_CSV
        };
        
        ClientResponse cresponse = client.callProcedure(Initialize.class.getSimpleName(), params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
    }
    
    private Map<Integer, VoltTable> evictData(Client client) throws Exception {
        String procName = VoltSystemProcedure.procCallName(EvictTuples.class);
        CatalogContext catalogContext = this.getCatalogContext();
        String tableNames[] = { VoterConstants.TABLENAME_VOTES };
        LatchableProcedureCallback callback = new LatchableProcedureCallback(catalogContext.numberOfPartitions);
        long evictBytes[] = { Integer.MAX_VALUE };
        for (int partition : catalogContext.getAllPartitionIds()) {
            Object params[] = { partition, tableNames, evictBytes };
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
        return (m);
    }

//    /**
//     * testVote
//     */
//    public void testVote() throws Exception {
//        Client client = this.getClient();
//        this.initializeDatabase(client);
//        
//        ClientResponse cresponse = client.callProcedure(Vote.class.getSimpleName(),
//                                                        TestVoterSuite.voteId++,
//                                                        TestVoterSuite.phoneNumber,
//                                                        TestVoterSuite.contestantNumber,
//                                                        TestVoterSuite.maxVotesPerPhoneNumber);
//        assertEquals(Status.OK, cresponse.getStatus());
//        VoltTable results[] = cresponse.getResults();
//        assertEquals(1, results.length);
//        assertEquals(VoterConstants.VOTE_SUCCESSFUL, results[0].asScalarLong());
//        
//        // Make sure that our vote is actually in the real table and materialized views
//        String query = "SELECT COUNT(*) FROM votes";
//        cresponse = client.callProcedure("@AdHoc", query);
//        assertEquals(Status.OK, cresponse.getStatus());
//        results = cresponse.getResults();
//        assertEquals(1, results.length);
//        assertEquals(1, results[0].asScalarLong());
//
//        
//        query = "SELECT * FROM v_votes_by_phone_number";
//        cresponse = client.callProcedure("@AdHoc", query);
//        assertEquals(Status.OK, cresponse.getStatus());
//        results = cresponse.getResults();
//        assertEquals(1, results.length);
//        System.err.println(results[0]);
//        assertTrue(results[0].advanceRow());
//        assertEquals(TestVoterSuite.phoneNumber, results[0].getLong(0));
//        //assertEquals(1, results[0].getLong(1));
//    }
    
    /**
     * testProfiling
     */
    public void testProfiling() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        // Force an eviction
        Map<Integer, VoltTable> evictResults = this.evictData(client);
        System.err.println(StringUtil.formatMaps(evictResults));
        System.err.println("-------------------------------");

        // Our stats should now come back with one block evicted
        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params[] = { SysProcSelector.ANTICACHE.name(), 0 };
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertEquals(cresponse.toString(), 1, cresponse.getResults().length);
        VoltTable statsResult = cresponse.getResults()[0];

        System.err.println("-------------------------------");
        System.err.println(VoltTableUtil.format(statsResult));

        // We need this just to get the name of the column
        AntiCacheManagerProfiler profiler = new AntiCacheManagerProfiler();
    //        statsResult.advanceRow();
    //        assertEquals(evictResult.getLong("BLOCKS_EVICTED"),
    //                     statsResult.getLong(profiler.eviction_time.getName().toUpperCase()));
    }

        

    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAntiCacheSuite.class);
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
        config = new LocalSingleProcessServer("voter-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer("voter-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster("voter-cluster.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
