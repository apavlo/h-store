package org.voltdb.regressionsuites;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.benchmark.voter.VoterProjectBuilder;
import edu.brown.benchmark.voter.procedures.Initialize;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.hstore.Hstoreservice.Status;

/**
 * Simple test suite for the VOTER benchmark
 * @author pavlo
 */
public class TestVoterSuite extends RegressionSuite {

    private static final String PREFIX = "voter";
    public static long voteId = 1;
    public static final long phoneNumber = 8675309; // Jenny
    public static final int contestantNumber = 1;
    public static final long maxVotesPerPhoneNumber = 5;
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestVoterSuite(String name) {
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

    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        String query = "SELECT COUNT(*) FROM contestants";
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(VoterConstants.NUM_CONTESTANTS, results[0].asScalarLong());
        System.err.println(results[0]);
    }

    /**
     * testVote
     */
    public void testVote() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        ClientResponse cresponse = client.callProcedure(Vote.class.getSimpleName(),
                                                        voteId++,
                                                        phoneNumber,
                                                        contestantNumber,
                                                        maxVotesPerPhoneNumber);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(VoterConstants.VOTE_SUCCESSFUL, results[0].asScalarLong());
        
        // Make sure that our vote is actually in the real table and materialized views
        String query = "SELECT COUNT(*) FROM votes";
        cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        results = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(1, results[0].asScalarLong());

        
        query = "SELECT * FROM v_votes_by_phone_number";
        cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        results = cresponse.getResults();
        assertEquals(1, results.length);
        System.err.println(results[0]);
        assertTrue(results[0].advanceRow());
        assertEquals(phoneNumber, results[0].getLong(0));
        //assertEquals(1, results[0].getLong(1));
    }
    
    /**
     * testVoteLimit
     */
    public void testVoteLimit() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client);
        
        // Make sure that the phone number is only allowed to vote up to
        // the limit and not anymore after that
        ClientResponse cresponse = null;
        for (int i = 0, cnt = (int)(maxVotesPerPhoneNumber*2); i < cnt; i++) {
            long expected = (i < maxVotesPerPhoneNumber ? VoterConstants.VOTE_SUCCESSFUL :
                                                          VoterConstants.ERR_VOTER_OVER_VOTE_LIMIT);
            cresponse = client.callProcedure(Vote.class.getSimpleName(),
                                             voteId++,
                                             phoneNumber,
                                             contestantNumber,
                                             maxVotesPerPhoneNumber);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            //assertEquals(expected, results[0].asScalarLong());
        } // FOR
    }
    
//    /**
//     * testViews
//     */
//    public void testVotesByContestantView() throws Exception {
//        Client client = this.getClient();
//        this.initializeDatabase(client);
//        
//        int num_contestants = VoterConstants.NUM_CONTESTANTS;
//        
//        // Insert the same number of votes for each contestant as their id number
//        long phoneNumber = TestVoterSuite.phoneNumber;
//        for (int contestant = 0 ; contestant < num_contestants; contestant++) {
//            for (int i = 0; i < contestant; i++) {
//                ClientResponse cresponse = client.callProcedure(Vote.class.getSimpleName(),
//                                                                voteId++,
//                                                                phoneNumber++,
//                                                                contestant,
//                                                                maxVotesPerPhoneNumber);
//                assertEquals(Status.OK, cresponse.getStatus());
//                VoltTable results[] = cresponse.getResults();
//                assertEquals(1, results.length);
//                assertEquals(VoterConstants.VOTE_SUCCESSFUL, results[0].asScalarLong());
//            } // FOR
//        } // FOR
//        
//        
//        // Now check that the view is correct
//        String sql = "SELECT * FROM v_votes_by_contestant_number";
//        ClientResponse cresponse = RegressionSuiteUtil.sql(client, sql);
//        assertEquals(Status.OK, cresponse.getStatus());
//        VoltTable results[] = cresponse.getResults();
//        assertEquals(1, results.length);
//        System.out.println(VoltTableUtil.format(results[0]));
//        
////        sql = "SELECT * FROM votes";
////        cresponse = RegressionSuiteUtil.sql(client, sql);
////        assertEquals(Status.OK, cresponse.getStatus());
////        results = cresponse.getResults();
////        assertEquals(1, results.length);
////        System.out.println(VoltTableUtil.format(results[0]));
//        
//        while (results[0].advanceRow()) {
//            int contestant = (int)results[0].getLong(0);
//            assert(contestant < VoterConstants.NUM_CONTESTANTS) : contestant;
//            int num_votes = (int)results[0].getLong(1);
//            
//            assertEquals(contestant, num_votes);
//        } // WHILE
//    }

    public static Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestVoterSuite.class);
        VoterProjectBuilder project = new VoterProjectBuilder();
        project.addAllDefaults();
        
        boolean success;
        VoltServerConfig config;
        
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
