package org.voltdb.regressionsuites;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.EvictTuples;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.HStoreSiteTestUtil.LatchableProcedureCallback;
import edu.brown.benchmark.users.UsersConstants;
import edu.brown.benchmark.users.UsersLoader;
import edu.brown.benchmark.users.UsersProjectBuilder;
import edu.brown.benchmark.users.procedures.GetUsers;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.StringUtil;

/**
 * Simple test suite for the Users benchmark
 */
public class TestUsersSuite extends RegressionSuite {

    private static final String PREFIX = "users";
    private static final int NUM_TUPLES = UsersConstants.USERS_SIZE;
    private static final int NOTIFY_TIMEOUT = 2000; // ms
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestUsersSuite(String name) {
        super(name);
    }
    
    private void initializeDatabase(final Client client, final int num_tuples) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.FIXED_SIZE=true",
            "BENCHMARK.NUM_RECORDS="+num_tuples,
            "BENCHMARK.LOADTHREADS=1",
        };
        final CatalogContext catalogContext = this.getCatalogContext();
        UsersLoader loader = new UsersLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        loader.load();
    }

    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client, NUM_TUPLES);
        
        String query = "SELECT COUNT(*) FROM " + UsersConstants.TABLENAME_USERS;
        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        assertEquals(Status.OK, cresponse.getStatus());
        VoltTable results[] = cresponse.getResults();
        assertEquals(1, results.length);
        assertEquals(NUM_TUPLES, results[0].asScalarLong());
    }

    /**
     * testReadRecord
     */
    public void testReadRecords() throws Exception {
        Client client = this.getClient();
        this.initializeDatabase(client, NUM_TUPLES);
        // Force an eviction
        Map<Integer, VoltTable> evictResults = this.evictData(client);
        for (int partition : evictResults.keySet()) {
            System.out.println("Partition " + partition);
            System.out.println(StringUtil.prefix("  ", VoltTableUtil.format(evictResults.get(partition))));
        }
        System.out.println("-------------------------------");

        String procName = GetUsers.class.getSimpleName();
        long user1 = 99998;
        long user2 = 1;
        Object params[] = { user1, user2  };
        ClientResponse cresponse = client.callProcedure(procName, params);
//        String query = "UPDATE USERS SET u_attr01= COUNT(*) FROM " + UsersConstants.TABLENAME_USERS;
//        ClientResponse cresponse = client.callProcedure("@AdHoc", query);
        
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(1, cresponse.getResults()[0].getRowCount());
    }

    private Map<Integer, VoltTable> evictData(Client client) throws Exception {
//      System.err.printf("Evicting data..."); 
      String procName = VoltSystemProcedure.procCallName(EvictTuples.class);
      CatalogContext catalogContext = this.getCatalogContext();
      String tableNames[] = { UsersConstants.TABLENAME_USERS };
      LatchableProcedureCallback callback = new LatchableProcedureCallback(1);
//      long evictBytes[] = { Integer.MAX_VALUE };
      long evictBytes[] = {1024 * 10};
      int numBlocks[] = { 1 };
      
      int remote_partition = catalogContext.getAllPartitionIdArray()[1];
      System.out.printf("Evicting data at partition %d...\n", remote_partition);
      String children[] = null;
          Object params[] = { remote_partition, tableNames, children, evictBytes, numBlocks };
          boolean result = client.callProcedure(callback, procName, params);
          assertTrue(result);
      
      
      // Wait until they all finish
      result = callback.latch.await(NOTIFY_TIMEOUT, TimeUnit.MILLISECONDS);
      assertTrue(callback.toString(), result);
      
      // Construct a mapping BasePartition->VoltTable
      Map<Integer, VoltTable> m = new TreeMap<Integer, VoltTable>();
      for (ClientResponse cr : callback.responses) {
          assertEquals(cr.toString(), Status.OK, cr.getStatus());
          assertEquals(cr.toString(), 1, cr.getResults().length);
          m.put(cr.getBasePartition(), cr.getResults()[0]);
      } // FOR
      assertEquals(1, m.size());
//      System.err.printf("Finished evicting data.");
      return (m);
  }


    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUsersSuite.class);
        builder.setGlobalConfParameter("site.exec_voltdb_procinfo", true);
        builder.setGlobalConfParameter("site.anticache_enable", true);
        builder.setGlobalConfParameter("site.anticache_profiling", false);
        builder.setGlobalConfParameter("site.anticache_reset", true);
        builder.setGlobalConfParameter("site.anticache_check_interval", Integer.MAX_VALUE);
        builder.setGlobalConfParameter("site.network_startup_wait", 60000);
        builder.setGlobalConfParameter("site.coordinator_sync_time", false);
        builder.setGlobalConfParameter("site.status_enable", false);
        builder.setGlobalConfParameter("site.txn_partition_id_managers", false);

        
        UsersProjectBuilder project = new UsersProjectBuilder();
        project.addAllDefaults();
        project.markTableEvictable(UsersConstants.TABLENAME_USERS);
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-1part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        ////////////////////////////////////////////////////////////
        // CONFIG #2: cluster of 2 nodes running 2 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX+"-cluster.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }

}
