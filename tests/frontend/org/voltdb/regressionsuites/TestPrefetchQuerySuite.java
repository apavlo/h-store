package org.voltdb.regressionsuites;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import edu.brown.benchmark.smallbank.SmallBankProjectBuilder;
import edu.brown.benchmark.smallbank.procedures.SendPayment;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.ProjectType;

/**
 * Regression tests for the query prefetching optimization
 * @author pavlo
 */
public class TestPrefetchQuerySuite extends RegressionSuite {
    
    private static final String PREFIX = "prefetch";

    /**
     * testPrefetch
     * @throws Exception 
     */
    @Test
    public void testPrefetch() throws Exception {
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
        TestSmallBankSuite.initializeSmallBankDatabase(catalogContext, client);
        
        // Check to make sure that we have some prefetch queries
        Procedure catalog_proc = catalogContext.procedures.getIgnoreCase(SendPayment.class.getSimpleName());
        assertNotNull(catalog_proc);
        int prefetch_ctr = 0;
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            assertNotNull(catalog_stmt);
            if (catalog_stmt.getPrefetchable()) prefetch_ctr++;
        } // FOR
        assertTrue(prefetch_ctr > 0);
        
        long acctIds[] = { 1l, 2l };
        double balances[] = { 100d, 0d };
        for (int i = 0; i < acctIds.length; i++) {
            TestSmallBankSuite.updateBalance(client, acctIds[i], balances[i]);
//            TestSmallBankSuite.checkBalance(client, acctIds[i], balances[i]);
        } // FOR
        
        // Run the SendPayment txn to send all the money from the first
        // account to the second account.
        ClientResponse cresponse = client.callProcedure(catalog_proc.getName(),
                                                        acctIds[0], acctIds[1], balances[0]);
        assertEquals(Status.OK, cresponse.getStatus());
        assertFalse(cresponse.toString(), cresponse.isSinglePartition());
        assertTrue(cresponse.toString(), cresponse.getDebug().hadPrefetchedQueries());
        
        VoltTable results[] = cresponse.getResults();
        assertEquals(2, results.length);
        for (int i = 0; i < results.length; i++) {
            assertEquals(1l, results[i].asScalarLong());
        } // FOR
        
        // Make sure the account balances have switched
        for (int i = 0; i < acctIds.length; i++) {
            TestSmallBankSuite.checkBalance(client, acctIds[i], balances[i == 0 ? 1 : 0]);
        } // FOR
    }
    
    /**
     * JUnit / RegressionSuite Boilerplate Constructor
     * @param name The name of this test suite
     */
    public TestPrefetchQuerySuite(String name) {
        super(name);
    }
 
    static public junit.framework.Test suite() throws IOException {
        File mappings = ParametersUtil.getParameterMappingsFile(ProjectType.SMALLBANK);
        File markovs = new File("files/markovs/smallbank-2p.markov.gz"); // HACK
        
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPrefetchQuerySuite.class);
        builder.setGlobalConfParameter("site.exec_prefetch_queries", true);
        builder.setGlobalConfParameter("site.exec_force_singlepartitioned", false);
        builder.setGlobalConfParameter("site.exec_voltdb_procinfo", false);
        builder.setGlobalConfParameter("site.markov_enable", true);
        builder.setGlobalConfParameter("site.markov_path", markovs.getAbsolutePath());
        builder.setGlobalConfParameter("site.markov_path_caching", true);
        builder.setGlobalConfParameter("site.txn_client_debug", true);
        builder.setGlobalConfParameter("site.network_startup_wait", 60000);
        builder.setGlobalConfParameter("client.txn_hints", false);
        
        SmallBankProjectBuilder project = new SmallBankProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addParameterMappings(mappings);
        
        VoltServerConfig config;
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX + "-2part.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        ////////////////////////////////////////////////////////////
        // CONFIG #3: cluster of 2 nodes running 1 site each, one replica
        ////////////////////////////////////////////////////////////
        config = new LocalCluster(PREFIX + "-cluster.jar", 2, 1, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config); 
        
        return builder;
    }
    
}
