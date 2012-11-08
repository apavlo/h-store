package org.voltdb.regressionsuites;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.LoadWarehouse;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.sysprocs.LoadMultipartitionTable;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.ProjectType;

/**
 * Special test cases for checking complex operations in the PlanOptimizer
 * @author pavlo
 */
public class TestPrefetchableSuite extends RegressionSuite {
    
    /** Procedures used by this suite */
    @SuppressWarnings("unchecked")
    static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        neworder.class, LoadWarehouse.class
    };
    private static final String PREFIX = "prefetch";
    private static final Random rand = new Random(0);

    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        Client client = this.getClient();
        RegressionSuiteUtil.initializeTPCCDatabase(this.getCatalog(), client);
        
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        for (String tableName : TPCCConstants.TABLENAMES) {
            String query = "SELECT COUNT(*) FROM " + tableName;
            ClientResponse cresponse = client.callProcedure(procName, query);
            assertEquals(Status.OK, cresponse.getStatus());
            VoltTable results[] = cresponse.getResults();
            assertEquals(1, results.length);
            long count = results[0].asScalarLong();
            assertTrue(tableName + " -> " + count, count > 0);
            // System.err.println(tableName + "\n" + VoltTableUtil.format(results[0]));
        } // FOR
    }
    
    /**
     * testPrefetch
     * @throws Exception 
     */
    @Test
    public void testPrefetch() throws Exception {
//        int num_tuples = 2;
        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        Procedure catalog_proc = catalog_db.getProcedures().get(neworder.class.getSimpleName());
        assertNotNull(catalog_proc);
        
        // Check to make sure that we have some prefetch queries
        int prefetch_ctr = 0;
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            assertNotNull(catalog_stmt);
            if (catalog_stmt.getPrefetchable()) prefetch_ctr++;
        } // FOR
        assertTrue(prefetch_ctr > 0);
        
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();
//        this.loadDatabase(client, num_tuples);
        RegressionSuiteUtil.initializeTPCCDatabase(catalogContext.catalog, client);
        // XXX this.checkDatabase(client, num_tuples);
        
        // Enable the feature on the server
        RegressionSuiteUtil.setHStoreConf(client, "site.markov_path_caching", "true");
        
        // Execute SquirrelsSingle asynchronously first, which will sleep and block the
        // PartitionExecutor. We will then invoke SquirrelsDistributed, which will get 
        // queued up waiting for the first txn to finish. This will guarantee that our 
        // prefetch query gets executed before the txn's control code is invoked
//        int a_id = 0; // rand.nextInt(num_tuples);
//        int sleep = 5000;
//        client.callProcedure(new NullCallback(), SquirrelsSingle.class.getSimpleName(), a_id, sleep);
        
        // Fire off a distributed neworder txn
        String procName = neworder.class.getSimpleName();
        Object params[] = RegressionSuiteUtil.generateNewOrder(catalogContext.numberOfPartitions, true, (short)2);
        
        ClientResponse cresponse = client.callProcedure(procName, params);
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        assertFalse(cresponse.toString(), cresponse.isSinglePartition());
        assertTrue(cresponse.toString(), cresponse.getDebug().hadPrefetchedQueries());
        
//        ClientResponse cr = client.callProcedure(neworder.class.getSimpleName(), a_id, sleep);
        System.err.println(cresponse.toString());
        assertEquals(cresponse.toString(), Status.OK, cresponse.getStatus());
        
        // Make sure that the transactions have committed at each partition
//        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_db)) {
//            cr = client.callProcedure("@ExecutorStatus", catalog_part.getId());
//            System.err.println(cr.toString());
//            assertEquals(cr.toString(), Status.OK, cr.getStatus());    
//        } // FOR
        
    }
    
    protected void checkDatabase(Client client, int a_expected) throws IOException, ProcCallException {
        ClientResponse cr = null;
        boolean adv;
        
        // TABLEA
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        cr = client.callProcedure(procName, "SELECT COUNT(*), SUM(A_NUM_B) FROM TABLEA");
//        System.err.println(cr.toString());
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
        assertEquals(cr.toString(), 1, cr.getResults().length);
        VoltTable a_results = cr.getResults()[0];
        adv = a_results.advanceRow();
        assert(adv);
        int a_count = (int)a_results.getLong(0);
        int b_expected = (int)a_results.getLong(1);
        assertEquals(a_expected, a_count);
        
        // TABLEB
        cr = client.callProcedure(procName, "SELECT COUNT(*) FROM TABLEB");
//        System.err.println(cr.toString());
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
        assertEquals(cr.toString(), 1, cr.getResults().length);
        VoltTable b_results = cr.getResults()[0];
        adv = b_results.advanceRow();
        assert(adv);
        int b_count = (int)b_results.getLong(0);
        assertEquals(b_expected, b_count);
    }
    
    protected void loadDatabase(Client client, int num_tuples) throws IOException, ProcCallException {
        final Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        
        Table a_table = catalog_db.getTables().get("TABLEA");
        assertNotNull(a_table);
        VoltTable a_vt = CatalogUtil.getVoltTable(a_table);
        Object a_row[] = new Object[a_table.getColumns().size()];
        
        Table b_table = catalog_db.getTables().get("TABLEB");
        assertNotNull(b_table);
        VoltTable b_vt = CatalogUtil.getVoltTable(b_table);
        Object b_row[] = new Object[b_table.getColumns().size()];
        
        int b_id = 0;
        int idx = 0;
        for (int a_id = 0; a_id < num_tuples; a_id++) {
            int num_b_records = rand.nextInt(5);
            idx = 0;
            a_row[idx++] = a_id;
            a_row[idx++] = 0;
            a_row[idx++] = num_b_records;
            a_vt.addRow(a_row);
            
            for (int i = 0; i < num_b_records; i++) {
                idx = 0;
                b_row[idx++] = b_id++;
                b_row[idx++] = a_id;
                b_row[idx++] = rand.nextInt(100);
                b_vt.addRow(b_row);
            } // FOR (TABLEB)
        } // FOR (TABLEA)
        
        ClientResponse cr = null;
        String procName = VoltSystemProcedure.procCallName(LoadMultipartitionTable.class);
        cr = client.callProcedure(procName, a_table.getName(), a_vt);
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
        
        cr = client.callProcedure(procName, b_table.getName(), b_vt);
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
    }
    
    /**
     * JUnit / RegressionSuite Boilerplate Constructor
     * @param name The name of this test suite
     */
    public TestPrefetchableSuite(String name) {
        super(name);
    }
 
    static public junit.framework.Test suite() throws IOException {
        File mappings = ParametersUtil.getParameterMappingsFile(ProjectType.TPCC);
        File markovs = new File("files/markovs/vldb-august2012/tpcc-4p.markov.gz"); // HACK
        
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPrefetchableSuite.class);
        
        builder.setGlobalConfParameter("site.exec_prefetch_queries", true);
        builder.setGlobalConfParameter("site.exec_force_singlepartitioned", false);
        builder.setGlobalConfParameter("site.exec_voltdb_procinfo", false);
        builder.setGlobalConfParameter("client.txn_hints", false);
        builder.setGlobalConfParameter("site.markov_enable", true);
        builder.setGlobalConfParameter("site.markov_path", markovs.getAbsolutePath());
        builder.setGlobalConfParameter("site.txn_client_debug", true);
        builder.setGlobalConfParameter("site.network_startup_wait", 60000);
        
//        VoltProjectBuilder project = new VoltProjectBuilder(PREFIX);
//        project.addSchema(SquirrelsDistributed.class.getResource(PREFIX + "-ddl.sql"));
//        project.addTablePartitionInfo("TABLEA", "A_ID");
//        project.addTablePartitionInfo("TABLEB", "B_ID");
//        project.addProcedures(PROCEDURES);
//        project.markStatementPrefetchable(SquirrelsDistributed.class, "getRemote");
//        project.mapParameters(SquirrelsDistributed.class, 0, "getRemote", 0);
        
        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addParameterMappings(mappings);
        
        boolean success;
        
        // CLUSTER CONFIG #1
        // One site with four partitions running in this JVM
        config = new LocalSingleProcessServer(PREFIX + "-twoPart.jar", 4, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
 
        // CLUSTER CONFIG #2
        // Two sites, each with two partitions running in separate JVMs
        config = new LocalCluster(PREFIX + "-twoSiteTwoPart.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
 
        return builder;
    }
    
}
