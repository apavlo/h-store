package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.prefetchprocs.SquirrelsDistributed;
import org.voltdb.regressionsuites.prefetchprocs.SquirrelsSingle;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.utils.CollectionUtil;

/**
 * Special test cases for checking complex operations in the PlanOptimizer
 * @author pavlo
 */
public class TestPrefetchableSuite extends RegressionSuite {
    
    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { SquirrelsDistributed.class, SquirrelsSingle.class };
    private static final String PREFIX = "prefetch";
    private static final Random rand = new Random(0);

    @Test
    public void testPrefetch() throws IOException, ProcCallException {
        int num_tuples = 2;
        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        Procedure catalog_proc = catalog_db.getProcedures().get(SquirrelsDistributed.class.getSimpleName());
        assertNotNull(catalog_proc);
        
        // Check to make sure that we have some prefetch queries
        int prefetch_ctr = 0;
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            assertNotNull(catalog_stmt);
            if (catalog_stmt.getPrefetchable()) prefetch_ctr++;
        } // FOR
        assertEquals(1, prefetch_ctr);
        
        Client client = this.getClient();
        this.loadDatabase(client, num_tuples);
        // XXX this.checkDatabase(client, num_tuples);
        
        // Execute SquirrelsSingle asynchronously first, which will sleep and block the
        // PartitionExecutor. We will then invoke SquirrelsDistributed, which will get 
        // queued up waiting for the first txn to finish. This will guarantee that our 
        // prefetch query gets executed before the txn's control code is invoked
        int a_id = 0; // rand.nextInt(num_tuples);
        int sleep = 1000;
        client.callProcedure(new NullCallback(), SquirrelsSingle.class.getSimpleName(), a_id, sleep);
        
        ClientResponse cr = client.callProcedure(SquirrelsDistributed.class.getSimpleName(), a_id);
        System.err.println(cr.toString());
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
        
        // Make sure that the transactions have committed at each partition
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_db)) {
            cr = client.callProcedure("@ExecutorStatus", catalog_part.getId());
            System.err.println(cr.toString());
            assertEquals(cr.toString(), Status.OK, cr.getStatus());    
        } // FOR
        
    }
    
    protected void checkDatabase(Client client, int a_expected) throws IOException, ProcCallException {
        ClientResponse cr = null;
        boolean adv;
        
        // TABLEA
        cr = client.callProcedure("GetACount");
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
        cr = client.callProcedure("GetBCount");
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
        cr = client.callProcedure("@LoadMultipartitionTable", a_table.getName(), a_vt);
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
        
        cr = client.callProcedure("@LoadMultipartitionTable", b_table.getName(), b_vt);
        assertEquals(cr.toString(), Status.OK, cr.getStatus());
    }
    
    /**
     * JUnit / RegressionSuite Boilerplate Constructor
     * @param name The name of this test suite
     */
    public TestPrefetchableSuite(String name) {
        super(name);
    }
 
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPrefetchableSuite.class);
        VoltServerConfig config = null;
        
        VoltProjectBuilder project = new VoltProjectBuilder(PREFIX);
        project.addSchema(SquirrelsDistributed.class.getResource(PREFIX + "-ddl.sql"));
        project.addTablePartitionInfo("TABLEA", "A_ID");
        project.addTablePartitionInfo("TABLEB", "B_ID");
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("GetA", "SELECT * FROM TABLEA WHERE A_ID = ?");
        project.addStmtProcedure("GetACount", "SELECT COUNT(*), SUM(A_NUM_B) FROM TABLEA");
        project.addStmtProcedure("GetBCount", "SELECT COUNT(*) FROM TABLEB");
        project.markStatementPrefetchable(SquirrelsDistributed.class, "getRemote");
        project.mapParameters(SquirrelsDistributed.class, 0, "getRemote", 0);
        
        // CLUSTER CONFIG #1
        // One site with four partitions running in this JVM
        config = new LocalSingleProcessServer(PREFIX + "-twoPart.jar", 2, BackendTarget.NATIVE_EE_JNI);
        config.setConfParameter("site.exec_prefetch_queries", true);
        config.setConfParameter("site.exec_force_singlepartitioned", false);
        config.setConfParameter("site.exec_voltdb_procinfo", true);
        config.setConfParameter("client.txn_hints", false);
        config.compile(project);
        builder.addServerConfig(config);
 
        // CLUSTER CONFIG #2
        // Two sites, each with two partitions running in separate JVMs
//        config = new LocalCluster(PREFIX + "-twoSiteTwoPart.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        config.setConfParameter("site.exec_prefetch_queries", true);
//        config.setConfParameter("site.exec_force_singlepartitioned", false);
//        config.setConfParameter("site.exec_voltdb_procinfo", true);
//        config.setConfParameter("client.txn_hints", false);
//        config.compile(project);
//        builder.addServerConfig(config);
 
        return builder;
    }
    
}
