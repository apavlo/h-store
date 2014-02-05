package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.Random;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.ByteBuilder;
import org.voltdb.benchmark.tpcc.procedures.GetTableCounts;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import edu.brown.hstore.Hstoreservice.Status;


public class TestJVMSnapshotSuite extends RegressionSuite {
	
    private static final String PREFIX = "jvmsnapshot";
    
    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
	public TestJVMSnapshotSuite(String name) {
		super(name);
	}
	
    /**
     * Supplemental classes needed by TPC-C procs.
     */
    public static final Class<?>[] SUPPLEMENTALS = {
        ByteBuilder.class, TPCCConstants.class
    };

    public void testTablecount() throws IOException, ProcCallException {
        Client client = getClient();
        ClientResponse cr = null;
        CatalogContext catalogContext = this.getCatalogContext();
        
        Random rand = this.getRandom();
        int num_tuples = 11;
        for (Table catalog_tbl : catalogContext.database.getTables()) {
            RegressionSuiteUtil.loadRandomData(client, catalog_tbl, rand, num_tuples);
        } // FOR (table)
        cr = client.callProcedure("@NoOp");
        
        // Now get the counts for the tables that we just loaded
        cr = client.callProcedure("@Adhoc", "SELECT COUNT(*) FROM warehouse;");
        System.err.println(cr);
        assertEquals(Status.OK, cr.getStatus());
        assertEquals(1, cr.getResults().length);
        VoltTable vt = cr.getResults()[0];
        while (vt.advanceRow()) {
            String tableName = vt.getString(0);
            int count = (int)vt.getLong(1);
            assertEquals(tableName, num_tuples, count);
        } // WHILE
    }
    
    public static Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJVMSnapshotSuite.class);
        builder.setGlobalConfParameter("site.jvmsnapshot_enable", true);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addSupplementalClasses(SUPPLEMENTALS);
        
        boolean success;
        
        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partition running on JNI backend
        /////////////////////////////////////////////////////////////
        config = new LocalSingleProcessServer(PREFIX+"-1part.jar", 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
