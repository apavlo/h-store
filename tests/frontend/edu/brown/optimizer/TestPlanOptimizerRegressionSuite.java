package edu.brown.optimizer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;

public class TestPlanOptimizerRegressionSuite extends RegressionSuite {

    @Test
    public void testMultipleAggregatesNoCount() throws IOException, ProcCallException {
        int num_tuples = 10;
        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        Table catalog_tbl = catalog_db.getTables().get("TABLEC");
        assertNotNull(catalog_tbl);
        
        Random rand = new Random(0);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int col_cnt = vt.getColumnCount();
        
        int totals[] = new int[col_cnt];
        Arrays.fill(totals, 0);
        
        for (int i = 0; i < num_tuples; i++) {
            Object row[] = new Object[col_cnt];
            int col = 0;
            row[col++] = i;
            row[col++] = i % 5;
            row[col++] = 1;
            for ( ; col < col_cnt; col++) {
                int val = rand.nextInt(100);
                row[col] = val;
                totals[col] += val;
            } // FOR
            vt.addRow(row);
        } // FOR
        
        Client client = this.getClient();
        ClientResponse cr = client.callProcedure("@LoadMultipartitionTable", catalog_tbl.getName(), vt);
        assertEquals(Status.OK, cr.getStatus());
        
        cr = client.callProcedure("MultipleAggregatesNoCount");
        assertEquals(Status.OK, cr.getStatus());
        assertEquals(cr.toString(), 1, cr.getResults().length);
        VoltTable result = cr.getResults()[0];
        assertNotNull(result);
        
        System.err.println(result);
        
    }
    
    /**
     * JUnit / RegressionSuite Boilerplate Constructor
     * @param name The name of this test suite
     */
    public TestPlanOptimizerRegressionSuite(String name) {
        super(name);
    }
 
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPlanOptimizerRegressionSuite.class);
        VoltProjectBuilder project = new BasePlanOptimizerTestCase.PlanOptimizerTestProjectBuilder("planoptreg");
        VoltServerConfig config = null;
 
        // Single Statement Procedures
//        project.addStmtProcedure("MultipleAggregatesNoCount",
//                "SELECT C_B_ID, SUM(C_VALUE0), SUM(C_VALUE1), " +
//                "       AVG(C_VALUE0), AVG(C_VALUE1) " +
//                "FROM TABLEC GROUP BY C_B_ID");
        
        project.addStmtProcedure("MultipleAggregatesNoCount",
                                 "SELECT C_B_A_ID, SUM(C_VALUE0), SUM(C_VALUE1), " +
                                 "       AVG(C_VALUE0), AVG(C_VALUE1) " +
                                 "FROM TABLEC GROUP BY C_B_A_ID");
 
        // CLUSTER CONFIG #1
        // One site with four partitions running in this JVM
        config = new LocalSingleProcessServer("planoptreg-twoPart.jar", 4, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);
 
        // CLUSTER CONFIG #2
        // Two sites, each with two partitions running in separate JVMs
//        config = new LocalCluster("planoptreg-twoSiteTwoPart.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        config.compile(project);
//        builder.addServerConfig(config);
 
        return builder;
    }
    
}
