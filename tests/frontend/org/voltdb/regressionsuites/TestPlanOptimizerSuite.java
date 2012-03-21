package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.LocalSingleProcessServer;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb.regressionsuites.VoltServerConfig;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.optimizer.BasePlanOptimizerTestCase;
import edu.brown.utils.StringUtil;

/**
 * Special test cases for checking complex operations in the PlanOptimizer
 * @author pavlo
 */
public class TestPlanOptimizerSuite extends RegressionSuite {
    
    private static final String PREFIX = "planopt";

    @Test
    public void testSingleAggregate() throws IOException, ProcCallException {
        int num_partitions = this.getServerConfig().getPartitionCount();
        Client client = this.getClient();
        final VoltTable vt = this.loadTable(client);
        
        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        Table catalog_tbl = catalog_db.getTables().get("TABLEC");
        assertNotNull(catalog_tbl);
        Column catalog_col = catalog_tbl.getColumns().get("C_VALUE0");
        assertNotNull(catalog_col);
        
        // Compute the AVG in Java so that we can compare
        Map<Integer, List<Integer>> values = new HashMap<Integer, List<Integer>>();
        vt.resetRowPosition();
        while (vt.advanceRow()) {
            int c_b_a_id = (int)vt.getLong(2) % num_partitions;
            if (values.containsKey(c_b_a_id) == false) {
                values.put(c_b_a_id, new ArrayList<Integer>());
            }
            values.get(c_b_a_id).add((int)vt.getLong(catalog_col.getIndex()));
        } // WHILE
        Map<Integer, Double> expectedValues = new HashMap<Integer, Double>();
        for (Integer c_b_a_id : values.keySet()) {
            int total = 0;
            for (Integer val : values.get(c_b_a_id))
                total += val.intValue();
            expectedValues.put(c_b_a_id, total / (double)values.get(c_b_a_id).size());
        } // FOR
        System.err.println("EXPECTED AVERAGES (C_VALUE0)\n" + StringUtil.formatMaps(expectedValues));
        
        VoltTable result = null;
        ClientResponse cr = client.callProcedure("SimpleAggregate");
        assertEquals(Status.OK, cr.getStatus());
        assertEquals(cr.toString(), 1, cr.getResults().length);
        result = cr.getResults()[0];
        assertNotNull(result);
        System.err.println(result);
        
        while (result.advanceRow()) {
            Integer c_b_a_id = Integer.valueOf((int)result.getLong(0));
            double actual = result.getDouble(1);
            Double expected = expectedValues.get(c_b_a_id);
            assertNotNull(c_b_a_id.toString(), expected);
            assertEquals(c_b_a_id.toString(), expected.doubleValue(), actual, 0.1);
        } // WHILE
    }
    
    @Test
    public void testMultipleAggregates() throws IOException, ProcCallException {
        int num_partitions = this.getServerConfig().getPartitionCount();
        Client client = this.getClient();
        final VoltTable vt = this.loadTable(client);
        VoltTable result = null;
        
        // Compute the AVG in Java so that we can compare
        Map<Integer, List<List<Integer>>> values = new HashMap<Integer, List<List<Integer>>>();
        vt.resetRowPosition();
        int num_cols = vt.getColumnCount();
        boolean first = false;
        while (vt.advanceRow()) {
            int c_b_a_id = (int)vt.getLong(2) % num_partitions;
            if (values.containsKey(c_b_a_id) == false) {
                values.put(c_b_a_id, new ArrayList<List<Integer>>());
                first = true;
            }
            for (int i = 0; i < num_cols; i++) {
                if (first) {
                    values.get(c_b_a_id).add(new ArrayList<Integer>());
                }
                values.get(c_b_a_id).get(i).add((int)vt.getLong(i));
            } // FOR
            first = false;
        } // WHILE
        
        Map<Integer, List<Double>> expectedValues = new HashMap<Integer, List<Double>>();
        for (Integer c_b_a_id : values.keySet()) {
            for (List<Integer> vals : values.get(c_b_a_id)) {
                List<Double> colValues = expectedValues.get(c_b_a_id); 
                if (colValues == null) {
                    colValues = new ArrayList<Double>();
                    expectedValues.put(c_b_a_id, colValues);
                }
                int total = 0;
                for (Integer val : vals) {
                    total += val.intValue();
                } // FOR
                colValues.add(total / (double)vals.size());
            } // FOR
        } // FOR
        
        ClientResponse cr = client.callProcedure("MultiAggregate");
        assertEquals(Status.OK, cr.getStatus());
        assertEquals(cr.toString(), 1, cr.getResults().length);
        result = cr.getResults()[0];
        assertNotNull(result);
        System.err.println(result);
        
        int query_offsets[] = { 3, 4 };
        int expected_offsets[] = { 3, 4 };
        while (result.advanceRow()) {
            Integer c_b_a_id = Integer.valueOf((int)result.getLong(0));
            for (int i = 0; i < query_offsets.length; i++) {
                double actual = result.getDouble(query_offsets[i]);
                Double expected = expectedValues.get(c_b_a_id).get(expected_offsets[i]);
                assertNotNull(c_b_a_id.toString(), expected);
                System.err.printf("[%d] => EXPECTED:%.1f / ACTUAL:%.1f\n", c_b_a_id, expected, actual);
                assertEquals(c_b_a_id.toString(), expected.doubleValue(), actual, 0.1);
            } // FOR
        } // WHILE
        
    }
    
    protected VoltTable loadTable(Client client) throws IOException, ProcCallException {
        int num_partitions = this.getServerConfig().getPartitionCount();
        int num_tuples = num_partitions * 10;

        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        Table catalog_tbl = catalog_db.getTables().get("TABLEC");
        assertNotNull(catalog_tbl);
        
        Random rand = new Random(0);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int col_cnt = vt.getColumnCount();
        for (int i = 0; i < num_tuples; i++) {
            Object row[] = new Object[col_cnt];
            int col = 0;
            row[col++] = i;
            row[col++] = i % 5;
            row[col++] = i % num_partitions;
            for ( ; col < col_cnt; col++) {
                int val = rand.nextInt(100);
                row[col] = val;
            } // FOR
            vt.addRow(row);
        } // FOR
        
        ClientResponse cr = client.callProcedure("@LoadMultipartitionTable", catalog_tbl.getName(), vt);
        assertEquals(Status.OK, cr.getStatus());
        
        return (vt);
    }
    
    /**
     * JUnit / RegressionSuite Boilerplate Constructor
     * @param name The name of this test suite
     */
    public TestPlanOptimizerSuite(String name) {
        super(name);
    }
 
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPlanOptimizerSuite.class);
        VoltProjectBuilder project = new BasePlanOptimizerTestCase.PlanOptimizerTestProjectBuilder(PREFIX);
        VoltServerConfig config = null;
 
        // Single Statement Procedures
        project.addStmtProcedure("SimpleAggregate",
                                 "SELECT C_B_A_ID, AVG(C_VALUE0) " +
                                 "FROM TABLEC GROUP BY C_B_A_ID");
        project.addStmtProcedure("MultiAggregate",
                                 "SELECT C_B_A_ID, SUM(C_VALUE0), SUM(C_VALUE1), " +
                                 "       AVG(C_VALUE0), AVG(C_VALUE1) " +
                                 "FROM TABLEC GROUP BY C_B_A_ID");
 
        // CLUSTER CONFIG #1
        // One site with four partitions running in this JVM
        config = new LocalSingleProcessServer(PREFIX + "-twoPart.jar", 4, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);
 
        // CLUSTER CONFIG #2
        // Two sites, each with two partitions running in separate JVMs
        config = new LocalCluster(PREFIX + "-twoSiteTwoPart.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);
 
        return builder;
    }
    
}
