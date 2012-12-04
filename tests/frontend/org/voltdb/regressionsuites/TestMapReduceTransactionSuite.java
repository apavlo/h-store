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
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.ByteBuilder;
import org.voltdb.benchmark.tpcc.procedures.MRquery1;
import org.voltdb.benchmark.tpcc.procedures.MRquery6;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;

public class TestMapReduceTransactionSuite extends RegressionSuite {
    
    private static final String PREFIX = "mr";
    
    /**
     * Supplemental classes needed by TPC-C procs.
     */
    public static final Class<?>[] SUPPLEMENTALS = {
        ByteBuilder.class, TPCCConstants.class
    };
    
    
    public TestMapReduceTransactionSuite(String name) {
        super(name);
    }
    
    @Test
    public void testMMRquery1 () throws IOException, ProcCallException {
        //int num_partitions = this.getServerConfig().getPartitionCount();
        Client client = this.getClient();
        final VoltTable vt = this.loadTable_ORDER_LINE(client);
        
        assertEquals(vt.getColumnCount(), 10);
        
        // Computer Query1 information
        /* MapReduce OLAP Experimental Queries
        addStmtProcedure("OLAPQuery1",
                         "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), " +
                         "       AVG(ol_quantity), AVG(ol_amount), COUNT(*)" +
                         "  FROM ORDER_LINE " +
                         " GROUP BY ol_number order by ol_number");*/
        
        // 0:ol_number,1:sum(ol_quantity),2:SUM(ol_amount),3:weight(ol_quantity),4:weight(ol_amount),5:sum
        List< List<Object>> rtLists = new ArrayList< List< Object >>();
        vt.resetRowPosition();
        Map<Integer,Integer> map = new HashMap<Integer,Integer>();
        //Set <Integer> keyset = new HashSet <Integer>(); 
        Integer ct = 0;
        while(vt.advanceRow()) {
            Integer key = new Integer ((int) vt.getLong(3));
            if (!map.containsKey(key)) {
                map.put(new Integer ((int) vt.getLong(3)),ct);
                List <Object> cont = new ArrayList<Object>();
                rtLists.add(ct,cont);
                
                cont.add(0,key);
                //rtLists.get(ct).add(0, vt.getLong(3));
                if (cont.size() < 2) cont.add(1, vt.getLong(7));
                else cont.set(1, vt.getLong(7) + ((Long)cont.get(1)).longValue());
                if (cont.size() < 3) cont.add(2, vt.getDouble(8));
                else cont.set(2, vt.getDouble(8) + ((Double)cont.get(2)).doubleValue());
                if (cont.size() < 4) cont.add(3, 1); 
                else cont.set(3, ((Integer)cont.get(3)).intValue() + 1);
                
                ct++;
            } else {
                int index = map.get(key);
                assertEquals(key, rtLists.get(index).get(0));
                //rtLists.get(ct).set(0, vt.getLong(3));
                rtLists.get(index).set(1, vt.getLong(7) + ((Long)rtLists.get(index).get(1)).longValue());
                rtLists.get(index).set(2, vt.getDouble(8) + ((Double)rtLists.get(index).get(2)).doubleValue());
                rtLists.get(index).set(3, ((Integer)rtLists.get(index).get(3) ).intValue()+ 1);
            }
        }
        
        // execute MapReduce Transaction to check the result
        ClientResponse cr = client.callProcedure("MRquery1");
        assertEquals(Status.OK, cr.getStatus());
        System.out.println("I am starting to compare the results...");
        int index = -1;
        // 0:ol_number,1:sum(ol_quantity),2:SUM(ol_amount),3:weight(ol_quantity),4:weight(ol_amount),5:sum
        for ( VoltTable v : cr.getResults()) {
            System.out.println("Jason,voltable:" + v);
            while (v.advanceRow()) {
                Integer key = new Integer ((int) v.getLong(0));
                assertTrue(map.containsKey(key));
                index = map.get(key);
                List <Object> cont = rtLists.get(index);
                System.out.println("Jason,result List:" + cont);
                long num =  ((Integer) cont.get(3)).longValue();
                assertEquals(key, cont.get(0));
                System.out.println("Compare (1):" + v.getLong(1) + " (2):"+ ((Long)cont.get(1)).longValue());
                assertEquals(((Long)cont.get(1)).longValue(), v.getLong(1));
                assertEquals(((Double)cont.get(2)).doubleValue(), v.getDouble(2), 0.1);
                assertEquals(v.getLong(3), ((Long)(v.getLong(1)/num)).longValue());
                assertEquals(v.getDouble(4), ((Double)(v.getDouble(2)/num)).doubleValue());
                assertEquals(v.getLong(5), num);
            }
            v.resetRowPosition();
        }
        
    }
    
    
    protected VoltTable loadTable_ORDER_LINE(Client client) throws IOException, ProcCallException {
        int num_partitions = this.getServerConfig().getPartitionCount();
        int num_tuples = num_partitions * 10;

        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        Table catalog_tbl = catalog_db.getTables().get("ORDER_LINE");
        assertNotNull(catalog_tbl);
        /*
        CREATE TABLE ORDER_LINE (
                OL_O_ID INTEGER DEFAULT '0' NOT NULL,
                OL_D_ID TINYINT DEFAULT '0' NOT NULL,
                OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
                OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
                OL_I_ID INTEGER DEFAULT NULL,
                OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
                OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
                OL_QUANTITY INTEGER DEFAULT NULL,
                OL_AMOUNT FLOAT DEFAULT NULL,
                OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
                PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
                CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
                CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
              );
              */
        
        Random rand = new Random(0);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int col_cnt = vt.getColumnCount();
        for (int i = 0; i < num_tuples; i++) {
            Object row[] = new Object[col_cnt];
            int col = 0;
            row[col++] = i; 
            row[col++] = i;
            row[col++] = i;
            row[col++] = rand.nextInt(10); // OL_NUMBER
            col+=3; // disregard OL_I_ID,OL_SUPPLY_W_ID,OL_DELIVERY_D
            row[col++] = i * 2;
            row[col++] = 1.2 * i;
            row[col++] = "null message";
            assertEquals(col, 10);
            vt.addRow(row);
        } // FOR
        
        ClientResponse cr = client.callProcedure("@LoadMultipartitionTable", catalog_tbl.getName(), vt);
        assertEquals(Status.OK, cr.getStatus());
        
        return (vt);
    }
    
    public static junit.framework.Test suite() {
        VoltServerConfig config = null;
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMapReduceTransactionSuite.class);

        // build up a project builder for the TPC-C app
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addSupplementalClasses(SUPPLEMENTALS);
        project.addProcedure(MRquery1.class);
        project.addProcedure(MRquery6.class);
        
        boolean success = false;
        
        // CLUSTER CONFIG #1
        // One site with four partitions running in this JVM
        config = new LocalSingleProcessServer(PREFIX + "-twoPart.jar", 2, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);
        
        // CLUSTER CONFIG #2
        // Two sites, each with two partitions running in separate JVMs
//        config = new LocalCluster(PREFIX + "-twoSiteTwoPart.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
//        config.setTestNameSuffix("mapBlocking_reduceBlocking");
//        config.setConfParameter("site.mr_map_blocking", true);
//        config.setConfParameter("site.mr_reduce_blocking", true);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);
//        
//        // CLUSTER CONFIG #3
//        config = new LocalCluster(PREFIX + "-twoSiteFourPart_rB.jar", 2, 4, 1, BackendTarget.NATIVE_EE_JNI);
//        config.setTestNameSuffix("mapBlocking_reduceNonBlocking");
//        config.setConfParameter("site.mr_map_blocking", true);
//        config.setConfParameter("site.mr_reduce_blocking", false);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);
//                
//        // CLUSTER CONFIG #4
//        config = new LocalCluster(PREFIX + "-twoSiteFourPart_mNB.jar",  2, 4, 1, BackendTarget.NATIVE_EE_JNI);
//        config.setTestNameSuffix("mapNonBlocking");
//        config.setConfParameter("site.mr_map_blocking", false);
//        config.setConfParameter("site.mr_reduce_blocking", false);
//        success = config.compile(project);
//        assert(success);
//        builder.addServerConfig(config);
        
        return builder;
    }
    
    
}
