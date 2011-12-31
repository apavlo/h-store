package edu.mit.hstore.dtxn;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.voltdb.ExecutionSite;
import org.voltdb.MockExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.mapreduce.procedures.MockMapReduce;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.TestHStoreCoordinator.AssertThreadGroup;
import edu.mit.hstore.TestVoltProcedureListener.MockHandler;

import junit.framework.TestCase;

public class TestMapReduceTransaction extends BaseTestCase{
    static final int NUM_ROWS = 10;
    static final Random rand = new Random();
    static Collection<Integer> all_partitions;
    static Class<? extends VoltProcedure> TARGET_PROCEDURE = MockMapReduce.class;
    
    
    private VoltTable table;
    private VoltMapReduceProcedure<?> voltProc;
    private VoltTable.ColumnInfo[] schema;
    
    
    private final int NUM_HOSTS               = 1;
    private final int NUM_SITES_PER_HOST      = 4;
    private final int NUM_PARTITIONS_PER_SITE = 2;
    private final int NUM_SITES               = (NUM_HOSTS * NUM_SITES_PER_HOST);
    
    private final HStoreSite sites[] = new HStoreSite[NUM_SITES_PER_HOST];
    private final HStoreCoordinator messengers[] = new HStoreCoordinator[NUM_SITES_PER_HOST];
    
    @Override
    protected void setUp() throws Exception {
        
            super.setUp(ProjectType.MAPREDUCE);
        
        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        this.initializeCluster(NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        for (int i = 0; i < NUM_SITES; i++) {
            Site catalog_site = this.getSite(i);
            
            // We have to make our fake ExecutionSites for each Partition at this site
            Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
            for (Partition catalog_part : catalog_site.getPartitions()) {
                executors.put(catalog_part.getId(), new MockExecutionSite(catalog_part.getId(), catalog, p_estimator));
            } // FOR
            
            this.sites[i] = new HStoreSite(catalog_site, executors, p_estimator);
            this.messengers[i] = this.sites[i].getCoordinator();
        } // FOR

        
        this.all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        
        this.voltProc = (VoltMapReduceProcedure<?>)ClassUtil.newInstance(TARGET_PROCEDURE, new Object[0], new Class<?>[0]);
        assertNotNull(this.voltProc);
        this.schema = this.voltProc.getMapOutputSchema();
        assertNotNull(this.schema);
        assert(this.schema.length > 0);
        this.table = new VoltTable(schema);
        Object row[] = new Object[this.schema.length];
        for (int i = 0; i < NUM_ROWS; i++) {
            for (int j = 0; j < row.length; j++) {
                row[j] = VoltTypeUtil.getRandomValue(schema[j].getType());
            } // FOR
            this.table.addRow(row);
            
        } // FOR
        
        
        assertEquals(NUM_ROWS, this.table.getRowCount());
    }
    
    /*ore$SendDataResponse$Builder.build(Unknown Source)
     [java]     at edu.mit.hstore.HStoreCoordinator.sendData(HStoreCoordinator.java:880)
     [java]     at edu.mit.hstore.util.MapReduceHelperThread.shuffle(MapReduceHelperThread.j
     * test storeData()
     */
    
    private void compareTables(VoltTable vt0, VoltTable vt1) {
        assertEquals(vt0.getRowCount(), vt1.getRowCount());
        assertEquals(vt0.getColumnCount(), vt1.getColumnCount());
        assert(vt1.getColumnCount() > 0);
        int rows = 0;
        while (vt0.advanceRow() && vt1.advanceRow()) {
            VoltTableRow row0 = vt0.fetchRow(vt0.getActiveRowIndex());
            VoltTableRow row1 = vt1.fetchRow(vt1.getActiveRowIndex());
            
            for (int i = 0; i < vt0.getColumnCount(); i++) {
//                System.err.println(i + ": " + row1.get(i));
                assertEquals(row0.get(i), row1.get(i));
            } // FOR
            rows++;
        } // WHILE
        assert(rows > 0);
    }
    
    public void testStoreData() throws Exception {
       MapReduceTransaction ts = new MapReduceTransaction (this.sites[0]);
       StoredProcedureInvocation request = new StoredProcedureInvocation(-1, TARGET_PROCEDURE.getSimpleName()); 
       Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
       ts.init(123456789, 0, 0, all_partitions, false, true, null, catalog_proc, request, null);
       

       ts.storeData(1, table);
       
       VoltTable result = ts.getReduceInputByPartition(1);
       assertNotNull(result);
       this.compareTables(this.table, result);
       
    }
}
