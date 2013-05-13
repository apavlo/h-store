package edu.brown.hstore.txns;

import java.util.Random;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.mapreduce.procedures.MockMapReduce;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.ProjectType;

public class TestMapReduceTransaction extends BaseTestCase{
    static final int NUM_ROWS = 10;
    static final Random rand = new Random();
    static Class<? extends VoltProcedure> TARGET_PROCEDURE = MockMapReduce.class;
    
    private VoltTable table;
    private VoltMapReduceProcedure<?> voltProc;
    private VoltTable.ColumnInfo[] schema;
    
    
    private final int NUM_HOSTS               = 1;
    private final int NUM_SITES_PER_HOST      = 4;
    private final int NUM_PARTITIONS_PER_SITE = 2;
    private final int NUM_SITES               = (NUM_HOSTS * NUM_SITES_PER_HOST);
    
    private final MockHStoreSite sites[] = new MockHStoreSite[NUM_SITES_PER_HOST];
//    private final HStoreCoordinator messengers[] = new HStoreCoordinator[NUM_SITES_PER_HOST];
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.MAPREDUCE);
        
        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        this.initializeCatalog(NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        for (int i = 0; i < NUM_SITES; i++) {
            this.sites[i] = new MockHStoreSite(i, catalogContext, HStoreConf.singleton());
            
            // We have to make our fake ExecutionSites for each Partition at this site
            for (int p : this.sites[i].getLocalPartitionIds().values()) {
                MockPartitionExecutor executor = new MockPartitionExecutor(p, catalogContext, p_estimator);
                this.sites[i].addPartitionExecutor(p, executor);
            } // FOR
        } // FOR

        
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
       MapReduceTransaction ts = new MapReduceTransaction(this.sites[0]);
       StoredProcedureInvocation request = new StoredProcedureInvocation(-1, TARGET_PROCEDURE.getSimpleName()); 
       Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
       
//       Collection<Integer> local_partitions = this.sites[0].getLocalPartitionIds();
//       System.err.println(local_partitions);
//       ts.init(123456789l, 0, 0, local_partitions, false, true, catalog_proc, request, null);
//       ts.storeData(1, table);
//       
//       VoltTable result = ts.getReduceInputByPartition(1);
//       assertNotNull(result);
//       this.compareTables(this.table, result);
       
    }
}
