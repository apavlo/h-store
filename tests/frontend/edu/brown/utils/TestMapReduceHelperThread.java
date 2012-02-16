package edu.brown.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.mapreduce.procedures.MockMapReduce;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockExecutionSite;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.util.MapReduceHelperThread;
import edu.brown.utils.ProjectType;

public class TestMapReduceHelperThread extends BaseTestCase{
    
    private final int NUM_HOSTS               = 1;
    private final int NUM_SITES_PER_HOST      = 2;
    private final int NUM_PARTITIONS_PER_SITE = 2;
    private final int NUM_SITES               = (NUM_HOSTS * NUM_SITES_PER_HOST);
    
    private final HStoreSite hstore_sites[] = new HStoreSite[NUM_SITES_PER_HOST];
    private final HStoreCoordinator coordinators[] = new HStoreCoordinator[NUM_SITES_PER_HOST];
    
    private final VoltTable.ColumnInfo columns[] = {
        new VoltTable.ColumnInfo("key", VoltType.STRING),
        new VoltTable.ColumnInfo("value", VoltType.BIGINT),
    };
    
    static final int NUM_ROWS = 10;
    static final Random rand = new Random();
    Collection<Integer> all_partitions;
    static Class<? extends VoltProcedure> TARGET_PROCEDURE = MockMapReduce.class;
    
    
    private VoltTable table;
    private VoltMapReduceProcedure<?> voltProc;
    private VoltTable.ColumnInfo[] schema;
    
    
    @Override
    public void setUp() throws Exception {
        super.setUp(ProjectType.MAPREDUCE);
        
        HStoreConf.singleton().site.coordinator_sync_time = false;
        
        this.initializeCluster(NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        for (int i = 0; i < NUM_SITES; i++) {
            Site catalog_site = this.getSite(i);
            this.hstore_sites[i] = new MockHStoreSite(catalog_site, HStoreConf.singleton());
            this.coordinators[i] = this.hstore_sites[i].initHStoreCoordinator();
            
            // We have to make our fake ExecutionSites for each Partition at this site
            for (Partition catalog_part : catalog_site.getPartitions()) {
                MockExecutionSite es = new MockExecutionSite(catalog_part.getId(), catalog, p_estimator);
                this.hstore_sites[i].addPartitionExecutor(catalog_part.getId(), es);
                es.initHStoreSite(this.hstore_sites[i]);
            } // FOR
        } // FOR
        
        this.all_partitions = CatalogUtil.getAllPartitionIds(this.catalog_db);
        this.voltProc = (VoltMapReduceProcedure<?>)ClassUtil.newInstance(TARGET_PROCEDURE, new Object[0], new Class<?>[0]);
        assertNotNull(this.voltProc);
        this.schema = this.voltProc.getMapOutputSchema();
        assertNotNull(this.schema);
        this.table = new VoltTable(schema);
       
    }
    
    @Override
    protected void tearDown() throws Exception {
        System.err.println("TEAR DOWN!");
        super.tearDown();
  
    }
    
    public void generateTable (MapReduceTransaction ts) {
        Object row[] = new Object[this.schema.length];
        for (int p: this.all_partitions) {
            for (int i = 0; i < NUM_ROWS; i++) {
                for (int j = 0; j < row.length; j++) {
                    row[j] = VoltTypeUtil.getRandomValue(schema[j].getType());
                } // FOR
                this.table.addRow(row);
            } // FOR
            ts.setMapOutputByPartition(this.table, p);
            this.table.clearRowData();
        }
    }
    
    public void testShuffle() {
        
        MapReduceTransaction ts = new MapReduceTransaction (this.hstore_sites[0]);
        StoredProcedureInvocation request = new StoredProcedureInvocation(-1, TARGET_PROCEDURE.getSimpleName()); 
        Procedure catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        ts.init(123456789l, 0, 0, all_partitions, false, true, catalog_proc, request, null);
        
        this.generateTable(ts);
        MapReduceHelperThread mr_helper = this.hstore_sites[0].getMapReduceHelper();
        ts.setShufflePhase();
        mr_helper.queue(ts);
        mr_helper.run();
        
        HashMap <Object,Integer> hashkey = new  HashMap<Object, Integer> ();
        VoltTable vt;
        for (Integer p: this.all_partitions) {
            vt = ts.getMapOutputByPartition(p);
            while(vt.advanceRow()) {
                Object key = vt.get(0);
                if(!hashkey.containsKey(key)) {
                    hashkey.put(key, p);
                } else {
                    assertEquals(hashkey.get(key), p);
                }
            }
            
        }
    }
    

}
