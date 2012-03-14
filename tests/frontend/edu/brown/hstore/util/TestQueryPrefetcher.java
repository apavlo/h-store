package edu.brown.hstore.util;

import java.util.List;
import java.util.Random;

import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.seats.procedures.UpdateReservation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.utils.ProjectType;

public class TestQueryPrefetcher extends BaseTestCase {
	private static final Long TXN_ID = 1000l;
	
	private static final Class<? extends VoltProcedure> TARGET_PREFETCH_PROCEDURE = UpdateReservation.class;
	private static final String TARGET_PREFETCH_STATEMENT = "CheckCustomer";
	
	private static final int NUM_HOSTS = 1;
	private static final int NUM_SITES = 4; // per host
	private static final int NUM_PARTITIONS = 4; // per site
	private static final int LOCAL_SITE = 0;
	private static final int LOCAL_PARTITION = 0;

	private final MockHStoreSite[] hstore_sites = new MockHStoreSite[NUM_SITES];
	private final HStoreCoordinator[] coordinators = new HStoreCoordinator[NUM_SITES];
	
    private LocalTransaction ts;
    
    private QueryPrefetcher prefetcher;
    private int[] partition_site_xref;
    private Random rand = new Random(0);

	Object proc_params[] = {
			100l,					// r_id
			LOCAL_PARTITION + 1l,	// c_id
			LOCAL_PARTITION,		// f_id
			rand.nextInt(100),		// seatnum
			rand.nextInt(10),		// attr_idx
			rand.nextLong(),		// attr_val
		};
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.SEATS);
        this.initializeCluster(NUM_HOSTS, NUM_SITES, NUM_PARTITIONS);
        
        this.prefetcher = new QueryPrefetcher(catalog_db, p_estimator);
        for (int i = 0; i < NUM_SITES; i++) {
            Site catalog_site = this.getSite(i);
            this.hstore_sites[i] = new MockHStoreSite(catalog_site, HStoreConf.singleton());
            this.coordinators[i] = this.hstore_sites[i].initHStoreCoordinator();
            
            // We have to make our fake ExecutionSites for each Partition at this site
            for (Partition catalog_part : catalog_site.getPartitions()) {
                MockPartitionExecutor es = new MockPartitionExecutor(catalog_part.getId(), catalog, p_estimator);
                this.hstore_sites[i].addPartitionExecutor(catalog_part.getId(), es);
                es.initHStoreSite(this.hstore_sites[i]);
            } // FOR
        } // FOR
        
        final ParameterSet params = new ParameterSet(proc_params);
        this.ts = new LocalTransaction(hstore_sites[LOCAL_SITE]) {
        	public org.voltdb.ParameterSet getProcedureParameters() {
        		return (params);
        	}
        };
        this.ts.testInit(TXN_ID,
                         LOCAL_PARTITION,
                         CatalogUtil.getAllPartitionIds(catalog_db),
                         this.getProcedure(TARGET_PREFETCH_PROCEDURE));
        
        this.partition_site_xref = new int[CatalogUtil.getNumberOfPartitions(catalog_db)];
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_db)) {
            this.partition_site_xref[catalog_part.getId()] = ((Site) catalog_part.getParent()).getId();
        } // FOR
    }
    
    /**
     * testGenerateWorkFragments
     */
    public void testGenerateWorkFragments() throws Exception {
    	TransactionInitRequest[] requests = prefetcher.generateWorkFragments(ts);
    	int num_sites = CatalogUtil.getNumberOfSites(catalog_db);
    	Procedure catalog_proc = this.getProcedure(TARGET_PREFETCH_PROCEDURE);
    	Statement catalog_stmt = this.getStatement(catalog_proc, TARGET_PREFETCH_STATEMENT);
    	catalog_stmt.setPrefetch(true);
    	catalog_proc.setPrefetch(true);
    	

    	
    	ts.setTransactionId(TXN_ID);
    	
    	
        // If none of the fragments need to be executed at the local site, 
    	
    	
    	// The WorkFragments are grouped by siteID.
    	assert(requests.length == num_sites);
    	for (int siteid = 0; siteid < num_sites; ++siteid) {
    		TransactionInitRequest request = requests[siteid];
    		List<WorkFragment> frags = request.getPrefetchFragmentsList();
    		for (WorkFragment frag : frags) {
    			assertEquals(siteid, this.partition_site_xref[frag.getPartitionId()]);
    		}
    	}
    	
    	// The WorkFragment doesn't exist for the base partition.
    	int base_site = this.partition_site_xref[LOCAL_PARTITION];
    	TransactionInitRequest request = requests[base_site];
    	for (WorkFragment frag : request.getPrefetchFragmentsList()) {
    		assertFalse(frag.getPartitionId() == LOCAL_PARTITION);
    	}
    	
    	// Make sure all the WorkFragments are there.
    	
    	
    	
    }
    
    
}
