package edu.brown.hstore.util;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.voltdb.ParameterSet;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.seats.procedures.NewReservation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

public class TestPrefetchQueryPlanner extends BaseTestCase {
    private static final Long TXN_ID = 1000l;

    private static final Class<? extends VoltProcedure> TARGET_PREFETCH_PROCEDURE = NewReservation.class;
    private static final String TARGET_PREFETCH_STATEMENT = "GetCustomer";

    private static final int NUM_HOSTS = 1;
    private static final int NUM_SITES = 4; // per host
    private static final int NUM_PARTITIONS = 1; // per site
    private static final int LOCAL_SITE = 0;
    private static final int LOCAL_PARTITION = 0;

    private final MockHStoreSite[] hstore_sites = new MockHStoreSite[NUM_SITES];
    private final HStoreCoordinator[] coordinators = new HStoreCoordinator[NUM_SITES];

    private LocalTransaction ts;

    private PrefetchQueryPlanner prefetcher;
    private int[] partition_site_xref;
    private Random rand = new Random(0);

    Object proc_params[] = {
        100l, // r_id
        LOCAL_PARTITION + 1l, // c_id
        LOCAL_PARTITION, // f_id
        this.rand.nextInt(100), // seatnum
        100d, // price
        new long[0], // attrs
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.SEATS);
        this.initializeCluster(NUM_HOSTS, NUM_SITES, NUM_PARTITIONS);

        Procedure catalog_proc = this.getProcedure(TARGET_PREFETCH_PROCEDURE);
        Statement catalog_stmt = this.getStatement(catalog_proc, TARGET_PREFETCH_STATEMENT);
        catalog_stmt.setPrefetchable(true);
        catalog_proc.setPrefetchable(true);

        // Hard-code ParameterMapping
        int mappings[][] = {
            // StmtParameter -> ProcParameter
            { 0, 1 },
        };
        List<ProcParameter> procParams = CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index");
        List<StmtParameter> stmtParams = CatalogUtil.getSortedCatalogItems(catalog_stmt.getParameters(), "index");
        assertNotNull(stmtParams);
        assertEquals(catalog_stmt.getParameters().size(), mappings.length);
        for (int m[] : mappings) {
            stmtParams.get(m[0]).setProcparameter(procParams.get(m[1]));
        } // FOR

        this.prefetcher = new PrefetchQueryPlanner(catalog_db, p_estimator);
        for (int i = 0; i < NUM_SITES; i++) {
            Site catalog_site = this.getSite(i);
            this.hstore_sites[i] = new MockHStoreSite(catalog_site, HStoreConf.singleton());
            this.coordinators[i] = this.hstore_sites[i].initHStoreCoordinator();

            // We have to make our fake ExecutionSites for each Partition at
            // this site
            // for (Partition catalog_part : catalog_site.getPartitions()) {
            // MockPartitionExecutor es = new
            // MockPartitionExecutor(catalog_part.getId(), catalog,
            // p_estimator);
            // this.hstore_sites[i].addPartitionExecutor(catalog_part.getId(),
            // es);
            // es.initHStoreSite(this.hstore_sites[i]);
            // } // FOR
        } // FOR

        final ParameterSet params = new ParameterSet(this.proc_params);
        this.ts = new LocalTransaction(this.hstore_sites[LOCAL_SITE]) {
            @Override
            public org.voltdb.ParameterSet getProcedureParameters() {
                return (params);
            }
        };

        // Generate the minimum set of partitions that we need to touch
        Set<Integer> partitions = new HashSet<Integer>();
        for (int idx : new int[] { 2, 1 }) {
            Object val = this.proc_params[idx];
            int p = p_estimator.getHasher().hash(val);
            System.err.println(val + " -> " + p);
            partitions.add(p);
        } // FOR

        this.ts.testInit(TXN_ID, LOCAL_PARTITION, partitions, this.getProcedure(TARGET_PREFETCH_PROCEDURE));

        this.partition_site_xref = new int[CatalogUtil.getNumberOfPartitions(catalog_db)];
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_db)) {
            this.partition_site_xref[catalog_part.getId()] = ((Site) catalog_part.getParent()).getId();
        } // FOR
    }

    /**
     * testGenerateWorkFragments
     */
    public void testGenerateWorkFragments() throws Exception {
        int num_sites = CatalogUtil.getNumberOfSites(catalog_db);

        this.ts.setTransactionId(TXN_ID);
        TransactionInitRequest[] requests = this.prefetcher.generateWorkFragments(this.ts);
        assertEquals(num_sites, requests.length);

        // The TransactionInitRequest for the local partition will be the
        // default, the next partition will have a regular WorkFragment, and the
        // rest will be null.
        System.err.println(StringUtil.join("\n\n", requests));
        /*
         * TransactionInitRequest default_request =
         * TransactionInitRequest.newBuilder()
         * .setTransactionId(this.ts.getTransactionId())
         * .setProcedureId(this.ts.getProcedure().getId())
         * .addAllPartitions(this.ts.getPredictTouchedPartitions()) .build();
         */
        int base_site = this.partition_site_xref[LOCAL_PARTITION];
        int remote_site = this.partition_site_xref[LOCAL_PARTITION + 1];
        assertNotSame(base_site, remote_site);

        assertNotNull(requests[base_site]);
        assertEquals(0, requests[base_site].getPrefetchFragmentsCount());

        assertNotNull(requests[remote_site]);
        assertEquals(1, requests[remote_site].getPrefetchFragmentsCount());
        assertNull(requests[2]);
        assertNull(requests[3]);

        // The WorkFragments are grouped by siteID.
        assert (requests.length == num_sites);
        for (int siteid = 0; siteid < num_sites; ++siteid) {
            TransactionInitRequest request = requests[siteid];
            if (request != null && request.getPrefetchFragmentsCount() > 0) {
                List<WorkFragment> frags = request.getPrefetchFragmentsList();
                for (WorkFragment frag : frags) {
                    assertEquals(siteid, this.partition_site_xref[frag.getPartitionId()]);
                    assertTrue(frag.getPrefetch());
                }
            }
        }

        // The WorkFragment doesn't exist for the base partition.
        TransactionInitRequest request = requests[base_site];
        for (WorkFragment frag : request.getPrefetchFragmentsList()) {
            assertFalse(frag.getPartitionId() == LOCAL_PARTITION);
            assertTrue(frag.getPrefetch());
        }

    }

}
