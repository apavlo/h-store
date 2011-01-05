package edu.brown.benchmark.locality;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.locality.LocalityConstants.ExecutionType;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.FixCatalog;
import edu.brown.utils.ProjectType;

public class TestLocalityClient extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestLocalityClient.class);
    protected static final int SCALE_FACTOR = 20;
    private ExecutionType TYPE;
    
    protected LocalityClient client;
    protected Long current_tablesize;
    protected Long current_batchsize;
    protected Long total_rows = 0l;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.LOCALITY);
        //this.addPartitions(NUM_PARTITIONS);
        String args[] = {
                "scalefactor=" + SCALE_FACTOR,
                "type=" + TYPE
            };
        this.client = new LocalityClient(args);
        Catalog new_Catalog = FixCatalog.addHostInfo(catalog, 1, 2,4);
        //Catalog new_Catalog = FixCatalog.addHostInfo(catalog, 2, 4,6);
        //Catalog new_Catalog = FixCatalog.addHostInfo(catalog, 8, 15,3);
        //Catalog new_Catalog = FixCatalog.addHostInfo(catalog, 3, 3,7);
        //Catalog new_Catalog = FixCatalog.addHostInfo(catalog, 3, 2, 7);
        this.client.setCatalog(new_Catalog);        
    }
    
    /**
     * This test case inputs an a_id and checks that the returned
     * a_id exists on the same partition number as the original a_id
     * @throws Exception
     */
    public void testgetDataIdSamePartition() throws Exception {
    	client.setType(LocalityConstants.ExecutionType.SAME_PARTITION);
    	int a_id = client.m_rng.nextInt(LocalityClient.table_sizes.get(LocalityConstants.TABLENAME_TABLEA).intValue());
    	int aidpartitionnum = a_id % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
    	long a_id2 = LocalityClient.getDataId(a_id, client.m_rng, LocalityConstants.ExecutionType.SAME_PARTITION, client.getCatalog());
    	long a_id2_partition_num = a_id2 % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
    	assertEquals(aidpartitionnum, a_id2_partition_num);
    }

    /**
     * This test case inputs an a_id and checks that the returned
     * a_id exists on the same site but different partition number
     * @throws Exception
     */
    public void testgetDataIdSameSite() throws Exception {
    	client.setType(LocalityConstants.ExecutionType.SAME_SITE);
    	System.out.println("Value to randomize on: " + LocalityClient.table_sizes.get(LocalityConstants.TABLENAME_TABLEA).intValue());    		
    	System.out.println("Total num partitions: " + CatalogUtil.getCluster(client.getCatalog()).getNum_partitions());
        int a_id = client.m_rng.nextInt(LocalityClient.table_sizes.get(LocalityConstants.TABLENAME_TABLEA).intValue());
        //int a_id = 974;
    	int aidpartitionnum = a_id % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
    	String aidSiteName = CatalogUtil.getPartitionById(client.getCatalog(), aidpartitionnum).getParent().getName();
    	long a_id2 = LocalityClient.getDataId(a_id, client.m_rng, LocalityConstants.ExecutionType.SAME_SITE, client.getCatalog());
    	long aid2partitionnum = a_id2 % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
    	String aid2SiteName = CatalogUtil.getPartitionById(client.getCatalog(), (int)aid2partitionnum).getParent().getName();
    	assertTrue(aidpartitionnum != aid2partitionnum);
    	assertEquals(aidSiteName, aid2SiteName);
    }

    /**
     * This test case inputs an a_id and checks that the returned
     * a_id exists on the same host but different sites
     * @throws Exception
     */
    public void testgetDataIdSameHost() throws Exception {
    	client.setType(LocalityConstants.ExecutionType.SAME_HOST);
        int a_id = client.m_rng.nextInt(LocalityClient.table_sizes.get(LocalityConstants.TABLENAME_TABLEA).intValue());
    	int aidpartitionnum = a_id % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
    	Site a_id_site = CatalogUtil.getPartitionById(client.getCatalog(), aidpartitionnum).getParent();
    	String aid_host_name = a_id_site.getHost().getName();
    	long a_id2 = LocalityClient.getDataId(a_id, client.m_rng, LocalityConstants.ExecutionType.SAME_HOST, client.getCatalog());
    	long a_id2_partitionnum = a_id2 % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
    	Site a_id2_site = CatalogUtil.getPartitionById(client.getCatalog(), (int)a_id2_partitionnum).getParent();
    	String aid2_host_name = a_id2_site.getHost().getName();
    	assertTrue(a_id_site != a_id2_site);
    	assertEquals(aid_host_name, aid2_host_name);
    }

    /**
     * This test case inputs an a_id and checks that the returned
     * a_id exists on a different host
     * @throws Exception
     */
    public void testgetDataIdRemoteHost() throws Exception {
    	if (client.getCatalog().getClusters().size() > 1)
    	{
        	client.setType(LocalityConstants.ExecutionType.REMOTE_HOST);
            int a_id = client.m_rng.nextInt(LocalityClient.table_sizes.get(LocalityConstants.TABLENAME_TABLEA).intValue());
        	int aidpartitionnum = a_id % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
        	Site a_id_site = CatalogUtil.getPartitionById(client.getCatalog(), aidpartitionnum).getParent();
        	String aid_host_name = a_id_site.getHost().getName();
        	long a_id2 = LocalityClient.getDataId(a_id, client.m_rng, LocalityConstants.ExecutionType.REMOTE_HOST, client.getCatalog());
        	long a_id2_partitionnum = a_id2 % CatalogUtil.getCluster(client.getCatalog()).getNum_partitions();
        	Site a_id2_site = CatalogUtil.getPartitionById(client.getCatalog(), (int)a_id2_partitionnum).getParent();
        	String aid2_host_name = a_id2_site.getHost().getName();
        	assertTrue(aid_host_name != aid2_host_name);    		
    	}
    }

}