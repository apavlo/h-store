package edu.brown.catalog;

import java.util.*;

import org.voltdb.catalog.*;

import edu.brown.utils.*;
import edu.brown.BaseTestCase;

public class TestFixCatalog extends BaseTestCase {

    private static final int NUM_HOSTS = 10;
    private static final int NUM_PARTITIONS_PER_HOST = 1;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
    }
    
    /**
     * testAddHostInfo
     */
    public void testAddHostInfo() throws Exception {
        Catalog new_catalog = FixCatalog.addHostInfo(catalog, NUM_HOSTS, NUM_PARTITIONS_PER_HOST);
        Cluster catalog_clus = CatalogUtil.getCluster(new_catalog);
        assertEquals(NUM_PARTITIONS_PER_HOST * NUM_HOSTS, CatalogUtil.getNumberOfPartitions(new_catalog));

        Set<Host> hosts = new HashSet<Host>();
        for (Site catalog_site : catalog_clus.getSites()) {
            Host catalog_host = catalog_site.getHost();
            assertNotNull(catalog_host);
            assertFalse(hosts.contains(catalog_host));
            hosts.add(catalog_site.getHost()); 

            assertFalse(catalog_site.getPartitions().isEmpty());
            for (Partition catalog_part : catalog_site.getPartitions()) {
                assertNotNull(catalog_part);
            }
        } // FOR
    }
}
