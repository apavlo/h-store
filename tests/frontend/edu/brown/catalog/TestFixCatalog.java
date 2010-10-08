package edu.brown.catalog;

import java.util.*;

import org.voltdb.catalog.*;

import edu.brown.utils.*;
import edu.brown.BaseTestCase;

public class TestFixCatalog extends BaseTestCase {

    private static final int NUM_HOSTS = 10;
    private static final int NUM_PARTITIONS_PER_HOST = 2;
    
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

        Set<Partition> seen_partitions = new HashSet<Partition>();
        Set<Host> hosts = new HashSet<Host>();
        for (Site catalog_site : catalog_clus.getSites()) {
            // System.err.println(CatalogUtil.debug(catalog_site));
            
            Host catalog_host = catalog_site.getHost();
            assertNotNull(catalog_host);
            //assertFalse(hosts.contains(catalog_host));
            hosts.add(catalog_site.getHost());

            assertFalse(catalog_site.getPartitions().isEmpty());
            for (Partition catalog_part : catalog_site.getPartitions()) {
                assertNotNull(catalog_part);
                assertFalse(catalog_part.toString(), seen_partitions.contains(catalog_part));
                seen_partitions.add(catalog_part);
            } // FOR
        } // FOR
        assertEquals(NUM_HOSTS, hosts.size());
        assertFalse(seen_partitions.isEmpty());
    }
}
