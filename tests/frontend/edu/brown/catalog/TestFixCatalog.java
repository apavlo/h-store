package edu.brown.catalog;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestFixCatalog extends BaseTestCase {

    private static final int NUM_HOSTS = 10;
    private static final int NUM_SITES_PER_HOST = 2;
    private static final int NUM_PARTITIONS_PER_SITE = 2;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
    }

    /**
     * testAddHostInfo
     */
    public void testAddHostInfo() throws Exception {
        Catalog new_catalog = FixCatalog.cloneCatalog(catalog, NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        Cluster catalog_clus = CatalogUtil.getCluster(new_catalog);
        assertEquals(NUM_PARTITIONS_PER_SITE * NUM_SITES_PER_HOST * NUM_HOSTS, CatalogUtil.getNumberOfPartitions(new_catalog));

        Set<Host> seen_hosts = new HashSet<Host>();
        Set<Site> seen_sites = new HashSet<Site>();
        Set<Partition> seen_partitions = new HashSet<Partition>();

        Set<Integer> seen_site_ids = new HashSet<Integer>();
        Set<Integer> seen_partition_ids = new HashSet<Integer>();

        for (Host catalog_host : catalog_clus.getHosts()) {
            assertNotNull(catalog_host);
            Collection<Site> sites = CatalogUtil.getSitesForHost(catalog_host);
            assertEquals(sites.toString(), NUM_SITES_PER_HOST, sites.size());

            for (Site catalog_site : sites) {
                assertEquals(catalog_host, catalog_site.getHost());
                assertEquals(NUM_PARTITIONS_PER_SITE, catalog_site.getPartitions().size());
                assertFalse(seen_site_ids.contains(catalog_site.getId()));

                for (Partition catalog_part : catalog_site.getPartitions()) {
                    assertNotNull(catalog_part);
                    assertFalse(catalog_part.toString(), seen_partitions.contains(catalog_part));
                    assertFalse(seen_partition_ids.contains(catalog_part.getId()));
                    seen_partitions.add(catalog_part);
                    seen_partition_ids.add(catalog_part.getId());
                } // FOR (partitions)
                seen_sites.add(catalog_site);
            } // FOR (sites)
            seen_hosts.add(catalog_host);
        } // FOR (hosts)
        assertEquals(NUM_HOSTS, seen_hosts.size());
        assertEquals(NUM_HOSTS * NUM_SITES_PER_HOST, seen_sites.size());
        assertEquals(NUM_HOSTS * NUM_SITES_PER_HOST * NUM_PARTITIONS_PER_SITE, seen_partitions.size());
        assertEquals(seen_partition_ids.size(), catalog_clus.getNum_partitions());
    }
}
