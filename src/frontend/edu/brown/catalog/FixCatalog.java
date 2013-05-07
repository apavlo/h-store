package edu.brown.catalog;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.hstore.HStoreConstants;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public abstract class FixCatalog {
    private static final Logger LOG = Logger.getLogger(FixCatalog.class);

    public static final int HOSTS = 1;
    public static final int HOST_CORES = 2;
    public static final int HOST_THREADS_PER_CORE = 1;
    public static final long HOST_MEMORY = 1073741824l;

    private static final Set<String> LOCALHOST_TYPOS = new HashSet<String>();
    static {
        LOCALHOST_TYPOS.add("locahost");
        LOCALHOST_TYPOS.add("localhst");
        LOCALHOST_TYPOS.add("locahlost");
        LOCALHOST_TYPOS.add("loclhost");
        LOCALHOST_TYPOS.add("localhos");
        LOCALHOST_TYPOS.add("loclhst");
    };
    
    /**
     * Added a hosts/sites/partitions in the catalog. Returns a clone of the
     * Catalog
     * 
     * @param orig_catalog
     * @param triplets
     *            - [0] host [1] port# [2] site#
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Catalog cloneCatalog(Catalog orig_catalog, ClusterConfiguration cc) {
        Catalog catalog = CatalogCloner.cloneBaseCatalog(orig_catalog, Site.class, Host.class, Partition.class);
        return FixCatalog.updateCatalog(catalog, cc);
    }
    
    public static Catalog cloneCatalog(Catalog orig_catalog, int num_hosts, int num_sites_per_host, int num_partitions_per_site) {
        return (FixCatalog.cloneCatalog(orig_catalog, "node-%02d", num_hosts, num_sites_per_host, num_partitions_per_site));
    }

    public static Catalog cloneCatalog(Catalog orig_catalog, String hostname_format, int num_hosts, int num_sites_per_host, int num_partitions_per_site) {
        ClusterConfiguration cc = new ClusterConfiguration(hostname_format, num_hosts, num_sites_per_host, num_partitions_per_site);
        return (FixCatalog.cloneCatalog(orig_catalog, cc));
    }

    /**
     * Write the host/sites/partitions directly to the given catalog
     * 
     * @param catalog
     * @param cc
     * @return
     */
    public static Catalog updateCatalog(Catalog catalog, ClusterConfiguration cc) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);

        // Add a bunch of hosts and partitions to this mofo
        assert (catalog_clus != null);
        int host_id = HStoreConstants.FIRST_PARTITION_ID;

        int partition_ctr = 0;
        catalog_clus.getHosts().clear();
        catalog_clus.getSites().clear();
        for (String host : cc.getHosts()) {
            if (LOCALHOST_TYPOS.contains(host)) {
                String msg = String.format("POSSIBLE TYPO IN HOSTNAME '%s'. " +
                                           "DID YOU MEAN 'localhost'?", host);
                msg = StringBoxUtil.box(msg);
                LOG.warn("");
                for (String line : StringUtil.splitLines(msg)) {
                    LOG.warn(StringUtil.bold(line));
                } // FOR
                LOG.warn("");
            }
            
            String host_name = String.format("host%02d", host_id);
            Host catalog_host = catalog_clus.getHosts().add(host_name);
            assert (catalog_host != null);
            catalog_host.setId(host_id);
            catalog_host.setIpaddr(host);
            LOG.debug("Created new host " + catalog_host + " on node '" + host + "'");

            int proc_port = HStoreConstants.DEFAULT_PORT;
            int messenger_port = proc_port + HStoreConstants.MESSENGER_PORT_OFFSET;

            // Now create the sites for this host
            for (Integer siteid : cc.getSites(host)) {
                LOG.debug("Adding Site #" + siteid + " on " + host);

                Site catalog_site = catalog_clus.getSites().add(siteid.toString());
                assert (catalog_site != null);
                catalog_site.setId(siteid);
                catalog_site.setHost(catalog_host);
                catalog_site.setProc_port(proc_port++);
                catalog_site.setMessenger_port(messenger_port++);

                // Add all the partitions
                for (Integer partition_id : cc.getPartitionIds(host, siteid)) {
                    Partition catalog_part = catalog_site.getPartitions().add(partition_id.toString());
                    assert (catalog_part != null);
                    catalog_part.setId(partition_id);
                    partition_ctr++;
                } // FOR

            } // FOR
            host_id++;
              // LOG.debug("Added " + ctr + " partitions for " + catalog_host);
        } // FOR
        catalog_clus.setNum_partitions(partition_ctr);
        LOG.info(String.format("Updated host information in catalog with %d host%s and %d partitions",
                               catalog_clus.getHosts().size(),
                               (catalog_clus.getHosts().size() > 1 ? "s" : ""),
                               partition_ctr));
        return (catalog);
    }
    
    public static Catalog updateCatalog(Catalog orig_catalog, int num_hosts, int num_sites_per_host, int num_partitions_per_site) {
        return (FixCatalog.updateCatalog(orig_catalog, "node-%02d", num_hosts, num_sites_per_host, num_partitions_per_site));
    }

    public static Catalog updateCatalog(Catalog orig_catalog, String hostname_format, int num_hosts, int num_sites_per_host, int num_partitions_per_site) {
        ClusterConfiguration cc = new ClusterConfiguration(hostname_format, num_hosts, num_sites_per_host, num_partitions_per_site);
        return (FixCatalog.updateCatalog(orig_catalog, cc));
    }

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser.DISABLE_UPDATE_CATALOG = true;
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG_TYPE, ArgumentsParser.PARAM_CATALOG_OUTPUT);

        // ProjectType type = args.catalog_type;
        String catalogOutputPath = args.getParam(ArgumentsParser.PARAM_CATALOG_OUTPUT);

        // Populate Parameter Mappings
        if (args.hasParam(ArgumentsParser.PARAM_MAPPINGS)) {
            File input_path = args.getFileParam(ArgumentsParser.PARAM_MAPPINGS);
            if (input_path.exists()) {
                ParameterMappingsSet mappings = new ParameterMappingsSet();
                mappings.load(input_path, args.catalog_db);
                ParametersUtil.applyParameterMappings(args.catalog_db, mappings);
                LOG.debug("Applied ParameterMappings file to '" + input_path + "' catalog parameter mappings...");
            } else {
                LOG.warn("ParameterMappings file '" + input_path + "' does not exist. Ignoring...");
            }
        }

        // Fix the catalog!
        // populateCatalog(args.catalog_db, type);

        // Populate host information
        Catalog new_catalog = args.catalog;
        if (args.hasIntParam(ArgumentsParser.PARAM_CATALOG_NUM_HOSTS)) {
            String host_format = args.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS);

            int num_hosts = args.getIntParam(ArgumentsParser.PARAM_CATALOG_NUM_HOSTS);
            int num_sites_per_host = (args.hasIntParam(ArgumentsParser.PARAM_CATALOG_SITES_PER_HOST) ? args.getIntParam(ArgumentsParser.PARAM_CATALOG_SITES_PER_HOST) : 2);
            int num_partitions_per_site = (args.hasIntParam(ArgumentsParser.PARAM_CATALOG_PARTITIONS_PER_SITE) ? args.getIntParam(ArgumentsParser.PARAM_CATALOG_PARTITIONS_PER_SITE) : 2);

            if (host_format == null) {
                FixCatalog.updateCatalog(new_catalog, num_hosts, num_sites_per_host, num_partitions_per_site);
            } else {
                FixCatalog.updateCatalog(new_catalog, host_format, num_hosts, num_sites_per_host, num_partitions_per_site);
            }

        // Use host list
        } else {
            String hostsInfo = args.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS);
            ClusterConfiguration cc = new ClusterConfiguration(hostsInfo);
            FixCatalog.updateCatalog(new_catalog, cc);
        }

        // Now construct the new Dtxn.Coordinator configuration
        // String new_dtxn = HStoreDtxnConf.toHStoreDtxnConf(new_catalog);

        // We need to write this things somewhere now...
        FileUtil.writeStringToFile(new File(catalogOutputPath), new_catalog.serialize());
        LOG.debug("Wrote updated catalog specification to '" + catalogOutputPath + "'");

        // FileUtil.writeStringToFile(new File(dtxnOutputPath), new_dtxn);
        // LOG.info("Wrote updated Dtxn.Coordinator configuration to '" +
        // dtxnOutputPath + "'");

        return;
    }

}