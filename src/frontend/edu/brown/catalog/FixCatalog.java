package edu.brown.catalog;

import java.io.*;
import java.util.*;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;

import org.voltdb.VoltDB;
import org.voltdb.catalog.*;
import org.voltdb.utils.Pair;

import edu.brown.correlations.ParameterCorrelations;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;


public abstract class FixCatalog {
    private static final Logger LOG = Logger.getLogger(FixCatalog.class);
    
    public static final int HOSTS = 1;
    public static final int HOST_CORES = 2;
    public static final int HOST_THREADS_PER_CORE = 1;
    public static final long HOST_MEMORY = 1073741824l; 

    /**
     * Added a hosts/sites/partitions in the catalog.
     * @param orig_catalog
     * @param triplets - [0] host [1] port# [2] site#
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Catalog addHostInfo(Catalog orig_catalog, ClusterConfiguration cc) {
        Catalog catalog = CatalogUtil.cloneBaseCatalog(orig_catalog, Site.class, Host.class, Partition.class);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        
        // Add a bunch of hosts and partitions to this mofo
        assert(catalog_clus != null);
        int host_id = VoltDB.FIRST_SITE_ID;
        
        int partition_ctr = 0;
        for (String host : cc.getHosts()) {
            String host_name = String.format("host%02d", host_id++);
            Host catalog_host = catalog_clus.getHosts().add(host_name);
            assert(catalog_host != null);
            catalog_host.setIpaddr(host);
            LOG.debug("Created new host " + catalog_host + " on node '" + host + "'");

            int proc_port = VoltDB.DEFAULT_PORT;
            int dtxn_port = 30000;
            int messenger_port = dtxn_port + 10000;
            int partition_port = messenger_port + 10000;
            
            // Now create the sites for this host
            for (Integer siteid : cc.getSites(host)) {
                LOG.debug("Adding Site #" + siteid + " on " + host);
                
                Site catalog_site = catalog_clus.getSites().add(siteid.toString());
                assert(catalog_site != null);
                catalog_site.setId(siteid);
                catalog_site.setHost(catalog_host);
                catalog_site.setProc_port(proc_port++);
                catalog_site.setDtxn_port(dtxn_port++);
                catalog_site.setMessenger_port(messenger_port++);
                
                // Add all the partitions
                for (Integer partition_id : cc.getPartitionIds(host, siteid)) {
                    Partition catalog_part = catalog_site.getPartitions().add(partition_id.toString());
                    assert(catalog_part != null);
                    catalog_part.setId(partition_id);
                    catalog_part.setDtxn_port(partition_port++);
                    partition_ctr++;
                } // FOR
                
            } // FOR
            // LOG.debug("Added " + ctr + " partitions for " + catalog_host);
        } // FOR
        catalog_clus.setNum_partitions(partition_ctr);
        LOG.debug("Updated host information in catalog with " + (host_id-1) + " new hosts and " + partition_ctr + " partitions");
        return (catalog);
    }
    
    public static Catalog addHostInfo(Catalog orig_catalog, int num_hosts, int num_sites_per_host, int num_partitions_per_site) {
        ClusterConfiguration cc = new ClusterConfiguration();
        int siteid = 0;
        int partitionid = 0;
        
        for (int host = 0; host < num_hosts; host++) {
            String hostname = String.format("node-%02d", host);
            for (int site = 0; site < num_sites_per_host; site++) {
                for (int partition = 0; partition < num_partitions_per_site; partition++) {
                    cc.addPartition(hostname, siteid, partitionid++);
                } // FOR (partitions)
                siteid++;
            } // FOR (sites)
        } // FOR (hosts)
        return (FixCatalog.addHostInfo(orig_catalog, cc));
    }
    
    /**
     * 
     * @param catalog_db
     * @throws Exception
     */
    public static void populateCatalog(Database catalog_db, ProjectType type) throws Exception {
        //
        // Foreign Keys
        //
        Map<String, ForeignKeysUtil.ForeignKeyMapping> foreign_keys = ForeignKeysUtil.FOREIGN_KEYS.get(type);
        if (foreign_keys != null) {
            try {
                ForeignKeysUtil.populateCatalog(catalog_db, foreign_keys);
                LOG.info("Updated foreign key dependencies in catalog for schema type '" + type + "'");
            } catch (RuntimeException ex) {
                // Ignore these...
                LOG.warn(ex.getLocalizedMessage());
            }
        }
        
        //
        // StmtParameter->ProcParameter Mapping
        //
        Map<String, ParametersUtil.ParameterMapping> param_map = ParametersUtil.getParameterMapping(type);
        if (param_map != null) {
            try {
                ParametersUtil.populateCatalog(catalog_db, param_map);
                LOG.info("Updated parameter mapping information in catalog for schema type '" + type + "'");
            } catch (RuntimeException ex) {
                // Ignore these...
                LOG.warn(ex.getLocalizedMessage());
            }
        }
        
        return;
    }
    
    
    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG_TYPE,
                     ArgumentsParser.PARAM_CATALOG_OUTPUT);
        
        // ProjectType type = args.catalog_type;
        String output_path = args.getParam(ArgumentsParser.PARAM_CATALOG_OUTPUT);
        
        // Populate Parameter Mappings
        if (args.hasParam(ArgumentsParser.PARAM_CORRELATIONS)) {
            File input_path = new File(args.getParam(ArgumentsParser.PARAM_CORRELATIONS));
            if (input_path.exists()) {
                ParameterCorrelations correlations = new ParameterCorrelations();
                correlations.load(input_path.getAbsolutePath(), args.catalog_db);
                ParametersUtil.applyParameterCorrelations(args.catalog_db, correlations);
                LOG.info("Applied correlations file to '" + input_path + "' catalog parameter mappings...");
            } else {
                LOG.warn("Correlations file '" + input_path + "' does not exist. Ignoring...");
            }
        }
        
        // Fix the catalog!
        // populateCatalog(args.catalog_db, type);

        // Populate host information
        Catalog new_catalog = args.catalog;
        String hosts_list = "";
        if (args.hasIntParam(ArgumentsParser.PARAM_SIMULATOR_NUM_HOSTS)) {
            int num_hosts = args.getIntParam(ArgumentsParser.PARAM_SIMULATOR_NUM_HOSTS);
            int num_partitions_per_host = (args.hasIntParam(ArgumentsParser.PARAM_SIMULATOR_HOST_PARTITIONS) ?
                                                args.getIntParam(ArgumentsParser.PARAM_SIMULATOR_HOST_PARTITIONS) : 4);
            int partition_id = 0;
            for (int host = 0; host < num_hosts; host++) {
                for (int partition = 0; partition < num_partitions_per_host; partition++) {
                    assert(false); // FIXME
                    if (!(host == 0 && partition == 0)) hosts_list += ",";
                    hosts_list += String.format("node-%02d:%d", host, partition_id);
                    partition_id++;
                } // FOR
            } // FOR
        } else {
            hosts_list = args.getParam(ArgumentsParser.PARAM_SIMULATOR_HOST);
        }
        
        ClusterConfiguration cc = new ClusterConfiguration();
        Set<Integer> partitions = new HashSet<Integer>();
        for (String host_info : hosts_list.split(",")) {
            String data[] = host_info.split(":");
            assert(data.length == 3) : "Invalid host information '" + host_info + "'";
            
            String host = data[0];
            int site = Integer.parseInt(data[1]);
            int partition = Integer.parseInt(data[2]);
            
            if (partitions.contains(partition)) {
                throw new Exception("Duplicate partition id #" + partition + " for host '" + host + "'");
            }
            partitions.add(partition);
            cc.addPartition(host, site, partition);
        } // FOR
        new_catalog = FixCatalog.addHostInfo(new_catalog, cc);
        
//        
//        Catalog new_catalog = args.catalog;
//        if (args.hasParam(ArgumentsParser.PARAM_SIMULATOR_HOST)) {
//            new_catalog = createClusterCatalog(args);
//        }
        
        //
        // We need to write this things somewhere now...
        //
        FileUtil.writeStringToFile(new File(output_path), new_catalog.serialize());
        LOG.info("Wrote updated catalog specification to '" + output_path + "'");

        return;
    }
}