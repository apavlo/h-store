package edu.brown.catalog;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

import org.voltdb.VoltDB;
import org.voltdb.catalog.*;
import org.voltdb.utils.Pair;

import edu.brown.correlations.ParameterCorrelations;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;


public abstract class FixCatalog {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(FixCatalog.class.getName());
    
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
    public static Catalog addHostInfo(Catalog orig_catalog, List<String[]> triplets) {
        Catalog catalog = CatalogUtil.cloneBaseCatalog(orig_catalog, Site.class, Host.class, Partition.class);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        
        Map<String, Set<Pair<Integer, Integer>>> host_info = new HashMap<String, Set<Pair<Integer, Integer>>>();
        for (String triplet[] : triplets) {
            String host = triplet[0];
            Integer port = Integer.parseInt(triplet[1]);
            Integer siteid = Integer.parseInt(triplet[2]);
            
            if (!host_info.containsKey(host)) {
                host_info.put(host, new HashSet<Pair<Integer,Integer>>());
            }
            host_info.get(host).add(new Pair<Integer, Integer>(port, siteid));
        } // FOR
        
        //
        // Add a bunch of hosts and partitions to this mofo
        //
        assert(catalog_clus != null);
        int host_id = VoltDB.FIRST_SITE_ID;
        int partition_ctr = 0;
        for (String host : host_info.keySet()) {
            String host_name = String.format("host%02d", host_id++);
            Host catalog_host = catalog_clus.getHosts().add(host_name);
            assert(catalog_host != null);
            catalog_host.setIpaddr(host);
            LOG.debug("Created new host " + catalog_host + " on node '" + host + "'");

            // Create a one-to-one mapping from Sites->Partitions on this Host
            int ctr = 0;
            for (Pair<Integer, Integer> pair : host_info.get(host)) {
                Integer port = pair.getFirst();
                Integer siteid = pair.getSecond();
                LOG.debug("Adding Site/Partition #" + siteid + " on " + host + ":" + port);
                
                Partition catalog_part = catalog_clus.getPartitions().add(siteid.toString());
                assert(catalog_part != null);
                catalog_part.setId(siteid);
                partition_ctr++;
                
                Site catalog_site = catalog_clus.getSites().add(siteid.toString());
                assert(catalog_site != null);
                catalog_site.setPartition(catalog_part);
                catalog_site.setHost(catalog_host);
                catalog_site.setPort(port);
                
                ctr++;
            } // FOR
            LOG.debug("Added " + ctr + " partitions for " + catalog_host);
        } // FOR
        LOG.debug("Updated host information in catalog with " + (host_id-1) + " new hosts and " + partition_ctr + " partitions");
        return (catalog);
    }
    
    public static Catalog addHostInfo(Catalog orig_catalog, int hosts, int partitions_per_host) {
        Set<Class<? extends CatalogType>> skipped_types = new HashSet<Class<? extends CatalogType>>();
        skipped_types.add(Site.class);
        skipped_types.add(Host.class);
        skipped_types.add(Partition.class);
        
        Catalog catalog = CatalogUtil.cloneBaseCatalog(orig_catalog, skipped_types);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        
        //
        // Add a bunch of hosts and partitions to this mofo
        //
        assert(catalog_clus != null);
        int partition_id = 1;
        for (int i = 0; i < hosts; i++) {
            String name = "host" + i;
            Host catalog_host = catalog_clus.getHosts().add(name);

            //
            // Set Attributes
            //
            catalog_host.setIpaddr("localhost");
//            catalog_host.setCores(HOST_CORES);
//            catalog_host.setMemory(HOST_MEMORY);
//            catalog_host.setThreadspercore(HOST_THREADS_PER_CORE);
//            LOG.info("Set ipaddress to " + catalog_host.getIpaddr() + " for " + catalog_host);
            
            //
            // Create a 1-to-1 mapping from Hosts->Sites->Partitions
            //
            for (int ii = 0; ii < partitions_per_host; ii++) {
                String site_name = Integer.toString(partition_id++);
                Partition catalog_part = catalog_clus.getPartitions().add(site_name);
                assert(catalog_part != null);
                
                Site catalog_site = catalog_clus.getSites().add(site_name);
                assert(catalog_site != null);
                catalog_site.setPartition(catalog_part);
                catalog_site.setHost(catalog_host);
            } // FOR
            
//            for (int j = 0; j < HOST_CORES; j++) {
//                for (int k = 0; k < HOST_THREADS_PER_CORE; k++) {
//                    if (!(i == 0 && j == 0 && k == 0)) {
//                        String name = "part" + i + "-" + j + "-" + k;
//                        cluster.getPartitions().add(name);
//                    }
//                } // FOR
//            } // FOR
            LOG.info("Added " + HOST_CORES + " partitions for " + catalog_host);
        } // FOR
        LOG.info("Updated host information in catalog by adding " + HOSTS + " hosts");
        return (catalog);
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
    
    public static Catalog createClusterCatalog(ArgumentsParser args) throws Exception {
        int host_cores = HOST_CORES;
        int host_threads = HOST_THREADS_PER_CORE;
        long host_memory = HOST_MEMORY;
        
        if (args.hasParam(ArgumentsParser.PARAM_SIMULATOR_HOST_CORES)) {
            host_cores = args.getIntParam(ArgumentsParser.PARAM_SIMULATOR_HOST_CORES);
        }
        if (args.hasParam(ArgumentsParser.PARAM_SIMULATOR_HOST_THREADS)) {
            host_threads = args.getIntParam(ArgumentsParser.PARAM_SIMULATOR_HOST_THREADS);
        }
        if (args.hasParam(ArgumentsParser.PARAM_SIMULATOR_HOST_MEMORY)) {
            host_memory = args.getLongParam(ArgumentsParser.PARAM_SIMULATOR_HOST_MEMORY);
        }
        
        //
        // Create a new catalog copied from the original but without any hosts+partitions+sites
        //
        Set<Class<? extends CatalogType>> skip_types = new HashSet<Class<? extends CatalogType>>();
        skip_types.add(Host.class);
        skip_types.add(Partition.class);
        skip_types.add(Site.class);
        Catalog new_catalog = CatalogUtil.cloneBaseCatalog(args.catalog, skip_types);
        Cluster cluster = CatalogUtil.getCluster(new_catalog);
        assert(cluster != null);
        Database catalog_db = CatalogUtil.getDatabase(cluster);
        assert(catalog_db != null);
        CatalogUtil.cloneConstraints(args.catalog_db, CatalogUtil.getDatabase(new_catalog));
        
        //
        // The hosts parameter should be a comma-separated list of hosts
        //
        String hosts_list = args.getParam(ArgumentsParser.PARAM_SIMULATOR_HOST);
        int partition_id = 0;
        for (String host : hosts_list.split(",")) {
            host = host.trim();
            Integer base_port = args.getIntParam(ArgumentsParser.PARAM_SIMULATOR_PORT);
            if (base_port == null) throw new Exception("Missing base port number for simulator hosts");
            
            cluster.getHosts().add(host);
            Host catalog_host = cluster.getHosts().get(host);
            catalog_host.setIpaddr(host);
            catalog_host.setCorespercpu(host_cores);
            catalog_host.setMemory((int) host_memory);
            catalog_host.setThreadspercore(host_threads);
            
            //
            // Create a new site+partition for each of the cores on the box
            //
            for (int core_idx = 0; core_idx < host_cores; core_idx++) {
                for (int thread_idx = 0; thread_idx < host_threads; thread_idx++) {
                    String partition_name = Integer.toString(partition_id); // host + "-" + core_idx + "-" + thread_idx;
                    Partition catalog_part = cluster.getPartitions().add(partition_name);
                    catalog_part.setId(partition_id++);
                    Site catalog_site = cluster.getSites().add(partition_name);
                    catalog_site.setHost(catalog_host);
                    catalog_site.setPort(base_port++);
                    catalog_site.setPartition(catalog_part);
                    
                    LOG.info("Created new partition at " + host + ":" + catalog_site.getPort());
                } // FOR
            } // FOR
            LOG.info("Added " + CatalogUtil.getSitesForHost(catalog_host).size() + " partitions for " + catalog_host.getName());    
        } // FOR
        
        return (new_catalog);
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
                int port = 33333;
                for (int partition = 0; partition < num_partitions_per_host; partition++) {
                    if (!(host == 0 && partition == 0)) hosts_list += ",";
                    hosts_list += String.format("node-%02d:%d:%d", host, port, partition_id);
                    partition_id++;
                } // FOR
            } // FOR
        } else {
            hosts_list = args.getParam(ArgumentsParser.PARAM_SIMULATOR_HOST);
        }
            
        ArrayList<String[]> triplets = new ArrayList<String[]>();
        Set<Integer> partitions = new HashSet<Integer>();
        for (String host_info : hosts_list.split(",")) {
            String data[] = host_info.split(":");
            assert(data.length == 3) : "Invalid host information '" + host_info + "'";
            
            int partition = Integer.parseInt(data[2]);
            if (partitions.contains(partition)) {
                throw new Exception("Duplicate partition id #" + partition + " for host '" + data[0] + "'");
            }
            partitions.add(partition);
            triplets.add(data);
        } // FOR
        new_catalog = FixCatalog.addHostInfo(new_catalog, triplets);
        
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