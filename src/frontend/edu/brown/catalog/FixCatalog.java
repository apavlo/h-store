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

    public static class ClusterConfiguration {
        private final Map<String, Map<Integer, Set<PartitionConfiguration>>> host_sites = new HashMap<String, Map<Integer, Set<PartitionConfiguration>>>();
        
        public void addPartition(String host, int port, int site, int partition) {
            PartitionConfiguration pc = new PartitionConfiguration(host, port, site, partition);
            
            // Host -> Sites
            if (!this.host_sites.containsKey(host)) {
                this.host_sites.put(host, new HashMap<Integer, Set<PartitionConfiguration>>());
            }
            if (!this.host_sites.get(host).containsKey(site)) {
                this.host_sites.get(host).put(site, new HashSet<PartitionConfiguration>());
            }
            this.host_sites.get(host).get(site).add(pc);
        }
        
        public Collection<String> getHosts() {
            return (this.host_sites.keySet());
        }
        
        public Collection<Integer> getSites(String host) {
            return (this.host_sites.get(host).keySet());
        }
        
        public Integer getPort(String host, int site) {
            Integer port = null;
            if (this.host_sites.containsKey(host)) {
                if (this.host_sites.get(host).containsKey(site)) {
                    PartitionConfiguration pc = CollectionUtil.getFirst(this.host_sites.get(host).get(site));
                    port = pc.getPort();
                }
            }
            return (port);
        }
        
        public Collection<PartitionConfiguration> getPartitions(String host, int site) {
            return (this.host_sites.get(host).get(site));
        }
        
        public Collection<Integer> getPartitionIds(String host, int site) {
            Set<Integer> ids = new ListOrderedSet<Integer>();
            for (PartitionConfiguration pc : this.getPartitions(host, site)) {
                ids.add(pc.getPartition());
            } // FOR
            return (ids);
        }
        
    }
    
    private static class PartitionConfiguration {
        private final String host;
        private final int port;
        private final int site;
        private final int partition;
        
        public PartitionConfiguration(String host, int port, int site, int partition) {
            this.host = host;
            this.port = port;
            this.site = site;
            this.partition = partition;
        }
        
        public String getHost() {
            return host;
        }
        
        public int getPort() {
            return port;
        }
        
        public int getSite() {
            return site;
        }
        
        public int getPartition() {
            return partition;
        }
    }
    
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

            // Now create the sites for this host
            for (Integer siteid : cc.getSites(host)) {
                Integer port = cc.getPort(host, siteid);
                LOG.debug("Adding Site #" + siteid + " on " + host + ":" + port);
                
                Site catalog_site = catalog_clus.getSites().add(siteid.toString());
                assert(catalog_site != null);
                catalog_site.setId(siteid);
                catalog_site.setHost(catalog_host);
                catalog_site.setPort(port);
                catalog_site.setMessenger_port(port + 10000); // HACK!!!
                
                // Add all the partitions
                for (Integer partition_id : cc.getPartitionIds(host, siteid)) {
                    Partition catalog_part = catalog_site.getPartitions().add(partition_id.toString());
                    assert(catalog_part != null);
                    catalog_part.setId(siteid);
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
            int port = 33333;
            for (int site = 0; site < num_sites_per_host; site++) {
                for (int partition = 0; partition < num_partitions_per_site; partition++) {
                    cc.addPartition(hostname, port, siteid, partitionid++);
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
                    Site catalog_site = cluster.getSites().add(partition_name);
                    catalog_site.setHost(catalog_host);
                    catalog_site.setPort(base_port++);
                    
                    Partition catalog_part = catalog_site.getPartitions().add(partition_name);
                    catalog_part.setId(partition_id++);
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
        
        ClusterConfiguration cc = new ClusterConfiguration();
        Set<Integer> partitions = new HashSet<Integer>();
        for (String host_info : hosts_list.split(",")) {
            String data[] = host_info.split(":");
            assert(data.length == 4) : "Invalid host information '" + host_info + "'";
            
            String host = data[0];
            int port = Integer.parseInt(data[1]);
            int site = Integer.parseInt(data[2]);
            int partition = Integer.parseInt(data[3]);
            
            if (partitions.contains(partition)) {
                throw new Exception("Duplicate partition id #" + partition + " for host '" + host + "'");
            }
            partitions.add(partition);
            cc.addPartition(host, port, site, partition);
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