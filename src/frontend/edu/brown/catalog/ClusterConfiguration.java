package edu.brown.catalog;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.set.ListOrderedSet;

import edu.brown.utils.CollectionUtil;

/**
 * 
 * @author pavlo
 */
public class ClusterConfiguration {
    private final Map<String, Map<Integer, Set<PartitionConfiguration>>> host_sites = new HashMap<String, Map<Integer, Set<PartitionConfiguration>>>();
    
    
    /**
     * PartitionConfiguration
     */
    private class PartitionConfiguration {
        private final String host;
        private final int site;
        private final int partition;
        
        public PartitionConfiguration(String host, int site, int partition) {
            this.host = host;
            this.site = site;
            this.partition = partition;
        }
        
        public String getHost() {
            return host;
        }
        
        public int getSite() {
            return site;
        }
        
        public int getPartition() {
            return partition;
        }
    }
    
    public void addPartition(String host, int site, int partition) {
        PartitionConfiguration pc = new PartitionConfiguration(host, site, partition);
        
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
    
    
    private Collection<PartitionConfiguration> getPartitions(String host, int site) {
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