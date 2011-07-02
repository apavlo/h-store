package edu.brown.catalog;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreSite;

public class CatalogInfo {

    private static final String HOST_INNER = "\u251c";
    private static final String HOST_LAST = "\u2514";
    
    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        // Just print out the Host/Partition Information
        int num_hosts = CatalogUtil.getCluster(args.catalog).getHosts().size();
        int num_sites = CatalogUtil.getCluster(args.catalog).getSites().size();
        int num_partitions = CatalogUtil.getNumberOfPartitions(args.catalog);
        
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Catalog File", args.catalog_path.getAbsolutePath());
        m.put("# of Hosts", num_hosts);
        m.put("# of Sites", num_sites);
        m.put("# of Partitions", num_partitions);
        System.out.println(StringUtil.formatMaps(":", false, false, false, true, m));
        System.out.println("Cluster Information:\n");
        
        int num_cols = 4;
        String cols[] = new String[num_cols];
        for (int i = 0; i < num_cols; i++) cols[i] = "";
        
        Map<Host, Set<Site>> hosts = CatalogUtil.getSitesPerHost(args.catalog);
        Set<String> partition_ids = new TreeSet<String>();
        String partition_f = "%0" + Integer.toString(num_partitions).length() + "d";
        int i = 0;
        for (Host catalog_host : hosts.keySet()) {
            int idx = i % num_cols;
            
            cols[idx] += String.format("[%02d] HOST %s\n", i, catalog_host.getIpaddr());
            Set<Site> sites = hosts.get(catalog_host);
            int j = 0;
            for (Site catalog_site : sites) {
                partition_ids.clear();
                for (Partition catalog_part : catalog_site.getPartitions()) {
                    partition_ids.add(String.format(partition_f, catalog_part.getId()));
                } // FOR
                String prefix = (++j == sites.size() ? HOST_LAST : HOST_INNER);
                cols[idx] += String.format("     %s SITE %s: %s\n", prefix, HStoreSite.formatSiteName(catalog_site.getId()), partition_ids);
            } // FOR
            cols[idx] += "\n";
            i++;
        } // FOR
        System.out.println(StringUtil.columns(cols));
    }

}
