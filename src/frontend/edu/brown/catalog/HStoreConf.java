package edu.brown.catalog;

import java.util.*;
import java.util.Map.Entry;

import org.voltdb.catalog.*;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;

public abstract class HStoreConf {
    
    /**
     * Converts a host/site/partition information stored in a catalog object to 
     * a configuration file used by our DTXN system
     * @param catalog
     * @return
     * @throws Exception
     */
    public static String toHStoreConf(Catalog catalog) throws Exception {
        Map<Host, Set<Site>> host_partitions = CatalogUtil.getHostPartitions(catalog);
        TreeMap<Integer, String> sorted_output = new TreeMap<Integer, String>();
        for (Host catalog_host : host_partitions.keySet()) {
            for (Site catalog_site : host_partitions.get(catalog_host)) {
                Partition catalog_part = catalog_site.getPartition();
                String hostinfo = catalog_host.getIpaddr() + " " +catalog_site.getPort();
                sorted_output.put(catalog_part.getId(), hostinfo);
            } // FOR
        } // FOR
        
        StringBuilder buffer = new StringBuilder();
        String add = "";
        for (Entry<Integer, String> e : sorted_output.entrySet()) {
            buffer.append(add)
                  .append(e.getKey() + "\n")
                  .append(e.getValue());
            add = "\n\n";
        }
        return (buffer.toString());
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_SIMULATOR_CONF_OUTPUT);
        
        String contents = HStoreConf.toHStoreConf(args.catalog);
        assert(!contents.isEmpty());
        
        String output_path = args.getParam(ArgumentsParser.PARAM_SIMULATOR_CONF_OUTPUT);
        FileUtil.writeStringToFile(output_path, contents);
    }
}