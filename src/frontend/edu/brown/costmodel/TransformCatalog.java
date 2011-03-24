package edu.brown.costmodel;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Site;

import edu.brown.catalog.CatalogUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

public class TransformCatalog {
    private static final Logger LOG = Logger.getLogger(TransformTransactionTraces.class);
    
    /** Processes the catalog information and writes it
     * File format like the following:
     * # of sites
     * site # : host #
     * @param host_site
     */
    public static void transform(Database catalogDb) {
        StringBuilder sb = new StringBuilder();
        TreeMap<Integer, Integer> site_host = new TreeMap<Integer, Integer>();
        Iterator<Cluster> cluster_it = catalogDb.getCatalog().getClusters().iterator();
        int total_sites = 0;
        int host = 0;
        while (cluster_it.hasNext()) {
            Cluster c = cluster_it.next();
            Iterator<Host> host_it = c.getHosts().iterator();
            while (host_it.hasNext()) {
                Host h = host_it.next();
                Iterator<Site> site_it = CatalogUtil.getSitesForHost(h).iterator();
                while (site_it.hasNext()) {
                    site_host.put(site_it.next().getId(), host);
                    total_sites++;
                }
                host++;
            }
        }
        for (Map.Entry<Integer, Integer> site : site_host.entrySet()) {
            sb.append(site.getValue() + ":" + site.getKey() + "\n");
        }
        assert (total_sites > 0) : "No sites!!!!";
        Writer writer;
        try {
            writer = new FileWriter("/home/sw47/Desktop/system.info");
            writer.append(String.valueOf(total_sites) + "\n");
            writer.append(sb.toString());
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
