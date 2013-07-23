package edu.brown.catalog;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;

import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.MathUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.StringUtil;
import edu.brown.workload.Workload;

public class CatalogInfo {
    private static final Logger LOG = Logger.getLogger(CatalogInfo.class);

    private static final String HOST_INNER = "\u251c";
    private static final String HOST_LAST = "\u2514";

    public static double complexity(ArgumentsParser args, Database catalog_db, Workload workload) throws Exception {
        AccessGraph agraph = new AccessGraph(catalog_db);
        DesignerInfo info = new DesignerInfo(args);
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            // Skip if there are no transactions in the workload for this
            // procedure
            if (workload.getTraces(catalog_proc).isEmpty() || catalog_proc.getSystemproc())
                continue;
            new AccessGraphGenerator(info, catalog_proc).generate(agraph);
        } // FOR

        double ret = 1;
        for (Table catalog_tbl : catalog_db.getTables()) {
            DesignerVertex v = agraph.getVertex(catalog_tbl);
            if (v == null)
                continue;

            Set<Column> used_cols = new HashSet<Column>();
            Collection<DesignerEdge> edges = agraph.getIncidentEdges(v);
            if (edges == null)
                continue;
            for (DesignerEdge e : edges) {
                PredicatePairs cset = e.getAttribute(agraph, AccessGraph.EdgeAttributes.COLUMNSET);
                assert (cset != null) : e.debug();
                Collection<Column> cols = cset.findAllForParent(Column.class, catalog_tbl);
                assert (cols != null) : catalog_tbl + "\n" + cset.debug();
                used_cols.addAll(cols);
            }
            int num_cols = used_cols.size();

            // Picking columns + repl
            ret *= num_cols * num_cols;

            // Secondary Indexes
            for (int i = 0; i < num_cols - 1; i++) {
                ret *= MathUtil.factorial(num_cols - 1).doubleValue() / MathUtil.factorial(i).multiply(MathUtil.factorial(num_cols - 1 - i)).doubleValue();
            } // FOR

            System.err.println(catalog_tbl + ": " + num_cols + " - " + ret);
        }
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getParameters().isEmpty() || catalog_proc.getSystemproc())
                continue;
            ret *= catalog_proc.getParameters().size() * catalog_proc.getParameters().size();
            System.err.println(catalog_proc + ": " + catalog_proc.getParameters().size() + " - " + ret);
        }
        return (ret);
    }
    
    public static String getInfo(CatalogContext catalogContext) {
        StringBuilder sb = new StringBuilder();
        
        // Just print out the Host/Partition Information
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (catalogContext.jarPath != null) {
            m.put("Catalog File", catalogContext.jarPath.getAbsolutePath());
        }
        m.put("# of Hosts", catalogContext.numberOfHosts);
        m.put("# of Sites", catalogContext.numberOfSites);
        m.put("# of Partitions", catalogContext.numberOfPartitions);
        
        sb.append(StringUtil.formatMaps(":", false, false, false, true, true, true, m));
        sb.append("\nCluster Information:\n");

        Map<Host, Set<Site>> hosts = CatalogUtil.getSitesPerHost(catalogContext.catalog);
        Set<String> partition_ids = new TreeSet<String>();
        String partition_f = "%0" + Integer.toString(catalogContext.numberOfPartitions).length() + "d";

        int num_cols = Math.min(4, hosts.size());
        String cols[] = new String[num_cols];
        for (int i = 0; i < num_cols; i++)
            cols[i] = "";

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
                cols[idx] += String.format("     %s SITE %s: %s\n", prefix, HStoreThreadManager.formatSiteName(catalog_site.getId()), partition_ids);
            } // FOR
            cols[idx] += "\n";
            i++;
        } // FOR
        sb.append(StringUtil.columns(cols));
        
        return sb.toString();
    }

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);

//        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD)) {
//            m.put("Complexity", complexity(args, args.catalog_db, args.workload));
//        }
        
        System.out.println(getInfo(args.catalogContext));
        
        // Check for Sequential Scans
//        for (Procedure proc : args.catalogContext.database.getProcedures()) {
//            for (Statement stmt : proc.getStatements()) {
//                AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(stmt, true);
//                Collection<SeqScanPlanNode> scans = PlanNodeUtil.getPlanNodes(root, SeqScanPlanNode.class);
//                Collection<AggregatePlanNode> aggs = PlanNodeUtil.getPlanNodes(root, AggregatePlanNode.class);
//                if (scans.isEmpty() == false && aggs.isEmpty()) {
//                    LOG.warn("Sequential Scan: " + stmt.fullName());
//                }
//            } // FOR (stmt)
//        } // FOR (proc)
//        
        // DUMP PREFETCHABLE QUERIES
//        for (Procedure proc : args.catalogContext.database.getProcedures()) {
//            boolean hasPrefetchable = false;
//            for (Statement stmt : proc.getStatements()) {
//                if (stmt.getPrefetchable()) {
//                    System.out.println(stmt.fullName());
//                    hasPrefetchable = true;
//                }
//            }
//            if (hasPrefetchable) System.out.println();
//        }
        
        // DUMP SYSPROCS
//        for (Procedure proc : args.catalogContext.getSysProcedures()) {
//            System.out.println(proc.getName());
//            int ctr = 0;
//            for (ProcParameter param : CatalogUtil.getSortedCatalogItems(proc.getParameters(), "index")) {
//                System.out.printf("  [%02d] %s%s\n", ctr++,
//                                  VoltType.get(param.getType()),
//                                  (param.getIsarray() ? " <array>" : "")); 
//            } // FOR
//        } // FOR
    }

}
