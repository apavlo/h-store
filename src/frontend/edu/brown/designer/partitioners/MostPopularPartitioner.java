/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.ColumnSet;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 */
public class MostPopularPartitioner extends AbstractPartitioner {
    protected static final Logger LOG = Logger.getLogger(MostPopularPartitioner.class);
            
    /**
     * @param designer
     * @param info
     */
    public MostPopularPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    /* (non-Javadoc)
     * @see edu.brown.designer.partitioners.AbstractPartitioner#generate(edu.brown.designer.DesignerHints)
     */
    @Override
    public PartitionPlan generate(DesignerHints hints) throws Exception {
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();

        final PartitionPlan pplan = new PartitionPlan();
        
        // Generate an AccessGraph and select the column with the greatest weight for each table
        final AccessGraph agraph = this.generateAccessGraph();
        final boolean calculate_memory = (hints.force_replication_size_limit != null && hints.max_memory_per_partition != 0);
        
        double total_memory_used = 0.0;
        for (Vertex v : agraph.getVertices()) {
            Table catalog_tbl = v.getCatalogItem();
            String table_key = CatalogKey.createKey(catalog_tbl);
            if (trace) LOG.trace("Processing Table " + catalog_tbl.getName());
            
            TableStatistics ts = info.stats.getTableStatistics(catalog_tbl);
            assert(ts != null) : "Null TableStatistics for " + catalog_tbl;
            double size_ratio = (calculate_memory ? (ts.tuple_size_total / (double)hints.max_memory_per_partition) : 0);
            PartitionEntry pentry = null;
            if (trace) LOG.trace(String.format("MEMORY %-25s%.02f", catalog_tbl.getName() + ": ", size_ratio));
            
            // Replication
            if (hints.force_replication.contains(table_key) ||
                (calculate_memory && ts.readonly && size_ratio <= hints.force_replication_size_limit)) {
                total_memory_used += size_ratio;
                if (debug) LOG.debug(String.format("PARTITION %-25s%s", catalog_tbl.getName(), ReplicatedColumn.COLUMN_NAME));
                pentry = new PartitionEntry(PartitionMethodType.REPLICATION);
            
            } else {
                // If there are no edges, then we'll just skip it
                final Collection<Edge> edges = agraph.getIncidentEdges(v);
                if (edges.isEmpty()) {
                    if (trace) LOG.trace("No edges for " + catalog_tbl);
                    continue;
                }
                if (trace) LOG.trace(catalog_tbl + " has " + edges.size() + " edges in AccessGraph");
                
                Histogram column_histogram = new Histogram();
                // Map<Column, Double> unsorted = new HashMap<Column, Double>();
                for (Edge e : edges) {
                    Collection<Vertex> vertices = agraph.getIncidentVertices(e);
                    Vertex v0 = CollectionUtil.get(vertices, 0);
                    Vertex v1 = CollectionUtil.get(vertices, 1);
                    if (v0.equals(v) && v1.equals(v)) continue;
                    
                    double edge_weight = e.getTotalWeight();
                    ColumnSet cset = e.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET);
                    if (trace) LOG.trace("Examining ColumnSet for " + e + "\n" + cset);
                    
                    Histogram cset_histogram = cset.buildHistogramForType(Column.class);
                    if (trace) LOG.trace("Constructed Histogram for " + catalog_tbl + " from ColumnSet:\n" + cset_histogram.toString());
                    
                    Set<Column> columns = cset_histogram.values();
                    for (Column catalog_col : columns) {
                        if (!catalog_col.getParent().equals(catalog_tbl)) continue;
                        long cnt = cset_histogram.get(catalog_col);
                        column_histogram.put(catalog_col, Math.round(cnt * edge_weight));    
                    } // FOR
    //                System.err.println(cset.debug());
                    
    //                LOG.info("[" + e.getTotalWeight() + "]: " + cset);
                } // FOR
//                if (column_histogram.isEmpty()) {
//                    EventObserver observer = new EventObserver() {
//                        @Override
//                        public void update(Observable o, Object arg) {
//                            Vertex v = (Vertex)arg;
//                            
//                            for (Edge e : agraph.getIncidentEdges(v)) {
//                                System.err.println(e.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET));
//                            }
//                            System.err.println(StringUtil.repeat("-", 100));
//                        }
//                    };
//                    GraphVisualizationPanel.createFrame(agraph, observer).setVisible(true);
////                    ThreadUtil.sleep(10000);
//                }

                // We might not find anything if we are calculating the lower bounds using only one transaction
                if (column_histogram.isEmpty()) {
                    if (trace) LOG.trace("Failed to find any ColumnSets for " + catalog_tbl);
                    continue;
                }
                // assert(!column_histogram.isEmpty()) : 
                if (trace) LOG.trace("Column Histogram:\n" + column_histogram);
                
                total_memory_used += (size_ratio / (double)info.getNumPartitions());
                Column most_popular = column_histogram.getMaxCountValue();
                pentry = new PartitionEntry(PartitionMethodType.HASH, most_popular);
                if (debug) LOG.debug(String.format("PARTITION %-25s%s", catalog_tbl.getName(), most_popular.getName()));
            }
            pplan.table_entries.put(catalog_tbl, pentry);
        } // FOR
        assert(total_memory_used <= 100) : "Too much memory per partition: " + total_memory_used;
        
        if (hints.enable_procparameter_search) {
            if (debug) LOG.debug("Selecting partitioning ProcParameter for " + this.info.catalog_db.getProcedures().size() + " Procedures");
            pplan.apply(info.catalog_db);
            
            // Temporarily disable multi-attribute parameters
            boolean multiproc_orig = hints.enable_multi_partitioning;
            hints.enable_multi_partitioning = false;
            
            for (Procedure catalog_proc : this.info.catalog_db.getProcedures()) {
                if (catalog_proc.getSystemproc() || catalog_proc.getParameters().size() == 0) continue;
                List<String> param_order = BranchAndBoundPartitioner.generateProcParameterOrder(info, info.catalog_db, catalog_proc, hints);
                ProcParameter catalog_proc_param = CatalogKey.getFromKey(info.catalog_db, param_order.get(0), ProcParameter.class);
                if (debug) LOG.debug(String.format("PARTITION %-25s%s", catalog_proc.getName(), CatalogUtil.getDisplayName(catalog_proc_param)));
                
                // Create a new PartitionEntry for this procedure and set it to be always single-partitioned
                // We will check down below whether that's always true or not
                PartitionEntry pentry = new PartitionEntry(PartitionMethodType.HASH, catalog_proc_param);
                pentry.setSinglePartition(true);
                pplan.getProcedureEntries().put(catalog_proc, pentry);
            } // FOR
            
            hints.enable_multi_partitioning = multiproc_orig;
        }
        this.setProcedureSinglePartitionFlags(pplan, hints);
        return (pplan);
    }

}
