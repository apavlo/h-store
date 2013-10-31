/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
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
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.ProcedureEntry;
import edu.brown.designer.partitioners.plan.TableEntry;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public class MostPopularPartitioner extends AbstractPartitioner {
    private static final Logger LOG = Logger.getLogger(MostPopularPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private Long last_memory = null;

    /**
     * @param designer
     * @param info
     */
    public MostPopularPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    public Long getLastMemory() {
        return last_memory;
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.designer.partitioners.AbstractPartitioner#generate(edu.brown
     * .designer.DesignerHints)
     */
    @Override
    public PartitionPlan generate(DesignerHints hints) throws Exception {
        final PartitionPlan pplan = new PartitionPlan();

        // Generate an AccessGraph and select the column with the greatest
        // weight for each table
        final AccessGraph agraph = this.generateAccessGraph();
        final boolean calculate_memory = (hints.force_replication_size_limit != null && hints.max_memory_per_partition != 0);

        double total_partitionRatio = 0.0;
        long total_partitionSize = 0l;

        for (DesignerVertex v : agraph.getVertices()) {
            Table catalog_tbl = v.getCatalogItem();
            String table_key = CatalogKey.createKey(catalog_tbl);

            Collection<Column> forced_columns = hints.getForcedTablePartitionCandidates(catalog_tbl);
            TableStatistics ts = info.stats.getTableStatistics(catalog_tbl);
            assert (ts != null) : "Null TableStatistics for " + catalog_tbl;
            double partition_size = (calculate_memory ? (ts.tuple_size_total / (double) info.getNumPartitions()) : 0);
            double partition_ratio = (calculate_memory ? (ts.tuple_size_total / (double) hints.max_memory_per_partition) : 0);
            TableEntry pentry = null;

            if (debug.val) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Read Only", ts.readonly);
                m.put("Table Size", StringUtil.formatSize(ts.tuple_size_total));
                m.put("Table Partition Size", StringUtil.formatSize((long)partition_size));
                m.put("Table Partition Ratio", String.format("%.02f", partition_ratio));
                m.put("Total Partition Size", String.format("%s / %s", StringUtil.formatSize(total_partitionSize), StringUtil.formatSize(hints.max_memory_per_partition)));
                m.put("Total Partition Ratio", String.format("%.02f", total_partitionRatio));
                LOG.debug(String.format("%s\n%s", catalog_tbl.getName(), StringUtil.formatMaps(m)));
            }

            // -------------------------------
            // Replication
            // -------------------------------
            if (hints.force_replication.contains(table_key) || (calculate_memory && ts.readonly && hints.enable_replication_readonly && partition_ratio <= hints.force_replication_size_limit)) {
                total_partitionRatio += partition_ratio;
                total_partitionSize += ts.tuple_size_total;
                Column catalog_col = ReplicatedColumn.get(catalog_tbl);
                pentry = new TableEntry(PartitionMethodType.REPLICATION, catalog_col);
                if (debug.val)
                    LOG.debug(String.format("Replicating %s at all partitions [%s]", catalog_tbl.getName(), catalog_col.fullName()));

                // -------------------------------
                // Forced Selection
                // -------------------------------
            } else if (forced_columns.isEmpty() == false) {
                // Assume there is only one candidate
                assert (forced_columns.size() == 1) : "Unexpected number of forced columns: " + forced_columns;
                Column catalog_col = CollectionUtil.first(forced_columns);
                pentry = new TableEntry(PartitionMethodType.HASH, catalog_col);
                total_partitionRatio += partition_size / (double) hints.max_memory_per_partition;
                total_partitionSize += partition_size;
                if (debug.val)
                    LOG.debug(String.format("Forcing %s to be partitioned by specific column [%s]", catalog_tbl.getName(), catalog_col.fullName()));

                // -------------------------------
                // Select Most Popular
                // -------------------------------
            } else {
                // If there are no edges, then we'll just randomly pick a column
                // since it doesn't matter
                final Collection<DesignerEdge> edges = agraph.getIncidentEdges(v);
                if (edges.isEmpty())
                    continue;
                if (trace.val)
                    LOG.trace(catalog_tbl + " has " + edges.size() + " edges in AccessGraph");

                ObjectHistogram<Column> column_histogram = null;
                ObjectHistogram<Column> join_column_histogram = new ObjectHistogram<Column>();
                ObjectHistogram<Column> self_column_histogram = new ObjectHistogram<Column>();
                // Map<Column, Double> unsorted = new HashMap<Column, Double>();
                for (DesignerEdge e : edges) {
                    Collection<DesignerVertex> vertices = agraph.getIncidentVertices(e);
                    DesignerVertex v0 = CollectionUtil.get(vertices, 0);
                    DesignerVertex v1 = CollectionUtil.get(vertices, 1);
                    boolean self = (v0.equals(v) && v1.equals(v));
                    column_histogram = (self ? self_column_histogram : join_column_histogram);

                    double edge_weight = e.getTotalWeight();
                    PredicatePairs cset = e.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET);
                    if (trace.val)
                        LOG.trace("Examining ColumnSet for " + e.toString(true));

                    Histogram<Column> cset_histogram = cset.buildHistogramForType(Column.class);
                    Collection<Column> columns = cset_histogram.values();
                    if (trace.val)
                        LOG.trace("Constructed Histogram for " + catalog_tbl + " from ColumnSet:\n"
                                + cset_histogram.setDebugLabels(CatalogUtil.getHistogramLabels(cset_histogram.values())).toString(100, 50));
                    for (Column catalog_col : columns) {
                        if (!catalog_col.getParent().equals(catalog_tbl))
                            continue;
                        if (catalog_col.getNullable())
                            continue;
                        long cnt = cset_histogram.get(catalog_col);
                        if (trace.val)
                            LOG.trace("Found Match: " + catalog_col.fullName() + " [cnt=" + cnt + "]");
                        column_histogram.put(catalog_col, Math.round(cnt * edge_weight));
                    } // FOR
                    // System.err.println(cset.debug());
                    // LOG.info("[" + e.getTotalWeight() + "]: " + cset);
                } // FOR

                // If there were no join columns, then use the self-reference
                // histogram
                column_histogram = (join_column_histogram.isEmpty() ? self_column_histogram : join_column_histogram);
                if (column_histogram.isEmpty()) {
                    EventObserver<DesignerVertex> observer = new EventObserver<DesignerVertex>() {
                        @Override
                        public void update(EventObservable<DesignerVertex> o, DesignerVertex v) {
                            for (DesignerEdge e : agraph.getIncidentEdges(v)) {
                                LOG.info(e.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET));
                            }
                            LOG.info(StringUtil.repeat("-", 100));
                        }
                    };
                    LOG.info("Edges: " + edges);
                    GraphVisualizationPanel.createFrame(agraph, observer).setVisible(true);
                    // ThreadUtil.sleep(10000);
                }

                // We might not find anything if we are calculating the lower
                // bounds using only one transaction
                // if (column_histogram.isEmpty()) {
                // if (trace.val)
                // LOG.trace("Failed to find any ColumnSets for " +
                // catalog_tbl);
                // continue;
                // }
                assert (!column_histogram.isEmpty()) : "Failed to find any ColumnSets for " + catalog_tbl;
                if (trace.val)
                    LOG.trace("Column Histogram:\n" + column_histogram);

                Column catalog_col = CollectionUtil.first(column_histogram.getMaxCountValues());
                pentry = new TableEntry(PartitionMethodType.HASH, catalog_col, null, null);
                total_partitionRatio += partition_size / (double) hints.max_memory_per_partition;
                total_partitionSize += partition_size;

                if (debug.val)
                    LOG.debug(String.format("Selected %s's most popular column for partitioning [%s]", catalog_tbl.getName(), catalog_col.fullName()));
            }
            pplan.table_entries.put(catalog_tbl, pentry);

            if (debug.val)
                LOG.debug(String.format("Current Partition Size: %s", StringUtil.formatSize(total_partitionSize), StringUtil.formatSize(hints.max_memory_per_partition)));
            assert (total_partitionRatio <= 1) : String.format("Too much memory per partition: %s / %s", StringUtil.formatSize(total_partitionSize),
                    StringUtil.formatSize(hints.max_memory_per_partition));
        } // FOR

        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (pplan.getTableEntry(catalog_tbl) == null) {
                Column catalog_col = CollectionUtil.random(catalog_tbl.getColumns());
                assert (catalog_col != null) : "Failed to randomly pick column for " + catalog_tbl;
                pplan.table_entries.put(catalog_tbl, new TableEntry(PartitionMethodType.HASH, catalog_col, null, null));
                if (debug.val)
                    LOG.debug(String.format("No partitioning column selected for %s. Choosing a random attribute [%s]", catalog_tbl, catalog_col.fullName()));
            }
        } // FOR

        if (hints.enable_procparameter_search) {
            if (debug.val)
                LOG.debug("Selecting partitioning ProcParameter for " + this.info.catalogContext.database.getProcedures().size() + " Procedures");
            pplan.apply(info.catalogContext.database);

            // Temporarily disable multi-attribute parameters
            boolean multiproc_orig = hints.enable_multi_partitioning;
            hints.enable_multi_partitioning = false;

            for (Procedure catalog_proc : this.info.catalogContext.database.getProcedures()) {
                if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc))
                    continue;

                Set<String> param_order = PartitionerUtil.generateProcParameterOrder(info, info.catalogContext.database, catalog_proc, hints);
                if (param_order.isEmpty() == false) {
                    ProcParameter catalog_proc_param = CatalogKey.getFromKey(info.catalogContext.database, CollectionUtil.first(param_order), ProcParameter.class);
                    if (debug.val)
                        LOG.debug(String.format("PARTITION %-25s%s", catalog_proc.getName(), CatalogUtil.getDisplayName(catalog_proc_param)));

                    // Create a new PartitionEntry for this procedure and set it
                    // to be always single-partitioned
                    // We will check down below whether that's always true or
                    // not
                    ProcedureEntry pentry = new ProcedureEntry(PartitionMethodType.HASH, catalog_proc_param, true);
                    pplan.getProcedureEntries().put(catalog_proc, pentry);
                }
            } // FOR

            hints.enable_multi_partitioning = multiproc_orig;
        }
        this.setProcedureSinglePartitionFlags(pplan, hints);

        this.last_memory = total_partitionSize;

        return (pplan);
    }

}
