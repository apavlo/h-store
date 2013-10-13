/**
 * 
 */
package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 */
public class RandomPartitioner extends AbstractPartitioner {
    protected static final Logger LOG = Logger.getLogger(RandomPartitioner.class);

    private final Random rand = new Random(0);
    private boolean limit_columns = true;

    /**
     * Full Constructor
     * 
     * @param designer
     * @param info
     * @param limit_columns
     */
    public RandomPartitioner(Designer designer, DesignerInfo info, boolean limit_columns) {
        super(designer, info);
        this.setLimitedColumns(limit_columns);
    }

    /**
     * @param designer
     * @param info
     */
    public RandomPartitioner(Designer designer, DesignerInfo info) {
        this(designer, info, true);
    }

    public void setLimitedColumns(boolean flag) {
        LOG.debug("Limit Column Selection: " + (flag ? "ENABLED" : "DISABLED"));
        this.limit_columns = flag;
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.designer.partitioners.AbstractPartitioner#generate(edu.brown
     * .designer.DesignerHints)
     */
    @Override
    public PartitionPlan generate(DesignerHints hints) throws Exception {
        Map<CatalogType, CatalogType> pplan_map = new HashMap<CatalogType, CatalogType>();

        // Do we need to worry about memory?
        boolean calculate_memory = (hints.force_replication_size_limit != null && hints.max_memory_per_partition != 0);

        // Generate the list of which columns we will randomly select from for
        // each table
        Map<Table, List<Column>> table_columns = new HashMap<Table, List<Column>>();
        AccessGraph agraph = (this.limit_columns ? this.generateAccessGraph() : null);
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            List<Column> columns = new ArrayList<Column>();
            if (this.limit_columns) {
                List<Column> column_keys = PartitionerUtil.generateColumnOrder(info, agraph, catalog_tbl, hints, true, false);
                assert (!column_keys.isEmpty()) : "No potential partitioning columns selected for " + catalog_tbl;
                columns.addAll(column_keys);
            } else {
                CollectionUtil.addAll(columns, catalog_tbl.getColumns());
            }
            table_columns.put(catalog_tbl, columns);
        } // FOR

        // TABLES
        final int rounds = 10;
        int round = rounds;
        while (round >= (-1 * rounds)) {
            double total_memory = 0d;

            for (Table catalog_tbl : info.catalogContext.database.getTables()) {
                TableStatistics ts = info.stats.getTableStatistics(catalog_tbl);
                assert (ts != null);
                List<Column> columns = table_columns.get(catalog_tbl);
                assert (columns != null);
                int size = columns.size();

                // The way this works is that on each interation the value of
                // 'round' is
                // decreasing. This means that selecting a table replication
                // decreases every round
                // until it's at the point where we won't select replication at
                // all
                int idx = this.rand.nextInt((size * rounds) + (round < 0 ? 0 : round)) / rounds;

                Column catalog_col = null;
                if (idx == size) {
                    catalog_col = ReplicatedColumn.get(catalog_tbl);
                    if (calculate_memory)
                        total_memory += ts.tuple_size_total;
                } else {
                    catalog_col = columns.get(idx);
                    if (calculate_memory)
                        total_memory += ts.tuple_size_total / info.getNumPartitions();
                }
                pplan_map.put(catalog_tbl, catalog_col);
            } // FOR
            total_memory = total_memory / (double) hints.max_memory_per_partition;

            // If we're not over our memory limit, then quit here
            if (total_memory <= 1.0)
                break;
            LOG.debug("[" + round + "] TotalMemory=" + total_memory);

            // Otherwise make it so that we are less likely to select the
            // partitioning attribute
            // the next go around
            round--;
        } // WHILE

        // PROCEDURES
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            if (catalog_proc.getSystemproc())
                continue;
            int size = catalog_proc.getParameters().size();

            ProcParameter catalog_param = NullProcParameter.singleton(catalog_proc);
            if (size > 0) {
                int idx = this.rand.nextInt(size);
                catalog_param = catalog_proc.getParameters().get(idx);
            }
            pplan_map.put(catalog_proc, catalog_param);
        } // FOR

        return (PartitionPlan.createFromMap(pplan_map));
    }

}
