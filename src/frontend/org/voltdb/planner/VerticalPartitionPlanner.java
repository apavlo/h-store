package org.voltdb.planner;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class VerticalPartitionPlanner {
    private static final Logger LOG = Logger.getLogger(VerticalPartitionPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    
    /** convenience pointer to the database object in the catalog */
    private final Database catalog_db;

    /** Context object with planner-local information. */
    private final PlannerContext context;
    
    private final Map<Table, MaterializedViewInfo> vp_tables = new HashMap<Table, MaterializedViewInfo>();
    
    /**
     * @param context
     *            Information about context
     * @param catalogDb
     *            Catalog info about schema, metadata and procedures
     */
    public VerticalPartitionPlanner(PlannerContext context, Database catalogDb) {
        this.catalog_db = catalogDb;
        this.context = context;
        this.vp_tables.putAll(CatalogUtil.getVerticallyPartitionedTables(catalogDb));
    }
    
    /**
     * Process the given Statement and rewrite its query plan if it can take advantage of a 
     * vertical partitioning column
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public boolean process(Statement catalog_stmt) throws Exception {
        // Always skip if there are no vertically partitioned tables
        if (this.vp_tables.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": There are no vertically partitioned tables.");
            return (false);
        }
        
        // We can only work our magic on SELECTs
        QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
        if (qtype != QueryType.SELECT) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": QueryType is " + qtype + ".");
            return (false);
        }
        
        // Check whether this query references a table that has a vertical partition
        Collection<Table> tables = CollectionUtils.intersection(this.vp_tables.keySet(), CatalogUtil.getReferencedTables(catalog_stmt));
        if (tables.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": It does not reference a vertical partitioning table.");
            return (false);
        }

        // Now check whether the columns referenced doesn't include what the table is horizontally partitioned
        // but do include the columns that we have in our vertical partition
        Collection<Column> predicate_cols = CatalogUtil.getReferencedColumns(catalog_stmt);
        if (predicate_cols.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not reference any columns in its predicate.");
            return (false);
        }
        Collection<Column> output_cols = PlanNodeUtil.getOutputColumnsForStatement(catalog_stmt);
        assert(output_cols.isEmpty() == false);
        Set<Table> candidates = new HashSet<Table>();
        for (Table catalog_tbl : tables) {
            MaterializedViewInfo vp = this.vp_tables.get(catalog_tbl);
            assert(vp != null);
            Collection<Column> vp_cols = CatalogUtil.getColumns(vp.getGroupbycols());
            assert(vp_cols.isEmpty() == false);
            Column partitioning_col = catalog_tbl.getPartitioncolumn();
            assert(partitioning_col != null);
            
            // The current vertical partition is valid for this query if all the following are true:
            //  (1) The partitioning_col is in output_cols
            //  (2) The partitioning_col is *not* in the predicate_cols
            //  (3) At least one of the vertical partition's columns is in predicate_cols 
            if (trace.get()) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Partitioning Col", partitioning_col.fullName());
                m.put("Output Cols", output_cols);
                m.put("Predicate Cols", predicate_cols);
                m.put("VerticalP Cols", vp_cols);
                LOG.trace(String.format("Checking whether %s can use vertical partition for %s\n%s",
                                        catalog_stmt.fullName(), catalog_tbl.getName(), StringUtil.formatMaps(m)));
            }
            if (output_cols.contains(partitioning_col) && 
                predicate_cols.contains(partitioning_col) == false &&
                CollectionUtils.intersection(vp_cols, predicate_cols).isEmpty() == false) {
                if (trace.get()) LOG.trace("Valid VP Candidate: " + catalog_tbl);
                candidates.add(catalog_tbl);
            }
        } // FOR
        if (candidates.isEmpty()) {
            if (debug.get()) LOG.warn("Skipping " + catalog_stmt.fullName() + ": Query does not have any valid vertical partitioning references.");
            return (false);
        }
        
        // I think we're all good. So let's rewrite some SQL!
        if (debug.get()) LOG.debug(String.format("Attempting to optimize %s query plan using vertical partitions: %s", catalog_stmt.fullName(), candidates));
        this.optimizeQueryPlan(catalog_stmt, candidates);
        
        return (true);
    }
    
    protected void optimizeQueryPlan(Statement catalog_stmt, Collection<Table> target_tbls) throws Exception {
        
    }
    
}
