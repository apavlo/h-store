package edu.brown.catalog.special;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.StringUtil;

public class VerticalPartitionColumn extends MultiColumn {
    public static final String PREFIX = "*VerticalPartitionColumn*"; 
    
    private transient final Collection<Statement> catalog_stmts = new TreeSet<Statement>();
    private transient final Map<Statement, Statement> optimized = new HashMap<Statement, Statement>();
    private transient final Map<Statement, Statement> backups = new HashMap<Statement, Statement>();
    private transient boolean applied = false;
    private transient MaterializedViewInfo catalog_view;
    
    /**
     * THIS SHOULD NOT BE CALLED DIRECTLY
     * Use VerticalPartitionColumn.get()
     * @param attributes
     */
    public VerticalPartitionColumn(Collection<MultiColumn> attributes) {
        super((Collection<? extends Column>)attributes);
        
        // There should only be two elements
        // The first one is the horizontal partitioning parameter(s) 
        // The second one is the vertical partitioning parameter(s)
        assert(attributes.size() == 2) : "Total # of Attributes = " + this.getSize() + ": " + StringUtil.join(",", this);
    }
    
    public static VerticalPartitionColumn get(MultiColumn hp_cols, MultiColumn vp_cols) {
        return InnerMultiAttributeCatalogType.get(VerticalPartitionColumn.class, (Column)hp_cols, (Column)vp_cols);
    }

    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("VP Columns", super.toString());
        m.put("Statements", CatalogUtil.debug(this.catalog_stmts));
        m.put("Optimized Queries", StringUtil.join("\n", this.optimized.entrySet()));
        return StringUtil.formatMaps(m);
    }
    
    @Override
    public String getPrefix() {
        return (PREFIX);
    }
    
    @Override
    public void clear() {
        this.catalog_view = null;
        this.applied = false;
        this.optimized.clear();
        this.backups.clear();
    }
    
    public Collection<Statement> getStatements() {
        return (this.catalog_stmts);
    }
    public Statement getOptimizedQuery(Statement catalog_stmt) {
        return (this.optimized.get(catalog_stmt));
    }
    public void addOptimizedQueries(Map<Statement, Statement> queries) {
        this.optimized.putAll(queries);
    }
    
    /**
     * Return the Horizontal Partitioning Columns
     * @return
     */
    public Collection<Column> getHorizontalPartitionColumns() {
        return ((MultiColumn)this.get(0)).getAttributes();
    }
    public MultiColumn getHorizontalMultiColumn() {
        return ((MultiColumn)this.get(0));
    }

    /**
     * Return the Vertical Partitioning Columns
     * @return
     */
    public Collection<Column> getVerticalPartitionColumns() {
        return ((MultiColumn)this.get(1)).getAttributes();
    }
    public MultiColumn getVerticalMultiColumn() {
        return ((MultiColumn)this.get(1));
    }
    public List<String> getVerticalPartitionColumnNames() {
        List<String> columnNames = new ArrayList<String>();
        for (Column catalog_col : this.getVerticalPartitionColumns()) {
            columnNames.add(catalog_col.getName());
        } // FOR
        return (columnNames);
    }
    
    /**
     * Returns true if we have vertical partition columns defined
     * @return
     */
    public boolean hasVerticalPartitionColumns() {
        Collection<Column> cols = this.getVerticalPartitionColumns();
        return (cols != null && cols.size() > 0);
    }

    // --------------------------------------------------------------------------------------------
    // CATALOG UPDATING METHODS
    // --------------------------------------------------------------------------------------------
    
    /**
     * Create the MaterializedView catalog object for this vertical partition candidate
     */
    public MaterializedViewInfo updateCatalog() {
        Table catalog_tbl = this.getParent();
        assert(this.applied == false) : "Trying to apply " + this + " more than once";
        if (this.catalog_view == null) {
            try {
                this.catalog_view = VoltCompiler.addVerticalPartition(catalog_tbl, this.getVerticalPartitionColumns(), true);
                assert(catalog_view != null);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to create vertical partition for " + this, ex);
            }
        } else {
            catalog_tbl.getViews().add(this.catalog_view);    
        }
            
        // Apply the new Statement query plans
        for (Entry<Statement, Statement> e : this.optimized.entrySet()) {
            // Make a back-up for each original Statement
            Statement backup = this.backups.get(e.getKey());
            if (backup == null) {
                backup = new Statement();
                this.backups.put(e.getKey(), backup);
            }
            CatalogUtil.copyQueryPlans(e.getKey(), backup);
            
            // Then copy the optimized query plans
            CatalogUtil.copyQueryPlans(e.getValue(), e.getKey());
        } // FOR
        this.applied = true;
        return (this.catalog_view);
    }
    
    /**
     * 
     */
    public void revertCatalog() {
        assert(this.catalog_view != null);
        assert(this.applied == false) : "Trying to undo " + this + " before applying";
        
        Table catalog_tbl = this.getParent();
        assert(catalog_tbl.getViews().contains(this.catalog_view));
        catalog_tbl.getViews().remove(this.catalog_view);
        
        // Restore the original query plans from the backups
        for (Statement catalog_stmt : this.optimized.keySet()) {
            Statement backup = this.backups.get(catalog_stmt);
            assert(backup != null) : "Missing backup for " + catalog_stmt.fullName();
            CatalogUtil.copyQueryPlans(backup, catalog_stmt);
        } // FOR
        this.applied = true;
    }
    

}
