package edu.brown.utils;

import java.lang.reflect.Array;
import java.util.*;

import org.apache.log4j.Logger;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.designer.ColumnSet;
import edu.brown.designer.DesignerUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.statistics.Histogram;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

/**
 * 
 * @author pavlo
 *
 */
public class PartitionEstimator {
    private static final Logger LOG = Logger.getLogger(PartitionEstimator.class.getName());
    
    // ----------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------
    private Database catalog_db;
    private final AbstractHasher hasher;
    private final Set<Integer> all_partitions = new HashSet<Integer>();
    
    // PlanFragment Key -> CacheEntry(Column Key -> StmtParameter Indexes)
    private final Map<String, CacheEntry> frag_cache_entries = new HashMap<String, CacheEntry>();
    
    // Statement Key -> CacheEntry(Column Key -> StmtParam Indexes)
    private final Map<String, CacheEntry> stmt_cache_entries = new HashMap<String, CacheEntry>();
    
    // Table Key -> All cache entries for Statements that reference Table
    private final Map<String, Set<CacheEntry>> table_cache_xref = new HashMap<String, Set<CacheEntry>>();
    
    private final Random rand = new Random(100);
    
    /**
     * CacheEntry
     * ColumnKey -> Set<StmtParameterIndex>
     */
    protected class CacheEntry extends HashMap<String, Set<Integer>> {
        private static final long serialVersionUID = 1L;
        private final QueryType query_type;
        private boolean contains_or = false;
        private final HashSet<String> table_keys = new HashSet<String>();
        private final HashSet<String> broadcast_tables = new HashSet<String>();
        
        private final transient HashSet<Table> tables = new HashSet<Table>();
        private transient boolean is_valid = true;
        private transient Integer last_catalog_hashcode = null;
        
        public CacheEntry(QueryType query_type) {
            this.query_type = query_type;
        }
        /**
         * 
         * @param key
         * @param param_idx
         * @param catalog_tbls
         */
        public void put(String key, int param_idx, Table...catalog_tbls) {
            if (!this.containsKey(key)) {
                this.put(key, new HashSet<Integer>());
            }
            this.get(key).add(param_idx);
            for (Table catalog_tbl : catalog_tbls) {
                this.table_keys.add(CatalogKey.createKey(catalog_tbl));
            } // FOR
        }
        public void setContainsOr(boolean flag) {
            this.contains_or = flag;
        }
        public boolean isContainsOr() {
            return (this.contains_or);
        }
        /**
         * The catalog object for this CacheEntry references a table without any
         * predicates on columns, so we need to mark it as having to always be
         * broadcast (unless it is replicated) 
         * @param catalog_tbls
         */
        public void markAsBroadcast(Table...catalog_tbls) {
            for (Table catalog_tbl : catalog_tbls) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                this.table_keys.add(table_key);
                this.broadcast_tables.add(table_key);
            } // FOR
        }
        /**
         * Does the catalog object represented by this CacheEntry have to be broadcast to
         * all partitions because of the given Table
         * @param catalog_tbl
         * @return
         */
        public boolean isMarkedAsBroadcast(Table catalog_tbl) {
            return (this.broadcast_tables.contains(CatalogKey.createKey(catalog_tbl)));
        }
        /**
         * Get all of the tables referenced in this CacheEntry
         * @return
         */
        public Set<Table> getTables() {
            int cur_catalog_hashcode = catalog_db.hashCode();
            // We have to update the cache set if don't have all of the entries we need or the catalog has changed
            if (this.last_catalog_hashcode == null ||
                this.last_catalog_hashcode != cur_catalog_hashcode || 
                this.tables.size() < this.table_keys.size()) {
                LOG.trace("Generating list of tables used by cache entry");
                this.tables.clear();
                for (String table_key : this.table_keys) {
                    this.tables.add(CatalogKey.getFromKey(catalog_db, table_key, Table.class));
                } // FOR
                this.last_catalog_hashcode = cur_catalog_hashcode;
            }
            return (this.tables);
        }
        public boolean hasTable(Table catalog_tbl) {
            return (this.table_keys.contains(CatalogKey.createKey(catalog_tbl)));
        }
        
        public void setValid() {
            this.is_valid = true;
        }
        public void setInvalid() {
            this.is_valid = false;
            this.table_keys.clear();
            this.clear();
        }
        public boolean isValid() {
            return (this.is_valid);
        }
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[IsValid=" + this.is_valid + ", ")
              .append("Tables=" + this.table_keys + ", ")
              .append("Broadcast=" + this.broadcast_tables + ", ")
              .append("ParameterMappings=" + super.toString() + "]");
            return (sb.toString());
        }
    }; // END CLASS

    // ----------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------

    /**
     * Convenience constructor that uses DefaultHasher 
     */
    public PartitionEstimator(Database catalog_db) {
        this(catalog_db, new DefaultHasher(catalog_db, CatalogUtil.getNumberOfPartitions(catalog_db)));
    }
    
    /**
     * Constructor
     * @param args
     */
    public PartitionEstimator(Database catalog_db, AbstractHasher hasher) {
        this.catalog_db = catalog_db;
        this.hasher = hasher;
        this.initCatalog(catalog_db);
    }
    
    /**
     * Initialize a new catalog for this PartitionEstimator
     * @param new_catalog_db
     */
    public void initCatalog(Database new_catalog_db) {
        // Check whether any of our cache partition columns have changed
        // In which cache we know need to invalidate our cache entries
        /*
        if (this.catalog_db != null) {
            for (Table catalog_tbl : this.catalog_db.getTables()) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                Table new_catalog_tbl = new_catalog_db.getTables().get(catalog_tbl.getName()); 
                
                // This table is not in our new catalog or it doesn't have a partitioning column
                if (new_catalog_tbl == null || new_catalog_tbl.getPartitioncolumn() == null) {
                    LOG.debug("Removing partitioning information for " + catalog_tbl);
                    if (this.table_partition_columns.containsKey(table_key)) this.invalidateTableCache(table_key);
                    this.table_partition_columns.remove(table_key);
                    continue;
                }
                // Otherwise we will always just update our cache
                // Invalidate the table's cache if the partitioning column has changed
                String new_partition_key = CatalogKey.createKey(new_catalog_tbl.getPartitioncolumn());
                String orig_partition_key = this.table_partition_columns.get(table_key); 
                if (orig_partition_key != null && !new_partition_key.equals(orig_partition_key)) {
                    this.invalidateTableCache(table_key);
                }
                this.table_partition_columns.put(table_key, new_partition_key);
            } // FOR
        } */
        this.catalog_db = new_catalog_db;
//        System.err.println("++++ CATALOG HASH: " + this.catalog_db.hashCode());
        
        // Generate a list of all the partition ids, so that we can quickly
        // add them to the output when estimating later on
        if (this.all_partitions.size() != this.hasher.getNumPartitions()) {
            this.all_partitions.clear();
            for (int i = 0, cnt = this.hasher.getNumPartitions(); i < cnt; i++) {
                this.all_partitions.add(i);
            } // FOR
            LOG.debug("Initialized PartitionEstimator with " + this.hasher.getNumPartitions() + " partitions " +
                      "using the " + this.hasher.getClass().getSimpleName() + " hasher");
        }
    }
    
    /**
     * This is the method that actually picks the Statement apart and figures out
     * where the columns and parameters are used together
     * @param catalog_stmt
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private synchronized void generateCache(final Statement catalog_stmt) throws Exception {
        final boolean debug = LOG.isDebugEnabled(); // catalog_stmt.getName().equalsIgnoreCase("GetNewDestination");
        final boolean trace = LOG.isTraceEnabled();
        
        // Check whether we already have a CacheEntry for the Statement that we can reuse
        String stmt_key = CatalogKey.createKey(catalog_stmt);
        QueryType stmt_type = QueryType.get(catalog_stmt.getQuerytype());
        PartitionEstimator.CacheEntry stmt_cache = this.stmt_cache_entries.get(stmt_key);
        if (stmt_cache == null) {
            stmt_cache = new PartitionEstimator.CacheEntry(stmt_type);
        } else {
            // assert(stmt_cache.isValid()) : "Unexpected invalid cache entry for " + CatalogUtil.getDisplayName(catalog_stmt);
            stmt_cache.setValid();
        }
        Set<Table> stmt_tables = CatalogUtil.getReferencedTables(catalog_stmt);
        if (debug) LOG.debug("Generating partitioning cache for " + catalog_stmt);
        
        // Important: Work through the fragments in reverse so that we go from the bottom of the tree up.
        // We are assuming that we can get the StmtParameter->Column mapping that we need from either the
        // multi-partition plan or the single-partition plan and that the mapping will be the same in both
        // cases. Therefore, we don't need to differentiate whether we are picking apart one or the other,
        // nor do we need to switch to a different cache entry for the Statement if we realize that we
        // are going to be single-partition or not.
        // We have to go through all of the fragments because we don't know which set the system will
        // be calling at runtime.
        CatalogMap<?> fragment_sets[] = new CatalogMap<?>[]{
                catalog_stmt.getFragments(),
                catalog_stmt.getMs_fragments(),
        };
        for (int i = 0; i < fragment_sets.length; i++) {
            if (fragment_sets[i] == null || fragment_sets[i].isEmpty()) continue;
            CatalogMap<PlanFragment> fragments = (CatalogMap<PlanFragment>)fragment_sets[i];
            boolean singlesited = (i == 0);
            if (trace) LOG.trace("Analyzing " + fragments.size() + " " + (singlesited ? "single" : "multi") + "-sited fragments for " + catalog_stmt);
            
            // Examine each fragment and pick apart how the tables are referenced
            // The order doesn't matter here
            for (PlanFragment catalog_frag : fragments) {
                // Again, always check whether we already have a CacheEntry for the PlanFragment that we can reuse
                String frag_key = CatalogKey.createKey(catalog_frag);
                PartitionEstimator.CacheEntry frag_cache = this.frag_cache_entries.get(frag_key);
                if (frag_cache == null) {
                    frag_cache = new PartitionEstimator.CacheEntry(stmt_type);
                } else if (frag_cache.isValid()) {
                    assert(!frag_cache.isValid()) : 
                        "Cache entry for " + CatalogUtil.getDisplayName(catalog_frag) + " is marked as valid when we were expecting to be invalid\n" + this.toString();
                    frag_cache.setValid();
                }
                
                AbstractPlanNode root = QueryPlanUtil.deserializePlanFragment(catalog_frag);
                Set<Table> frag_tables = CatalogUtil.getReferencedTables(catalog_db, root);
                Table tables_arr[] = new Table[frag_tables.size()];
                tables_arr = frag_tables.toArray(tables_arr);
                assert(tables_arr.length == frag_tables.size());
                if (trace) LOG.trace("Analyzing fragment #" + catalog_frag);
                
                // Check whether the predicate expression in this PlanFragment contains an OR
                // We need to know this if we get hit with Multi-Column Partitioning
                // XXX: Why does this matter??
                Set<ExpressionType> exp_types = PlanNodeUtil.getScanExpressionTypes(catalog_db, root);
                if (exp_types.contains(ExpressionType.CONJUNCTION_OR)) {
                    if (debug) LOG.warn(CatalogUtil.getDisplayName(catalog_frag) + " contains OR conjunction. Cannot be used with multi-column partitioning");
                    stmt_cache.setContainsOr(true);
                    frag_cache.setContainsOr(true);
                }
                
                // If there are no tables, then we need to double check that the "non-transactional" flag
                // is set for the fragment. This means that this fragment does not operate directly on
                // a persistent table in the database.
                // We'll add an entry in the cache using our special "no tables" flag. This means that
                // the fragment needs to be executed locally.
                if (frag_tables.isEmpty()) {
                    String msg = "The fragment " + catalog_frag + " in " + CatalogUtil.getDisplayName(catalog_stmt) + " does not reference any tables";
                    if (!catalog_frag.getNontransactional()) {
                        throw new Exception(msg + " but the non-transactional flag is not set");
                    }
                    if (trace) LOG.trace(msg);
                }
                if (trace) LOG.trace("Fragment Tables: " + frag_tables);
                
                // We only need to find where the partition column is referenced
                // If it's not in there, then this query has to be broadcasted to all nodes
                // Note that we pass all the tables that are part of the fragment, since we need
                // to be able to handle joins
                ColumnSet cset = DesignerUtil.extractFragmentColumnSet(catalog_frag, false, tables_arr);
                assert(cset != null);
                Map<Column, Set<Column>> column_joins = new TreeMap<Column, Set<Column>>();
                if (trace) LOG.trace("Extracted Column Set for " + Arrays.toString(tables_arr) + ":\n" + cset.debug());
                
                // If there are no columns, then this fragment is doing a full table scan
                if (cset.isEmpty() && tables_arr.length > 0) {
                    if (trace) LOG.trace("No columns accessed in " + catalog_frag + " despite reading " + tables_arr.length + " tables");
                    stmt_cache.markAsBroadcast(tables_arr);
                    frag_cache.markAsBroadcast(tables_arr);
                    
                // Fragment references the columns for our tables. Pick them apart!
                } else {
                    // First go through all the entries and add any mappings from
                    // Columns to StmtParameters to our stmt_cache
                    for (ColumnSet.Entry entry : cset) {
                        if (trace) LOG.trace("Examining extracted ColumnSetEntry: " + entry);
                        
                        // Column = Column
                        if (entry.getFirst() instanceof Column && entry.getSecond() instanceof Column) {
                            Column col0 = (Column)entry.getFirst();
                            Column col1 = (Column)entry.getSecond();
    
                            if (!entry.getComparisonExp().equals(ExpressionType.COMPARE_EQUAL)) {
                                LOG.warn("Unsupported non-equality join in " + catalog_stmt + ": " + entry);
                            } else {
                                if (!column_joins.containsKey(col0)) column_joins.put(col0, new TreeSet<Column>());
                                if (!column_joins.containsKey(col1)) column_joins.put(col1, new TreeSet<Column>());
                                column_joins.get(col0).add(col1);
                                column_joins.get(col1).add(col0);
                            }
                            continue;
                        }
                        
                        // Look for predicates with StmtParameters
                        for (Table catalog_tbl : frag_tables) {
                            Column catalog_col = null;
                            StmtParameter catalog_param = null;
//                            if (trace) {
//                                LOG.trace("Current Table: " + catalog_tbl.hashCode());
//
//                                if (entry.getFirst() != null) {
//                                    LOG.trace("entry.getFirst().getParent(): " + (entry.getFirst().getParent() != null ? entry.getFirst().getParent().hashCode() : entry.getFirst() + " parent is null?"));
//                                    
//                                    if (entry.getFirst().getParent() instanceof Table) {
//                                        Table parent = entry.getFirst().getParent();
//                                        if (parent.getName().equals(catalog_tbl.getName())) {
//                                            assert(parent.equals(catalog_tbl)) : "Mismatch on " + parent.getName() + "???";
//                                        }
//                                    }
//                                    
//                                } else {
//                                    LOG.trace("entry.getFirst():             " + null);
//                                }
//                                if (entry.getSecond() != null) {
//                                    LOG.trace("entry.getSecond().getParent(): " + (entry.getSecond().getParent() != null ? entry.getSecond().getParent().hashCode() : entry.getSecond() + " parent is null?"));
//                                } else {
//                                    LOG.trace("entry.getSecond():             " + null);
//                                }
//                            }
    
                            // Column = StmtParameter
                            if (entry.getFirst().getParent() != null && entry.getFirst().getParent().equals(catalog_tbl) &&
                                entry.getSecond() instanceof StmtParameter) {
                                catalog_col = (Column)entry.getFirst();
                                catalog_param = (StmtParameter)entry.getSecond();
                            // StmtParameter = Column
                            } else if (entry.getSecond().getParent() != null && entry.getSecond().getParent().equals(catalog_tbl) &&
                                       entry.getFirst() instanceof StmtParameter) {
                                catalog_col = (Column)entry.getSecond();
                                catalog_param = (StmtParameter)entry.getFirst();
                            }
                            if (catalog_col != null && catalog_param != null) {
                                if (trace) LOG.trace("[" + CatalogUtil.getDisplayName(catalog_tbl) + "] Adding cache entry for " + CatalogUtil.getDisplayName(catalog_frag) + ": " + entry);
                                String column_key = CatalogKey.createKey(catalog_col); 
                                stmt_cache.put(column_key, catalog_param.getIndex(), catalog_tbl);
                                frag_cache.put(column_key, catalog_param.getIndex(), catalog_tbl);
                            }
                        } // FOR (tables)
                        if (trace) LOG.trace("-------------------");
                    } // FOR (entry)
                
                    // We now have to take a second pass through the column mappings
                    // This will pick-up those columns that are joined together where one of them is also referenced
                    // with an input parameter. So we will map the input parameter to the second column as well
                    PartitionEstimator.populateColumnJoins(column_joins);
                    
                    for (Column catalog_col : column_joins.keySet()) {
                        String column_key = CatalogKey.createKey(catalog_col);
                        
                        // Otherwise, we have to examine the the ColumnSet and look for any reference to this column
                        if (trace) LOG.trace("Trying to find all references to " + CatalogUtil.getDisplayName(catalog_col));
                        for (Column other : column_joins.get(catalog_col)) {
                            String other_column_key = CatalogKey.createKey(other);
    
                            // IMPORTANT: If the other entry is a column from another table and we don't
                            // have a reference in stmt_cache for ourselves, then we can look
                            // to see if this guy was used against a StmtParameter some where else in the Statement
                            // If this is the case, then we can substitute that mofo in it's place
                            if (stmt_cache.containsKey(column_key)) {
                                for (Integer param_idx : stmt_cache.get(column_key)) {
                                    if (trace) LOG.trace("Linking " + CatalogUtil.getDisplayName(other) + " to parameter #" + param_idx + " because of " + CatalogUtil.getDisplayName(catalog_col));
                                    stmt_cache.put(other_column_key, param_idx, (Table)other.getParent());
                                    frag_cache.put(other_column_key, param_idx, (Table)other.getParent());
                                } // FOR (StmtParameter.Index)
                            }
                        } // FOR (Column)
                    } // FOR (Column)
                }
                if (trace) LOG.trace(frag_cache);
                
                // Loop through all of our tables and make sure that there is an entry in the PlanFragment CacheEntrry
                // If there isn't, then that means there was no predicate on the table and therefore the PlanFragment
                // must be broadcast to all partitions (unless it is replicated)
                for (Table catalog_tbl : tables_arr) {
                    if (!frag_cache.hasTable(catalog_tbl)) {
                        if (trace) LOG.trace("No column predicate for " + CatalogUtil.getDisplayName(catalog_tbl) + ". " +
                                             "Marking as broadcast for " + CatalogUtil.getDisplayName(catalog_frag) + ": " +
                                             frag_cache.getTables());
                        frag_cache.markAsBroadcast(catalog_tbl);
                        stmt_cache.markAsBroadcast(catalog_tbl);
                    }
                } // FOR
                
                // Store the Fragment cache and update the Table xref mapping
                this.frag_cache_entries.put(frag_key, frag_cache);
                this.addTableCacheXref(frag_cache, frag_tables);
            } // FOR (fragment)
            
            // Then for updates we need to look to see whether they are updating an attribute that they
            // are partitioned on. If so, then it gets dicey because we need to know the value...
            if (stmt_type == QueryType.UPDATE) {
                for (Table catalog_tbl : CatalogUtil.getReferencedTables(catalog_stmt)) {
                    ColumnSet update_cset = new ColumnSet();
                    List<Table> tables = new ArrayList<Table>();
                    tables.add(catalog_tbl);
                    AbstractPlanNode root_node = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
                    DesignerUtil.extractUpdateColumnSet(catalog_stmt, catalog_db, update_cset, root_node, true, tables);

                    boolean found = false;
                    for (ColumnSet.Entry entry : update_cset) {
                        Column catalog_col = null;
                        StmtParameter catalog_param = null;
                        
                        // For now we only care up look-ups using parameters
                        if (entry.getFirst() instanceof StmtParameter) {
                            catalog_param = (StmtParameter)entry.getFirst();
                            catalog_col = (Column)entry.getSecond();
                        } else if (entry.getSecond() instanceof StmtParameter) {
                            catalog_param = (StmtParameter)entry.getSecond();
                            catalog_col = (Column)entry.getFirst();
                        } else {
                            if (trace) LOG.trace("Skipping entry " + entry + " when examing the update information for " + catalog_tbl);
                            continue;
                        }
                        assert(catalog_col != null);
                        assert(catalog_param != null);
                        stmt_cache.put(CatalogKey.createKey(catalog_col), catalog_param.getIndex(), catalog_tbl);
                        found = true;
                    } // FOR
                    if (found && trace) LOG.trace("UpdatePlanNode in " + catalog_stmt + " modifies " + catalog_tbl);
                } // FOR
            } // IF (UPDATE)
        } // FOR (single-partition vs multi-partition)
        
        // Add the Statement cache entry and update the Table xref map
        this.stmt_cache_entries.put(stmt_key, stmt_cache);
        this.addTableCacheXref(stmt_cache, stmt_tables);
    }
    
    /**
     * Update the cache entry xref mapping for a set of tables
     * @param entry
     * @param tables
     */
    private void addTableCacheXref(CacheEntry entry, Collection<Table> tables) {
        for (Table catalog_tbl : tables) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            if (!this.table_cache_xref.containsKey(table_key)) {
                this.table_cache_xref.put(table_key, new HashSet<CacheEntry>());
            }
            this.table_cache_xref.get(table_key).add(entry);
        } // FOR
    }
    
    public Database getDatabase() {
        return catalog_db;
    }
    
    /**
     * Return the hasher used in this estimator instance
     * @return
     */
    public AbstractHasher getHasher() {
        return (this.hasher);
    }
    
    /**
     * Return the partition for the given VoltTableRow
     * @param catalog_tbl
     * @param row
     * @return
     * @throws Exception
     */
    public int getPartition(final Table catalog_tbl, final VoltTableRow row) throws Exception {
        assert(!catalog_tbl.getIsreplicated()) : "Trying to partition replicated table: " + catalog_tbl;
        final boolean debug = LOG.isDebugEnabled();
        if (debug) LOG.debug("Calculating partition for VoltTableRow from " + catalog_tbl);
        
        int partition = -1;
        Column catalog_col = catalog_tbl.getPartitioncolumn();
        assert(catalog_col != null) : "Null partition column: " + catalog_tbl;

        // Multi-Column Partitioning
        if (catalog_col instanceof MultiColumn) {
            MultiColumn mc = (MultiColumn)catalog_col;
            if (debug) LOG.debug(catalog_tbl.getName() + " MultiColumn: " + mc);
            
            Object values[] = new Object[mc.size()];
            for (int i = 0; i < values.length; i++) {
                Column inner = mc.get(i);
                VoltType type = VoltType.get(inner.getType()); 
                values[i] = row.get(inner.getIndex(), type);
            } // FOR
            partition = this.hasher.multiValueHash(values);
            
        // Single-Column Partitioning
        } else {
            if (debug) LOG.debug(catalog_tbl.getName() + " SingleColumn: " + catalog_col);
            
            VoltType type = VoltType.get(catalog_col.getType()); 
            Object value = row.get(catalog_col.getIndex(), type);
            partition = this.hasher.hash(value, catalog_col);
        }
        assert(partition >= 0) : "Invalid partition for " + catalog_tbl; 
        return (partition);
    }
    
    /**
     * Returns the target partition for a StoredProcedureInvocation instance
     * @param invocation
     * @return
     * @throws Exception
     */
    public int getBasePartition(StoredProcedureInvocation invocation) throws Exception {
        Procedure catalog_proc = this.catalog_db.getProcedures().get(invocation.getProcName());
        return (this.getBasePartition(catalog_proc, invocation.getParams().toArray()));
    }
    
    /**
     * Returns the target partition for a stored procedure invocation
     * @param catalog_proc
     * @param params
     * @return
     * @throws Exception
     */
    public Integer getBasePartition(final Procedure catalog_proc, Object params[]) throws Exception {
        return (this.getBasePartition(catalog_proc, params, false));
    }
    
    /**
     * 
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public Integer getBasePartition(final TransactionTrace txn_trace) throws Exception {
        return (this.getBasePartition(txn_trace.getCatalogItem(this.catalog_db), txn_trace.getParams(), true));
    }
    
    public Integer getBasePartition(final Procedure catalog_proc, Object params[], boolean force) throws Exception {
        assert(catalog_proc != null);
        final int partition_param_idx = catalog_proc.getPartitionparameter();
        final boolean debug = LOG.isDebugEnabled();
        
        if (catalog_proc.getParameters().isEmpty()) {
            if (debug) LOG.debug(catalog_proc + " has no parameters. Executing as multi-partition");
            return (null);
        } else if (!force && partition_param_idx == NullProcParameter.PARAM_IDX) {
            if (debug) LOG.debug(catalog_proc + " does not have a pre-defined partition parameter. Executing as multi-partition");
            return (null);
//        } else if (!force && !catalog_proc.getSinglepartition()) {
//            if (debug) LOG.debug(catalog_proc + " is not marked as single-partitioned. Executing as multi-partition");
//            return (null);
        }
        assert(partition_param_idx >= 0) : "Invalid Partitioning ProcParameter #" + partition_param_idx;
        assert(partition_param_idx < catalog_proc.getParameters().size()): "Invalid Partitioning ProcParameter #" + partition_param_idx + " [ProcParmaeters=" + catalog_proc.getParameters().size() + "]";
        ProcParameter catalog_param = catalog_proc.getParameters().get(partition_param_idx);
        int partition = -1;
        
        // Special Case: MultiProcParameter
        if (catalog_param instanceof MultiProcParameter) {
            MultiProcParameter mpp = (MultiProcParameter)catalog_param;
            if (debug) LOG.debug(catalog_proc.getName() + " MultiProcParameter: " + mpp);
            int hashes[] = new int[mpp.size()];
            for (int i = 0; i < hashes.length; i++) {
                int mpp_param_idx = mpp.get(i).getIndex();
                assert(mpp_param_idx >= 0) : "Invalid Partitioning MultiProcParameter #" + mpp_param_idx;
                assert(mpp_param_idx < params.length) : CatalogUtil.getDisplayName(mpp) + " < " + params.length;
                hashes[i] = this.calculatePartition(catalog_proc, params[mpp_param_idx]);
                if (debug) LOG.debug(mpp.get(i) + " value[" + params[mpp_param_idx] + "] => hash[" + hashes[i] + "]");
            } // FOR
            partition = this.hasher.multiValueHash(hashes);
            if (debug) LOG.debug(Arrays.toString(hashes) + " => " + partition);
        // Single ProcParameter
        } else {
            partition = this.calculatePartition(catalog_proc, params[partition_param_idx]);
        }
        return (partition);
    }

    /**
     * 
     * @param xact
     * @return
     * @throws Exception
     */
    public Set<Integer> getAllPartitions(final TransactionTrace xact) throws Exception {
        Set<Integer> partitions = new HashSet<Integer>();
        int base_partition = this.getBasePartition(xact.getCatalogItem(this.catalog_db), xact.getParams(), true);
        partitions.add(base_partition);
        
        for (QueryTrace query : xact.getQueries()) {
            partitions.addAll(this.getPartitions(query, base_partition));
        } // FOR
        
        return (partitions);
    }
    
    /**
     * Return the list of partitions that this QueryTrace object will touch
     * @param query
     * @return
     */
    public Set<Integer> getPartitions(final QueryTrace query, Integer base_partition) throws Exception {
        return (this.getPartitions(query.getCatalogItem(this.catalog_db), query.getParams(), base_partition));
    }
    
    /**
     * Return the table -> partitions mapping for the given QueryTrace object
     * @param query
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, Set<Integer>> getTablePartitions(final QueryTrace query, Integer base_partition) throws Exception {
        return (this.getTablePartitions(query.getCatalogItem(this.catalog_db), query.getParams(), base_partition));
    }
    
    /**
     * 
     * @param catalog_stmt
     * @param params
     * @return
     */
    public Set<Integer> getPartitions(final Statement catalog_stmt, Object params[], Integer base_partition) throws Exception {
        // Loop through this Statement's plan fragments and get the partitions
        // Note that we will use the single-sited fragments (if available) since they will be
        // faster for us to figure out what partitions has the data that this statement needs 
        Set<Integer> partitions = new HashSet<Integer>();
        CatalogMap<PlanFragment> fragments = (catalog_stmt.getHas_singlesited() ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
        for (PlanFragment catalog_frag : fragments) {
            partitions.addAll(this.getPartitions(catalog_frag, params, base_partition));
        } // FOR
        return (partitions);
    }

    /**
     * Return all of the partitions per table for the given Statement object
     * @param catalog_stmt
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, Set<Integer>> getTablePartitions(final Statement catalog_stmt, Object params[], Integer base_partition) throws Exception {
        Map<String, Set<Integer>> all_partitions = new HashMap<String, Set<Integer>>();
        CatalogMap<PlanFragment> fragments = (catalog_stmt.getHas_singlesited() ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
        for (PlanFragment catalog_frag : fragments) {
            try {
                Map<String, Set<Integer>> frag_partitions = this.getTablePartitions(catalog_frag, params, base_partition);
                for (String table_key : frag_partitions.keySet()) {
                    if (!all_partitions.containsKey(table_key)) {
                        all_partitions.put(table_key, frag_partitions.get(table_key));
                    } else {
                        all_partitions.get(table_key).addAll(frag_partitions.get(table_key));
                    }
                } // FOR
            } catch (AssertionError ex) {
                LOG.fatal("Failed to calculate table partitions for " + CatalogUtil.getDisplayName(catalog_frag));
                throw new RuntimeException(ex);
            }
        } // FOR
        return (all_partitions);
    }
    
    /**
     * Return the list partitions that this fragment needs to be sent to based on the parameters
     * @param catalog_frag
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Set<Integer> getPartitions(final PlanFragment catalog_frag, Object params[], Integer base_partition) throws Exception {
        Map<String, Set<Integer>> partitions = this.getTablePartitions(catalog_frag, params, base_partition);
        Set<Integer> ret = new HashSet<Integer>();
        int max_partitions = this.all_partitions.size();
        for (Set<Integer> table_partitions : partitions.values()) {
            ret.addAll(table_partitions);
            if (ret.size() == max_partitions) break;
        }
        return (ret);
    }    
        
    /**
     * 
     * @param catalog_frag
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, Set<Integer>> getTablePartitions(final PlanFragment catalog_frag, Object params[], Integer base_partition) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        if (trace) LOG.trace("Estimating partitions for PlanFragment #" + catalog_frag.getName());
        String frag_key = CatalogKey.createKey(catalog_frag);
        Statement catalog_stmt = (Statement)catalog_frag.getParent();
        
        // Check whether we have generate the cache entries for this Statement
        // The CacheEntry object just tells us what input parameter to use for hashing
        // to figure out where we need to go for each table.
        PartitionEstimator.CacheEntry cache_entry = null;
        synchronized (this) {
            cache_entry = this.frag_cache_entries.get(frag_key);
            if (cache_entry == null) {
                this.generateCache(catalog_stmt);
                cache_entry = this.frag_cache_entries.get(frag_key);
            }
        }
        assert(cache_entry != null);
        
        if (trace) LOG.trace("Getting for PlanFragment #" + catalog_frag.getName());
        Map<String, Set<Integer>> partitions = this.getTablePartitions(cache_entry, params, base_partition);
        if (debug) LOG.debug("PlanFragment #" + catalog_frag.getName() + ": " + partitions);
        return (partitions);
    }
    
    /**
     * 
     * @param cache_entry
     * @param params
     * @param base_partition
     * @return
     */
    protected Map<String, Set<Integer>> getTablePartitions(PartitionEstimator.CacheEntry cache_entry, Object params[], Integer base_partition) {
        // Hash the input parameters to determine what partitions we're headed to
        Map<String, Set<Integer>> partitions = new HashMap<String, Set<Integer>>();
        final int max_partitions = this.all_partitions.size();
        QueryType stmt_type = cache_entry.query_type;
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();

        // Go through each table referenced in this CacheEntry and look-up the parameters that the partitioning
        // columns are referenced against to determine what partitions we need to go to
        // IMPORTANT: If there are no tables (meaning it's some PlanFragment that combines data output 
        //            from other PlanFragments), then won't return anything because it is up to whoever 
        //            to figure out where to send this PlanFragment (it may be at the coordinator)
        for (Table catalog_tbl : cache_entry.getTables()) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            Set<Integer> table_partitions = new HashSet<Integer>();

            // Easy Case: If this table is replicated and this query is a scan, then
            // we're in the clear and there's nothing else we need to do here for the
            // current table (but we still need to check the other guys).
            // Conversely, if it's replicated but we're performing an update or a 
            // delete, then we know it's not single-sited.
            if (catalog_tbl.getIsreplicated()) {
                if (stmt_type == QueryType.SELECT) {
                    if (trace) LOG.trace("Cache entry " + cache_entry + " will execute on the local partition");
                    if (base_partition != null) table_partitions.add(base_partition);
                } else if (stmt_type == QueryType.INSERT || stmt_type == QueryType.UPDATE || stmt_type == QueryType.DELETE) {
                    if (trace) LOG.trace("Cache entry " + cache_entry + " must be broadcast to all partitions");
                    table_partitions.addAll(this.all_partitions);
                } else {
                    assert(false) : "Unexpected query type: " + stmt_type;
                }
            // Otherwise calculate the partition value based on this table's partitioning column 
            } else {
                // Grab the parameter mapping for this column
                Column catalog_col = catalog_tbl.getPartitioncolumn();
                String column_key = CatalogKey.createKey(catalog_col);
                if (trace) LOG.trace("Partitioning Column: " + CatalogUtil.getDisplayName(catalog_col));
                
                // Special Case: Multi-Column Partitioning
                // Strap on your seatbelts, we're going in!!!
                if (catalog_col instanceof MultiColumn) {
                    
                    // HACK: All multi-column look-ups on queries with an OR must be broadcast
                    if (cache_entry.isContainsOr()) {
                        if (debug) LOG.warn("Trying to use multi-column partitioning [" + catalog_col + "] on query that contains an 'OR': " + cache_entry);
                        table_partitions.addAll(this.all_partitions);
                    } else {
                        MultiColumn mc = (MultiColumn)catalog_col;
                        @SuppressWarnings("unchecked")
                        HashSet<Integer> mc_partitions[] = new HashSet[]{ new HashSet<Integer>(), new HashSet<Integer>() };
                        
                        if (trace) LOG.trace("Calculating columns for multi-partition colunmn: " + mc);
                        boolean is_valid = true;
                        for (int i = 0; i < 2; i++) {
                            Column mc_column = mc.get(i);
                            String mc_column_key = CatalogKey.createKey(mc_column);
                            // assert(cache_entry.get(mc_column_key) != null) : "Null CacheEntry: " + mc_column_key;
                            if (cache_entry.get(mc_column_key) != null) {
                                this.calculatePartitions(mc_partitions[i], params, cache_entry.get(mc_column_key), mc_column);
                            }
                            
                            // Unless we have partition values for both keys, then it has to be a broadcast 
                            if (mc_partitions[i].isEmpty()) {
                                if (debug) LOG.warn("No partitions for " + mc_column + " from " + mc + ". Cache entry " + cache_entry + " must be broadcast to all partitions");
                                table_partitions.addAll(this.all_partitions);
                                is_valid = false;
                                break;
                            }
                            if (trace) LOG.trace(CatalogUtil.getDisplayName(mc_column) + ": " + mc_partitions[i]);
                        } // FOR
                        
                        // Now if we're here, then we have partitions for both of the columns and we're legit
                        // We therefore just need to take the cross product of the two sets and hash them together
                        if (is_valid) {
                            for (int part0 : mc_partitions[0]) {
                                for (int part1 : mc_partitions[1]) {
                                    int partition = this.hasher.multiValueHash(part0, part1);
                                    table_partitions.add(partition);
                                    if (trace) LOG.trace("MultiColumn Partitions[" + part0 + ", " + part1 + "] => " + partition);
                                } // FOR
                            } // FOR
                        }
                    }
                } else {
                    Set<Integer> param_idxs = cache_entry.get(column_key);
                    if (trace) LOG.trace("Param Indexes: " + param_idxs);
    
                    // Important: If there is no entry for this partitioning column, then we have to broadcast this mofo
                    if (param_idxs == null || param_idxs.isEmpty()) {
                        if (debug) LOG.debug("No parameter mapping for " + CatalogUtil.getDisplayName(catalog_col) + ". " +
                                             "Fragment must be broadcast to all partitions");
                        table_partitions.addAll(this.all_partitions);
    
                    // If there is nothing special, just shove off and have this method figure things out for us
                    } else {
                        if (trace) LOG.trace("Calculating partitions normally for " + cache_entry);
                        this.calculatePartitions(table_partitions, params, param_idxs, catalog_col);
                    }
                }
            } // ELSE
            assert(table_partitions.size() <= max_partitions);
            partitions.put(table_key, table_partitions);
        } // FOR
        return (partitions);
    }
    
    /**
     * Calculate the partitions touched for the given column
     * @param partitions
     * @param params
     * @param param_idxs
     * @param catalog_col
     */
    private void calculatePartitions(Set<Integer> partitions, Object params[], Set<Integer> param_idxs, Column catalog_col) {
        final boolean trace = LOG.isTraceEnabled();
        
        // Note that we have to go through all of the mappings from the partitioning column
        // to parameters. This can occur when the partitioning column is referenced multiple times
        for (Integer param_idx : param_idxs) {
            // IMPORTANT: Check if the parameter is an array. If it is, then we have to 
            // loop through and get the hash of all of the values
            if (ClassUtil.isArray(params[param_idx])) {
                int num_elements = Array.getLength(params[param_idx]);
                if (trace) LOG.trace("Parameter #" + param_idx + " is an array. Calculating multiple partitions...");
                for (int i = 0; i < num_elements; i++) {
                    Object value = Array.get(params[param_idx], i);
                    int partition_id = this.hasher.hash(value, catalog_col);
                    if (trace) LOG.trace(CatalogUtil.getDisplayName(catalog_col) + " HASHING PARAM ARRAY[" + param_idx + "][" + i + "]: " + value + " -> " + partition_id);
                    partitions.add(partition_id);
                } // FOR
            // Primitive
            } else {
                int partition_id = this.hasher.hash(params[param_idx], catalog_col);    
                if (trace) LOG.trace(CatalogUtil.getDisplayName(catalog_col) + " HASHING PARAM[" + param_idx + "]: " + params[param_idx] + " -> " + partition_id);
                partitions.add(partition_id);                
            }
        } // FOR
    }
    
    /**
     * Return the partition for a given procedure's parameter value
     * @param catalog_proc
     * @param partition_param_val
     * @return
     * @throws Exception
     */
    private int calculatePartition(Procedure catalog_proc, Object partition_param_val) throws Exception {
        final boolean debug = LOG.isDebugEnabled();
        
        // If the parameter is an array, then just use the first value
        if (ClassUtil.isArray(partition_param_val)) {
            int num_elements = Array.getLength(partition_param_val);
            if (num_elements == 0) {
                if (debug) LOG.debug("Empty partitioning parameter array for " + catalog_proc);
                partition_param_val = rand.nextInt();
            } else {
                partition_param_val = Array.get(partition_param_val, 0);
            }
        } else if (partition_param_val == null) {
            if (debug) LOG.warn("Null ProcParameter value: " + catalog_proc);
            partition_param_val = rand.nextInt();
        }
        return (this.hasher.hash(partition_param_val, catalog_proc));
    }

    /**
     * Debug output
     */
    @Override
    public String toString() {
        String ret = "";
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            StringBuilder sb = new StringBuilder();
            boolean has_entries = false;
            sb.append(CatalogUtil.getDisplayName(catalog_proc)).append(":\n");
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                String stmt_key = CatalogKey.createKey(catalog_stmt);
                CacheEntry stmt_cache = this.stmt_cache_entries.get(stmt_key);
                if (stmt_cache == null) continue;
                has_entries = true;
                sb.append("  " + catalog_stmt.getName() + ": ").append(stmt_cache).append("\n");
                
                for (PlanFragment catalog_frag : CatalogUtil.getAllPlanFragments(catalog_stmt)) {
                    String frag_key = CatalogKey.createKey(catalog_frag);
                    CacheEntry frag_cache = this.frag_cache_entries.get(frag_key);
                    if (frag_cache == null) continue;
                    sb.append("    PlanFragment[" + catalog_frag.getName() + "]: ").append(frag_cache).append("\n");
                }
            } // FOR
            if (has_entries) ret += sb.toString() + StringUtil.SINGLE_LINE;
        } // FOR
        return (ret);
    }
    
    /**
     * 
     * @param column_joins
     */
    protected static void populateColumnJoins(final Map<Column, Set<Column>> column_joins) {
        int orig_size = 0;
        for (Set<Column> cols : column_joins.values()) {
            orig_size += cols.size();
        }
        // First we have to take the Cartesian product of all mapped joins
        for (Column c0 : column_joins.keySet()) {
            // For each column that c0 is joined with, add a reference to c0 for all the columns 
            // that the other column references
            for (Column c1 : column_joins.get(c0)) {
                assert(!c1.equals(c0));
                for (Column c2 : column_joins.get(c1)) {
                    if (!c0.equals(c2)) column_joins.get(c2).add(c0);
                } // FOR
            } // FOR
        } // FOR

        int new_size = 0;
        for (Set<Column> cols : column_joins.values()) {
            new_size += cols.size();
        }
        if (new_size != orig_size) populateColumnJoins(column_joins);
    }
    
    /**
     * Generate a histogram of the base partitions used for all of the transactions in the workload
     * @param workload
     * @return
     * @throws Exception
     */
    public Histogram buildBasePartitionHistogram(Workload workload) throws Exception {
        final Histogram h = new Histogram();
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            int base_partition = this.getBasePartition(txn_trace);
            h.put(base_partition);
        } // FOR
        return (h);
    }
}