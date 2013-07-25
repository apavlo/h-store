/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ConstantValue;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogPair;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.RandomProcParameter;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.pools.FastObjectPool;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * This class is used to calculate what partitions various operations will need to execute on.
 * For example, this is used to figure out what partitions each query invocation will need to
 * touch at runtime.
 * <B>NOTE:</B> These are deterministic calculations. We call it an "estimator" because
 * we can get the partitions touched by an operation without actually running a txn.
 * or executing a query. 
 * @author pavlo
 */
public class PartitionEstimator {
    private static final Logger LOG = Logger.getLogger(PartitionEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    private CatalogContext catalogContext;
    private final AbstractHasher hasher;
    private PartitionSet all_partitions = new PartitionSet();
    private int num_partitions;

    private final Map<Procedure, ProcParameter> cache_procPartitionParameters = new HashMap<Procedure, ProcParameter>();
    private final Map<Table, Column> cache_tablePartitionColumns = new HashMap<Table, Column>();
    
    /**
     * Statement -> StmtParameter Offsets
     */
    private final Map<Statement, int[]> cache_stmtPartitionParameters = new HashMap<Statement, int[]>();

    /**
     * PlanFragment Key -> CacheEntry(Column Key -> StmtParameter Indexes)
     */
    private final Map<String, CacheEntry> cache_fragmentEntries = new HashMap<String, CacheEntry>();

    /**
     * Statement Key -> CacheEntry(Column Key -> StmtParam Indexes)
     */
    private final Map<String, CacheEntry> cache_statementEntries = new HashMap<String, CacheEntry>();

    /**
     * Table Key -> All cache entries for Statements that reference Table
     */
    private final Map<String, Set<CacheEntry>> table_cache_xref = new HashMap<String, Set<CacheEntry>>();

    /**
     * CacheEntry ColumnKey -> Parameter List
     * The parameters could be either StmtParameters or ConstantValues 
     */
    private final class CacheEntry {
        private final QueryType query_type;
        private boolean contains_or = false;
        
        private final Map<Column, List<Pair<ExpressionType, CatalogType>>> predicates = new HashMap<Column, List<Pair<ExpressionType,CatalogType>>>();
        private final Collection<String> table_keys = new HashSet<String>();
        private final Collection<String> broadcast_tables = new HashSet<String>();

        /**
         * The array of Table objects for the table_keys.
         * This is just a fast cache so that we don't have to use an iterable and
         * so that we can re-use the PartitionEstimator's cache when the catalog changes.
         */
        private transient Table tables[];
        
        /**
         * Whether the table in the tables array is replicated
         */
        private transient boolean is_replicated[];
        private transient boolean is_array[]; // parameters
        private transient boolean is_valid = true;
        private transient boolean cache_valid = false;

        public CacheEntry(QueryType query_type) {
            this.query_type = query_type;
        }

        /**
         * 
         * @param key
         * @param param
         * @param expType
         * @param catalog_tbl
         */
        public void put(Column key, CatalogType param, ExpressionType expType, Table catalog_tbl) {
            assert(param instanceof StmtParameter || param instanceof ConstantValue);
            List<Pair<ExpressionType, CatalogType>> params = this.predicates.get(key);
            if (params == null) {
                params = new ArrayList<Pair<ExpressionType, CatalogType>>();
                this.predicates.put(key, params);
            }
            params.add(Pair.of(expType, param));
            this.table_keys.add(CatalogKey.createKey(catalog_tbl));
        }

        public void markContainsOR(boolean flag) {
            this.contains_or = flag;
        }

        public boolean isMarkedContainsOR() {
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
        public void markAsBroadcast(Collection<Table> catalog_tbls) {
            for (Table catalog_tbl : catalog_tbls) {
                this.markAsBroadcast(catalog_tbl);
            }
        }
        
        public boolean hasBroadcast() {
            return (this.broadcast_tables.isEmpty() == false);
        }

        /**
         * Get all of the tables referenced in this CacheEntry
         * @return
         */
        public Table[] getTables() {
            if (this.cache_valid == false) {
                // We have to update the cache set if don't have all of the
                // entries we need or the catalog has changed
                synchronized (this) {
                    if (this.cache_valid == false) {
                        if (trace.val)
                            LOG.trace("Generating list of tables used by cache entry");
                        
                        this.tables = new Table[this.table_keys.size()];
                        this.is_replicated = new boolean[this.tables.length];
                        int i = 0;
                        for (String table_key : this.table_keys) {
                            Table catalog_tbl = CatalogKey.getFromKey(catalogContext.database, table_key, Table.class);
                            this.tables[i] = catalog_tbl;
                            this.is_replicated[i++] = catalog_tbl.getIsreplicated();
                        } // FOR
                    }
                    this.cache_valid = true;
                } // SYNCH
            }
            return (this.tables);
        }

        public boolean hasTable(Table catalog_tbl) {
            return (this.table_keys.contains(CatalogKey.createKey(catalog_tbl)));
        }
        public void setValid() {
            this.is_valid = true;
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
              .append("Predicates=" + this.predicates + "]");
            return (sb.toString());
        }
    }; // END CLASS

    /**
     * PartitionSet pool used by calculatePartitionsForCache
     */
    private final FastObjectPool<PartitionSet> partitionSetPool = new FastObjectPool<PartitionSet>(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            return (new PartitionSet());
        }

        public void passivateObject(Object obj) throws Exception {
            ((PartitionSet)obj).clear();
        };
    }, 100);

    /**
     * PartitionSet[4] pool used by calculatePartitionsForCache.
     * This really is only needed for MultiColumn partitioning columns.
     */
    private final FastObjectPool<PartitionSet[]> mcPartitionSetPool = new FastObjectPool<PartitionSet[]>(new BasePoolableObjectFactory() {
        @Override
        public Object makeObject() throws Exception {
            // XXX: Why is this hardcoded at 4 elements?
            return (new PartitionSet[] {
                        new PartitionSet(),
                        new PartitionSet(),
                        new PartitionSet(),
                        new PartitionSet()
             });
        }

        public void passivateObject(Object obj) throws Exception {
            PartitionSet sets[] = (PartitionSet[])obj;
            for (PartitionSet s : sets) s.clear();
        };
    }, 1000);

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Convenience constructor that uses DefaultHasher
     */
    public PartitionEstimator(CatalogContext catalogContext) {
        this(catalogContext, new DefaultHasher(catalogContext, catalogContext.numberOfPartitions));
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public PartitionEstimator(CatalogContext catalogContext, AbstractHasher hasher) {
        this.catalogContext = catalogContext;
        this.hasher = hasher;
        this.initCatalog(catalogContext);
        
        if (trace.val)
            LOG.trace("Created a new PartitionEstimator with a " + hasher.getClass() + " hasher!");
    }

    // ----------------------------------------------------------------------------
    // BASE DATA MEMBERS METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the current CatalogContext used in this instance
     * @return
     */
    public CatalogContext getCatalogContext() {
        return (this.catalogContext);
    }
    
    /**
     * Return the hasher used in this instance
     * @return
     */
    public AbstractHasher getHasher() {
        return (this.hasher);
    }

    /**
     * Initialize a new catalog for this PartitionEstimator
     * @param new_catalog_db
     */
    public void initCatalog(CatalogContext newCatalogContext) {
        // Check whether any of our cache partition columns have changed
        // In which cache we know need to invalidate our cache entries
        this.catalogContext = newCatalogContext;
        this.hasher.init(catalogContext);
        this.clear();
        this.buildCatalogCache();
    }

    private synchronized void buildCatalogCache() {
        for (Procedure catalog_proc : this.catalogContext.database.getProcedures()) {
            if (catalog_proc.getParameters().size() > 0) {
                ProcParameter catalog_param = null;
                int param_idx = catalog_proc.getPartitionparameter();
                if (param_idx == NullProcParameter.PARAM_IDX || catalog_proc.getParameters().isEmpty()) {
                    catalog_param = NullProcParameter.singleton(catalog_proc);
                } else if (param_idx == RandomProcParameter.PARAM_IDX) {
                    catalog_param = RandomProcParameter.singleton(catalog_proc);
                } else {
                    catalog_param = catalog_proc.getParameters().get(param_idx);
                }
                this.cache_procPartitionParameters.put(catalog_proc, catalog_param);
                if (debug.val)
                    LOG.debug(catalog_proc + " ProcParameter Cache: " + (catalog_param != null ? catalog_param.fullName() : catalog_param));
            }
        } // FOR

        for (Table catalog_tbl : this.catalogContext.database.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            Column catalog_col = catalog_tbl.getPartitioncolumn();
            
            // Use the underlying partitioning column for views
            if (catalog_tbl.getMaterializer() != null) {
                catalog_col = catalog_tbl.getMaterializer().getPartitioncolumn();
            }
            
            if (catalog_col instanceof VerticalPartitionColumn) {
                catalog_col = ((VerticalPartitionColumn) catalog_col).getHorizontalColumn();
                assert ((catalog_col instanceof VerticalPartitionColumn) == false) : catalog_col;
            }
            this.cache_tablePartitionColumns.put(catalog_tbl, catalog_col);
            if (debug.val)
                LOG.debug(String.format("%s Partition Column Cache: %s", catalog_tbl.getName(), catalog_col));
        } // FOR
        for (CacheEntry entry : this.cache_fragmentEntries.values()) {
            entry.cache_valid = false;
        }
        for (CacheEntry entry : this.cache_statementEntries.values()) {
            entry.cache_valid = false;
        }

        // Generate a list of all the partition ids, so that we can quickly
        // add them to the output when estimating later on
        if (this.all_partitions.size() != this.hasher.getNumPartitions()) {
            this.all_partitions = this.catalogContext.getAllPartitionIds();
            this.num_partitions = this.all_partitions.size();
            assert (this.hasher.getNumPartitions() == this.num_partitions);
            if (debug.val)
                LOG.debug(String.format("Initialized PartitionEstimator with %d partitions using the %s hasher", this.num_partitions, this.hasher.getClass().getSimpleName()));
        }
    }

    /**
     * Completely clear the PartitionEstimator's internal cache This should only
     * really be used for testing
     */
    public void clear() {
        this.cache_procPartitionParameters.clear();
        this.cache_tablePartitionColumns.clear();
        this.cache_fragmentEntries.clear();
        this.cache_statementEntries.clear();
        this.cache_stmtPartitionParameters.clear();
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL CACHE METHODS
    // ----------------------------------------------------------------------------

    /**
     * This is the method that actually picks the Statement apart and figures
     * out where the columns and parameters are used together. This is probably the most
     * important method in the entire code base, so tread lightly in here...
     * 
     * @param catalog_stmt
     * @throws Exception
     */
    private synchronized void generateCache(final Statement catalog_stmt) throws Exception {
        // Check whether we already have a CacheEntry for the Statement that we
        // can reuse
        String stmt_key = CatalogKey.createKey(catalog_stmt);
        QueryType stmt_type = QueryType.get(catalog_stmt.getQuerytype());
        PartitionEstimator.CacheEntry stmt_cache = this.cache_statementEntries.get(stmt_key);
        if (stmt_cache == null) {
            stmt_cache = new PartitionEstimator.CacheEntry(stmt_type);
        } else {
            // assert(stmt_cache.isValid()) :
            // "Unexpected invalid cache entry for " +
            // CatalogUtil.getDisplayName(catalog_stmt);
            stmt_cache.setValid();
        }
        Collection<Table> stmt_tables = CatalogUtil.getReferencedTables(catalog_stmt);
        if (debug.val)
            LOG.debug("Generating partitioning cache for " + catalog_stmt);

        // IMPORTANT: Work through the fragments in reverse so that we go from
        // the bottom of the tree up. 
        // We are assuming that we can get the StmtParameter->Column mapping that we need 
        // from either the multi-partition plan or the single-partition plan and that the mapping 
        // will be the same in both cases. Therefore, we don't need to differentiate whether we
        // are picking apart one or the other, nor do we need to switch to a different cache entry
        // for the Statement if we realize that we are going to be single-partition or not. 
        // We have to go through all of the fragments because we don't know which set 
        // the system will be calling at runtime.
        CatalogMap<?> fragment_sets[] = new CatalogMap<?>[] { catalog_stmt.getFragments(), catalog_stmt.getMs_fragments(), };
        for (int i = 0; i < fragment_sets.length; i++) {
            if (fragment_sets[i] == null || fragment_sets[i].isEmpty())
                continue;
            @SuppressWarnings("unchecked")
            CatalogMap<PlanFragment> fragments = (CatalogMap<PlanFragment>) fragment_sets[i];
            boolean singlesited = (i == 0);
            if (trace.val)
                LOG.trace("Analyzing " + fragments.size() + " " + (singlesited ? "single" : "multi") + "-sited fragments for " + catalog_stmt.fullName());

            // Examine each fragment and pick apart how the tables are referenced
            // The order doesn't matter here
            for (PlanFragment catalog_frag : fragments) {
                // Again, always check whether we already have a CacheEntry for
                // the PlanFragment that we can reuse
                String frag_key = CatalogKey.createKey(catalog_frag);
                PartitionEstimator.CacheEntry frag_cache = this.cache_fragmentEntries.get(frag_key);
                if (frag_cache == null) {
                    frag_cache = new PartitionEstimator.CacheEntry(stmt_type);
                } else if (frag_cache.isValid()) {
                    assert (!frag_cache.isValid()) : "Cache entry for " + CatalogUtil.getDisplayName(catalog_frag) + " is marked as valid when we were expecting to be invalid\n" + this.toString();
                    frag_cache.setValid();
                }

                AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
                Collection<Table> frag_tables = CatalogUtil.getReferencedTablesForTree(catalogContext.database, root);
                // Table tables_arr[] = new Table[frag_tables.size()];
                // tables_arr = frag_tables.toArray(tables_arr);
                // assert (tables_arr.length == frag_tables.size());
                if (trace.val)
                    LOG.trace("Analyzing " + catalog_frag.fullName());

                // Check whether the predicate expression in this PlanFragment contains an OR
                // We need to know this if we get hit with Multi-Column Partitioning
                // XXX: Why does this matter??
                Collection<ExpressionType> exp_types = PlanNodeUtil.getScanExpressionTypes(root);
                if (exp_types.contains(ExpressionType.CONJUNCTION_OR)) {
                    if (debug.val)
                        LOG.warn(String.format("%s contains %s. Cannot be used with multi-column partitioning",
                                 catalog_frag.fullName(), ExpressionType.CONJUNCTION_OR));
                    stmt_cache.markContainsOR(true);
                    frag_cache.markContainsOR(true);
                }

                // If there are no tables, then we need to double check that the "non-transactional"
                // flag is set for the fragment. This means that this fragment does not operate directly
                // on a persistent table in the database. We'll add an entry in the cache using our
                // special "no tables" flag. This means that the fragment needs to be executed locally.
                if (frag_tables.isEmpty()) {
                    String msg = catalog_frag.fullName() + " does not reference any tables";
                    if (!catalog_frag.getNontransactional()) {
                        LOG.warn(catalog_stmt.fullName() + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false)));
                        for (PlanFragment f : fragments) {
                            LOG.warn("singlePartiton=" + singlesited + " - " + f.fullName() + "\n" + PlanNodeUtil.debug(PlanNodeUtil.getPlanNodeTreeForPlanFragment(f)));
                        }
                        throw new Exception(msg + " but the non-transactional flag is not set");
                    }
                    if (trace.val)
                        LOG.trace(msg);
                }
                if (trace.val && frag_tables.isEmpty() == false )
                    LOG.trace("Fragment Tables: " + frag_tables);

                // We only need to find where the partition column is referenced
                // If it's not in there, then this query has to be broadcasted to all nodes
                // Note that we pass all the tables that are part of the fragment, since 
                // we need to be able to handle joins
                PredicatePairs predicates = CatalogUtil.extractFragmentPredicates(catalog_frag, false, frag_tables);
                assert (predicates != null);
                Map<Column, Set<Column>> column_joins = new TreeMap<Column, Set<Column>>();
                if (trace.val)
                    LOG.trace("Extracted PredicatePairs for " + frag_tables + ":\n" + predicates.debug());

                // -------------------------------
                // If there are no columns, then this fragment is doing a full table scan
                // -------------------------------
                if (predicates.isEmpty() && frag_tables.size() > 0) {
                    if (trace.val)
                        LOG.trace("No columns accessed in " + catalog_frag + " despite reading " + frag_tables.size() + " tables");
                    stmt_cache.markAsBroadcast(frag_tables);
                    frag_cache.markAsBroadcast(frag_tables);
                }
                // -------------------------------
                // Fragment references the columns for our tables. Pick them apart!
                // -------------------------------
                else {
                    // First go through all the entries and add any mappings from
                    // Columns to StmtParameters to our stmt_cache
                    for (CatalogPair pair : predicates) {
                        if (trace.val)
                            LOG.trace(String.format("Examining extracted %s: %s",
                                      pair.getClass().getSimpleName(), pair));

                        // Column = Column
                        if (pair.getFirst() instanceof Column && pair.getSecond() instanceof Column) {
                            Column col0 = (Column) pair.getFirst();
                            Column col1 = (Column) pair.getSecond();
                            
                            // If this table is a view, then we need to check whether 
                            // we have to point the column down to the origin column
                            if (col0.getMatviewsource() != null) {
                                col0 = col0.getMatviewsource();
                            }
                            if (col1.getMatviewsource() != null) {
                                col1 = col1.getMatviewsource();
                            }

                            if (!pair.getComparisonExp().equals(ExpressionType.COMPARE_EQUAL)) {
                                if (debug.val)
                                    LOG.warn(String.format("Unsupported non-equality join in %s: %s",
                                             catalog_stmt.fullName(), pair));
                            } else {
                                if (!column_joins.containsKey(col0))
                                    column_joins.put(col0, new TreeSet<Column>());
                                if (!column_joins.containsKey(col1))
                                    column_joins.put(col1, new TreeSet<Column>());
                                column_joins.get(col0).add(col1);
                                column_joins.get(col1).add(col0);
                            }
                            continue;
                        }
                        
                        // Look for predicates with StmtParameters or ConstantValues
                        for (Table catalog_tbl : frag_tables) {
                            Column catalog_col = null;
                            CatalogType catalog_param = null;
                            
                            // *********************************** DEBUG ***********************************
                            if (trace.val) {
                                LOG.trace("Current Table: " + catalog_tbl.hashCode());
                                if (pair.getFirst() != null) {
                                    LOG.trace("entry.getFirst().getParent(): " + (pair.getFirst().getParent() != null ?
                                                    pair.getFirst().getParent().hashCode() :
                                                    pair.getFirst() + " parent is null?"));
                            
                                    if (pair.getFirst().getParent() instanceof Table) {
                                        Table parent = pair.getFirst().getParent();
                                        if (parent.getName().equals(catalog_tbl.getName())) {
                                            assert(parent.equals(catalog_tbl)) :
                                                "Mismatch on " + parent.getName() + "???";
                                        }
                                    }
                                } else {
                                    LOG.trace("entry.getFirst():             " + null);
                                }
                                if (pair.getSecond() != null) {
                                    LOG.trace("entry.getSecond().getParent(): " + (pair.getSecond().getParent() != null ?
                                                  pair.getSecond().getParent().hashCode() :
                                                  pair.getSecond() + " parent is null?"));
                                } else {
                                    LOG.trace("entry.getSecond():             " + null);
                                }
                            }
                            // *********************************** DEBUG ***********************************

                            // Column = (StmtParameter or ConstantValue)
                            if (pair.getFirst().getParent() != null && pair.getFirst().getParent().equals(catalog_tbl) &&
                                    (pair.getSecond() instanceof StmtParameter || pair.getSecond() instanceof ConstantValue) ) {
                                catalog_col = (Column) pair.getFirst();
                                catalog_param = pair.getSecond();
                            
                            } 
                            // (StmtParameter or ConstantValue) = Column
                            else if (pair.getSecond().getParent() != null && pair.getSecond().getParent().equals(catalog_tbl) && 
                                    (pair.getFirst() instanceof StmtParameter || pair.getFirst() instanceof ConstantValue)) {
                                catalog_col = (Column) pair.getSecond();
                                catalog_param = pair.getFirst();
                            }
                            if (catalog_col != null && catalog_param != null) {
                                // If this table is a view, then we need to check whether 
                                // we have to point the column down to the origin column
                                if (catalog_col.getMatviewsource() != null) {
                                    if (debug.val)
                                        LOG.debug("Found View Column: " + catalog_col.fullName() + " -> " + catalog_col.getMatviewsource().fullName());
                                    catalog_col = catalog_col.getMatviewsource();
                                }
                                if (trace.val)
                                    LOG.trace(String.format("[%s] Adding cache entry for %s: %s -> %s",
                                                            CatalogUtil.getDisplayName(catalog_tbl),
                                                            CatalogUtil.getDisplayName(catalog_frag),
                                                            CatalogUtil.getDisplayName(catalog_col),
                                                            CatalogUtil.getDisplayName(catalog_param)));
                                stmt_cache.put(catalog_col, catalog_param, pair.getComparisonExp(), catalog_tbl);
                                frag_cache.put(catalog_col, catalog_param, pair.getComparisonExp(), catalog_tbl);
                            }
                        } // FOR (tables)
                        if (trace.val)
                            LOG.trace("-------------------");
                    } // FOR (entry)

                    // We now have to take a second pass through the column mappings
                    // This will pick-up those columns that are joined together where one of them 
                    // is also referenced with an input parameter. So we will map the input
                    // parameter to the second column as well
                    PartitionEstimator.populateColumnJoinSets(column_joins);

                    for (Column catalog_col : column_joins.keySet()) {
                        // Otherwise, we have to examine the the ColumnSet and
                        // look for any reference to this column
                        if (trace.val)
                            LOG.trace("Trying to find all references to " + CatalogUtil.getDisplayName(catalog_col));
                        for (Column other_col : column_joins.get(catalog_col)) {
                            // IMPORTANT: If the other entry is a column from another table and we don't
                            // have a reference in stmt_cache for ourselves, then we can look to see if 
                            // this guy was used against a StmtParameter some where else in the Statement
                            // If this is the case, then we can substitute that mofo in it's place
                            if (stmt_cache.predicates.containsKey(catalog_col)) {
                                for (Pair<ExpressionType, CatalogType> pair : stmt_cache.predicates.get(catalog_col)) {
                                    if (trace.val)
                                        LOG.trace(String.format("Linking %s to predicate %s because of %s",
                                                  other_col.fullName(), pair, catalog_col.fullName()));
                                    
                                    ExpressionType expType = pair.getFirst(); 
                                    CatalogType param = pair.getSecond();
                                    stmt_cache.put(other_col, param, expType, (Table)other_col.getParent());
                                    frag_cache.put(other_col, param, expType, (Table)other_col.getParent());
                                } // FOR (StmtParameter.Index)
                            }
                        } // FOR (Column)
                    } // FOR (Column)
                }
                if (trace.val)
                    LOG.trace(frag_cache.toString());

                // Loop through all of our tables and make sure that there is an entry in the PlanFragment CacheEntrry
                // If there isn't, then that means there was no predicate on the table and therefore the PlanFragment
                // must be broadcast to all partitions (unless it is replicated)
                for (Table catalog_tbl : frag_tables) {
                    if (!frag_cache.hasTable(catalog_tbl)) {
                        if (trace.val)
                            LOG.trace(String.format("No column predicate for %s. Marking as broadcast for %s: %s",
                                      catalog_tbl.fullName(), catalog_frag.fullName(), frag_cache.getTables()));
                        frag_cache.markAsBroadcast(catalog_tbl);
                        stmt_cache.markAsBroadcast(catalog_tbl);
                    }
                } // FOR

                // Store the Fragment cache and update the Table xref mapping
                this.cache_fragmentEntries.put(frag_key, frag_cache);
                this.addTableCacheXref(frag_cache, frag_tables);
            } // FOR (fragment)

            // Then for updates we need to look to see whether they are updating an attribute 
            // that they are partitioned on. If so, then it gets dicey because we need to
            // know the value...
            if (stmt_type == QueryType.UPDATE) {
                List<Table> tables = new ArrayList<Table>();
                PredicatePairs update_cset = new PredicatePairs();
                for (Table catalog_tbl : CatalogUtil.getReferencedTables(catalog_stmt)) {
                    update_cset.clear();
                    tables.clear();
                    tables.add(catalog_tbl);
                    AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
                    CatalogUtil.extractUpdatePredicates(catalog_stmt, catalogContext.database, update_cset, root_node, true, tables);

                    boolean found = false;
                    for (CatalogPair pair : update_cset) {
                        Column catalog_col = null;
                        CatalogType catalog_param = null;

                        // For now we only care up look-ups using StmtParameters or ConstantValues
                        if (pair.getFirst() instanceof StmtParameter || pair.getFirst() instanceof ConstantValue) {
                            catalog_col = (Column) pair.getSecond();
                            catalog_param = pair.getFirst();
                        }
                        else if (pair.getSecond() instanceof StmtParameter || pair.getSecond() instanceof ConstantValue) {
                            catalog_col = (Column) pair.getFirst();
                            catalog_param = pair.getSecond();
                        }
                        else {
                            if (trace.val)
                                LOG.trace(String.format("Skipping entry %s when examing the update information for %s",
                                          pair, catalog_tbl));
                            continue;
                        }
                        assert (catalog_col != null);
                        assert (catalog_param != null);
                        stmt_cache.put(catalog_col, catalog_param, pair.getComparisonExp(), catalog_tbl);
                        found = true;
                    } // FOR
                    if (trace.val && found)
                        LOG.trace("UpdatePlanNode in " + catalog_stmt.fullName() + " modifies " + catalog_tbl);
                } // FOR
            } // IF (UPDATE)
        } // FOR (single-partition vs multi-partition)

        // Add the Statement cache entry and update the Table xref map
        this.cache_statementEntries.put(stmt_key, stmt_cache);
        this.addTableCacheXref(stmt_cache, stmt_tables);
    }

    /**
     * Update the cache entry xref mapping for a set of tables
     * 
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

    // ----------------------------------------------------------------------------
    // TABLE ROW METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the partition for the given VoltTableRow
     * 
     * @param catalog_tbl
     * @param row
     * @return
     * @throws Exception
     */
    public int getTableRowPartition(final Table catalog_tbl, final VoltTableRow row) throws Exception {
        assert (!catalog_tbl.getIsreplicated()) : "Trying to partition replicated table: " + catalog_tbl;
        if (debug.val)
            LOG.debug("Calculating partition for VoltTableRow from " + catalog_tbl);

        int partition = -1;
        Column catalog_col = this.cache_tablePartitionColumns.get(catalog_tbl);
        assert (catalog_col != null) : "Null partition column: " + catalog_tbl;
        assert ((catalog_col instanceof VerticalPartitionColumn) == false) : "Invalid partitioning column: " + catalog_col.fullName();

        // Multi-Column Partitioning
        if (catalog_col instanceof MultiColumn) {
            MultiColumn mc = (MultiColumn) catalog_col;
            if (debug.val)
                LOG.debug(catalog_tbl.getName() + " MultiColumn: " + mc);

            Object values[] = new Object[mc.size()];
            for (int i = 0; i < values.length; i++) {
                Column inner = mc.get(i);
                VoltType type = VoltType.get(inner.getType());
                values[i] = row.get(inner.getIndex(), type);
            } // FOR
            partition = this.hasher.multiValueHash(values);

            // Single-Column Partitioning
        } else {
            VoltType type = VoltType.get(catalog_col.getType());
            Object value = row.get(catalog_col.getIndex(), type);
            partition = this.hasher.hash(value, catalog_col);
            if (debug.val)
                LOG.debug(String.format("%s SingleColumn: Value=%s / Partition=%d", catalog_col.fullName(), value, partition));
        }
        assert (partition >= 0) : "Invalid partition for " + catalog_tbl;
        return (partition);
    }

    // ----------------------------------------------------------------------------
    // BASE PARTITION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns the target partition for a StoredProcedureInvocation instance
     * @param invocation
     * @return
     * @throws Exception
     */
    public int getBasePartition(StoredProcedureInvocation invocation) throws Exception {
        Procedure catalog_proc = this.catalogContext.database.getProcedures().get(invocation.getProcName());
        if (catalog_proc == null) {
            catalog_proc = this.catalogContext.database.getProcedures().getIgnoreCase(invocation.getProcName());
        }
        assert(catalog_proc != null) :
            "Invalid procedure name '" + invocation.getProcName() + "'";
        return (this.getBasePartition(catalog_proc, invocation.getParams().toArray(), false));
    }

    /**
     * Returns the target partition for a stored procedure + parameters
     * @param catalog_proc
     * @param params
     * @return
     * @throws Exception
     */
    public int getBasePartition(final Procedure catalog_proc, final Object params[]) throws Exception {
        return (this.getBasePartition(catalog_proc, params, false));
    }

    /**
     * Return the target partition for a TransactionTrace
     * @param txn_trace
     * @return
     * @throws Exception
     */
    public int getBasePartition(final TransactionTrace txn_trace) throws Exception {
        if (debug.val)
            LOG.debug("Calculating base partition for " + txn_trace.toString());
        return (this.getBasePartition(txn_trace.getCatalogItem(this.catalogContext.database), txn_trace.getParams(), true));
    }

    /**
     * Main method for calculating the base partition for a stored procedure
     * 
     * @param catalog_proc
     * @param params
     * @param force
     * @return
     * @throws Exception
     */
    public int getBasePartition(final Procedure catalog_proc, final Object params[], boolean force) throws Exception {
        assert(catalog_proc != null);
        assert(params != null);
//        assert(catalog_proc.getParameters().size() == params.length) :
//            String.format("Invalid number of ProcParameters for %s: %d != %d",
//                          catalog_proc, catalog_proc.getParameters().size(), params.length);
        ProcParameter catalog_param = this.cache_procPartitionParameters.get(catalog_proc);

        if (catalog_param == null && force) {
            if (force) {
                int idx = catalog_proc.getPartitionparameter();
                if (idx == NullProcParameter.PARAM_IDX || catalog_proc.getParameters().isEmpty()) {
                    catalog_param = NullProcParameter.singleton(catalog_proc);
                } else if (idx == RandomProcParameter.PARAM_IDX) {
                    catalog_param = RandomProcParameter.singleton(catalog_proc);
                } else {
                    catalog_param = catalog_proc.getParameters().get(idx);
                }
                this.cache_procPartitionParameters.put(catalog_proc, catalog_param);
                if (debug.val)
                    LOG.debug("Added cached " + catalog_param + " for " + catalog_proc);
            } else {
                if (debug.val)
                    LOG.debug(catalog_proc + " has no parameters. No base partition for you!");
                return (HStoreConstants.NULL_PARTITION_ID);
            }
        }

        if (force == false && (catalog_param == null || catalog_param instanceof NullProcParameter)) {
            if (debug.val)
                LOG.debug(catalog_proc + " does not have a pre-defined partition parameter. No base partition!");
            return (HStoreConstants.NULL_PARTITION_ID);
            // } else if (!force && !catalog_proc.getSinglepartition()) {
            // if (debug.val) LOG.debug(catalog_proc +
            // " is not marked as single-partitioned. Executing as multi-partition");
            // return (null);
        }
        int partition = HStoreConstants.NULL_PARTITION_ID;
        boolean is_array = catalog_param.getIsarray();

        // Special Case: RandomProcParameter
        if (catalog_param instanceof RandomProcParameter) {
            partition = RandomProcParameter.rand.nextInt(this.num_partitions);
        }
        // Special Case: MultiProcParameter
        else if (catalog_param instanceof MultiProcParameter) {
            MultiProcParameter mpp = (MultiProcParameter) catalog_param;
            if (debug.val)
                LOG.debug(catalog_proc.getName() + " MultiProcParameter: " + mpp);
            int hashes[] = new int[mpp.size()];
            for (int i = 0; i < hashes.length; i++) {
                int mpp_param_idx = mpp.get(i).getIndex();
                assert (mpp_param_idx >= 0) : "Invalid Partitioning MultiProcParameter #" + mpp_param_idx;
                assert (mpp_param_idx < params.length) : CatalogUtil.getDisplayName(mpp) + " < " + params.length;
                int hash = this.calculatePartition(catalog_proc, params[mpp_param_idx], is_array);
                hashes[i] = (hash == HStoreConstants.NULL_PARTITION_ID ? 0 : hash);
                if (debug.val)
                    LOG.debug(mpp.get(i) + " value[" + params[mpp_param_idx] + "] => hash[" + hashes[i] + "]");
            } // FOR
            partition = this.hasher.multiValueHash(hashes);
            if (debug.val)
                LOG.debug(Arrays.toString(hashes) + " => " + partition);
        }
        // Single ProcParameter
        else {
            if (debug.val)
                LOG.debug("Calculating base partition using " + catalog_param.fullName() + ": " + params[catalog_param.getIndex()]);
            assert(catalog_param.getIndex() >= 0) : "Invalid parameter offset " + catalog_param.fullName();
            partition = this.calculatePartition(catalog_proc, params[catalog_param.getIndex()], is_array);
        }
        return (partition);
    }

    // ----------------------------------------------------------------------------
    // DETAILED PARTITON METHODS
    // ----------------------------------------------------------------------------

    /**
     * Populate the given set with all of the partition ids that all of the QueryTraces
     * in this TransactionTrace will touch based on the current catalog.
     * Note that this estimate will also include the base partition where the txn's control
     * code will execute.
     * @param partitions
     * @param xact
     * @throws Exception
     */
    public void getAllPartitions(final PartitionSet partitions,
                                 final TransactionTrace xact) throws Exception {
        Procedure catalog_proc = xact.getCatalogItem(this.catalogContext.database);
        int base_partition = this.getBasePartition(catalog_proc, xact.getParams(), true);
        partitions.add(base_partition);
        for (QueryTrace query : xact.getQueries()) {
            this.getAllPartitions(partitions,
                                  query.getCatalogItem(this.catalogContext.database),
                                  query.getParams(),
                                  base_partition);
        } // FOR
    }

    /**
     * Populate the given set with all of the partition ids that this QueryTrace
     * will touch based on the current catalog.
     * @param partitions
     * @param query
     * @param base_partition
     * @throws Exception
     */
    public void getAllPartitions(final PartitionSet partitions,
                                 final QueryTrace query,
                                 final int base_partition) throws Exception {
        Statement catalog_stmt = query.getCatalogItem(this.catalogContext.database);
        this.getAllPartitions(partitions, catalog_stmt, query.getParams(), base_partition);
    }

    /**
     * Populate the given set with all of the partition ids that this Statement with the
     * given parameters will touch based on the current catalog.
     * @param all_partitions
     * @param catalog_stmt
     * @param params
     * @param base_partition
     * @throws Exception
     */
    public void getAllPartitions(final PartitionSet all_partitions, final Statement catalog_stmt, final Object params[], final int base_partition) throws Exception {
        // Note that we will use the single-sited fragments (if available) since they will be
        // faster for us to figure out what partitions has the data that this statement needs
        CatalogMap<PlanFragment> fragments = (catalog_stmt.getHas_singlesited() ? 
                                                    catalog_stmt.getFragments() :
                                                    catalog_stmt.getMs_fragments());
        this.getAllFragmentPartitions(null, all_partitions, fragments.values(), params, base_partition);
    }

    // ----------------------------------------------------------------------------
    // STATEMENT PARTITION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the table -> partitions mapping for the given QueryTrace object
     * 
     * @param query
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, PartitionSet> getTablePartitions(final QueryTrace query,
                                                        final int base_partition) throws Exception {
        Statement catalog_stmt = query.getCatalogItem(this.catalogContext.database);
        return (this.getTablePartitions(catalog_stmt, query.getParams(), base_partition));
    }
    
    /**
     * Return all of the partitions per table for the given Statement object
     * 
     * @param catalog_stmt
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<String, PartitionSet> getTablePartitions(final Statement catalog_stmt,
                                                        final Object params[],
                                                        final int base_partition) throws Exception {
        Map<String, PartitionSet> all_partitions = new HashMap<String, PartitionSet>();
        CatalogMap<PlanFragment> fragments = (catalog_stmt.getHas_singlesited() ? catalog_stmt.getFragments() : catalog_stmt.getMs_fragments());
        for (PlanFragment catalog_frag : fragments) {
            try {
                Map<String, PartitionSet> frag_partitions = new HashMap<String, PartitionSet>();
                this.calculatePartitionsForFragment(frag_partitions, null, catalog_frag, params, base_partition);
                for (String table_key : frag_partitions.keySet()) {
                    if (!all_partitions.containsKey(table_key)) {
                        all_partitions.put(table_key, frag_partitions.get(table_key));
                    } else {
                        all_partitions.get(table_key).addAll(frag_partitions.get(table_key));
                    }
                } // FOR
            } catch (Throwable ex) {
                throw new Exception("Failed to calculate table partitions for " + catalog_frag.fullName(), ex);
            }
        } // FOR
        return (all_partitions);
    }

    // ----------------------------------------------------------------------------
    // FRAGMENT PARTITON METHODS
    // ----------------------------------------------------------------------------

    /**
     * Return the set of StmtParameter offsets that can be used to figure out
     * what partitions the Statement invocation will touch. This is used to
     * quickly figure out whether that invocation is single-partition or not. If
     * this Statement will always be multi-partition, or if the tables it
     * references uses a MultiColumn partitioning attribute, then the return
     * set will be null. This is at a coarse-grained level. You still need to
     * use the other PartitionEstimator methods to figure out where to send
     * PlanFragments.
     * 
     * @param catalog_stmt
     * @return
     */
    public int[] getStatementEstimationParameters(final Statement catalog_stmt) {
        if (debug.val)
            LOG.debug("Retrieving estimation parameter offsets for " + catalog_stmt.fullName());
        
        int[] all_param_idxs = this.cache_stmtPartitionParameters.get(catalog_stmt);
        if (all_param_idxs == null) {
            List<Integer> param_idxs = new ArrayList<Integer>();

            // Assume single-partition
            if (catalog_stmt.getHas_singlesited() == false) {
                if (debug.val)
                    LOG.warn("There is no single-partition query plan for " + catalog_stmt.fullName());
                return (null);
            }

            for (PlanFragment catalog_frag : catalog_stmt.getFragments().values()) {
                PartitionEstimator.CacheEntry cache_entry = null;
                try {
                    cache_entry = this.getFragmentCacheEntry(catalog_frag);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to retrieve CacheEntry for " + catalog_frag.fullName());
                }

                // If this PlanFragment has a broadcast, then this statment
                // can't be used for fast look-ups
                if (cache_entry.hasBroadcast()) {
                    if (debug.val)
                        LOG.warn(String.format("%s contains an operation that must be broadcast." +
                        		 "Cannot be used for fast look-ups", catalog_frag.fullName()));
                    return (null);
                }

                for (Table catalog_tbl : cache_entry.getTables()) {
                    if (catalog_tbl.getMaterializer() != null) {
                        catalog_tbl = catalog_tbl.getMaterializer();
                    }
                    Column partition_col = catalog_tbl.getPartitioncolumn();
                    if (partition_col instanceof MultiColumn) {
                        if (debug.val)
                            LOG.warn(String.format("%s references %s, which is partitioned on %s. " +
                            		 "Cannot be used for fast look-ups",
                            		 catalog_frag.fullName(), catalog_tbl.getName(), partition_col.fullName()));
                        return (null);
                    }
                    else if (partition_col != null && cache_entry.predicates.containsKey(partition_col)) {
                        for (Pair<ExpressionType, CatalogType> pair : cache_entry.predicates.get(partition_col)) {
                            if (pair.getFirst() == ExpressionType.COMPARE_EQUAL &&
                                    pair.getSecond() instanceof StmtParameter) {
                                param_idxs.add(((StmtParameter)pair.getSecond()).getIndex());
                            }
                        } // FOR
                    }
                } // FOR
                if (param_idxs.isEmpty() == false) all_param_idxs = CollectionUtil.toIntArray(param_idxs);
            } // FOR
            this.cache_stmtPartitionParameters.put(catalog_stmt, all_param_idxs);
        }
        return (all_param_idxs);
    }

    /**
     * @param frag_partitions
     * @param fragments
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public Map<PlanFragment, PartitionSet> getAllFragmentPartitions(final Map<PlanFragment, PartitionSet> frag_partitions,
                                                                    final PlanFragment fragments[],
                                                                    final Object params[],
                                                                    final int base_partition) throws Exception {
        this.getAllFragmentPartitions(frag_partitions, null, fragments, params, base_partition);
        return (frag_partitions);
    }

    /**
     * Populate a mapping from PlanFragments to PartitionSets.
     * <B>NOTE:</B> This is the one to use at runtime in the BatchPlanner because it doesn't
     * allocate any new Collections!
     * 
     * @param frag_partitions
     * @param frag_all_partitions
     * @param fragments
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public void getAllFragmentPartitions(final Map<PlanFragment, PartitionSet> frag_partitions,
                                         final PartitionSet frag_all_partitions,
                                         final PlanFragment fragments[],
                                         final Object params[],
                                         final int base_partition) throws Exception {
        // Loop through this Statement's plan fragments and get the partitions
        for (PlanFragment catalog_frag : fragments) {
            PartitionSet partitions = null;

            // If we have a FragPartion map, then use an entry from that
            if (frag_partitions != null) {
                partitions = frag_partitions.get(catalog_frag);
                if (partitions == null) {
                    partitions = new PartitionSet();
                    frag_partitions.put(catalog_frag, partitions);
                } else {
                    partitions.clear();
                }
            // Otherwise use our AllPartitions set
            } else {
                partitions = frag_all_partitions;
            }
            assert(partitions != null);

            this.calculatePartitionsForFragment(null,
                                                partitions,
                                                catalog_frag,
                                                params,
                                                base_partition);

            // If there were no partitions, then the PlanFragment needs to be
            // execute on the base partition
            // Because these are the PlanFragments that aggregate the results together
            // XXX: Not sure if this is right, but it's 5:30pm on a snowy night
            // so it's good enough for me...
            if (partitions.isEmpty())
                partitions.add(base_partition);

            if (frag_partitions != null && frag_all_partitions != null)
                frag_all_partitions.addAll(partitions);
        } // FOR
    }

    /**
     * Return the list partitions that this fragment needs to be sent to based
     * on the parameters
     * 
     * @param catalog_frag
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    public PartitionSet getPartitions(final PartitionSet partitions, 
                                      final PlanFragment catalog_frag,
                                      final Object params[],
                                      final int base_partition) throws Exception {
        this.calculatePartitionsForFragment(null, partitions, catalog_frag, params, base_partition);
        return (partitions);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL CALCULATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * @param catalog_frag
     * @param params
     * @param base_partition
     * @return
     * @throws Exception
     */
    private void calculatePartitionsForFragment(final Map<String, PartitionSet> entry_partitions,
                                                final PartitionSet all_partitions,
                                                final PlanFragment catalog_frag,
                                                final Object params[],
                                                final int base_partition) throws Exception {
        if (trace.val)
            LOG.trace("Estimating partitions for PlanFragment #" + catalog_frag.fullName());
        PartitionEstimator.CacheEntry cache_entry = this.getFragmentCacheEntry(catalog_frag);
        this.calculatePartitionsForCache(cache_entry,
                                         params,
                                         base_partition,
                                         entry_partitions,
                                         all_partitions);
        if (debug.val) {
            if (entry_partitions != null)
                LOG.debug(String.format("%s Table Partitions: %s", catalog_frag.fullName(), entry_partitions));
            if (all_partitions != null)
                LOG.debug(String.format("%s All Partitions: %s", catalog_frag.fullName(), all_partitions));
        }
        return;
    }

    private PartitionEstimator.CacheEntry getFragmentCacheEntry(PlanFragment catalog_frag) throws Exception {
        String frag_key = CatalogKey.createKey(catalog_frag);
        // Check whether we have generate the cache entries for this Statement
        // The CacheEntry object just tells us what input parameter to use for
        // hashing to figure out where we need to go for each table.
        PartitionEstimator.CacheEntry cache_entry = this.cache_fragmentEntries.get(frag_key);
        if (cache_entry == null) {
            synchronized (this) {
                cache_entry = this.cache_fragmentEntries.get(frag_key);
                if (cache_entry == null) {
                    Statement catalog_stmt = (Statement) catalog_frag.getParent();
                    this.generateCache(catalog_stmt);
                    cache_entry = this.cache_fragmentEntries.get(frag_key);
                }
            } // SYNCHRONIZED
        }
        assert (cache_entry != null) : "Failed to retrieve CacheEntry for " + catalog_frag.fullName();
        return (cache_entry);
    }

    /**
     * This is the most important method here! This is where we actually calculate what
     * partitions the given element is going to touch. Given a target CacheEntry, we'll
     * look at what each table it accesses and then use the parameter mapping offset to find
     * the values that correspond to the table's partitioning columns.  
     * 
     * This method can either update the PartitionSets for each individual table that
     * the target accesses or a global PartitionSet. Both of these parameters are optional.   
     * 
     * @param target
     * @param params
     * @param base_partition
     * @param entry_table_partitions
     * @param entry_all_partitions
     * @throws Exception
     */
    private void calculatePartitionsForCache(final CacheEntry target,
                                             final Object params[],
                                             final int base_partition,
                                             final Map<String, PartitionSet> entry_table_partitions,
                                             final PartitionSet entry_all_partitions) throws Exception {

        // Hash the input parameters to determine what partitions we're headed to
        QueryType stmt_type = target.query_type;

        // Update cache
        if (target.is_array == null) {
            target.is_array = new boolean[params.length];
            for (int i = 0; i < target.is_array.length; i++) {
                target.is_array[i] = ClassUtil.isArray(params[i]);
            } // FOR
        }

        final PartitionSet table_partitions = this.partitionSetPool.borrowObject();
        assert(table_partitions != null);

        // Go through each table referenced in this CacheEntry and look-up the parameters that the 
        // partitioning columns are referenced against to determine what partitions we need to go to
        // IMPORTANT: If there are no tables (meaning it's some PlanFragment that combines data output
        // from other PlanFragments), then won't return anything because it is up to whoever
        // to figure out where to send this PlanFragment (it may be at the coordinator)
        Table tables[] = target.getTables();
        if (trace.val) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("CacheEntry", target.toString());
            m.put("Tables", tables);
            m.put("Params", Arrays.toString(params));
            m.put("Base Partition", base_partition);
            LOG.trace("Calculating partitions for " + target.query_type + "\n" + StringUtil.formatMaps(m));
        }
        for (int table_idx = 0; table_idx < target.is_replicated.length; table_idx++) {
            final Table catalog_tbl = tables[table_idx];

            // REPLICATED TABLE
            if (target.is_replicated[table_idx]) {
                switch (stmt_type) {
                    // If this table is replicated and this query is a scan,
                    // then we're in the clear and there's nothing else we need to do here
                    // for the current table (but we still need to check the other guys).
                    case SELECT:
                        if (trace.val)
                            LOG.trace("Cache entry " + target + " will execute on the local partition");
                        if (base_partition != HStoreConstants.NULL_PARTITION_ID)
                            table_partitions.add(base_partition);
                        break;
                    // Conversely, if it's replicated but we're performing an update or
                    // a delete, then we know it's not single-sited. The modification has
                    // to be broadcast to all partitions.
                    case INSERT:
                    case UPDATE:
                    case DELETE: 
                        if (trace.val)
                            LOG.trace("Cache entry " + target + " must be broadcast to all partitions");
                        table_partitions.addAll(this.all_partitions);
                        break;
                    // BUSTED (like your mom)
                    default:
                        assert (false) : "Unexpected query type: " + stmt_type;
                } // SWITCH
            }
            // NON-REPLICATED TABLE
            else {
                // We need to calculate the partition value based on this table's partitioning column
                Column catalog_col = cache_tablePartitionColumns.get(catalog_tbl);
                if (trace.val)
                    LOG.trace("Partitioning Column: " + (catalog_col != null ? catalog_col.fullName() : catalog_col));

                // MULTI-COLUMN PARTITIONING
                // Strap on your seatbelts, we're going in!!!
                if (catalog_col instanceof MultiColumn) {
                    // HACK: All multi-column look-ups on queries with an OR
                    // must be broadcast
                    if (target.isMarkedContainsOR()) {
                        if (debug.val)
                            LOG.warn("Trying to use multi-column partitioning [" + catalog_col.fullName() + "] on query that contains an 'OR': " + target);
                        table_partitions.addAll(this.all_partitions);
                    } else {
                        MultiColumn mc = (MultiColumn) catalog_col;
                        PartitionSet mc_partitions[] = this.mcPartitionSetPool.borrowObject();

                        if (trace.val)
                            LOG.trace("Calculating columns for multi-partition colunmn: " + mc);
                        boolean is_valid = true;
                        for (int i = 0, mc_cnt = mc.size(); i < mc_cnt; i++) {
                            Column mc_column = mc.get(i);
                            // assert(cache_entry.get(mc_column_key) != null) :
                            // "Null CacheEntry: " + mc_column_key;
                            if (target.predicates.containsKey(mc_column)) {
                                this.calculatePartitions(mc_partitions[i],
                                                         params,
                                                         target.is_array,
                                                         target.predicates.get(mc_column),
                                                         mc_column);
                            }

                            // Unless we have partition values for both keys,
                            // then it has to be a broadcast
                            if (mc_partitions[i].isEmpty()) {
                                if (debug.val)
                                    LOG.warn(String.format("No partitions for %s from %s. " +
                                    		 "Cache entry %s must be broadcast to all partitions",
                                    		 mc_column.fullName(), mc.fullName(), target));
                                table_partitions.addAll(this.all_partitions);
                                is_valid = false;
                                break;
                            }
                            if (trace.val)
                                LOG.trace(CatalogUtil.getDisplayName(mc_column) + ": " + mc_partitions[i]);
                        } // FOR

                        // Now if we're here, then we have partitions for both
                        // of the columns and we're legit
                        // We therefore just need to take the cross product of
                        // the two sets and hash them together
                        if (is_valid) {
                            for (int part0 : mc_partitions[0]) {
                                for (int part1 : mc_partitions[1]) {
                                    int partition = this.hasher.multiValueHash(part0, part1);
                                    table_partitions.add(partition);
                                    if (trace.val)
                                        LOG.trace(String.format("MultiColumn Partitions[%d, %d] => %d",
                                                  part0, part1, partition));
                                } // FOR
                            } // FOR
                        }
                        this.mcPartitionSetPool.returnObject(mc_partitions);
                    }
                }
                // SINGLE COLUMN PARTITIONING
                else {
                    List<Pair<ExpressionType, CatalogType>> predicates = target.predicates.get(catalog_col);
                    if (trace.val)
                        LOG.trace("Param Indexes: " + predicates);

                    // Important: If there is no entry for this partitioning
                    // column, then we have to broadcast this mofo
                    if (predicates == null || predicates.isEmpty()) {
                        if (debug.val)
                            LOG.debug(String.format("No parameter mapping for %s. Fragment must be broadcast to all partitions",
                                      CatalogUtil.getDisplayName(catalog_col)));
                        table_partitions.addAll(this.all_partitions);

                        // If there is nothing special, just shove off and have
                        // this method figure things out for us
                    } else {
                        if (trace.val)
                            LOG.trace("Calculating partitions normally for " + target);
                        this.calculatePartitions(table_partitions, params, target.is_array, predicates, catalog_col);
                    }
                }
            } // ELSE
            assert (table_partitions.size() <= this.num_partitions);

            if (entry_table_partitions != null) {
                String table_key = CatalogKey.createKey(catalog_tbl);
                PartitionSet table_p = entry_table_partitions.get(table_key);
                if (table_p == null) {
                    entry_table_partitions.put(table_key, new PartitionSet(table_partitions));
                } else {
                    table_p.clear();
                    table_p.addAll(table_partitions);
                }
            }
            if (entry_all_partitions != null) {
                entry_all_partitions.addAll(table_partitions);
            }
            // OPTIMIZATION: If we aren't calculating the individual partitions for each table
            // separately (i.e., we are calculating the "global" partitions needed for the cache entry),
            // then we can check whether we are already touching all partitions. If so, then that means
            // there are no more partitions to add to the set and therefore we can stop here.
            if (entry_table_partitions == null && entry_all_partitions.size() == this.num_partitions)
                break;
        } // FOR
        this.partitionSetPool.returnObject(table_partitions);
        return;
    }

    /**
     * Calculate the partitions touched for the given column
     * 
     * @param partitions
     * @param params
     * @param predicates
     * @param catalog_col
     */
    private void calculatePartitions(final PartitionSet partitions,
                                     final Object params[],
                                     final boolean is_array[],
                                     final List<Pair<ExpressionType, CatalogType>> predicates,
                                     final Column catalog_col) throws Exception {
        // Note that we have to go through all of the mappings from the partitioning column
        // to parameters. This can occur when the partitioning column is referenced multiple times
        // This allows us to handle complex WHERE clauses and what not.
        for (Pair<ExpressionType, CatalogType> pair : predicates) {
            ExpressionType expType = pair.getFirst();
            CatalogType param = pair.getSecond();
            
            // HACK HACK HACK
            // If this is not an equality comparison, then it has to go to all partitions.
            // If we ever want to support smarter range partitioning, then 
            // we will need to move the logic that examines the expression type into
            // the hasher code.
            if (expType != ExpressionType.COMPARE_EQUAL) {
                partitions.addAll(this.all_partitions);
                break;
            }
            
            // STATEMENT PARAMETER
            // This is the common case
            if (param instanceof StmtParameter) {
                int param_idx = ((StmtParameter)param).getIndex();
                
                // IMPORTANT: Check if the parameter is an array. If it is, then we 
                // have to loop through and get the hash of all of the values
                if (is_array[param_idx]) {
                    int num_elements = Array.getLength(params[param_idx]);
                    if (trace.val)
                        LOG.trace(String.format("%s is an array. Calculating multiple partitions", param));
                    for (int i = 0; i < num_elements; i++) {
                        Object value = Array.get(params[param_idx], i);
                        int partition_id = this.hasher.hash(value, catalog_col);
                        if (trace.val)
                            LOG.trace(String.format("%s HASHING PARAM ARRAY[%d][%d]: %s -> %d",
                            		  catalog_col.fullName(), param_idx, i, value, partition_id));
                        partitions.add(partition_id);
                    } // FOR
                    
                }
                // Primitive Value
                else {
                    int partition_id = this.hasher.hash(params[param_idx], catalog_col);
                    if (trace.val)
                        LOG.trace(String.format("%s HASHING PARAM [%d]: %s -> %d",
                                  catalog_col.fullName(), param_idx, params[param_idx], partition_id));
                    partitions.add(partition_id);
                }
            }
            // CONSTANT VALUE
            // This is more rare
            else if (param instanceof ConstantValue) {
                ConstantValue const_param = (ConstantValue)param;
                VoltType vtype = VoltType.get(const_param.getType());
                Object const_value = VoltTypeUtil.getObjectFromString(vtype, const_param.getValue());
                int partition_id = this.hasher.hash(const_value);
                partitions.add(partition_id);
            }
            // BUSTED!
            else {
                throw new RuntimeException("Unexpected parameter type: " + param.fullName());
            }
        } // FOR
        return;
    }

    /**
     * Return the partition touched for a given procedure's parameter value.
     * If the given parameter is an array, then we will just use the first element.
     * @param catalog_proc
     * @param partition_param_val
     * @param is_array Whether the value is an array.
     * @return
     * @throws Exception
     */
    private int calculatePartition(final Procedure catalog_proc,
                                   Object param_val,
                                   final boolean is_array) throws Exception {
        // If the parameter is an array, then just use the first value
        if (is_array) {
            int num_elements = Array.getLength(param_val);
            if (num_elements == 0) {
                if (debug.val)
                    LOG.warn("Empty partitioning parameter array for " + catalog_proc);
                return (HStoreConstants.NULL_PARTITION_ID);
            } else {
                param_val = Array.get(param_val, 0);
            }
        } else if (param_val == null) {
            if (debug.val)
                LOG.warn("Null ProcParameter value: " + catalog_proc);
            return (HStoreConstants.NULL_PARTITION_ID);
        }
        return (this.hasher.hash(param_val, catalog_proc));
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    /**
     * Debug output
     */
    @Override
    public String toString() {
        String ret = "";
        for (Procedure catalog_proc : this.catalogContext.database.getProcedures()) {
            StringBuilder sb = new StringBuilder();
            boolean has_entries = false;
            sb.append(CatalogUtil.getDisplayName(catalog_proc)).append(":\n");
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                String stmt_key = CatalogKey.createKey(catalog_stmt);
                CacheEntry stmt_cache = this.cache_statementEntries.get(stmt_key);
                if (stmt_cache == null)
                    continue;
                has_entries = true;
                sb.append("  " + catalog_stmt.getName() + ": ").append(stmt_cache).append("\n");

                for (PlanFragment catalog_frag : CatalogUtil.getAllPlanFragments(catalog_stmt)) {
                    String frag_key = CatalogKey.createKey(catalog_frag);
                    CacheEntry frag_cache = this.cache_fragmentEntries.get(frag_key);
                    if (frag_cache == null)
                        continue;
                    sb.append("    PlanFragment[" + catalog_frag.getName() + "]: ").append(frag_cache).append("\n");
                }
            } // FOR
            if (has_entries)
                ret += sb.toString() + StringUtil.SINGLE_LINE;
        } // FOR
        return (ret);
    }

    /**
     * For each Column key in the given map, recursively populate their sets to contain 
     * the Cartesian product of all the other Columns' sets. 
     * @param column_joins
     */
    protected static void populateColumnJoinSets(final Map<Column, Set<Column>> column_joins) {
        int orig_size = 0;
        for (Collection<Column> cols : column_joins.values()) {
            orig_size += cols.size();
        }
        // First we have to take the Cartesian product of all mapped joins
        for (Column c0 : column_joins.keySet()) {
            // For each column that c0 is joined with, add a reference to c0 for
            // all the columns that the other column references
            for (Column c1 : column_joins.get(c0)) {
                assert (!c1.equals(c0));
                for (Column c2 : column_joins.get(c1)) {
                    if (!c0.equals(c2))
                        column_joins.get(c2).add(c0);
                } // FOR
            } // FOR
        } // FOR

        int new_size = 0;
        for (Collection<Column> cols : column_joins.values()) {
            new_size += cols.size();
        }
        if (new_size != orig_size)
            populateColumnJoinSets(column_joins);
    }

    /**
     * Pre-load the cache entries for all Statements
     * 
     * @param catalog_db
     */
    public void preload() {
        assert (this.catalogContext != null);
        for (Procedure catalog_proc : this.catalogContext.database.getProcedures()) {
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                try {
                    this.generateCache(catalog_stmt);
                    this.getStatementEstimationParameters(catalog_stmt);
                } catch (Exception ex) {
                    LOG.fatal("Failed to generate cache for " + catalog_stmt.fullName(), ex);
                    System.exit(1);
                }
            } // FOR
        } // FOR

        for (CacheEntry entry : this.cache_fragmentEntries.values()) {
            entry.getTables();
        }
        for (CacheEntry entry : this.cache_statementEntries.values()) {
            entry.getTables();
        }

    }
}