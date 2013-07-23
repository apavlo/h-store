/***************************************************************************
 * Copyright (C) 2009 by H-Store Project * Brown University * Massachusetts
 * Institute of Technology * Yale University * * Permission is hereby granted,
 * free of charge, to any person obtaining * a copy of this software and
 * associated documentation files (the * "Software"), to deal in the Software
 * without restriction, including * without limitation the rights to use, copy,
 * modify, merge, publish, * distribute, sublicense, and/or sell copies of the
 * Software, and to * permit persons to whom the Software is furnished to do so,
 * subject to * the following conditions: * * The above copyright notice and
 * this permission notice shall be * included in all copies or substantial
 * portions of the Software. * * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT
 * WARRANTY OF ANY KIND, * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT.* IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR * OTHER DEALINGS IN THE SOFTWARE. *
 ***************************************************************************/
package edu.brown.costmodel;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.RandomProcParameter;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.Filter;

/**
 * @author pavlo
 */
public class SingleSitedCostModel extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(SingleSitedCostModel.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final Set<Long> DEBUG_TRACE_IDS = new HashSet<Long>();
    static {
        // DEBUG_TRACE_IDS.add(1251416l);
    }

    // ----------------------------------------------------
    // COST WEIGHTS
    // ----------------------------------------------------

    public static final double COST_UNKNOWN_QUERY = 1d;
    public static final double COST_SINGLESITE_QUERY = 1d;
    public static final double COST_MULTISITE_QUERY = 10d;

    // ----------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------

    /**
     * TransactionTrace Id -> TransactionCacheEntry
     */
    private final Map<Long, TransactionCacheEntry> txn_entries = new LinkedHashMap<Long, TransactionCacheEntry>();
    /**
     * TableKeys that were replicated when we calculated them It means that have
     * to switch the partitions to be the base partition (if we have one)
     */
    private final Set<String> replicated_tables = new HashSet<String>();
    /**
     * For each procedure, the list of tables that are accessed in the
     * ProcedureKey -> Set<TableKey>
     */
    private final Map<String, Set<String>> touched_tables = new HashMap<String, Set<String>>();
    /**
     * Table Key -> Set<QueryCacheEntry>
     */
    private final Map<String, Set<QueryCacheEntry>> cache_tableXref = new LinkedHashMap<String, Set<QueryCacheEntry>>();
    /**
     * Procedure Key -> Set<TransactionCacheEntry>
     */
    private final Map<String, Set<TransactionCacheEntry>> cache_procXref = new LinkedHashMap<String, Set<TransactionCacheEntry>>();
    /**
     * Query Key -> Set<QueryCacheEntry>
     */
    private final Map<String, Set<QueryCacheEntry>> cache_stmtXref = new HashMap<String, Set<QueryCacheEntry>>();

    private final Set<Long> last_invalidateTxns = new HashSet<Long>();

    /**
     * Cost Estimate Explanation
     */
    public class TransactionCacheEntry implements Cloneable {
        private final String proc_key;
        private final QueryCacheEntry query_entries[];
        private final long txn_id;
        private final short weight;
        private final int total_queries;
        private boolean singlesited = true;
        private int base_partition = HStoreConstants.NULL_PARTITION_ID;
        private int examined_queries = 0;
        private int singlesite_queries = 0;
        private int multisite_queries = 0;
        private int unknown_queries = 0;
        private ObjectHistogram<Integer> touched_partitions = new ObjectHistogram<Integer>();

        private TransactionCacheEntry(String proc_key, long txn_trace_id, int weight, int total_queries) {
            this.proc_key = proc_key;
            this.txn_id = txn_trace_id;
            this.weight = (short) weight;
            this.total_queries = total_queries;
            this.query_entries = new QueryCacheEntry[this.total_queries];
        }

        public TransactionCacheEntry(String proc_key, TransactionTrace txn_trace) {
            this(proc_key, txn_trace.getTransactionId(), txn_trace.getWeight(), txn_trace.getWeightedQueryCount());
        }

        public QueryCacheEntry[] getQueryCacheEntries() {
            return (this.query_entries);
        }

        public long getTransactionId() {
            return (this.txn_id);
        }

        private void resetQueryCounters() {
            this.examined_queries = 0;
            this.singlesite_queries = 0;
            this.multisite_queries = 0;
            this.unknown_queries = 0;
        }

        public String getProcedureKey() {
            return (this.proc_key);
        }

        protected void addTouchedPartition(int partition) {
            this.touched_partitions.put(partition);
        }

        public ObjectHistogram<Integer> getAllTouchedPartitionsHistogram() {
            ObjectHistogram<Integer> copy = new ObjectHistogram<Integer>(this.touched_partitions);
            assert (this.touched_partitions.getValueCount() == copy.getValueCount());
            assert (this.touched_partitions.getSampleCount() == copy.getSampleCount());
            if (this.base_partition != HStoreConstants.NULL_PARTITION_ID && !copy.contains(this.base_partition)) {
                copy.put(this.base_partition);
            }
            return (copy);
        }

        public boolean isComplete() {
            return (this.total_queries == this.examined_queries);
        }

        public boolean isSinglePartitioned() {
            return this.singlesited;
        }

        protected void setSingleSited(boolean singlesited) {
            this.singlesited = singlesited;
        }

        public int getExecutionPartition() {
            return (this.base_partition);
        }

        /**
         * Returns the total number of queries in this TransactionTrace
         * 
         * @return
         */
        public int getTotalQueryCount() {
            return this.total_queries;
        }

        public int getExaminedQueryCount() {
            return this.examined_queries;
        }

        protected void setExaminedQueryCount(int examined_queries) {
            this.examined_queries = examined_queries;
        }

        public int getSingleSiteQueryCount() {
            return this.singlesite_queries;
        }

        protected void setSingleSiteQueryCount(int singlesite_queries) {
            this.singlesite_queries = singlesite_queries;
        }

        public int getMultiSiteQueryCount() {
            return this.multisite_queries;
        }

        protected void setMultiSiteQueryCount(int multisite_queries) {
            this.multisite_queries = multisite_queries;
        }

        public int getUnknownQueryCount() {
            return this.unknown_queries;
        }

        protected void setUnknownQueryCount(int unknown_queries) {
            this.unknown_queries = unknown_queries;
        }

        public Collection<Integer> getTouchedPartitions() {
            return (this.touched_partitions.values());
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            TransactionCacheEntry clone = (TransactionCacheEntry) super.clone();
            clone.touched_partitions = new ObjectHistogram<Integer>(this.touched_partitions);
            return (clone);
        }

        @Override
        public String toString() {
            return ("TransactionCacheEntry[" + CatalogKey.getNameFromKey(this.proc_key) + ":Trace#" + this.txn_id + "]");
        }

        public String debug() {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("HashCode", this.hashCode());
            m.put("Weight", this.weight);
            m.put("Base Partition", this.base_partition);
            m.put("Is SingleSited", this.singlesited);
            m.put("# of Total Queries", this.total_queries);
            m.put("# of Examined Queries", this.examined_queries);
            m.put("# of SingleSited Queries", this.singlesite_queries);
            m.put("# of MultiSite Queries", this.multisite_queries);
            m.put("# of Unknown Queries", this.unknown_queries);
            m.put("Touched Partitions", String.format("SAMPLE COUNT=%d\n%s", this.touched_partitions.getSampleCount(), this.touched_partitions.toString()));
            return (this.toString() + "\n" + StringUtil.formatMaps(m));
        }
    }

    /**
     * Query Cache Entry
     */
    public class QueryCacheEntry implements Cloneable {
        public final long txn_id;
        public final int weight;
        public final int query_trace_idx;
        public boolean singlesited = true;
        public boolean invalid = false;
        public boolean unknown = false;

        /** Table Key -> Set[PartitionId] **/
        private Map<String, PartitionSet> partitions = new HashMap<String, PartitionSet>(SingleSitedCostModel.this.num_tables);

        /** All partitions **/
        private PartitionSet all_partitions = new PartitionSet();

        /**
         * Constructor
         * 
         * @param txn_id
         * @param query_idx
         */
        private QueryCacheEntry(long txn_id, int query_idx, int weight) {
            this.txn_id = txn_id;
            this.query_trace_idx = query_idx;
            this.weight = weight;
        }

        public QueryCacheEntry(TransactionTrace xact, QueryTrace query, int weight) {
            this(xact.getTransactionId(), xact.getQueries().indexOf(query), weight);
        }

        public long getTransactionId() {
            return (this.txn_id);
        }

        public int getQueryIdx() {
            return (this.query_trace_idx);
        }

        public boolean isInvalid() {
            return this.invalid;
        }

        public boolean isSinglesited() {
            return this.singlesited;
        }

        public boolean isUnknown() {
            return this.unknown;
        }

        public void addPartition(String table_key, int partition) {
            this.getPartitions(table_key).add(partition);
            this.all_partitions.add(partition);
        }

        public void removePartition(String table_key, int partition) {
            this.getPartitions(table_key).remove(partition);

            // Check whether any other table references this partition
            // If not, then remove it from the all_partitions set
            boolean found = false;
            for (Entry<String, PartitionSet> e : this.partitions.entrySet()) {
                if (e.getKey().equals(table_key))
                    continue;
                if (e.getValue().contains(partition)) {
                    found = true;
                    break;
                }
            } // FOR
            if (!found)
                this.all_partitions.remove(partition);
        }

        public void addAllPartitions(String table_key, PartitionSet partitions) {
            this.getPartitions(table_key).addAll(partitions);
            this.all_partitions.addAll(partitions);
        }

        private PartitionSet getPartitions(String table_key) {
            PartitionSet p = this.partitions.get(table_key);
            if (p == null) {
                p = new PartitionSet();
                this.partitions.put(table_key, p);
            }
            return (p);
        }

        public PartitionSet getAllPartitions() {
            return (this.all_partitions);
        }

        public Set<String> getTableKeys() {
            return (this.partitions.keySet());
        }

        public Map<String, PartitionSet> getPartitionValues() {
            return (this.partitions);
        }

        @Override
        public Object clone() throws CloneNotSupportedException {
            QueryCacheEntry clone = (QueryCacheEntry) super.clone();
            clone.all_partitions = new PartitionSet(this.all_partitions);
            clone.partitions = new HashMap<String, PartitionSet>();
            for (String key : this.partitions.keySet()) {
                clone.partitions.put(key, new PartitionSet(this.partitions.get(key)));
            } // FOR
            return (clone);
        }

        @Override
        public String toString() {
            // You know you just love this!
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("Txn Id#", this.txn_id);
            m.put("Weight", this.weight);
            m.put("Query Trace Idx#", this.query_trace_idx);
            m.put("Is SingleSited", this.singlesited);
            m.put("Is Invalid", this.invalid);
            m.put("Is Unknown", this.unknown);
            m.put("All Partitions", this.all_partitions);
            m.put("Table Partitions", this.partitions);
            return "QueryCacheEntry:\n" + StringUtil.formatMaps(m);
        }
    } // CLASS

    // ----------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------

    /**
     * Default Constructor
     * 
     * @param catalog_db
     */
    public SingleSitedCostModel(CatalogContext catalogContext) {
        this(catalogContext, new PartitionEstimator(catalogContext)); // FIXME
    }

    /**
     * I forget why we need this...
     */
    public SingleSitedCostModel(CatalogContext catalogContext, PartitionEstimator p_estimator) {
        super(SingleSitedCostModel.class, catalogContext, p_estimator);

        // Initialize cache data structures
        if (catalogContext != null) {
            for (String table_key : CatalogKey.createKeys(catalogContext.database.getTables())) {
                this.cache_tableXref.put(table_key, new HashSet<QueryCacheEntry>());
            } // FOR
            for (String stmt_key : CatalogKey.createKeys(CatalogUtil.getAllStatements(catalogContext.database))) {
                this.cache_stmtXref.put(stmt_key, new HashSet<QueryCacheEntry>());
            } // FOR

            // List of tables touched by each partition
            if (trace.val)
                LOG.trace("Building cached list of tables touched by each procedure");
            for (Procedure catalog_proc : catalogContext.database.getProcedures()) {
                if (catalog_proc.getSystemproc())
                    continue;
                String proc_key = CatalogKey.createKey(catalog_proc);
                try {
                    Collection<String> table_keys = CatalogKey.createKeys(CatalogUtil.getReferencedTables(catalog_proc));
                    this.touched_tables.put(proc_key, new HashSet<String>(table_keys));
                    if (trace.val)
                        LOG.trace(catalog_proc + " Touched Tables: " + table_keys);
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to calculate touched tables for " + catalog_proc, ex);
                }
            } // FOR
        }
    }

    @Override
    public void clear(boolean force) {
        super.clear(force);

        this.txn_entries.clear();
        this.last_invalidateTxns.clear();

        for (Collection<QueryCacheEntry> c : this.cache_tableXref.values()) {
            c.clear();
        }
        for (Collection<QueryCacheEntry> c : this.cache_stmtXref.values()) {
            c.clear();
        }
        for (Collection<TransactionCacheEntry> c : this.cache_procXref.values()) {
            c.clear();
        }

        assert (this.histogram_txn_partitions.getSampleCount() == 0);
        assert (this.histogram_txn_partitions.getValueCount() == 0);
        assert (this.histogram_query_partitions.getSampleCount() == 0);
        assert (this.histogram_query_partitions.getValueCount() == 0);
    }

    public int getWeightedTransactionCount() {
        int ctr = 0;
        for (TransactionCacheEntry txn_entry : this.txn_entries.values()) {
            ctr += txn_entry.weight;
        } // FOR
        return (ctr);
    }

    public Collection<TransactionCacheEntry> getTransactionCacheEntries() {
        return (this.txn_entries.values());
    }

    public Collection<QueryCacheEntry> getAllQueryCacheEntries() {
        Set<QueryCacheEntry> all = new HashSet<QueryCacheEntry>();
        for (TransactionCacheEntry t : this.txn_entries.values()) {
            for (QueryCacheEntry q : t.query_entries) {
                if (q != null)
                    all.add(q);
            } // FOR (queries)
        } // FOR (txns)
        return (all);
    }

    protected TransactionCacheEntry getTransactionCacheEntry(TransactionTrace txn_trace) {
        return (this.txn_entries.get(txn_trace.getTransactionId()));
    }

    protected TransactionCacheEntry getTransactionCacheEntry(long txn_id) {
        return (this.txn_entries.get(txn_id));
    }

    public Collection<QueryCacheEntry> getQueryCacheEntries(TransactionTrace txn_trace) {
        return (this.getQueryCacheEntries(txn_trace.getTransactionId()));
    }

    public Collection<QueryCacheEntry> getQueryCacheEntries(long txn_id) {
        TransactionCacheEntry txn_entry = this.txn_entries.get(txn_id);
        if (txn_entry != null) {
            return (CollectionUtil.addAll(new ArrayList<QueryCacheEntry>(), txn_entry.query_entries));
        }
        return (null);
    }

    public Collection<QueryCacheEntry> getQueryCacheEntries(Statement catalog_stmt) {
        String stmt_key = CatalogKey.createKey(catalog_stmt);
        return (this.cache_stmtXref.get(stmt_key));
    }

    @Override
    public void prepareImpl(final CatalogContext catalogContext) {
        if (trace.val)
            LOG.trace("Prepare called!");

        // Recompute which tables have switched from Replicated to
        // Non-Replicated
        this.replicated_tables.clear();
        for (Table catalog_tbl : catalogContext.database.getTables()) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            if (catalog_tbl.getIsreplicated()) {
                this.replicated_tables.add(table_key);
            }
            if (trace.val)
                LOG.trace(catalog_tbl + " => isReplicated(" + this.replicated_tables.contains(table_key) + ")");
        } // FOR
    }

    // --------------------------------------------------------------------------------------------
    // INVALIDATION METHODS
    // --------------------------------------------------------------------------------------------

    protected Collection<Long> getLastInvalidateTransactionIds() {
        return (this.last_invalidateTxns);
    }

    /**
     * Invalidate a single QueryCacheEntry Returns true if the query's
     * TransactionCacheEntry parent needs to be invalidated as well
     * 
     * @param txn_entry
     * @param query_entry
     * @param invalidate_removedTouchedPartitions
     * @return
     */
    private boolean invalidateQueryCacheEntry(TransactionCacheEntry txn_entry, QueryCacheEntry query_entry, ObjectHistogram<Integer> invalidate_removedTouchedPartitions) {
        if (trace.val)
            LOG.trace("Invalidate:" + query_entry);
        boolean invalidate_txn = false;

        if (query_entry.isUnknown()) {
            txn_entry.unknown_queries -= query_entry.weight;
        } else {
            txn_entry.examined_queries -= query_entry.weight;
        }
        if (query_entry.isSinglesited()) {
            txn_entry.singlesite_queries -= query_entry.weight;
        } else {
            txn_entry.multisite_queries -= query_entry.weight;
        }

        // DEBUG!
        if (txn_entry.singlesite_queries < 0 || txn_entry.multisite_queries < 0) {
            LOG.error("!!! NEGATIVE QUERY COUNTS - TRACE #" + txn_entry.getTransactionId() + " !!!");
            LOG.error(txn_entry.debug());
            LOG.error(StringUtil.SINGLE_LINE);
            for (QueryCacheEntry q : txn_entry.query_entries) {
                LOG.error(q);
            }
        }
        assert (txn_entry.examined_queries >= 0) : txn_entry + " has negative examined queries!\n" + txn_entry.debug();
        assert (txn_entry.unknown_queries >= 0) : txn_entry + " has negative unknown queries!\n" + txn_entry.debug();
        assert (txn_entry.singlesite_queries >= 0) : txn_entry + " has negative singlesited queries!\n" + txn_entry.debug();
        assert (txn_entry.multisite_queries >= 0) : txn_entry + " has negative multisited queries!\n" + txn_entry.debug();

        // Populate this histogram so that we know what to remove from the
        // global histogram
        invalidate_removedTouchedPartitions.put(query_entry.getAllPartitions(), query_entry.weight * txn_entry.weight);

        // Remove the partitions this query touches from the txn's touched
        // partitions histogram
        final String debugBefore = txn_entry.debug();
        try {
            txn_entry.touched_partitions.dec(query_entry.getAllPartitions(), query_entry.weight);
        } catch (Throwable ex) {
            LOG.error(debugBefore, ex);
            throw new RuntimeException(ex);
        }

        // If this transaction is out of queries, then we'll remove it
        // completely
        if (txn_entry.examined_queries == 0) {
            invalidate_txn = true;
        }

        // Make sure that we do this last so we can subtract values from the
        // TranasctionCacheEntry
        query_entry.invalid = true;
        query_entry.singlesited = true;
        query_entry.unknown = true;
        for (PartitionSet q_partitions : query_entry.partitions.values()) {
            q_partitions.clear();
        } // FOR
        query_entry.all_partitions.clear();

        this.query_ctr.addAndGet(-1 * query_entry.weight);
        return (invalidate_txn);
    }

    /**
     * Invalidate a single TransactionCacheEntry
     * 
     * @param txn_entry
     */
    private void invalidateTransactionCacheEntry(TransactionCacheEntry txn_entry) {
        if (trace.val)
            LOG.trace("Removing Transaction:" + txn_entry);

        // If we have a base partition value, then we have to remove an entry
        // from the
        // histogram that keeps track of where the java executes
        if (txn_entry.base_partition != HStoreConstants.NULL_PARTITION_ID) {
            if (trace.val)
                LOG.trace("Resetting base partition [" + txn_entry.base_partition + "] and updating histograms");
            // NOTE: We have to remove the base_partition from these histograms
            // but not the
            // histogram_txn_partitions because we will do that down below
            this.histogram_java_partitions.dec(txn_entry.base_partition, txn_entry.weight);
            if (this.isJavaExecutionWeightEnabled()) {
                txn_entry.touched_partitions.dec(txn_entry.base_partition, (int)Math.round(txn_entry.weight * this.getJavaExecutionWeight()));
            }
            if (trace.val)
                LOG.trace("Global Java Histogram:\n" + this.histogram_java_partitions);
        }

        this.histogram_procs.dec(txn_entry.proc_key, txn_entry.weight);
        this.txn_ctr.addAndGet(-1 * txn_entry.weight);
        this.cache_procXref.get(txn_entry.proc_key).remove(txn_entry);
        this.txn_entries.remove(txn_entry.getTransactionId());
        this.last_invalidateTxns.add(txn_entry.getTransactionId());
    }

    /**
     * Invalidate cache entries for the given CatalogKey
     * 
     * @param catalog_key
     */
    @Override
    public synchronized void invalidateCache(String catalog_key) {
        if (!this.use_caching)
            return;
        if (trace.val)
            LOG.trace("Looking to invalidate cache records for: " + catalog_key);
        int query_ctr = 0;
        int txn_ctr = 0;

        Set<TransactionCacheEntry> invalidate_modifiedTxns = new HashSet<TransactionCacheEntry>();
        Set<String> invalidate_targetKeys = new HashSet<String>();
        Collection<QueryCacheEntry> invalidate_queries = null;

        // ---------------------------------------------
        // Table Key
        // ---------------------------------------------
        if (this.cache_tableXref.containsKey(catalog_key)) {
            if (trace.val)
                LOG.trace("Invalidate QueryCacheEntries for Table " + catalog_key);
            invalidate_queries = this.cache_tableXref.get(catalog_key);
        }
        // ---------------------------------------------
        // Statement Key
        // ---------------------------------------------
        else if (this.cache_stmtXref.containsKey(catalog_key)) {
            if (trace.val)
                LOG.trace("Invalidate QueryCacheEntries for Statement " + catalog_key);
            invalidate_queries = this.cache_stmtXref.get(catalog_key);
        }

        if (invalidate_queries != null && invalidate_queries.isEmpty() == false) {
            if (debug.val)
                LOG.debug(String.format("Invalidating %d QueryCacheEntries for %s", invalidate_queries.size(), catalog_key));
            ObjectHistogram<Integer> invalidate_removedTouchedPartitions = new ObjectHistogram<Integer>();
            for (QueryCacheEntry query_entry : invalidate_queries) {
                if (query_entry.isInvalid())
                    continue;

                // Grab the TransactionCacheEntry and enable zero entries in its
                // touched_partitions
                // This will ensure that we know which partitions to remove from
                // the costmodel's
                // global txn touched partitions histogram
                TransactionCacheEntry txn_entry = this.txn_entries.get(query_entry.getTransactionId());
                assert (txn_entry != null) : "Missing Txn #Id: " + query_entry.getTransactionId();
                txn_entry.touched_partitions.setKeepZeroEntries(true);

                boolean invalidate_txn = this.invalidateQueryCacheEntry(txn_entry, query_entry, invalidate_removedTouchedPartitions);
                query_ctr++;
                if (invalidate_txn) {
                    this.invalidateTransactionCacheEntry(txn_entry);
                    txn_ctr++;
                }

                // Always mark the txn as invalidated
                invalidate_modifiedTxns.add(txn_entry);
            } // FOR

            // We can now remove the touched query partitions if we have any
            if (!invalidate_removedTouchedPartitions.isEmpty()) {
                if (trace.val)
                    LOG.trace("Removing " + invalidate_removedTouchedPartitions.getSampleCount() + " partition touches for " + query_ctr + " queries");
                this.histogram_query_partitions.dec(invalidate_removedTouchedPartitions);
            }
        }

        // ---------------------------------------------
        // Procedure Key
        // ---------------------------------------------
        if (this.cache_procXref.containsKey(catalog_key)) {
            if (trace.val)
                LOG.trace("Invalidate Cache for Procedure " + catalog_key);

            // NEW: If this procedure accesses any table that is replicated,
            // then we also need to invalidate
            // the cache for that table so that the tables are wiped

            // We just need to unset the base partition
            for (TransactionCacheEntry txn_entry : this.cache_procXref.get(catalog_key)) {
                assert (txn_entry != null);
                if (txn_entry.base_partition != HStoreConstants.NULL_PARTITION_ID) {
                    if (trace.val)
                        LOG.trace("Unset base_partition for " + txn_entry);
                    txn_entry.touched_partitions.setKeepZeroEntries(true);
                    this.histogram_java_partitions.dec(txn_entry.base_partition, txn_entry.weight);
                    if (this.isJavaExecutionWeightEnabled()) {
                        txn_entry.touched_partitions.dec(txn_entry.base_partition, (int)Math.round(txn_entry.weight * this.getJavaExecutionWeight()));
                    }
                    txn_entry.base_partition = HStoreConstants.NULL_PARTITION_ID;
                    invalidate_modifiedTxns.add(txn_entry);
                    txn_ctr++;
                }
            } // FOR

            // If this procedure touches any replicated tables, then blow away
            // the cache for that
            // so that we get new base partition calculations. It's just easier
            // this way
            for (String table_key : this.touched_tables.get(catalog_key)) {
                if (this.replicated_tables.contains(table_key)) {
                    if (trace.val)
                        LOG.trace(catalog_key + " => " + table_key + ": is replicated and will need to be invalidated too!");
                    invalidate_targetKeys.add(table_key);
                }
            } // FOR
        }

        // Update the TransactionCacheEntry objects that we modified in the loop
        // above
        if (trace.val && !invalidate_modifiedTxns.isEmpty())
            LOG.trace("Updating partition information for " + invalidate_modifiedTxns.size() + " TransactionCacheEntries");
        for (TransactionCacheEntry txn_entry : invalidate_modifiedTxns) {
            // Get the list of partitions that are no longer being touched by this txn
            // We remove these from the costmodel's global txn touched histogram
            Collection<Integer> zero_partitions = txn_entry.touched_partitions.getValuesForCount(0);
            if (!zero_partitions.isEmpty()) {
                if (trace.val)
                    LOG.trace("Removing " + zero_partitions.size() + " partitions for " + txn_entry);
                this.histogram_txn_partitions.dec(zero_partitions, txn_entry.weight);
            }

            // Then disable zero entries from the histogram so that our counts
            // don't get screwed up
            txn_entry.touched_partitions.setKeepZeroEntries(false);

            // Then check whether we're still considered multi-partition
            boolean new_singlesited = (txn_entry.multisite_queries == 0);
            if (!txn_entry.singlesited && new_singlesited) {
                if (trace.val) {
                    LOG.trace("Switching " + txn_entry + " from multi-partition to single-partition");
                    LOG.trace("Single-Partition Transactions:\n" + this.histogram_sp_procs);
                    LOG.trace("Multi-Partition Transactions:\n" + this.histogram_mp_procs);
                }
                this.histogram_mp_procs.dec(txn_entry.getProcedureKey(), txn_entry.weight);
                if (txn_entry.examined_queries > 0)
                    this.histogram_sp_procs.put(txn_entry.getProcedureKey(), txn_entry.weight);
            } else if (txn_entry.singlesited && txn_entry.examined_queries == 0) {
                this.histogram_sp_procs.dec(txn_entry.getProcedureKey(), txn_entry.weight);
            }
            txn_entry.singlesited = new_singlesited;
        } // FOR

        // Sanity Check: If we don't have any TransactionCacheEntries, then the
        // histograms should all be wiped out!
        if (this.txn_entries.size() == 0) {
            if (!this.histogram_java_partitions.isEmpty() || !this.histogram_txn_partitions.isEmpty() || !this.histogram_query_partitions.isEmpty()) {
                LOG.warn("MODIFIED TXNS: " + invalidate_modifiedTxns.size());
                for (TransactionCacheEntry txn_entry : invalidate_modifiedTxns) {
                    LOG.warn(txn_entry.debug() + "\n");
                }
            }
            assert (this.histogram_mp_procs.isEmpty()) : this.histogram_mp_procs;
            assert (this.histogram_sp_procs.isEmpty()) : this.histogram_sp_procs;
            assert (this.histogram_java_partitions.isEmpty()) : this.histogram_java_partitions;
            assert (this.histogram_txn_partitions.isEmpty()) : this.histogram_txn_partitions;
            assert (this.histogram_query_partitions.isEmpty()) : this.histogram_query_partitions;
        }
        if (debug.val)
            assert (this.getWeightedTransactionCount() == this.txn_ctr.get()) : this.getWeightedTransactionCount() + " == " + this.txn_ctr.get();

        if (debug.val && (query_ctr > 0 || txn_ctr > 0))
            LOG.debug("Invalidated Cache [" + catalog_key + "]: Queries=" + query_ctr + ", Txns=" + txn_ctr);

        if (!invalidate_targetKeys.isEmpty()) {
            if (debug.val)
                LOG.debug("Calling invalidateCache for " + invalidate_targetKeys.size() + " dependent catalog items of " + catalog_key);

            // We have to make a copy here, otherwise the recursive call will
            // blow away our list
            for (String next_catalog_key : new HashSet<String>(invalidate_targetKeys)) {
                this.invalidateCache(next_catalog_key);
            } // FOR
        }
    }

    // --------------------------------------------------------------------------------------------
    // ESTIMATION METHODS
    // --------------------------------------------------------------------------------------------

    private final Map<String, PartitionSet> temp_stmtPartitions = new HashMap<String, PartitionSet>();
    private final PartitionSet temp_txnOrigPartitions = new PartitionSet();
    private final PartitionSet temp_txnNewPartitions = new PartitionSet();

    @Override
    public double estimateTransactionCost(CatalogContext catalogContext, Workload workload, Filter filter, TransactionTrace txn_trace) throws Exception {
        // Sanity Check: If we don't have any TransactionCacheEntries, then the
        // histograms should all be wiped out!
        if (this.txn_entries.size() == 0) {
            assert (this.histogram_txn_partitions.isEmpty()) : this.histogram_txn_partitions;
            assert (this.histogram_query_partitions.isEmpty()) : this.histogram_query_partitions;
        }

        TransactionCacheEntry txn_entry = this.processTransaction(catalogContext, txn_trace, filter);
        assert (txn_entry != null);
        if (debug.val)
            LOG.debug(txn_trace + ": " + (txn_entry.singlesited ? "Single" : "Multi") + "-Partition");

        if (!txn_entry.singlesited) {
            return (COST_MULTISITE_QUERY * txn_entry.weight);
        }
        if (txn_entry.unknown_queries > 0) {
            return (COST_UNKNOWN_QUERY * txn_entry.weight);
        }
        return (COST_SINGLESITE_QUERY * txn_entry.weight);
    }

    /**
     * Create a new TransactionCacheEntry and update our histograms
     * appropriately
     * 
     * @param txn_trace
     * @param proc_key
     * @return
     */
    protected TransactionCacheEntry createTransactionCacheEntry(TransactionTrace txn_trace, String proc_key) {
        final int txn_weight = (this.use_txn_weights ? txn_trace.getWeight() : 1);

        if (this.use_caching && !this.cache_procXref.containsKey(proc_key)) {
            this.cache_procXref.put(proc_key, new HashSet<TransactionCacheEntry>());
        }

        TransactionCacheEntry txn_entry = new TransactionCacheEntry(proc_key, txn_trace);
        this.txn_entries.put(txn_trace.getTransactionId(), txn_entry);
        if (this.use_caching) {
            this.cache_procXref.get(proc_key).add(txn_entry);
        }
        if (trace.val)
            LOG.trace("New " + txn_entry);

        // Update txn counter
        this.txn_ctr.addAndGet(txn_weight);

        // Record that we executed this procedure
        this.histogram_procs.put(proc_key, txn_weight);

        // Always record that it was single-partition in the beginning... we can
        // switch later on
        this.histogram_sp_procs.put(proc_key, txn_weight);

        return (txn_entry);
    }

    /**
     * @param txn_entry
     * @param txn_trace
     * @param catalog_proc
     * @param proc_param_idx
     */
    protected void setBasePartition(TransactionCacheEntry txn_entry, int base_partition) {
        // If the partition is null, then there's nothing we can do here other
        // than just pick a random one
        // For now we'll always pick zero to keep things consistent
        if (base_partition == HStoreConstants.NULL_PARTITION_ID) {
            txn_entry.base_partition = 0;
            if (trace.val)
                LOG.trace("Base partition for " + txn_entry + " is null. Setting to default '" + txn_entry.base_partition + "'");
        } else {
            txn_entry.base_partition = base_partition;
        }

        // Record what partition the VoltProcedure executed on
        // We'll throw the base_partition into the txn_entry's touched
        // partitions histogram, but notice
        // that we can weight how much the java execution costs
        if (this.isJavaExecutionWeightEnabled()) {
            int weight = (int)Math.round(txn_entry.weight * this.getJavaExecutionWeight());
            txn_entry.touched_partitions.put(txn_entry.base_partition, weight);
        }
        this.histogram_java_partitions.put(txn_entry.base_partition, txn_entry.weight);
    }

    /**
     * Returns whether a transaction is single-sited for the given catalog, and
     * the number of queries that were examined. This is where all the magic
     * happens
     * 
     * @param txn_trace
     * @param filter
     * @return
     * @throws Exception
     */
    protected TransactionCacheEntry processTransaction(CatalogContext catalogContext, TransactionTrace txn_trace, Filter filter) throws Exception {
        final long txn_id = txn_trace.getTransactionId();
        final int txn_weight = (this.use_txn_weights ? txn_trace.getWeight() : 1);
        final boolean debug_txn = DEBUG_TRACE_IDS.contains(txn_id);
        if (debug.val)
            LOG.debug(String.format("Processing new %s - Weight:%d", txn_trace, txn_weight));

        // Check whether we have a completed entry for this transaction already
        TransactionCacheEntry txn_entry = null;
        if (this.use_caching) {
            txn_entry = this.txn_entries.get(txn_id);

            // If we have a TransactionCacheEntry then we need to check that:
            // (1) It has a base partition
            // (2) All of its queries have been examined
            if (txn_entry != null && txn_entry.base_partition != HStoreConstants.NULL_PARTITION_ID && txn_entry.examined_queries == txn_trace.getQueries().size()) {
                if (trace.val)
                    LOG.trace("Using complete cached entry " + txn_entry);
                return (txn_entry);
            }
        }

        final Procedure catalog_proc = txn_trace.getCatalogItem(catalogContext.database);
        assert (catalog_proc != null);
        final String proc_key = CatalogKey.createKey(catalog_proc);

        // Initialize a new Cache entry for this txn
        if (txn_entry == null) {
            txn_entry = this.createTransactionCacheEntry(txn_trace, proc_key);
        }

        // We need to keep track of what partitions we have already added into
        // the various histograms
        // that we are using to keep track of things so that we don't have
        // duplicate entries
        // Make sure to use a new HashSet, otherwise our set will get updated
        // when the Histogram changes
        temp_txnOrigPartitions.clear();
        temp_txnOrigPartitions.addAll(txn_entry.touched_partitions.values());
        if (this.use_caching == false)
            assert (temp_txnOrigPartitions.isEmpty()) : txn_trace + " already has partitions?? " + temp_txnOrigPartitions;

        // If the partitioning parameter is set for the StoredProcedure and we
        // haven't gotten the
        // base partition (where the java executes), then yeah let's do that
        // part here
        int proc_param_idx = catalog_proc.getPartitionparameter();
        if (proc_param_idx != NullProcParameter.PARAM_IDX && txn_entry.base_partition == HStoreConstants.NULL_PARTITION_ID) {
            assert (proc_param_idx == RandomProcParameter.PARAM_IDX || proc_param_idx >= 0) : "Invalid ProcParameter Index " + proc_param_idx;
            assert (proc_param_idx < catalog_proc.getParameters().size()) : "Invalid ProcParameter Index " + proc_param_idx;

            int base_partition = HStoreConstants.NULL_DEPENDENCY_ID;
            try {
                base_partition = this.p_estimator.getBasePartition(catalog_proc, txn_trace.getParams(), true);
            } catch (Exception ex) {
                LOG.error("Unexpected error from PartitionEstimator for " + txn_trace, ex);
            }
            this.setBasePartition(txn_entry, base_partition);
            if (trace.val)
                LOG.trace("Base partition for " + txn_entry + " is '" + txn_entry.base_partition + "' using parameter #" + proc_param_idx);
        }

        if (trace.val)
            LOG.trace(String.format("%s [totalQueries=%d, examinedQueries=%d]\n%s", txn_entry, txn_entry.getTotalQueryCount(), txn_entry.getExaminedQueryCount(), txn_trace.debug(catalogContext.database)));

        // For each table, we need to keep track of the values that was used
        // when accessing their partition columns. This allows us to determine
        // whether we're hitting tables all on the same site
        // Table Key -> Set<Partition #>
        temp_stmtPartitions.clear();

        // Loop through each query that was executed and look at each table that
        // is referenced to see what attribute it is being looked up on.
        // -----------------------------------------------
        // Dear Andy of the future,
        //
        // The reason why we have a cost model here instead of just using the 
        // PartitionEstimator is because the PartitionEstimator only gives you
        // back information for single queries, whereas in this code we are trying 
        // figure out partitioning information for all of the queries in a transaction.
        //
        // Sincerely,
        // Andy of 04/22/2010
        // -----------------------------------------------
        assert (!txn_trace.getQueries().isEmpty());
        txn_entry.resetQueryCounters();
        long query_partitions = 0;
        if (debug_txn)
            LOG.info(txn_entry.debug());
        boolean txn_singlesited_orig = txn_entry.singlesited;
        boolean is_first = (txn_entry.getExaminedQueryCount() == 0);
        int query_idx = -1;
        for (QueryTrace query_trace : txn_trace.getQueries()) {
            // We don't want to multiple the query's weight by the txn's weight
            // because
            // we scale things appropriately for the txn outside of this loop
            int query_weight = (this.use_query_weights ? query_trace.getWeight() : 1);
            query_idx++;
            if (debug.val)
                LOG.debug("Examining " + query_trace + " from " + txn_trace);
            final Statement catalog_stmt = query_trace.getCatalogItem(catalogContext.database);
            assert (catalog_stmt != null);

            // If we have a filter and that filter doesn't want us to look at
            // this query, then we will just skip it and check the other ones
            if (filter != null && filter.apply(query_trace) != Filter.FilterResult.ALLOW) {
                if (trace.val)
                    LOG.trace(query_trace + " is filtered. Skipping...");
                txn_entry.unknown_queries += query_weight;
                continue;
            }

            // Check whether we have a cache entry for this QueryTrace
            QueryCacheEntry query_entry = txn_entry.query_entries[query_idx];
            if (this.use_caching && query_entry != null && !query_entry.isInvalid()) {
                if (trace.val)
                    LOG.trace("Got cached " + query_entry + " for " + query_trace);

                // Grab all of TableKeys in this QueryCacheEntry and add the
                // partitions that they touch
                // to the Statement partition map. We don't need to update the
                // TransactionCacheEntry
                // or any histograms because they will have been updated when
                // the QueryCacheEntry is created
                for (String table_key : query_entry.getTableKeys()) {
                    if (!temp_stmtPartitions.containsKey(table_key)) {
                        temp_stmtPartitions.put(table_key, new PartitionSet());
                    }
                    temp_stmtPartitions.get(table_key).addAll(query_entry.getPartitions(table_key));
                } // FOR
                txn_entry.examined_queries += query_weight;
                query_partitions += query_entry.getAllPartitions().size();

                // Create a new QueryCacheEntry
            } else {
                if (trace.val)
                    LOG.trace(String.format("Calculating new cost information for %s - Weight:%d", query_trace, query_weight));
                if (this.use_caching == false || query_entry == null) {
                    query_entry = new QueryCacheEntry(txn_trace, query_trace, query_weight);
                }
                this.query_ctr.addAndGet(query_weight);
                query_entry.invalid = false;

                // QUERY XREF
                if (this.use_caching) {
                    txn_entry.query_entries[query_idx] = query_entry;
                    String stmt_key = CatalogKey.createKey(catalog_stmt);
                    Set<QueryCacheEntry> cache = this.cache_stmtXref.get(stmt_key);
                    if (cache == null) {
                        cache = new HashSet<QueryCacheEntry>();
                        this.cache_stmtXref.put(stmt_key, cache);
                    }
                    cache.add(query_entry);
                }

                // Give the QueryTrace to the PartitionEstimator to get back a
                // mapping from TableKeys
                // to sets of partitions that were touched by the query.
                // XXX: What should we do if the TransactionCacheEntry's base
                // partition hasn't been calculated yet?
                // Let's just throw it at the PartitionEstimator and let it figure out what to do...
                Map<String, PartitionSet> table_partitions = this.p_estimator.getTablePartitions(query_trace, txn_entry.base_partition);
                StringBuilder sb = null;
                if (trace.val) {
                    sb = new StringBuilder();
                    sb.append("\n" + StringUtil.SINGLE_LINE + query_trace + " Table Partitions:");
                }
                assert (!table_partitions.isEmpty()) : "Failed to get back table partitions for " + query_trace;
                for (Entry<String, PartitionSet> e : table_partitions.entrySet()) {
                    assert (e.getValue() != null) : "Null table partitions for '" + e.getKey() + "'";

                    // If we didn't get anything back, then that means that we
                    // know we need to touch this
                    // table but the PartitionEstimator doesn't have enough
                    // information yet
                    if (e.getValue().isEmpty())
                        continue;
                    if (trace.val)
                        sb.append("\n  " + e.getKey() + ": " + e.getValue());

                    // Update the cache xref mapping so that we know this Table
                    // is referenced by this QueryTrace
                    // This will allow us to quickly find the QueryCacheEntry in
                    // invalidate()
                    if (this.use_caching) {
                        Set<QueryCacheEntry> cache = this.cache_tableXref.get(e.getKey());
                        if (cache == null) {
                            cache = new HashSet<QueryCacheEntry>();
                            this.cache_tableXref.put(e.getKey(), cache);
                        }
                        cache.add(query_entry);
                    }

                    // Ok, so now update the variables in our QueryCacheEntry
                    query_entry.singlesited = (query_entry.singlesited && e.getValue().size() == 1);
                    query_entry.addAllPartitions(e.getKey(), e.getValue());

                    // And then update the Statement partitions map to include
                    // all of the partitions
                    // that this query touched
                    if (!temp_stmtPartitions.containsKey(e.getKey())) {
                        temp_stmtPartitions.put(e.getKey(), e.getValue());
                    } else {
                        temp_stmtPartitions.get(e.getKey()).addAll(e.getValue());
                    }
                } // FOR (Entry<TableKey, Set<Partitions>>
                if (trace.val)
                    LOG.trace(sb.toString() + "\n" + query_trace.debug(catalogContext.database) + "\n" + StringUtil.SINGLE_LINE.trim());

                // Lastly, update the various histogram that keep track of which
                // partitions are accessed:
                //  (1) The global histogram for the cost model of partitions touched by all queries
                //  (2) The TransactionCacheEntry histogram of which partitions are touched by all queries
                // 
                // Note that we do not want to update the global histogram for the cost model on the
                // partitions touched by txns, because we don't want to count the same partition multiple times
                // Note also that we want to do this *outside* of the loop above, otherwise we will count
                // the same partition multiple times if the query references more than one table!
                this.histogram_query_partitions.put(query_entry.getAllPartitions(), query_weight * txn_weight);
                txn_entry.touched_partitions.put(query_entry.getAllPartitions(), query_weight);
                int query_num_partitions = query_entry.getAllPartitions().size();
                query_partitions += query_num_partitions;

                // If the number of partitions is zero, then we will classify
                // this query as currently
                // being unknown. This can happen if the query needs does a
                // single look-up on a replicated
                // table but we don't know the base partition yet
                if (query_num_partitions == 0) {
                    if (trace.val)
                        LOG.trace("# of Partitions for " + query_trace + " is zero. Marking as unknown for now");
                    txn_entry.unknown_queries += query_weight;
                    query_entry.unknown = true;
                    query_entry.invalid = true;
                } else {
                    txn_entry.examined_queries += query_weight;
                    query_entry.unknown = false;
                }
            } // if (new cache entry)

            // If we're not single-sited, well then that ruins it for everyone
            // else now doesn't it??
            if (query_entry.getAllPartitions().size() > 1) {
                if (debug.val)
                    LOG.debug(query_trace + " is being marked as multi-partition: " + query_entry.getAllPartitions());
                query_entry.singlesited = false;
                txn_entry.singlesited = false;
                txn_entry.multisite_queries += query_weight;
            } else {
                if (debug.val)
                    LOG.debug(query_trace + " is marked as single-partition");
                query_entry.singlesited = true;
                txn_entry.singlesite_queries += query_weight;
            }

            if (debug_txn)
                LOG.info(query_entry);
        } // FOR (QueryTrace)

        // Now just check whether this sucker has queries that touch more than one partition
        // We do this one first because it's the fastest and will pick up enough of them
        if (txn_entry.touched_partitions.getValueCount() > 1) {
            if (trace.val)
                LOG.trace(txn_trace + " touches " + txn_entry.touched_partitions.getValueCount() + " different partitions");
            txn_entry.singlesited = false;

            // Otherwise, now that we have processed all of queries that we
            // could, we need to check whether the values of the StmtParameters used on the partitioning
            // column of each table all hash to the same value. If they don't, then we know we can't
            // be single-partition
        } else {
            for (Entry<String, PartitionSet> e : temp_stmtPartitions.entrySet()) {
                String table_key = e.getKey();
                Table catalog_tbl = CatalogKey.getFromKey(catalogContext.database, table_key, Table.class);
                if (catalog_tbl.getIsreplicated()) {
                    continue;
                }

                Column table_partition_col = catalog_tbl.getPartitioncolumn();
                PartitionSet partitions = e.getValue();

                // If there is more than one partition, then we'll never be multi-partition 
                // so we can stop our search right here.
                if (partitions.size() > 1) {
                    if (trace.val)
                        LOG.trace(String.format("%s references %s's partitioning attribute %s on %d different partitions -- VALUES %s", catalog_proc.getName(), catalog_tbl.getName(),
                                table_partition_col.fullName(), partitions.size(), partitions));
                    txn_entry.singlesited = false;
                    break;

                // Make sure that the partitioning ProcParameter hashes to the same 
                // site as the value used on the partitioning column for this table
                } else if (!partitions.isEmpty() && txn_entry.base_partition != HStoreConstants.NULL_PARTITION_ID) {
                    int tbl_partition = CollectionUtil.first(partitions);
                    if (txn_entry.base_partition != tbl_partition) {
                        if (trace.val)
                            LOG.trace(txn_trace + " executes on Partition #" + txn_entry.base_partition + " " + "but partitioning column " + CatalogUtil.getDisplayName(table_partition_col) + " "
                                    + "references Partition #" + tbl_partition);
                        txn_entry.singlesited = false;
                        break;
                    }
                }
            } // FOR (table_key)
        }

        // Update the histograms if they are switching from multi-partition to
        // single-partition for the first time
        // NOTE: We always want to do this, even if this is the first time we've
        // looked at this txn
        // This is because createTransactionCacheEntry() will add this txn to
        // the single-partition histogram
        if (txn_singlesited_orig && !txn_entry.singlesited) {
            if (trace.val)
                LOG.trace("Switching " + txn_entry + " histogram info from single- to multi-partition [is_first=" + is_first + "]");
            this.histogram_sp_procs.dec(proc_key, txn_weight);
            this.histogram_mp_procs.put(proc_key, txn_weight);
        }

        // IMPORTANT: If the number of partitions touched in this txn have changed 
        // since before we examined a bunch of queries, then we need to update the
        // various histograms and counters.
        // This ensures that we do not double count partitions
        if (txn_entry.touched_partitions.getValueCount() != temp_txnOrigPartitions.size()) {
            assert (txn_entry.touched_partitions.getValueCount() > temp_txnOrigPartitions.size());
            // Remove the partitions that we already know that we touch and then
            // update
            // the histogram keeping track of which partitions our txn touches
            temp_txnNewPartitions.clear();
            temp_txnNewPartitions.addAll(txn_entry.touched_partitions.values());
            temp_txnNewPartitions.removeAll(temp_txnOrigPartitions);
            this.histogram_txn_partitions.put(temp_txnNewPartitions, txn_weight);
            if (trace.val)
                LOG.trace(String.format("Updating %s histogram_txn_partitions with %d new partitions [new_sample_count=%d, new_value_count=%d]\n%s", txn_trace, temp_txnNewPartitions.size(),
                        this.histogram_txn_partitions.getSampleCount(), this.histogram_txn_partitions.getValueCount(), txn_entry.debug()));
        }

        // // Sanity check
        // for (Object o : this.histogram_query_partitions.values()) {
        // int partition = (Integer)o;
        // long cnt = this.histogram_query_partitions.get(partition);
        // if (partition == 0) System.err.println(txn_trace + ": Partition0 = "
        // + cnt + " [histogram_txn=" +
        // this.histogram_txn_partitions.get(partition) + "]");
        // if (cnt > 0) assert(this.histogram_txn_partitions.get(partition) !=
        // 0) : "Histogram discrepency:\n" + txn_entry;
        // } // FOR
        // if (txn_trace.getId() % 100 == 0)
        // System.err.println(this.histogram_query_partitions);

        return (txn_entry);
    }

    /**
     * MAIN!
     * 
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD, ArgumentsParser.PARAM_PARTITION_PLAN);
        assert (args.workload.getTransactionCount() > 0) : "No transactions were loaded from " + args.workload_path;

        if (args.hasParam(ArgumentsParser.PARAM_CATALOG_HOSTS)) {
            ClusterConfiguration cc = new ClusterConfiguration(args.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS));
            args.updateCatalog(FixCatalog.cloneCatalog(args.catalog, cc), null);
        }

        // Enable compact output
        final boolean table_output = (args.getOptParams().contains("table"));

        // If given a PartitionPlan, then update the catalog
        File pplan_path = new File(args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN));
        PartitionPlan pplan = new PartitionPlan();
        pplan.load(pplan_path, args.catalogContext.database);
        if (args.getBooleanParam(ArgumentsParser.PARAM_PARTITION_PLAN_REMOVE_PROCS, false)) {
            for (Procedure catalog_proc : pplan.proc_entries.keySet()) {
                pplan.setNullProcParameter(catalog_proc);
            } // FOR
        }
        if (args.getBooleanParam(ArgumentsParser.PARAM_PARTITION_PLAN_RANDOM_PROCS, false)) {
            for (Procedure catalog_proc : pplan.proc_entries.keySet()) {
                pplan.setRandomProcParameter(catalog_proc);
            } // FOR
        }
        pplan.apply(args.catalogContext.database);

        System.out.println("Applied PartitionPlan '" + pplan_path + "' to catalog\n" + pplan);
        System.out.print(StringUtil.DOUBLE_LINE);
        // if (!table_output) {
        //
        // }
        // } else if (!table_output) {
        // System.err.println("PartitionPlan file '" + pplan_path +
        // "' does not exist. Ignoring...");
        // }
        if (args.hasParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT)) {
            String output = args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT);
            if (output.equals("-"))
                output = pplan_path.getAbsolutePath();
            pplan.save(new File(output));
            System.out.println("Saved PartitionPlan to '" + output + "'");
        }

        System.out.flush();

        // TODO: REMOVE STORED PROCEDURE ROUTING FOR SCHISM

        long singlepartition = 0;
        long multipartition = 0;
        long total = 0;
        SingleSitedCostModel costmodel = new SingleSitedCostModel(args.catalogContext);
        PartitionSet all_partitions = args.catalogContext.getAllPartitionIds();
        // costmodel.setEntropyWeight(4.0);
        // costmodel.setJavaExecutionWeightEnabled(true);
        // costmodel.setJavaExecutionWeight(100);

        // XXX: 2011-10-28
        costmodel.setCachingEnabled(true);
        ObjectHistogram<String> hist = new ObjectHistogram<String>();
        for (int i = 0; i < 2; i++) {
            ProfileMeasurement time = new ProfileMeasurement("costmodel").start();
            hist.clear();
            for (AbstractTraceElement<? extends CatalogType> element : args.workload) {
                if (element instanceof TransactionTrace) {
                    total++;
                    TransactionTrace xact = (TransactionTrace) element;
                    boolean is_singlesited = costmodel.processTransaction(args.catalogContext, xact, null).singlesited;
                    if (is_singlesited) {
                        singlepartition++;
                        hist.put(xact.getCatalogItemName());
                    } else {
                        multipartition++;
                        if (!hist.contains(xact.getCatalogItemName()))
                            hist.put(xact.getCatalogItemName(), 0);
                    }
                }
            } // FOR
            System.err.println("ESTIMATE TIME: " + time.stop().getTotalThinkTimeSeconds());
            break; // XXX
        } // FOR
        // long total_partitions_touched_txns =
        // costmodel.getTxnPartitionAccessHistogram().getSampleCount();
        // long total_partitions_touched_queries =
        // costmodel.getQueryPartitionAccessHistogram().getSampleCount();

        Histogram<Integer> h = null;
        if (!table_output) {
            System.out.println("Workload Procedure Histogram:");
            System.out.println(StringUtil.addSpacers(args.workload.getProcedureHistogram().toString()));
            System.out.print(StringUtil.DOUBLE_LINE);

            System.out.println("SinglePartition Procedure Histogram:");
            System.out.println(StringUtil.addSpacers(hist.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);

            System.out.println("Java Execution Histogram:");
            h = costmodel.getJavaExecutionHistogram();
            h.setKeepZeroEntries(true);
            h.put(all_partitions, 0);
            System.out.println(StringUtil.addSpacers(h.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);

            System.out.println("Transaction Partition Histogram:");
            h = costmodel.getTxnPartitionAccessHistogram();
            h.setKeepZeroEntries(true);
            h.put(all_partitions, 0);
            System.out.println(StringUtil.addSpacers(h.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);

            System.out.println("Query Partition Touch Histogram:");
            h = costmodel.getQueryPartitionAccessHistogram();
            h.setKeepZeroEntries(true);
            h.put(all_partitions, 0);
            System.out.println(StringUtil.addSpacers(h.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);
        }

        Map<String, Object> maps[] = new Map[2];
        int idx = 0;
        LinkedHashMap<String, Object> m = null;

        // Execution Cost
        m = new LinkedHashMap<String, Object>();
        m.put("SINGLE-PARTITION", singlepartition);
        m.put("MULTI-PARTITION", multipartition);
        m.put("TOTAL", total + " [" + singlepartition / (double) total + "]");
        m.put("PARTITIONS TOUCHED (TXNS)", costmodel.getTxnPartitionAccessHistogram().getSampleCount());
        m.put("PARTITIONS TOUCHED (QUERIES)", costmodel.getQueryPartitionAccessHistogram().getSampleCount());
        maps[idx++] = m;

        // Utilization
        m = new LinkedHashMap<String, Object>();
        costmodel.getJavaExecutionHistogram().setKeepZeroEntries(false);
        int active_partitions = costmodel.getJavaExecutionHistogram().getValueCount();
        m.put("ACTIVE PARTITIONS", active_partitions);
        m.put("IDLE PARTITIONS", (all_partitions.size() - active_partitions));
        // System.out.println("Partitions Touched By Queries: " +
        // total_partitions_touched_queries);

        Histogram<Integer> entropy_h = costmodel.getJavaExecutionHistogram();
        m.put("JAVA SKEW", SkewFactorUtil.calculateSkew(all_partitions.size(), entropy_h.getSampleCount(), entropy_h));

        entropy_h = costmodel.getTxnPartitionAccessHistogram();
        m.put("TRANSACTION SKEW", SkewFactorUtil.calculateSkew(all_partitions.size(), entropy_h.getSampleCount(), entropy_h));

        // TimeIntervalCostModel<SingleSitedCostModel> timecostmodel = new
        // TimeIntervalCostModel<SingleSitedCostModel>(args.catalog_db,
        // SingleSitedCostModel.class, 1);
        // timecostmodel.estimateCost(args.catalog_db, args.workload);
        // double entropy = timecostmodel.getLastEntropyCost()
        m.put("UTILIZATION", (costmodel.getJavaExecutionHistogram().getValueCount() / (double) all_partitions.size()));
        maps[idx++] = m;

        System.out.println(StringUtil.formatMaps(maps));
    }
}
