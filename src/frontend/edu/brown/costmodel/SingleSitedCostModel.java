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
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.designer.partitioners.PartitionPlan;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.Workload;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload.Filter;

/**
 * @author pavlo
 */
public class SingleSitedCostModel extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(SingleSitedCostModel.class);
    
    private static final Set<Long> DEBUG_TRACE_IDS = new HashSet<Long>();
    static {
//        DEBUG_TRACE_IDS.add(1251416l);
    }

    
    // ----------------------------------------------------
    // COST WEIGHTS
    // ----------------------------------------------------

    public static double COST_UNKNOWN_QUERY = 1d;
    public static double COST_SINGLESITE_QUERY = 1d;
    public static double COST_MULTISITE_QUERY = 10d;

    // ----------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------

    /**
     * TransactionTrace Id -> TransactionCacheEntry
     */
    protected final Map<Long, TransactionCacheEntry> txn_entries = new LinkedHashMap<Long, TransactionCacheEntry>();

    /**
     * QueryTrace Id -> QueryCacheEntry
     */
    protected final Map<Long, QueryCacheEntry> query_entries = new LinkedHashMap<Long, QueryCacheEntry>();

    /**
     * TableKeys that were replicated when we calculated them
     * It means that have to switch the partitions to be the base partition (if we have one)
     */
    protected final Set<String> replicated_tables = new HashSet<String>();

    /**
     * For each procedure, the list of tables that are accessed in the 
     * ProcedureKey -> Set<TableKey>
     */
    protected final Map<String, Set<String>> touched_tables = new HashMap<String, Set<String>>();
    
    
    /**
     * Table Key -> Set<QueryCacheEntry>
     */
    protected final Map<String, Set<QueryCacheEntry>> cache_tbl_xref = new LinkedHashMap<String, Set<QueryCacheEntry>>();

    /**
     * Procedure Key -> Set<TransactionCacheEntry>
     */
    protected final Map<String, Set<TransactionCacheEntry>> cache_proc_xref = new LinkedHashMap<String, Set<TransactionCacheEntry>>();

    /**
     * Cost Estimate Explanation
     */
    public class TransactionCacheEntry implements Cloneable {
        private final String proc_key;
        private final long txn_trace_id;
        private final int total_queries;
        private boolean singlesited = true;
        private Integer base_partition = null;
        private int examined_queries = 0;
        private int singlesite_queries = 0;
        private int multisite_queries = 0;
        private int unknown_queries = 0;
        private final Histogram touched_partitions = new Histogram();

        public TransactionCacheEntry(String proc_key, long txn_trace_id, int total_queries) {
            this.proc_key = proc_key;
            this.txn_trace_id = txn_trace_id;
            this.total_queries = total_queries;
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
        
        public Set<Integer> getAllTouchedPartitions() {
            Set<Integer> partitions = this.touched_partitions.values();
            if (this.base_partition != null && !partitions.contains(this.base_partition)) {
                partitions = new HashSet<Integer>();
                for (Object o : this.touched_partitions.values())
                    partitions.add((Integer) o);
                partitions.add(this.base_partition);
            }
            return (partitions);
        }

        public Histogram getAllTouchedPartitionsHistogram() {
            Histogram copy = new Histogram(this.touched_partitions);
            assert (this.touched_partitions.getValueCount() == copy.getValueCount());
            assert (this.touched_partitions.getSampleCount() == copy.getSampleCount());
            if (this.base_partition != null && !copy.contains(this.base_partition)) {
                copy.put(this.base_partition);
            }
            return (copy);
        }

        public boolean isComplete() {
            return (this.total_queries == this.examined_queries);
        }

        public boolean isSingleSited() {
            return this.singlesited;
        }
        protected void setSingleSited(boolean singlesited) {
            this.singlesited = singlesited;
        }

        public Integer getExecutionPartition() {
            return (this.base_partition);
        }

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

        public Set<Integer> getTouchedPartitions() {
            return (this.touched_partitions.values());
        }

        @Override
        protected TransactionCacheEntry clone() throws CloneNotSupportedException {
            TransactionCacheEntry clone = new TransactionCacheEntry(this.proc_key, this.txn_trace_id, this.total_queries);
            clone.base_partition = this.base_partition;
            clone.singlesited = this.singlesited;
            clone.examined_queries = this.examined_queries;
            clone.singlesite_queries = this.singlesite_queries;
            clone.multisite_queries = this.multisite_queries;
            clone.unknown_queries = this.unknown_queries;
            clone.touched_partitions.putHistogram(this.touched_partitions);
            return (clone);
        }

        @Override
        public String toString() {
            return ("TransactionCacheEntry[" + CatalogKey.getNameFromKey(this.proc_key) + ":Trace#" + this.txn_trace_id + "]");
        }

        public String debug() {
            StringBuilder buffer = new StringBuilder();
            buffer.append(this.toString() + "\n");
            buffer.append("base_partition:      ").append(this.base_partition).append("\n");
            buffer.append("singlesited_xact:    ").append(this.singlesited).append("\n");
            buffer.append("total_queries:       ").append(this.total_queries).append("\n");
            buffer.append("examined_queries:    ").append(this.examined_queries).append("\n");
            buffer.append("singlesite_queries:  ").append(this.singlesite_queries).append("\n");
            buffer.append("multisite_queries:   ").append(this.multisite_queries).append("\n");
            buffer.append("unknown_queries:     ").append(this.unknown_queries).append("\n");
            buffer.append("touched_partitions:  ").append(this.touched_partitions.getSampleCount()).append("\n")
                                                  .append(StringUtil.addSpacers(this.touched_partitions.toString()));
            return (buffer.toString());
        }
    }

    /**
     * Query Cache Entry
     */
    public class QueryCacheEntry implements Cloneable {
        public final long txn_trace_id;
        public final long query_trace_id;
        public boolean singlesited = true;
        public boolean invalid = false;
        public boolean unknown = false;

        // Table Key -> Set<Partition #>
        private final Map<String, Set<Integer>> partitions = new HashMap<String, Set<Integer>>();

        // All partitions
        private final Set<Integer> all_partitions = new HashSet<Integer>();

        /**
         * Constructor
         * 
         * @param xact_id
         * @param query_id
         */
        private QueryCacheEntry(long xact_id, long query_id) {
            this.txn_trace_id = xact_id;
            this.query_trace_id = query_id;
        }

        public QueryCacheEntry(TransactionTrace xact, QueryTrace query) {
            this(xact.getId(), query.getId());
        }

        public long getTransactionId() {
            return (this.txn_trace_id);
        }

        public long getQueryId() {
            return (this.query_trace_id);
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
            for (Entry<String, Set<Integer>> e : this.partitions.entrySet()) {
                if (e.getKey().equals(table_key)) continue;
                if (e.getValue().contains(partition)) {
                    found = true;
                    break;
                }
            } // FOR
            if (!found) this.all_partitions.remove(partition);
        }

        public void addAllPartitions(String table_key, Collection<Integer> partitions) {
            this.getPartitions(table_key).addAll(partitions);
            this.all_partitions.addAll(partitions);
        }

        private Set<Integer> getPartitions(String table_key) {
            Set<Integer> p = this.partitions.get(table_key);
            if (p == null) {
                p = new HashSet<Integer>();
                this.partitions.put(table_key, p);
            }
            return (p);
        }

        public Set<Integer> getAllPartitions() {
            return (this.all_partitions);
        }

        public Set<String> getTableKeys() {
            return (this.partitions.keySet());
        }

        public Map<String, Set<Integer>> getPartitionValues() {
            return (this.partitions);
        }

        @Override
        protected QueryCacheEntry clone() throws CloneNotSupportedException {
            QueryCacheEntry clone = new QueryCacheEntry(this.txn_trace_id, this.query_trace_id);
            clone.singlesited = this.singlesited;
            clone.invalid = this.invalid;
            clone.all_partitions.addAll(this.all_partitions);
            for (String key : this.partitions.keySet()) {
                clone.partitions.put(key, new HashSet<Integer>());
                clone.partitions.get(key).addAll(this.partitions.get(key));
            } // FOR
            return (clone);
        }

        @Override
        public String toString() {
            // You know you just love this!
            return new StringBuilder()
                        .append("QueryCacheEntry[")
                        .append("xact_trace_id=").append(this.txn_trace_id).append(", ")
                        .append("query_trace_id=").append(this.query_trace_id).append(", ")
                        .append("singlesited=").append(this.singlesited).append(", ")
                        .append("partition_values=").append(this.partitions).append(", ")
                        .append("invalid=").append(this.invalid).append(", ")
                        .append("unknown=").append(this.unknown)
                        .append("]").toString();
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
    public SingleSitedCostModel(Database catalog_db) {
        this(catalog_db, new PartitionEstimator(catalog_db)); // FIXME
    }
    
    /**
     * I forget why we need this...
     */
    public SingleSitedCostModel(Database catalog_db, PartitionEstimator p_estimator) {
        super(SingleSitedCostModel.class, catalog_db, p_estimator);

        // Initialize cache data structures
        if (catalog_db != null) {
            for (String table_key : CatalogKey.createKeys(catalog_db.getTables())) {
                this.cache_tbl_xref.put(table_key, new HashSet<QueryCacheEntry>());
            } // FOR
     
            // List of tables touched by each partition
            try {
                for (Procedure catalog_proc : catalog_db.getProcedures()) {
                    if (catalog_proc.getSystemproc()) continue;
                    String proc_key = CatalogKey.createKey(catalog_proc);
                    Collection<String> table_keys = CatalogKey.createKeys(CatalogUtil.getReferencedTables(catalog_proc));
                    this.touched_tables.put(proc_key, new HashSet<String>(table_keys));
                    if (LOG.isTraceEnabled()) LOG.trace(catalog_proc + " Touched Tables: " + table_keys);
                } // FOR
            } catch (Exception ex) {
                LOG.fatal("Unexpected error", ex);
                System.exit(1);
            }
        }
    }

    @Override
    public void clear(boolean force) {
        super.clear(force);
        this.cache_proc_xref.clear();
        this.cache_tbl_xref.clear();
        this.query_entries.clear();
        this.txn_entries.clear();

        assert(this.histogram_txn_partitions.getSampleCount() == 0);
        assert(this.histogram_txn_partitions.getValueCount() == 0);
        assert(this.histogram_query_partitions.getSampleCount() == 0);
        assert(this.histogram_query_partitions.getValueCount() == 0);
    }

    /**
     * Awesome! Creates a deep cloned copy of the cache entries
     */
    @Override
    public SingleSitedCostModel clone(Database catalog_db) throws CloneNotSupportedException {
        SingleSitedCostModel clone = new SingleSitedCostModel(catalog_db, this.p_estimator);

        // Histograms
        // TODO(pavlo) this.histogram_access

        // Table Query Cache
        for (String key : this.cache_tbl_xref.keySet()) {
            clone.cache_tbl_xref.put(key, new HashSet<QueryCacheEntry>());
            for (QueryCacheEntry entry : this.cache_tbl_xref.get(key)) {
                if (clone.query_entries.containsKey(entry.getQueryId())) {
                    QueryCacheEntry clone_entry = entry.clone();
                    clone.cache_tbl_xref.get(key).add(clone_entry);
                    clone.query_entries.put(entry.getQueryId(), clone_entry);
                }
            } // FOR
        } // FOR

        // Transaction Cache
        for (Long key : this.txn_entries.keySet()) {
            TransactionCacheEntry entry = this.txn_entries.get(key);
            TransactionCacheEntry clone_entry = entry.clone();
            clone.txn_entries.put(key, clone_entry);
        } // FOR

        return (clone);
    }
    
    public Collection<TransactionCacheEntry> getTransactionCacheEntries() {
        return (this.txn_entries.values());
    }
    public Collection<QueryCacheEntry> getQueryCacheEntries() {
        return (this.query_entries.values());
    }

    public TransactionCacheEntry getTransactionCacheEntry(TransactionTrace xact) {
        return (this.txn_entries.get(xact.getId()));
    }
    
    public Set<QueryCacheEntry> getQueryCacheEntries(TransactionTrace xact) {
        Set<QueryCacheEntry> ret = new HashSet<QueryCacheEntry>();
        for (QueryTrace query : xact.getQueries()) {
            ret.add(this.query_entries.get(query.getId()));
        } // FOR
        return (ret);
    }
    
    public Set<QueryCacheEntry> getQueryCacheEntries(long txn_trace_id) {
        Set<QueryCacheEntry> ret = new HashSet<QueryCacheEntry>();
        for (QueryCacheEntry qce : this.query_entries.values()) {
            if (qce.txn_trace_id == txn_trace_id) ret.add(qce);
        } // FOR
        return (ret);
    }
    
    @Override
    public void prepareImpl(final Database catalog_db) {
        final boolean trace = LOG.isTraceEnabled(); 
        if (trace) LOG.trace("Prepare called!");
        
        // This is the start of a new run through the workload, so we need to
        // reinit our PartitionEstimator
        // so that we are getting the proper catalog objects back
        this.p_estimator.initCatalog(catalog_db);
        this.num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        
        // Recompute which tables have switched from Replicated to Non-Replicated
        this.replicated_tables.clear();
        for (Table catalog_tbl : catalog_db.getTables()) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            if (catalog_tbl.getIsreplicated()) {
                this.replicated_tables.add(table_key);
            }
            if (trace) LOG.trace(catalog_tbl + " => isReplicated(" + this.replicated_tables.contains(table_key) + ")");
        } // FOR
    }

    /**
     * 
     */
    @Override
    public double estimateTransactionCost(Database catalog_db, Workload workload, Workload.Filter filter, TransactionTrace xact) throws Exception {
        // Sanity Check: If we don't have any TransactionCacheEntries, then the histograms should all be wiped out!
        if (this.txn_entries.size() == 0) {
            assert(this.histogram_txn_partitions.isEmpty()) : this.histogram_txn_partitions;
            assert(this.histogram_query_partitions.isEmpty()) : this.histogram_query_partitions;
        }
        
        TransactionCacheEntry est = this.processTransaction(catalog_db, xact, filter);
        assert (est != null);
        if (LOG.isDebugEnabled()) LOG.debug(xact + ": " + (est.singlesited ? "Single" : "Multi"));

        if (!est.singlesited) {
            return (COST_MULTISITE_QUERY);
        }
        if (est.unknown_queries > 0) {
            return (COST_UNKNOWN_QUERY);
        }
        return (COST_SINGLESITE_QUERY);
    }

    // Keep track of what Transactions were modified in this process so that 
    // we can go back and clean-up their touched_partitions histograms
    // This histogram will keep track of what partitions we need to remove from the
    // query access histogram
    // 2010-11-05: I moved this out because the profiler said that we were spending to much time allocating them over
    //             and over again
//    private final Set<TransactionCacheEntry> invalidate_modifiedTxns = new HashSet<TransactionCacheEntry>();
//    private final Set<String> invalidate_targetKeys = new HashSet<String>();
//    private final Histogram invalidate_removedTouchedPartitions = new Histogram();
    
    /**
     * Invalidate cache entries for the given CatalogKey
     * 
     * @param catalog_key
     */
    @Override
    public synchronized void invalidateCache(String catalog_key) {
        if (!this.use_caching) return;
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();
        int query_ctr = 0;
        int txn_ctr = 0;

        Set<TransactionCacheEntry> invalidate_modifiedTxns = new HashSet<TransactionCacheEntry>();
        Set<String> invalidate_targetKeys = new HashSet<String>();
        Histogram invalidate_removedTouchedPartitions = new Histogram();
        
        // ---------------------------------------------
        // Table Key
        // ---------------------------------------------
        if (this.cache_tbl_xref.containsKey(catalog_key)) {
            if (trace) LOG.trace("Invalidate Cache for Table '" + CatalogKey.getNameFromKey(catalog_key) + "'");


//            this.invalidate_modifiedTxns.clear();
//            this.invalidate_targetKeys.clear();
//            this.invalidate_removedTouchedPartitions.clear();
            
            for (QueryCacheEntry query_entry : this.cache_tbl_xref.get(catalog_key)) {
                if (query_entry.isInvalid()) continue;
                if (trace) LOG.trace("Invalidate QueryCacheEntry:" + query_entry);
                
                // Grab the TransactionCacheEntry and enable zero entries in its touched_partitions
                // This will ensure that we know which partitions to remove from the costmodel's 
                // global txn touched partitions histogram 
                final long txn_trace_id = query_entry.getTransactionId();
                TransactionCacheEntry txn_entry = this.txn_entries.get(query_entry.getTransactionId());
                assert (txn_entry != null);
                txn_entry.touched_partitions.setKeepZeroEntries(true);
                
                invalidate_modifiedTxns.add(txn_entry);
                
                if (query_entry.isUnknown()) {
                    txn_entry.unknown_queries--;
                } else {
                    txn_entry.examined_queries--;
                }
                if (query_entry.isSinglesited()) {
                    txn_entry.singlesite_queries--;
                } else {
                    txn_entry.multisite_queries--;
                }
                
                // DEBUG!
                if (txn_entry.singlesite_queries < 0 || txn_entry.multisite_queries < 0) {
                    System.err.println("!!! NEGATIVE QUERY COUNTS - TRACE #" + txn_trace_id + " !!!");
                    System.err.println(txn_entry.debug());
                    System.err.println("-----------------------------");
                    for (QueryCacheEntry q : this.getQueryCacheEntries(txn_trace_id)) {
                        System.err.println(q + "\n");
                    }
                }
                assert(txn_entry.examined_queries >= 0) : txn_entry + " has negative examined queries!\n" +  txn_entry.debug();
                assert(txn_entry.unknown_queries >= 0) : txn_entry + " has negative unknown queries!\n" +  txn_entry.debug();
                assert(txn_entry.singlesite_queries >= 0) : txn_entry + " has negative singlesited queries!\n" +  txn_entry.debug();
                assert(txn_entry.multisite_queries >= 0) : txn_entry + " has negative multisited queries!\n" +  txn_entry.debug();
                
                // Populate this histogram so that we know what to remove from the global histogram
                invalidate_removedTouchedPartitions.putAll(query_entry.getAllPartitions());
                
                // Remove the partitions this query touches from the txn's touched partitions histogram
                txn_entry.touched_partitions.removeValues(query_entry.getAllPartitions());

                // If this transaction is out of queries, then we'll remove it completely
                if (txn_entry.examined_queries == 0) {
                    if (trace) LOG.trace("Removing Transaction:" + txn_entry);
                    
                    // If we have a base partition value, then we have to remove an entry from the
                    // histogram that keeps track of where the java executes
                    if (txn_entry.base_partition != null) {
                        if (trace) LOG.trace("Resetting base partition [" +  txn_entry.base_partition + "] and updating histograms");
                        // NOTE: We have to remove the base_partition from these histograms but not the
                        // histogram_txn_partitions because we will do that down below
                        this.histogram_java_partitions.remove(txn_entry.base_partition);
                        if (this.isJavaExecutionWeightEnabled()) txn_entry.touched_partitions.remove(txn_entry.base_partition, this.getJavaExecutionWeight());
                        if (trace) LOG.trace("Global Java Histogram:\n" + this.histogram_java_partitions);
                    }
                    
                    this.histogram_procs.remove(txn_entry.proc_key);
                    this.txn_ctr.decrementAndGet();
                    this.txn_entries.remove(query_entry.getTransactionId());
                    this.cache_proc_xref.get(txn_entry.proc_key).remove(txn_entry);
                    this.txn_entries.remove(query_entry.getTransactionId());
                    txn_ctr++;
                }

                // Make sure that we do this last so we can subtract values from the TranasctionCacheEntry
                query_entry.invalid = true;
                query_entry.singlesited = true;
                query_entry.unknown = true;
                for (Set<Integer> q_partitions : query_entry.partitions.values()) {
                    q_partitions.clear();
                } // FOR
                query_entry.all_partitions.clear();

                this.query_ctr.decrementAndGet();
                query_ctr++;
            } // FOR
            
            // We can now remove the touched query partitions if we have any
            if (!invalidate_removedTouchedPartitions.isEmpty()) {
                if (trace) LOG.trace("Removing " + invalidate_removedTouchedPartitions.getSampleCount() + " partition touches for " + query_ctr + " queries");
                this.histogram_query_partitions.removeHistogram(invalidate_removedTouchedPartitions);
            }
            
        // ---------------------------------------------
        // Procedure Key
        // ---------------------------------------------
        } else if (this.cache_proc_xref.containsKey(catalog_key)) {
            if (trace) LOG.trace("Invalidate Cache for Procedure '" + CatalogKey.getNameFromKey(catalog_key) + "'");

            invalidate_modifiedTxns.clear();
            invalidate_targetKeys.clear();
            
            // NEW: If this procedure accesses any table that is replicated, then we also need to invalidate
            // the cache for that table so that the tables are wiped 
                
            // We just need to unset the base partition
            for (TransactionCacheEntry txn_entry : this.cache_proc_xref.get(catalog_key)) {
                assert (txn_entry != null);
                if (txn_entry.base_partition != null) {
                    if (trace) LOG.trace("Unset base_partition for " + txn_entry);
                    txn_entry.touched_partitions.setKeepZeroEntries(true);
                    this.histogram_java_partitions.remove(txn_entry.base_partition);
                    if (this.isJavaExecutionWeightEnabled()) txn_entry.touched_partitions.remove(txn_entry.base_partition, this.getJavaExecutionWeight());
                    txn_entry.base_partition = null;
                    invalidate_modifiedTxns.add(txn_entry);
                    txn_ctr++;
                }
            } // FOR
            
            // If this procedure touches any replicated tables, then blow away the cache for that 
            // so that we get new base partition calculations. It's just easier this way
            for (String table_key : this.touched_tables.get(catalog_key)) {
                if (this.replicated_tables.contains(table_key)) {
                    if (trace) LOG.trace(catalog_key + " => " + table_key + ": is replicated and will need to be invalidated too!");
                    invalidate_targetKeys.add(table_key);
                }
            } // FOR
        }
        
        // Update the TransactionCacheEntry objects that we modified in the loop above
        if (trace && !invalidate_modifiedTxns.isEmpty()) LOG.trace("Updating partition information for " + invalidate_modifiedTxns.size() + " TransactinCacheEntries");
        for (TransactionCacheEntry txn_entry : invalidate_modifiedTxns) {
            // Get the list of partitions that are no longer being touched by this txn
            // We remove these from the costmodel's global txn touched histogram
            Set<Integer> zero_partitions = txn_entry.touched_partitions.getValuesForCount(0);
            if (!zero_partitions.isEmpty()) {
                if (trace) LOG.trace("Removing " + zero_partitions.size() + " partitions for " + txn_entry);
                this.histogram_txn_partitions.removeValues(zero_partitions);
            }
            
            // Then disable zero entries from the histogram so that our counts don't get screwed up 
            txn_entry.touched_partitions.setKeepZeroEntries(false);
            
            // Then check whether we're still considered multi-partition
            boolean new_singlesited = (txn_entry.multisite_queries == 0);
            if (!txn_entry.singlesited && new_singlesited) {
                if (trace) LOG.trace("Switching " + txn_entry + " from multi-partition to single-partition");
//                System.err.println("SingleP:\n" + this.histogram_sp_procs);
//                System.err.println("MultiP:\n" + this.histogram_mp_procs);
                this.histogram_mp_procs.remove(txn_entry.getProcedureKey());
                if (txn_entry.examined_queries > 0) this.histogram_sp_procs.put(txn_entry.getProcedureKey());
            } else if (txn_entry.singlesited && txn_entry.examined_queries == 0) {
                this.histogram_sp_procs.remove(txn_entry.getProcedureKey());
            }
            txn_entry.singlesited = new_singlesited;
        } // FOR
        
        // Sanity Check: If we don't have any TransactionCacheEntries, then the histograms should all be wiped out!
        if (this.txn_entries.size() == 0) {
            if (!this.histogram_java_partitions.isEmpty() || !this.histogram_txn_partitions.isEmpty() || !this.histogram_query_partitions.isEmpty()) {
                System.err.println("MODIFIED TXNS: " + invalidate_modifiedTxns.size());
                for (TransactionCacheEntry txn_entry : invalidate_modifiedTxns) {
                    System.err.println(txn_entry.debug() + "\n");
                }
            }
            assert(this.histogram_mp_procs.isEmpty()) : this.histogram_mp_procs;
            assert(this.histogram_sp_procs.isEmpty()) : this.histogram_sp_procs;
            assert(this.histogram_java_partitions.isEmpty()) : this.histogram_java_partitions;
            assert(this.histogram_txn_partitions.isEmpty()) : this.histogram_txn_partitions;
            assert(this.histogram_query_partitions.isEmpty()) : this.histogram_query_partitions;
        }
        assert(this.txn_entries.size() == this.txn_ctr.get()) : this.txn_entries.size() + " == " + this.txn_ctr.get();
        
        if (debug && (query_ctr > 0 || txn_ctr > 0))
            LOG.debug("Invalidated Cache [" + catalog_key + "]: Queries=" + query_ctr + ", Txns=" + txn_ctr);
        
        if (!invalidate_targetKeys.isEmpty()) {
            if (debug) LOG.debug("Calling invalidateCache for " + invalidate_targetKeys.size() + " dependent catalog items of " + catalog_key);
            
            // We have to make a copy here, otherwise the recursive call will blow away our list
            for (String next_catalog_key : new HashSet<String>(invalidate_targetKeys)) {
                this.invalidateCache(next_catalog_key);
            } // FOR
        }
    }

    /**
     * Create a new TransactionCacheEntry and update our histograms appropriately
     * @param txn_trace
     * @param proc_key
     * @return
     */
    protected TransactionCacheEntry createTransactionCacheEntry(TransactionTrace txn_trace, String proc_key) {
        final boolean trace = LOG.isTraceEnabled();
        
        if (this.use_caching && !this.cache_proc_xref.containsKey(proc_key)) {
            this.cache_proc_xref.put(proc_key, new HashSet<TransactionCacheEntry>());
        }

        TransactionCacheEntry txn_entry = new TransactionCacheEntry(proc_key, txn_trace.getId(), txn_trace.getQueries().size());
        this.txn_entries.put(txn_trace.getId(), txn_entry);
        if (this.use_caching) {
            this.cache_proc_xref.get(proc_key).add(txn_entry);
        }
        if (trace) LOG.trace("New " + txn_entry);

        // Update txn counter
        this.txn_ctr.incrementAndGet();

        // Record that we executed this procedure
        this.histogram_procs.put(proc_key);
        
        // Always record that it was single-partition in the beginning... we can switch later on
        this.histogram_sp_procs.put(proc_key);
        
        return (txn_entry);
    }

    /**
     * 
     * @param txn_entry
     * @param txn_trace
     * @param catalog_proc
     * @param proc_param_idx
     */
    protected void setBasePartition(TransactionCacheEntry txn_entry, Integer base_partition) {
        txn_entry.base_partition = base_partition;
        
        // If the partition is null, then there's nothing we can do here other than just pick a random one
        // For now we'll always pick zero to keep things consistent
        if (txn_entry.base_partition == null) txn_entry.base_partition = 0;

        // Record what partition the VoltProcedure executed on
        // We'll throw the base_partition into the txn_entry's touched partitions histogram, but notice
        // that we can weight how much the java execution costs
        if (this.isJavaExecutionWeightEnabled()) {
            txn_entry.touched_partitions.put(txn_entry.base_partition, this.getJavaExecutionWeight());
        }
        this.histogram_java_partitions.put(txn_entry.base_partition);
    }
    
    /**
     * Returns whether a transaction is single-sited for the given catalog, and
     * the number of queries that were examined.
     * 
     * @param txn_trace
     * @param filter
     * @return
     * @throws Exception
     */
    public TransactionCacheEntry processTransaction(Database catalog_db, TransactionTrace txn_trace, Workload.Filter filter) throws Exception {
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug_txn = DEBUG_TRACE_IDS.contains(txn_trace.getId());
        if (debug) LOG.debug("Processing new " + txn_trace.toString());
        
        // Check whether we have a completed entry for this transaction already
        TransactionCacheEntry txn_entry = this.txn_entries.get(txn_trace.getId());
        if (txn_entry != null && this.isCachingEnabled() &&
            txn_entry.examined_queries == txn_trace.getQueries().size() &&
            txn_entry.base_partition != null) {
            if (trace) LOG.trace("Using complete cached entry " + txn_entry);
            return (txn_entry);
        }

        this.num_partitions = CatalogUtil.getNumberOfPartitions(catalog_db);
        Procedure catalog_proc = txn_trace.getCatalogItem(catalog_db);
        assert (catalog_proc != null);
        String proc_key = CatalogKey.createKey(catalog_proc);
        int num_partitions = CatalogUtil.getNumberOfPartitions(catalog_proc);

        // Initialize a new Cache entry for this txn
        if (txn_entry == null) {
            txn_entry = this.createTransactionCacheEntry(txn_trace, proc_key);
        }

        // We need to keep track of what partitions we have already added into the various histograms
        // that we are using to keep track of things so that we don't have duplicate entries
        // Make sure to use a new HashSet, otherwise our set will get updated when the Histogram changes
        Set<Integer> temp = txn_entry.touched_partitions.values();
        Set<Integer> orig_partitions = new HashSet<Integer>(temp);
        if (!this.use_caching)
            assert (orig_partitions.isEmpty()) : txn_trace + " already has partitions?? " + orig_partitions;

        // If the partitioning parameter is set for the StoredProcedure and we haven't gotten the 
        // base partition (where the java executes), then yeah let's do that part here
        int proc_param_idx = catalog_proc.getPartitionparameter();
        if (proc_param_idx != NullProcParameter.PARAM_IDX && txn_entry.base_partition == null) {
            assert (proc_param_idx >= 0) : "Invalid ProcParameter Index " + proc_param_idx;
            assert (proc_param_idx < catalog_proc.getParameters().size()) : "Invalid ProcParameter Index " + proc_param_idx;
            
            Integer base_partition = null; 
            try {
                base_partition = this.p_estimator.getBasePartition(catalog_proc, txn_trace.getParams(), true);
            } catch (Exception ex) {
                LOG.error("Unexpected error from PartitionEstimator for " + txn_trace, ex);
            }
            this.setBasePartition(txn_entry, base_partition);
            if (trace) LOG.trace("Base partition for " + catalog_proc + " is '" + txn_entry.base_partition + "' using parameter #" + catalog_proc.getParameters().get(proc_param_idx));
        }

        if (trace)
            LOG.trace("Checking whether instance of " + catalog_proc.getName() + " is single-partition [" +
                      "num_queries=" + txn_trace.getQueryCount() + ", partition_count=" + num_partitions + "]");

        // For each table, we need to keep track of the values that was used
        // when accessing their partition columns. This allows us to determine
        // whether we're hitting tables all on the same site
        // Table Key -> Set<Partition #>
        Map<String, Set<Integer>> stmt_partitions = new HashMap<String, Set<Integer>>();

        // Loop through each query that was executed and look at each table that
        // is referenced to see what attribute it is being looked up on.
        // -----------------------------------------------
        // Dear Andy of the future,
        //
        // The reason why we have a cost model here instead of just using
        // PartitionEstimator is because the PartitionEstimator only gives you back information for
        // single queries, whereas in this code we are trying figure out partitioning
        // information for all of the queries in a transaction.
        //
        // Sincerely,
        // Andy of 04/22/2010
        // -----------------------------------------------
        assert (!txn_trace.getQueries().isEmpty());
        txn_entry.resetQueryCounters();
        long query_partitions = 0;
        if (debug_txn) LOG.info(txn_entry.debug());
        boolean txn_singlesited_orig = txn_entry.singlesited;
        for (QueryTrace query_trace : txn_trace.getQueries()) {
            if (debug) LOG.debug("Examining " + query_trace + " from " + txn_trace);
            Statement catalog_stmt = query_trace.getCatalogItem(catalog_db);
            assert (catalog_stmt != null);

            // If we have a filter and that filter doesn't want us to look at
            // this query, then we will just skip it and check the other ones
            if (filter != null && filter.apply(query_trace) != Filter.FilterResult.ALLOW) {
                if (trace) LOG.trace(query_trace + " is filtered. Skipping...");
                txn_entry.unknown_queries++;
                continue;
            }

            // Check whether we have a cache entry for this QueryTrace
            QueryCacheEntry query_entry = this.query_entries.get(query_trace.getId());
            if (this.use_caching && query_entry != null && !query_entry.isInvalid()) {
                if (trace) LOG.trace("Got cached " + query_entry + " for " + query_trace);

                // Grab all of TableKeys in this QueryCacheEntry and add the partitions that they touch
                // to the Statement partition map. We don't need to update the TransactionCacheEntry
                // or any histograms because they will have been updated when the QueryCacheEntry is created
                for (String table_key : query_entry.getTableKeys()) {
                    if (!stmt_partitions.containsKey(table_key)) {
                        stmt_partitions.put(table_key, new HashSet<Integer>());
                    }
                    stmt_partitions.get(table_key).addAll(query_entry.getPartitions(table_key));
                } // FOR
                txn_entry.examined_queries++;
                query_partitions += query_entry.getAllPartitions().size();

            // Create a new QueryCacheEntry
            } else {
                if (trace) LOG.trace("Calculating new cost information for " + query_trace);
                if (!this.isCachingEnabled() || query_entry == null) {
                    query_entry = new QueryCacheEntry(txn_trace, query_trace);
                }
                this.query_ctr.incrementAndGet();
                query_entry.invalid = false;
                this.query_entries.put(query_trace.getId(), query_entry);

                // Give the QueryTrace to the PartitionEstimator to get back a mapping from TableKeys
                // to sets of partitions that were touched by the query.
                // XXX: What should we do if the TransactionCacheEntry's base partition hasn't been calculated yet?
                //      Let's just throw it at the PartitionEstimator and let it figure out what to do...
                Map<String, Set<Integer>> table_partitions = this.p_estimator.getTablePartitions(query_trace, txn_entry.base_partition);
                StringBuilder sb = null; 
                if (trace) {
                    sb = new StringBuilder();
                    sb.append("\n" + StringUtil.SINGLE_LINE + query_trace + " Table Partitions:");
                }
                assert (!table_partitions.isEmpty()) : "Failed to get back table partitions for " + query_trace;
                for (Entry<String, Set<Integer>> e : table_partitions.entrySet()) {
                    // If we didn't get anything back, then that means that we know we need to touch this
                    // table but the PartitionEstimator doesn't have enough information yet
                    if (e.getValue().isEmpty()) continue;
                    if (trace) sb.append("\n  " + e.getKey() + ": " + e.getValue());

                    // Update the cache xref mapping so that we know this Table
                    // is referenced by this QueryTrace
                    // This will allow us to quickly find the QueryCacheEntry in
                    // invalidate()
                    if (this.use_caching) {
                        if (!this.cache_tbl_xref.containsKey(e.getKey())) {
                            this.cache_tbl_xref.put(e.getKey(), new HashSet<QueryCacheEntry>());
                        }
                        this.cache_tbl_xref.get(e.getKey()).add(query_entry);
                    }

                    // Ok, so now update the variables in our QueryCacheEntry
                    query_entry.singlesited = (query_entry.singlesited && e.getValue().size() == 1);
                    query_entry.addAllPartitions(e.getKey(), e.getValue());

                    // And then update the Statement partitions map to include
                    // all of the partitions
                    // that this query touched
                    if (!stmt_partitions.containsKey(e.getKey())) {
                        stmt_partitions.put(e.getKey(), e.getValue());
                    } else {
                        stmt_partitions.get(e.getKey()).addAll(e.getValue());
                    }
                } // FOR (Entry<TableKey, Set<Partitions>>
                if (trace) LOG.trace(sb.toString() + "\n" + StringUtil.SINGLE_LINE.trim());

                // Lastly, update the various histogram that keep track of which partitions are accessed:
                //  (1) The global histogram for the cost model of partitions touched by all queries
                //  (2) The TransactionCacheEntry histogram of which partitions are touched by all queries
                // Note that we do not want to update the global histogram for the cost model on the
                // partitions touched by txns, because we don't want to count the same partition multiple times
                // Note also that we want to do this *outside* of the loop above, otherwise we will count
                // the same partition multiple times if the query references more than one table!
                this.histogram_query_partitions.putAll(query_entry.getAllPartitions());
                txn_entry.touched_partitions.putAll(query_entry.getAllPartitions());
                int query_num_partitions = query_entry.getAllPartitions().size();
                query_partitions += query_num_partitions;
                
                // If the number of partitions is zero, then we will classify this query as currently
                // being unknown. This can happen if the query needs does a single look-up on a replicated
                // table but we don't know the base partition yet
                if (query_num_partitions == 0) {
                    if (trace) LOG.trace("# of Partitions for " + query_trace + " is zero. Marking as unknown for now");
                    txn_entry.unknown_queries++;
                    query_entry.unknown = true;
                    query_entry.invalid = true;
                } else {
                    txn_entry.examined_queries++;
                    query_entry.unknown = false;
                }
            } // if (new cache entry)

            // If we're not single-sited, well then that ruins it for everyone
            // else now doesn't it??
            if (query_entry.getAllPartitions().size() > 1) {
                if (trace) LOG.trace(query_trace + " is being marked as multi-partition: " + query_entry.getAllPartitions());
                query_entry.singlesited = false;
                txn_entry.singlesited = false;
                txn_entry.multisite_queries++;
            } else {
                if (trace) LOG.trace(query_trace + " is marked as single-partition");
                query_entry.singlesited = true;
                txn_entry.singlesite_queries++;
            }
            
            if (debug_txn) LOG.info(query_entry);
        } // FOR (QueryTrace)

        // Now just check whether this sucker has queries that touch more than one partition
        // We do this one first because it's the fastest and will pick up enough of them
        if (txn_entry.touched_partitions.getValueCount() > 1) {
            if (trace) LOG.trace(txn_trace + " touches " + txn_entry.touched_partitions.getValueCount() + " different partitions");
            txn_entry.singlesited = false;

        // Otherwise, now that we have processed all of queries that we could, we need to check
        // whether the values of the StmtParameters used on the partitioning column of each table
        // all hash to the same value. If they don't, then we know we can't sbe single-partition
        } else {
            for (Entry<String, Set<Integer>> entry: stmt_partitions.entrySet()) {
            	String table_key = entry.getKey();
                Table catalog_tbl = CatalogKey.getFromKey(catalog_db, table_key, Table.class);
                if (catalog_tbl.getIsreplicated()) {
                    continue;
                }

                Column table_partition_col = catalog_tbl.getPartitioncolumn();
                Set<Integer> hashes = entry.getValue();

                // If there is more than one partition, then we'll never be multi-partition so we
                // can stop our search right here.
                if (hashes.size() > 1) {
                    if (trace) LOG.trace(catalog_proc + " references " + catalog_tbl + "'s partitioning attribute " +
                                         table_partition_col + " on " + hashes.size() + " different partitions -- VALUES" + hashes);
                    txn_entry.singlesited = false;
                    break;

                // Make sure that the partitioning ProcParameter hashes to the same site as the value
                // used on the partitioning column for this table
                } else if (!hashes.isEmpty() && txn_entry.base_partition != null) {
                    int tbl_partition = CollectionUtil.getFirst(hashes);
                    if (txn_entry.base_partition != tbl_partition) {
                        if (trace) LOG.trace(txn_trace + " executes on Partition #" + txn_entry.base_partition + " " +
                                             "but partitioning column " + CatalogUtil.getDisplayName(table_partition_col) + " " +
                                             "references Partition #" + tbl_partition);
                        txn_entry.singlesited = false;
                        break;
                    }
                }
            } // FOR (table_key)
        }

        // Update the histograms if they are switching from multi-partition to single-partition for the first time
        if (txn_singlesited_orig && !txn_entry.singlesited) {
            if (trace) LOG.trace("Switching " + txn_entry + " histogram info from single- to multi-partition");
            this.histogram_sp_procs.remove(proc_key);
            this.histogram_mp_procs.put(proc_key);
        }
        
        // IMPORTANT: If the number of partitions touched in this txn have changed since before we examined
        // a bunch of queries, then we need to update the various histograms and counters
        // This ensures that we do not double count partitions
        if (txn_entry.touched_partitions.getValueCount() != orig_partitions.size()) {
            assert (txn_entry.touched_partitions.getValueCount() > orig_partitions.size());
            // Remove the partitions that we already know that we touch and then update
            // the histogram keeping track of which partitions our txn touches
            temp = txn_entry.touched_partitions.values();
            HashSet<Integer> new_partitions = new HashSet<Integer>(temp);
            new_partitions.removeAll(orig_partitions);
            this.histogram_txn_partitions.putAll(new_partitions);
            if (debug)
                LOG.debug("Updating " + txn_trace + " histogram_txn_partitions with " + new_partitions.size() +
                          " new partitions [new_sample_count=" + this.histogram_txn_partitions.getSampleCount() +
                          ", new_value_count=" + this.histogram_txn_partitions.getValueCount() + "]\n" + txn_entry.debug());
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

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
                ArgumentsParser.PARAM_CATALOG,
                ArgumentsParser.PARAM_WORKLOAD,
                ArgumentsParser.PARAM_PARTITION_PLAN
        );
        assert(args.workload.getTransactionCount() > 0) : "No transactions were loaded from " + args.workload_path;
        
        // Enable compact output
        final boolean table_output = (args.getOptParams().contains("table"));
        
        // If given a PartitionPlan, then update the catalog
        File pplan_path = new File(args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN));
//        if (pplan_path.exists()) {
            PartitionPlan pplan = new PartitionPlan();
            pplan.load(pplan_path.getAbsolutePath(), args.catalog_db);
            pplan.apply(args.catalog_db);
            
            System.out.println("Applied PartitionPlan '" + pplan_path + "' to catalog\n" + pplan);
            System.out.print(StringUtil.DOUBLE_LINE);
//            if (!table_output) {
//                
//            }
//        } else if (!table_output) {
//            System.err.println("PartitionPlan file '" + pplan_path + "' does not exist. Ignoring...");
//        }
        System.out.flush();

        long singlepartition = 0;
        long multipartition = 0;
        long total = 0;
        SingleSitedCostModel costmodel = new SingleSitedCostModel(args.catalog_db);
        List<Integer> all_partitions = CatalogUtil.getAllPartitionIds(args.catalog_db);
//        costmodel.setEntropyWeight(4.0);
//        costmodel.setJavaExecutionWeightEnabled(true);
//        costmodel.setJavaExecutionWeight(100);

        Histogram hist = new Histogram();
        for (AbstractTraceElement<? extends CatalogType> element : args.workload) {
            if (element instanceof TransactionTrace) {
                total++;
                TransactionTrace xact = (TransactionTrace) element;
                boolean is_singlesited = costmodel.processTransaction(args.catalog_db, xact, null).singlesited;
                if (is_singlesited) {
                    singlepartition++;
                    hist.put(xact.getCatalogItemName());
                } else {
                    multipartition++;
                    if (!hist.contains(xact.getCatalogItemName())) hist.put(xact.getCatalogItemName(), 0);
                }
            }
        } // FOR
//        long total_partitions_touched_txns = costmodel.getTxnPartitionAccessHistogram().getSampleCount();
//        long total_partitions_touched_queries = costmodel.getQueryPartitionAccessHistogram().getSampleCount();

        Histogram h = null;
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
            h.putAll(all_partitions, 0);
            System.out.println(StringUtil.addSpacers(h.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);
    
            System.out.println("Transaction Partition Histogram:");
            h = costmodel.getTxnPartitionAccessHistogram();
            h.setKeepZeroEntries(true);
            h.putAll(all_partitions, 0);
            System.out.println(StringUtil.addSpacers(h.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);
    
            System.out.println("Query Partition Touch Histogram:");
            h = costmodel.getQueryPartitionAccessHistogram();
            h.setKeepZeroEntries(true);
            h.putAll(all_partitions, 0);
            System.out.println(StringUtil.addSpacers(h.toString()));
            System.out.print(StringUtil.DOUBLE_LINE);
        }

        ListOrderedMap<String, Object> m = new ListOrderedMap<String, Object>();
        
        // Execution Cost
        m.put("SINGLE-PARTITION", singlepartition);
        m.put("MULTI-PARTITION", multipartition);
        m.put("TOTAL", total + " [" + singlepartition / (double) total + "]");
        m.put("XXX", null);

        // Utilization
        costmodel.getJavaExecutionHistogram().setKeepZeroEntries(false);
        int active_partitions = costmodel.getJavaExecutionHistogram().getValueCount();
        m.put("ACTIVE PARTITIONS", active_partitions);
        m.put("IDLE PARTITIONS", (all_partitions.size() - active_partitions));
//        System.out.println("Partitions Touched By Queries: " + total_partitions_touched_queries);

        Histogram entropy_h = costmodel.getJavaExecutionHistogram();
        m.put("JAVA SKEW", EntropyUtil.calculateEntropy(all_partitions.size(), entropy_h.getSampleCount(), entropy_h));
        
        entropy_h = costmodel.getTxnPartitionAccessHistogram();
        m.put("TRANSACTION SKEW", EntropyUtil.calculateEntropy(all_partitions.size(), entropy_h.getSampleCount(), entropy_h));
        
//        TimeIntervalCostModel<SingleSitedCostModel> timecostmodel = new TimeIntervalCostModel<SingleSitedCostModel>(args.catalog_db, SingleSitedCostModel.class, 1);
//        timecostmodel.estimateCost(args.catalog_db, args.workload);
//        double entropy = timecostmodel.getLastEntropyCost()
        m.put("UTILIZATION",  (costmodel.getJavaExecutionHistogram().getValueCount() / (double)all_partitions.size()));

        if (false && table_output) {
            String columns[] = {
                "IDLE PARTITIONS",
                "JAVA SKEW",
                "TRANSACTION SKEW",
                "SINGLE-PARTITION",
                "MULTI-PARTITION",
            };
            String add = "";
            for (String col : columns) {
                System.out.print(col + add + m.get(col));
                add = "\t";
            }
            System.out.println();
            
            
        } else {
            final String f = "%-25s%s";
            for (Entry<String, Object> e : m.entrySet()) {
                if (e.getKey().startsWith("XXX")) System.out.print(StringUtil.DOUBLE_LINE);
                else {
                    System.out.println(String.format(f, e.getKey().toUpperCase()+":", e.getValue().toString()));
                }
            } // FOR
        }
    }
}
