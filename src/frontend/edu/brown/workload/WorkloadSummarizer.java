package edu.brown.workload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.hashing.AbstractHasher;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;

public class WorkloadSummarizer {

    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final Collection<Procedure> procedures;
    private final Map<Table, List<Column>> candidate_columns;
    
    private class DuplicateTraceElements<CT extends CatalogType, T extends AbstractTraceElement<CT>> extends HashMap<CT, Map<String, Set<T>>> {
        private static final long serialVersionUID = 1L;
        private boolean has_duplicates = false;
        
        synchronized void add(CT catalog_item, String hash_str, T query_trace) {
            Map<String, Set<T>> m = this.get(catalog_item);
            if (m == null) {
                m = new ConcurrentHashMap<String, Set<T>>();
                this.put(catalog_item, m);
            }
            Set<T> s = m.get(hash_str);
            if (s == null) {
                s = new ListOrderedSet<T>();
                m.put(hash_str, s);
            }
            s.add(query_trace);
            this.has_duplicates = this.has_duplicates || s.size() > 1;
        }
        
        public Collection<T> getWeightedQueryTraces() {
            List<T> new_queries = new ArrayList<T>();
            for (CT catalog_item : this.keySet()) {
                for (Set<T> s : this.get(catalog_item).values()) {
                    int weight = s.size();
                    if (weight == 0) continue;
                    T q = CollectionUtil.getFirst(s);
                    q.setWeight((float)weight);
                    new_queries.add(q);
                } // FOR
            } // FOR
            return (new_queries);
        }
        
        @Override
        public void clear() {
            for (Map<String, Set<T>> m : this.values()) {
                for (Set<T> s : m.values()) {
                    s.clear();
                } // FOR
            } // FOR
            this.has_duplicates = false;
        }
        
        public boolean hasDuplicates() {
            return (this.has_duplicates);
        }
    }
    
    /**
     * Constructor
     * @param catalog_db
     * @param p_estimator
     * @param procedures
     * @param candidate_columns
     */
    public WorkloadSummarizer(Database catalog_db, PartitionEstimator p_estimator, Collection<Procedure> procedures, Map<Table, List<Column>> candidate_columns) {
        this.catalog_db = catalog_db;
        this.p_estimator = p_estimator;
        this.procedures = procedures;
        this.candidate_columns = candidate_columns;
    }
    
    public Workload process(Workload workload) {
        
        
        return (null);
    }
    

    
    /**
     * Removed duplicate query invocations within a single TransactionTrace. Duplicate QueryTraces will
     * be replaced with a single QueryTrace that is weighted. This will remove batch boundaries.
     * Returns a new workload that contains the new TransactionTraces instances with the 
     * pruned list of QueryTraces.
     * @param workload
     * @return
     */
    protected Workload removeDuplicateQueries(Workload workload) {
        AbstractHasher hasher = p_estimator.getHasher();
        Workload new_workload = new Workload(this.catalog_db.getCatalog());
        AtomicInteger trimmed_ctr = new AtomicInteger(0);
        
        DuplicateTraceElements<Statement, QueryTrace> duplicates = new DuplicateTraceElements<Statement, QueryTrace>();
        for (TransactionTrace txn_trace : workload) {
            duplicates.clear();            
            for (int batch = 0, cnt = txn_trace.getBatchCount(); batch < cnt; batch++) {

                for (QueryTrace query_trace : txn_trace.getBatchQueries(batch)) {
                    Statement catalog_stmt = query_trace.getCatalogItem(this.catalog_db);
                    
                    // Make a hash string for this query's parameters
                    String param_hashes = "";
                    for (Object p : query_trace.getParams()) {
                        param_hashes += (param_hashes.isEmpty() ? "" : "|") + hasher.hash(p);
                    } // FOR
                    
                    duplicates.add(catalog_stmt, param_hashes, query_trace);
                } // FOR
            } // FOR
            
            // If this TransactionTrace has duplicate queries, then we will want to consturct
            // a new TransactionTrace that has the weighted queries. Note that will cause us 
            // to have to remove any batches
            if (duplicates.hasDuplicates()) {
                TransactionTrace new_txn_trace = (TransactionTrace)txn_trace.clone();
                new_txn_trace.setQueries(duplicates.getWeightedQueryTraces());
                new_workload.addTransaction(new_txn_trace.getCatalogItem(this.catalog_db), new_txn_trace);
                trimmed_ctr.incrementAndGet();
            } else {
                new_workload.addTransaction(txn_trace.getCatalogItem(this.catalog_db), txn_trace);
            }
        } // FOR
        
        return (new_workload);
    }
    
}
