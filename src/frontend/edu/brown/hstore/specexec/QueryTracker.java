package edu.brown.hstore.specexec;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.ParameterSet;
import org.voltdb.catalog.Statement;

import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.PartitionSet;

/**
 * This class is used to track the order in which queries are invoked
 * in the entire transaction. 
 * @author pavlo
 */
public class QueryTracker {

    public static class QueryInvocation {
        public final Statement stmt;
        public final int counter;
        public final PartitionSet partitions;
        public final int paramsHash;
        
        public QueryInvocation(Statement stmt, int counter, PartitionSet partitions, int paramsHash) {
            this.stmt = stmt;
            this.counter = counter;
            this.partitions = partitions;
            this.paramsHash = paramsHash;
        }
    }
    
    private final List<QueryInvocation> invocations = new ArrayList<>(); 
    
    /**
     * Internal counter for the number of times that we've executed queries in the past.
     */
    private final Histogram<Statement> stmtCounters = new ObjectHistogram<Statement>(true);
 
    // ----------------------------------------------------------------------------
    // API METHODS
    // ----------------------------------------------------------------------------

    /**
     * Add a query invocation into this tracker and return the number of
     * times that we have executed the query in the past.
     * @param stmt
     * @param partitions
     * @param stmtParams
     * @return
     */
    public int addQuery(Statement stmt, PartitionSet partitions, ParameterSet stmtParams) {
        int counter = (int)this.stmtCounters.put(stmt) - 1;
        QueryInvocation pq = new QueryInvocation(stmt, counter, partitions, stmtParams.hashCode());
        this.invocations.add(pq);
        return (counter);
    }
    
    /**
     * Search for the QueryInvocation handle for the given Statement and counter.
     * If no match is found, null is returned.
     * @param stmt
     * @param counter
     * @return
     */
    public QueryInvocation findPrefetchedQuery(Statement stmt, int counter) {
        for (QueryInvocation pq : this.invocations) {
            if (pq.stmt.equals(stmt) && pq.counter == counter) {
                return (pq);
            }
        } // FOR
        return (null);
    }
    
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, cnt = this.invocations.size(); i < cnt; i++) {
            QueryInvocation qi = this.invocations.get(i);
            sb.append(String.format("[%02d] %s - #02d / partitions=%s / params=%d\n",
                      i, qi.stmt.fullName(), qi.counter, qi.partitions, qi.paramsHash));
        } // FOR
        return (sb.toString());
    }
}


