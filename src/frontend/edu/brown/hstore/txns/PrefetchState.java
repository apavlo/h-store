package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.ParameterSet;
import org.voltdb.catalog.Statement;

import com.google.protobuf.ByteString;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.pools.Poolable;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.PartitionSet;

/**
 * Special internal state information for when the txn requests prefetch queries
 * @author pavlo
 */
public class PrefetchState implements Poolable {
    
    public class PrefetchedQuery {
        public final Statement stmt;
        public final int counter;
        public final PartitionSet partitions;
        public final int paramsHash;
        
        public PrefetchedQuery(Statement stmt, int counter, PartitionSet partitions, int paramsHash) {
            this.stmt = stmt;
            this.counter = counter;
            this.partitions = partitions;
            this.paramsHash = paramsHash;
        }
    }
    
    private final Collection<PrefetchedQuery> prefetchQueries = new ArrayList<>(); 
    
    /**
     * Internal counter for the number of times that we've executed queries in the past.
     * If a Statement is not in this list, then we know that it wasn't prefetched.
     */
    private final Histogram<Statement> stmtCounters = new ObjectHistogram<Statement>(true);

    /**
     * Which partitions have received prefetch WorkFragments
     */
    protected final PartitionSet partitions = new PartitionSet();
    
    /**
     * The list of the FragmentIds that were sent out in a prefetch request
     * This should only be access from LocalTransaction
     */
    protected final List<Integer> fragmentIds = new ArrayList<Integer>();
    
    /** 
     * The list of prefetchable WorkFragments that were sent for this transaction, if any
     */
    protected List<WorkFragment> fragments = null;
    
    /**
     * The list of raw serialized ParameterSets for the prefetched WorkFragments,
     * if any (in lockstep with prefetch_fragments)
     */
    protected List<ByteString> paramsRaw = null;
    
    /**
     * The deserialized ParameterSets for the prefetched WorkFragments,
     */
    protected ParameterSet[] params = null;
    
    /**
     * 
     */
    protected final List<WorkResult> results = new ArrayList<WorkResult>();
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public PrefetchState(HStoreSite hstore_site) {
        // int num_partitions = hstore_site.getLocalPartitionIds().size();
    }
    
    public void init(AbstractTransaction ts) {
        // Nothing to do for now...
    }
    
    @Override
    public boolean isInitialized() {
        return (this.partitions.isEmpty() == false);
    }

    @Override
    public void finish() {
        this.partitions.clear();
        this.fragmentIds.clear();
        this.fragments = null;
        this.paramsRaw = null;
        this.params = null;
        this.results.clear();
    }
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    
    public PrefetchedQuery findPrefetchedQuery(Statement stmt, int counter) {
        for (PrefetchedQuery pq : this.prefetchQueries) {
            if (pq.stmt.equals(stmt) && pq.counter == counter) {
                return (pq);
            }
        } // FOR
        return (null);
    }
    

    // ----------------------------------------------------------------------------
    // API METHODS
    // ----------------------------------------------------------------------------

    /**
     * Mark the given query instance as being prefetched. This doesn't keep track
     * of whether the result has returned, only that we sent the prefetch request
     * out to the given partitions.
     * @param stmt
     * @param counter
     * @param partitions
     * @param stmtParams
     */
    public void markPrefetchedQuery(Statement stmt, int counter,
                                    PartitionSet partitions, ParameterSet stmtParams) {
        PrefetchedQuery pq = new PrefetchedQuery(stmt, counter, partitions, stmtParams.hashCode());
        this.prefetchQueries.add(pq);
    }
    
    
    /**
     * Update an internal counter for the number of times that we've invoked queries
     * @param stmt
     * @return
     */
    public final int updateStatementCounter(Statement stmt, PartitionSet partitions) {
        return (int)this.stmtCounters.put(stmt);
    }
    
    
    /**
     * Returns true if this query 
     * @param stmt
     * @param counter
     * @return
     */
    public final boolean isMarkedPrefetched(Statement stmt, int counter) {
        return (this.findPrefetchedQuery(stmt, counter) != null);
    }
    
}
