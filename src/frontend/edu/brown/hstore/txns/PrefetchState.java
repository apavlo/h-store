package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.ParameterSet;
import org.voltdb.catalog.Statement;

import com.google.protobuf.ByteString;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.specexec.QueryTracker;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

/**
 * Special internal state information for when the txn requests prefetch queries
 * @author pavlo
 */
public class PrefetchState implements Poolable {
    

    protected final QueryTracker prefetchTracker = new QueryTracker();
    
    protected final QueryTracker queryTracker = new QueryTracker();

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
    
    public QueryTracker getExecQueryTracker() {
        return (this.queryTracker);
    }
    
    public QueryTracker getPrefetchQueryTracker() {
        return (this.prefetchTracker);
    }
    
    
    // ----------------------------------------------------------------------------
    // INTERNAL METHODS
    // ----------------------------------------------------------------------------
    
    

    

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
        this.prefetchTracker.addQuery(stmt, partitions, stmtParams);
    }
    
    /**
     * Returns true if this query 
     * @param stmt
     * @param counter
     * @return
     */
    public final boolean isMarkedPrefetched(Statement stmt, int counter) {
        return (this.prefetchTracker.findPrefetchedQuery(stmt, counter) != null);
    }
    
}
