package edu.brown.hstore.txns;

import java.util.List;

import org.voltdb.ParameterSet;

import com.google.protobuf.ByteString;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.specexec.QueryTracker;
import edu.brown.pools.Poolable;
import edu.brown.utils.PartitionSet;

/**
 * Special internal state information for when the txn requests prefetch queries
 * @author pavlo
 */
public class PrefetchState implements Poolable {
    
    /**
     * Keep track of every query invocation made by the transaction so that
     * we can check whether we have already submitted the query as a prefetch
     */
    protected final QueryTracker queryTracker = new QueryTracker();

    /**
     * Which partitions have executed prefetch WorkFragments
     */
    protected final PartitionSet partitions = new PartitionSet();
    
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
        this.fragments = null;
        this.paramsRaw = null;
        this.params = null;
    }
    
    public QueryTracker getExecQueryTracker() {
        return (this.queryTracker);
    }
    
}
