package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.voltdb.ParameterSet;

import com.google.protobuf.ByteString;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.pools.Poolable;

/**
 * Special internal state information for when the txn requests prefetch queries
 * @author pavlo
 */
public class PrefetchState implements Poolable {

    /**
     * Which partitions have received prefetch WorkFragments
     */
    protected final BitSet partitions;
    
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
    
    public PrefetchState(HStoreSite hstore_site) {
        int num_partitions = hstore_site.getLocalPartitionIds().size();
        this.partitions = new BitSet(num_partitions);
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

    
    
}
