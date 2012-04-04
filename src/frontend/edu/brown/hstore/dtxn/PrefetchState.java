package edu.brown.hstore.dtxn;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.ParameterSet;

import com.google.protobuf.ByteString;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.utils.Poolable;

public class PrefetchState implements Poolable {

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
    
    @Override
    public boolean isInitialized() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void finish() {
        this.fragments = null;
        this.paramsRaw = null;
        this.params = null;
        this.results.clear();
    }

    
    
}
