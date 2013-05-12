package edu.brown.hstore.util;

import java.util.BitSet;

import org.voltdb.ParameterSet;

/**
 * Reusable ParameterSet array cache
 * @author pavlo
 */
public class ParameterSetArrayCache {

    /**
     * Reusable Batch ParameterSet Arrays
     */
    private ParameterSet[][] params; 
    
    private final BitSet paramsDirty;
    
    public ParameterSetArrayCache(int init_size) {
        this.params = new ParameterSet[init_size][];
        this.paramsDirty = new BitSet(this.params.length);
        for (int i = 0; i < this.params.length; i++) {
            this.params[i] = new ParameterSet[i];
            for (int j = 0; j < i; j++) {
                this.params[i][j] = new ParameterSet(true);
            } // FOR
        } // FOR
    }
    
    /**
     * Return a cached ParameterSet array. This should only be called internally at this 
     * PartitionExecutor or by the VoltProcedures managed by this PartitionExecutor.
     * This is just to reduce the number of objects that we need to allocate
     * @param size The number of ParameterSets in the array
     */
    public ParameterSet[] getParameterSet(int size) {
        if (size >= this.params.length) {
            ParameterSet[][] new_params = new ParameterSet[size+1][];
            System.arraycopy(this.params, 0, new_params, 0, this.params.length);
            this.params = new_params;
        }
        assert(size < this.params.length);
        if (this.params[size] == null) {
            this.params[size] = new ParameterSet[size];
            for (int i = 0; i < size; i++) {
                this.params[size][i] = new ParameterSet(true);
            } // FOR
        }
        this.paramsDirty.set(size);
        return (this.params[size]);
    }
    
    public void reset() {
        for (int i = 0, cnt = this.paramsDirty.length(); i < cnt; i++) {
            if (this.paramsDirty.get(i)) {
                for (int j = 0; j < i; j++) {
                    if (this.params[i][j] != null) this.params[i][j].clear();
                } // FOR
            }
        } // FOR
        this.paramsDirty.clear();
    }
    
}
