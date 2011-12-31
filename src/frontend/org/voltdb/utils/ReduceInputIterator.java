package org.voltdb.utils;

import java.util.Iterator;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

public class ReduceInputIterator<K> implements Iterator<VoltTableRow> {
    
    final VoltTable table;
    boolean isAdvanced;
    int jump;
    boolean isFinish;
    boolean isStart;
    
    K oldKey;
    public ReduceInputIterator(VoltTable table) {
        this.table = table;
        oldKey = null;
        isAdvanced = false;
        isFinish = false;
        jump = 3;
    }
    
    public boolean hasKey() {
        boolean result = this.table.advanceRow();
        return result;
    }
    
    @SuppressWarnings("unchecked")
    public K getKey() {
        return (K) this.table.get(0);
    }
    
    public boolean getFinish(){
        return this.isFinish;
    }
    
    /*
     * if there is next same key tuple in this VoltTable rows
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        if(isFinish == false){
            // this is the first time outer while
            if(jump == 3 && isAdvanced) { 
                jump = 2;
                return true;
            }
            if(jump < 2 ) {
                jump ++;
                return true;
            }
            if(isAdvanced) oldKey = this.getKey();
                
                if(this.hasKey()) {
                    isAdvanced = true;
                    
                    if(oldKey == null || oldKey.equals(this.getKey())) { 
                       
                        return true;
                    }else {
                        jump = 0;
                        oldKey = null;
                        return false;
                    }
                }else {
                    isFinish = true;
                    return false;
                }
            
            
        } else
            return false; // isFinish
    }

    @Override
    public VoltTableRow next() {
        assert (this.isAdvanced); 
       
        return  this.table.getRow();
    }

    @Override
    public void remove() {
        throw new NotImplementedException("Cannot remove from a VoltTable");
        //throw new UnsupportedOperationException("Cannot remove from a VoltTable");
    }
}
