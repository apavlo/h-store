package edu.brown.benchmark.locality.procedures;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class SetRemote extends VoltProcedure {

    /**
     * 
     * @param local_a_id
     * @param a_id
     * @param a_value
     * @param b_id
     * @param b_value
     * @return
     */
    public VoltTable[] run(long local_a_id, long a_id, String a_value, long b_id, String b_value) {
        // TODO
        return (null);
    }
    
}
