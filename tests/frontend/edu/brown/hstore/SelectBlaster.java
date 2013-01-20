package edu.brown.hstore;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.ycsb.YCSBConstants;

/**
 * Select Blaster Procedure
 */
@ProcInfo(
    singlePartition=true
)
public class SelectBlaster extends VoltProcedure {
    
    public final SQLStmt get = new SQLStmt(
        "SELECT * FROM " + YCSBConstants.TABLE_NAME + " WHERE ycsb_key = ?"
    );
    
    public long run(long keys0[], long keys1[], long keys2[]) {
        long keys[][] = { keys0, keys1, keys2 };
        for (int i = 1; i < keys.length; i++) {
            assert(keys[0].length == keys[i].length);
        } // FOR
        
        long start = System.nanoTime();
        for (int i = 0; i < keys0.length; i++) {
            for (int j = 0; j < keys.length; j++) {
                voltQueueSQL(get, keys[j][i]);
            } // FOR
            VoltTable vt[] = voltExecuteSQL();
            assert(vt.length == keys.length);

            for (int j = 0; j < keys.length; j++) {
                boolean adv = vt[j].advanceRow();
                assert(adv);
                assert(keys[j][i] == vt[j].getLong(0));
            } // FOR
        } // FOR
        return (System.nanoTime() - start);
    }
}