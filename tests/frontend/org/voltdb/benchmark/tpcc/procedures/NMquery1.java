package org.voltdb.benchmark.tpcc.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * 
 * @author xin
 */
@ProcInfo(
    partitionInfo = "TABLEA.A_ID: 0",
    singlePartition = true
)
public class NMquery1 extends VoltProcedure {

    public final SQLStmt NameCount = new SQLStmt(
            "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), " +
            "COUNT(*) FROM order_line " +
            //"WHERE ol_delivery_d > ? " +
            "GROUP BY ol_number order by ol_number");
   
    /**
     * 
     * @param a_id
     * @return
     */
    public VoltTable[] run(long a_id) {
        voltQueueSQL(NameCount, a_id);
        VoltTable[] tb = voltExecuteSQL();
        
       
        return (voltExecuteSQL());
    }
    
}
