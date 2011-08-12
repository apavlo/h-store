package edu.brown.benchmark.airline.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class UpdateCustomer extends VoltProcedure {
    
    public final SQLStmt GetCustomerId = new SQLStmt(
        "SELECT * " +
        "  FROM " + AirlineConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID = ? "
    );
    
    public final SQLStmt GetCustomerIdStr = new SQLStmt(
        "SELECT * " +
        "  FROM " + AirlineConstants.TABLENAME_CUSTOMER +
        " WHERE C_ID_STR = ? "
    );
            
    public final SQLStmt UpdateFF = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR00 = ?, " +
            "       FF_IATTR01 = FF_IATTR01 + ? " +
            " WHERE FF_C_ID = ? " + 
            "   AND FF_AL_ID = ? ");
    
    public VoltTable[] run(long c_id, String c_id_str, long attr0, long attr1) {
        if (c_id_str != null && c_id_str.isEmpty() == false) {
            voltQueueSQL(GetCustomerIdStr, c_id_str);
        } else {
            voltQueueSQL(GetCustomerId, c_id);
        }
        VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        if (results[0].getRowCount() == 0) {
            throw new VoltAbortException(String.format("No Customer information record found [c_id=%d, c_id_str=%s]",
                                                       c_id, c_id_str));
        }
        
//        voltQueueSQL(UpdateFF, attr0, attr1, c_id, al_id);
//        results = voltExecuteSQL();
//        assert (results.length == 1);
        return (results);
    }
}
