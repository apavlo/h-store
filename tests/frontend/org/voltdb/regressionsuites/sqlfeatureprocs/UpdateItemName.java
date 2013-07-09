package org.voltdb.regressionsuites.sqlfeatureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCConstants;

public class UpdateItemName extends VoltProcedure {
    
    public SQLStmt UpdateItems = new SQLStmt(
        "UPDATE " + TPCCConstants.TABLENAME_ITEM +
        "   SET I_NAME = ? " +
        " WHERE I_ID = ? "
    );

    public SQLStmt GetItem = new SQLStmt(
        "SELECT I_NAME FROM " + TPCCConstants.TABLENAME_ITEM +
        " WHERE I_ID = ?"
    );
    
    public VoltTable run(long id, String name) {
        voltQueueSQL(UpdateItems, name, id);
        voltQueueSQL(GetItem, id);
        VoltTable results[] = voltExecuteSQL(true);
        assert(results.length == 2);
        return (results[1]);
    }

}
