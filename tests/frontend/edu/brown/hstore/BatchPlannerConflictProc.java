package edu.brown.hstore;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BatchPlannerConflictProc extends VoltProcedure {
    public final SQLStmt ReplicatedInsert = new SQLStmt("INSERT INTO COUNTRY VALUES (?, ?, ?, ?);");
    public final SQLStmt ReplicatedSelect = new SQLStmt("SELECT * FROM COUNTRY");
    
    public VoltTable[] run(long id, String name, String code2, String code3) {
        voltQueueSQL(ReplicatedInsert, id, name, code2, code3);
        voltQueueSQL(ReplicatedInsert, id++, name, code2, code3);
        voltQueueSQL(ReplicatedInsert, id++, name, code2, code3);
        voltQueueSQL(ReplicatedSelect);
        return voltExecuteSQL(true);
    }
}