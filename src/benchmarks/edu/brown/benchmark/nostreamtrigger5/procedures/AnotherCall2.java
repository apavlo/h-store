package edu.brown.benchmark.nostreamtrigger5.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class AnotherCall2 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S11");
    }

    public final SQLStmt insertS12 = new SQLStmt("INSERT INTO S12 (value) SELECT * FROM S11;");

    public final SQLStmt insertS13 = new SQLStmt("INSERT INTO S13 (value) SELECT * FROM S12;");

    public final SQLStmt insertS14 = new SQLStmt("INSERT INTO S14 (value) SELECT * FROM S13;");

    public final SQLStmt insertS15 = new SQLStmt("INSERT INTO S15 (value) SELECT * FROM S14;");

    public final SQLStmt insertS16 = new SQLStmt("INSERT INTO S16 (value) SELECT * FROM S15;");

    public final SQLStmt insertS17 = new SQLStmt("INSERT INTO S17 (value) SELECT * FROM S16;");

    public final SQLStmt insertS18 = new SQLStmt("INSERT INTO S18 (value) SELECT * FROM S17;");

    public final SQLStmt insertS19 = new SQLStmt("INSERT INTO S19 (value) SELECT * FROM S18;");

    public final SQLStmt insertS20 = new SQLStmt("INSERT INTO S20 (value) SELECT * FROM S19;");

    public final SQLStmt insertS21 = new SQLStmt("INSERT INTO S21 (value) SELECT * FROM S20;");


    // delete statements
    public final SQLStmt deleteS11 = new SQLStmt("DELETE FROM S11;");

    public final SQLStmt deleteS12 = new SQLStmt("DELETE FROM S12;");

    public final SQLStmt deleteS13 = new SQLStmt("DELETE FROM S13;");

    public final SQLStmt deleteS14 = new SQLStmt("DELETE FROM S14;");

    public final SQLStmt deleteS15 = new SQLStmt("DELETE FROM S15;");

    public final SQLStmt deleteS16 = new SQLStmt("DELETE FROM S16;");

    public final SQLStmt deleteS17 = new SQLStmt("DELETE FROM S17;");

    public final SQLStmt deleteS18 = new SQLStmt("DELETE FROM S18;");

    public final SQLStmt deleteS19 = new SQLStmt("DELETE FROM S19;");

    public final SQLStmt deleteS20 = new SQLStmt("DELETE FROM S20;");

	public long run() {

        voltQueueSQL(insertS12);
        voltExecuteSQL();

        voltQueueSQL(insertS13);
        voltExecuteSQL();

        voltQueueSQL(insertS14);
        voltExecuteSQL();

        voltQueueSQL(insertS15);
        voltExecuteSQL();

        voltQueueSQL(insertS16);
        voltExecuteSQL();

        voltQueueSQL(insertS17);
        voltExecuteSQL();

        voltQueueSQL(insertS18);
        voltExecuteSQL();

        voltQueueSQL(insertS19);
        voltExecuteSQL();

        voltQueueSQL(insertS20);
        voltExecuteSQL();

        voltQueueSQL(insertS21);
        voltExecuteSQL();

        //delete
        voltQueueSQL(deleteS20);
        voltExecuteSQL();

        voltQueueSQL(deleteS19);
        voltExecuteSQL();

        voltQueueSQL(deleteS18);
        voltExecuteSQL();

        voltQueueSQL(deleteS17);
        voltExecuteSQL();

        voltQueueSQL(deleteS16);
        voltExecuteSQL();

        voltQueueSQL(deleteS15);
        voltExecuteSQL();

        voltQueueSQL(deleteS14);
        voltExecuteSQL();

        voltQueueSQL(deleteS13);
        voltExecuteSQL();

        voltQueueSQL(deleteS12);
        voltExecuteSQL();

        voltQueueSQL(deleteS11);
        voltExecuteSQL();

		return 0;
    }
}