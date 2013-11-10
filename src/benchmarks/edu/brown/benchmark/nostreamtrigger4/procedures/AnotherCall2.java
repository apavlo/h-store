package edu.brown.benchmark.nostreamtrigger4.procedures;

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
		addTriggerTable("S13");
    }

    public final SQLStmt insertS14 = new SQLStmt("INSERT INTO S14 (value) SELECT * FROM S13;");

    public final SQLStmt insertS15 = new SQLStmt("INSERT INTO S15 (value) SELECT * FROM S14;");

    public final SQLStmt insertS16 = new SQLStmt("INSERT INTO S16 (value) SELECT * FROM S15;");

    public final SQLStmt insertS17 = new SQLStmt("INSERT INTO S17 (value) SELECT * FROM S16;");

    public final SQLStmt insertS18 = new SQLStmt("INSERT INTO S18 (value) SELECT * FROM S17;");

    public final SQLStmt insertS19 = new SQLStmt("INSERT INTO S19 (value) SELECT * FROM S18;");

    public final SQLStmt insertS20 = new SQLStmt("INSERT INTO S20 (value) SELECT * FROM S19;");

    public final SQLStmt insertS21 = new SQLStmt("INSERT INTO S21 (value) SELECT * FROM S20;");

    public final SQLStmt insertS22 = new SQLStmt("INSERT INTO S22 (value) SELECT * FROM S21;");

    public final SQLStmt insertS23 = new SQLStmt("INSERT INTO S23 (value) SELECT * FROM S22;");

    public final SQLStmt insertS24 = new SQLStmt("INSERT INTO S24 (value) SELECT * FROM S23;");

    public final SQLStmt insertS25 = new SQLStmt("INSERT INTO S25 (value) SELECT * FROM S24;");

    // delete statements
    public final SQLStmt deleteS13 = new SQLStmt("DELETE FROM S13;");

    public final SQLStmt deleteS14 = new SQLStmt("DELETE FROM S14;");

    public final SQLStmt deleteS15 = new SQLStmt("DELETE FROM S15;");

    public final SQLStmt deleteS16 = new SQLStmt("DELETE FROM S16;");

    public final SQLStmt deleteS17 = new SQLStmt("DELETE FROM S17;");

    public final SQLStmt deleteS18 = new SQLStmt("DELETE FROM S18;");

    public final SQLStmt deleteS19 = new SQLStmt("DELETE FROM S19;");

    public final SQLStmt deleteS20 = new SQLStmt("DELETE FROM S20;");

    public final SQLStmt deleteS21 = new SQLStmt("DELETE FROM S21;");

    public final SQLStmt deleteS22 = new SQLStmt("DELETE FROM S22;");

    public final SQLStmt deleteS23 = new SQLStmt("DELETE FROM S23;");

    public final SQLStmt deleteS24 = new SQLStmt("DELETE FROM S24;");


	public long run() {


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

        voltQueueSQL(insertS22);
        voltExecuteSQL();

        voltQueueSQL(insertS23);
        voltExecuteSQL();

        voltQueueSQL(insertS24);
        voltExecuteSQL();

        voltQueueSQL(insertS25);
        voltExecuteSQL();

        
        //delete

        voltQueueSQL(deleteS24);
        voltExecuteSQL();

        voltQueueSQL(deleteS23);
        voltExecuteSQL();

        voltQueueSQL(deleteS22);
        voltExecuteSQL();

        voltQueueSQL(deleteS21);
        voltExecuteSQL();
        
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


		return 0;
    }
}