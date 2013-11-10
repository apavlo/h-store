package edu.brown.benchmark.nostreamtrigger5.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class AnotherCall3 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S21");
    }


    public final SQLStmt insertS22 = new SQLStmt("INSERT INTO S22 (value) SELECT * FROM S21;");

    public final SQLStmt insertS23 = new SQLStmt("INSERT INTO S23 (value) SELECT * FROM S22;");

    public final SQLStmt insertS24 = new SQLStmt("INSERT INTO S24 (value) SELECT * FROM S23;");

    public final SQLStmt insertS25 = new SQLStmt("INSERT INTO S25 (value) SELECT * FROM S24;");

    public final SQLStmt insertS26 = new SQLStmt("INSERT INTO S26 (value) SELECT * FROM S25;");

    public final SQLStmt insertS27 = new SQLStmt("INSERT INTO S27 (value) SELECT * FROM S26;");

    public final SQLStmt insertS28 = new SQLStmt("INSERT INTO S28 (value) SELECT * FROM S27;");

    public final SQLStmt insertS29 = new SQLStmt("INSERT INTO S29 (value) SELECT * FROM S28;");

    public final SQLStmt insertS30 = new SQLStmt("INSERT INTO S30 (value) SELECT * FROM S29;");

    public final SQLStmt insertS31 = new SQLStmt("INSERT INTO S31 (value) SELECT * FROM S30;");


    // delete statements

    public final SQLStmt deleteS21 = new SQLStmt("DELETE FROM S21;");

    public final SQLStmt deleteS22 = new SQLStmt("DELETE FROM S22;");

    public final SQLStmt deleteS23 = new SQLStmt("DELETE FROM S23;");

    public final SQLStmt deleteS24 = new SQLStmt("DELETE FROM S24;");

    public final SQLStmt deleteS25 = new SQLStmt("DELETE FROM S25;");

    public final SQLStmt deleteS26 = new SQLStmt("DELETE FROM S26;");

    public final SQLStmt deleteS27 = new SQLStmt("DELETE FROM S27;");

    public final SQLStmt deleteS28 = new SQLStmt("DELETE FROM S28;");

    public final SQLStmt deleteS29 = new SQLStmt("DELETE FROM S29;");

    public final SQLStmt deleteS30 = new SQLStmt("DELETE FROM S30;");


	public long run() {

        voltQueueSQL(insertS22);
        voltExecuteSQL();

        voltQueueSQL(insertS23);
        voltExecuteSQL();

        voltQueueSQL(insertS24);
        voltExecuteSQL();

        voltQueueSQL(insertS25);
        voltExecuteSQL();

        voltQueueSQL(insertS26);
        voltExecuteSQL();

        voltQueueSQL(insertS27);
        voltExecuteSQL();

        voltQueueSQL(insertS28);
        voltExecuteSQL();

        voltQueueSQL(insertS29);
        voltExecuteSQL();

        voltQueueSQL(insertS30);
        voltExecuteSQL();

        voltQueueSQL(insertS31);
        voltExecuteSQL();

        
        voltQueueSQL(deleteS30);
        voltExecuteSQL();

        voltQueueSQL(deleteS29);
        voltExecuteSQL();

        voltQueueSQL(deleteS28);
        voltExecuteSQL();

        voltQueueSQL(deleteS27);
        voltExecuteSQL();

        voltQueueSQL(deleteS26);
        voltExecuteSQL();

        voltQueueSQL(deleteS25);
        voltExecuteSQL();

        voltQueueSQL(deleteS24);
        voltExecuteSQL();

        voltQueueSQL(deleteS23);
        voltExecuteSQL();

        voltQueueSQL(deleteS22);
        voltExecuteSQL();

        voltQueueSQL(deleteS21);
        voltExecuteSQL();
        


		return 0;
    }
}