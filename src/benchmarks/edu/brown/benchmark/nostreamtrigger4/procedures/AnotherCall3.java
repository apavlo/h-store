package edu.brown.benchmark.nostreamtrigger4.procedures;

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
		addTriggerTable("S25");
    }


    public final SQLStmt insertS26 = new SQLStmt("INSERT INTO S26 (value) SELECT * FROM S25;");

    public final SQLStmt insertS27 = new SQLStmt("INSERT INTO S27 (value) SELECT * FROM S26;");

    public final SQLStmt insertS28 = new SQLStmt("INSERT INTO S28 (value) SELECT * FROM S27;");

    public final SQLStmt insertS29 = new SQLStmt("INSERT INTO S29 (value) SELECT * FROM S28;");

    public final SQLStmt insertS30 = new SQLStmt("INSERT INTO S30 (value) SELECT * FROM S29;");

    public final SQLStmt insertS31 = new SQLStmt("INSERT INTO S31 (value) SELECT * FROM S30;");

    public final SQLStmt insertS32 = new SQLStmt("INSERT INTO S32 (value) SELECT * FROM S31;");

    public final SQLStmt insertS33 = new SQLStmt("INSERT INTO S33 (value) SELECT * FROM S32;");

    public final SQLStmt insertS34 = new SQLStmt("INSERT INTO S34 (value) SELECT * FROM S33;");

    public final SQLStmt insertS35 = new SQLStmt("INSERT INTO S35 (value) SELECT * FROM S34;");

    public final SQLStmt insertS36 = new SQLStmt("INSERT INTO S36 (value) SELECT * FROM S35;");

    public final SQLStmt insertS37 = new SQLStmt("INSERT INTO S37 (value) SELECT * FROM S36;");


    // delete statements
    public final SQLStmt deleteS25 = new SQLStmt("DELETE FROM S25;");

    public final SQLStmt deleteS26 = new SQLStmt("DELETE FROM S26;");

    public final SQLStmt deleteS27 = new SQLStmt("DELETE FROM S27;");

    public final SQLStmt deleteS28 = new SQLStmt("DELETE FROM S28;");

    public final SQLStmt deleteS29 = new SQLStmt("DELETE FROM S29;");

    public final SQLStmt deleteS30 = new SQLStmt("DELETE FROM S30;");

    public final SQLStmt deleteS31 = new SQLStmt("DELETE FROM S31;");

    public final SQLStmt deleteS32 = new SQLStmt("DELETE FROM S32;");

    public final SQLStmt deleteS33 = new SQLStmt("DELETE FROM S33;");

    public final SQLStmt deleteS34 = new SQLStmt("DELETE FROM S34;");

    public final SQLStmt deleteS35 = new SQLStmt("DELETE FROM S35;");

    public final SQLStmt deleteS36 = new SQLStmt("DELETE FROM S36;");



	public long run() {


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

        voltQueueSQL(insertS32);
        voltExecuteSQL();

        voltQueueSQL(insertS33);
        voltExecuteSQL();

        voltQueueSQL(insertS34);
        voltExecuteSQL();

        voltQueueSQL(insertS35);
        voltExecuteSQL();

        voltQueueSQL(insertS36);
        voltExecuteSQL();

        voltQueueSQL(insertS37);
        voltExecuteSQL();


        voltQueueSQL(deleteS36);
        voltExecuteSQL();

        voltQueueSQL(deleteS35);
        voltExecuteSQL();

        voltQueueSQL(deleteS34);
        voltExecuteSQL();

        voltQueueSQL(deleteS33);
        voltExecuteSQL();

        voltQueueSQL(deleteS32);
        voltExecuteSQL();

        voltQueueSQL(deleteS31);
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


		return 0;
    }
}