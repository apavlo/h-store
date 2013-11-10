package edu.brown.benchmark.nostreamtrigger5.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class AnotherCall4 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S31");
    }


    public final SQLStmt insertS32 = new SQLStmt("INSERT INTO S32 (value) SELECT * FROM S31;");

    public final SQLStmt insertS33 = new SQLStmt("INSERT INTO S33 (value) SELECT * FROM S32;");

    public final SQLStmt insertS34 = new SQLStmt("INSERT INTO S34 (value) SELECT * FROM S33;");

    public final SQLStmt insertS35 = new SQLStmt("INSERT INTO S35 (value) SELECT * FROM S34;");

    public final SQLStmt insertS36 = new SQLStmt("INSERT INTO S36 (value) SELECT * FROM S35;");

    public final SQLStmt insertS37 = new SQLStmt("INSERT INTO S37 (value) SELECT * FROM S36;");

    public final SQLStmt insertS38 = new SQLStmt("INSERT INTO S38 (value) SELECT * FROM S37;");

    public final SQLStmt insertS39 = new SQLStmt("INSERT INTO S39 (value) SELECT * FROM S38;");

    public final SQLStmt insertS40 = new SQLStmt("INSERT INTO S40 (value) SELECT * FROM S39;");

    public final SQLStmt insertS41 = new SQLStmt("INSERT INTO S41 (value) SELECT * FROM S40;");


    // delete statements
    public final SQLStmt deleteS31 = new SQLStmt("DELETE FROM S31;");

    public final SQLStmt deleteS32 = new SQLStmt("DELETE FROM S32;");

    public final SQLStmt deleteS33 = new SQLStmt("DELETE FROM S33;");

    public final SQLStmt deleteS34 = new SQLStmt("DELETE FROM S34;");

    public final SQLStmt deleteS35 = new SQLStmt("DELETE FROM S35;");

    public final SQLStmt deleteS36 = new SQLStmt("DELETE FROM S36;");

    public final SQLStmt deleteS37 = new SQLStmt("DELETE FROM S37;");

    public final SQLStmt deleteS38 = new SQLStmt("DELETE FROM S38;");

    public final SQLStmt deleteS39 = new SQLStmt("DELETE FROM S39;");

    public final SQLStmt deleteS40 = new SQLStmt("DELETE FROM S40;");


	public long run() {

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

        voltQueueSQL(insertS38);
        voltExecuteSQL();

        voltQueueSQL(insertS39);
        voltExecuteSQL();

        voltQueueSQL(insertS40);
        voltExecuteSQL();

        voltQueueSQL(insertS41);
        voltExecuteSQL();

        
        //delete
        voltQueueSQL(deleteS40);
        voltExecuteSQL();

        voltQueueSQL(deleteS39);
        voltExecuteSQL();

        voltQueueSQL(deleteS38);
        voltExecuteSQL();

        voltQueueSQL(deleteS37);
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
        

		return 0;
    }
}