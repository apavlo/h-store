package edu.brown.benchmark.nostreamtrigger2.procedures;

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
		addTriggerTable("S26");
    }

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

    public final SQLStmt insertS38 = new SQLStmt("INSERT INTO S38 (value) SELECT * FROM S37;");

    public final SQLStmt insertS39 = new SQLStmt("INSERT INTO S39 (value) SELECT * FROM S38;");

    public final SQLStmt insertS40 = new SQLStmt("INSERT INTO S40 (value) SELECT * FROM S39;");

    public final SQLStmt insertS41 = new SQLStmt("INSERT INTO S41 (value) SELECT * FROM S40;");

    public final SQLStmt insertS42 = new SQLStmt("INSERT INTO S42 (value) SELECT * FROM S41;");

    public final SQLStmt insertS43 = new SQLStmt("INSERT INTO S43 (value) SELECT * FROM S42;");

    public final SQLStmt insertS44 = new SQLStmt("INSERT INTO S44 (value) SELECT * FROM S43;");

    public final SQLStmt insertS45 = new SQLStmt("INSERT INTO S45 (value) SELECT * FROM S44;");

    public final SQLStmt insertS46 = new SQLStmt("INSERT INTO S46 (value) SELECT * FROM S45;");

    public final SQLStmt insertS47 = new SQLStmt("INSERT INTO S47 (value) SELECT * FROM S46;");

    public final SQLStmt insertS48 = new SQLStmt("INSERT INTO S48 (value) SELECT * FROM S47;");

    public final SQLStmt insertS49 = new SQLStmt("INSERT INTO S49 (value) SELECT * FROM S48;");

    public final SQLStmt insertS50 = new SQLStmt("INSERT INTO S50 (value) SELECT * FROM S49;");

    public final SQLStmt insertS51 = new SQLStmt("INSERT INTO S51 (value) SELECT * FROM S50;");

    // delete statements

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

    public final SQLStmt deleteS37 = new SQLStmt("DELETE FROM S37;");

    public final SQLStmt deleteS38 = new SQLStmt("DELETE FROM S38;");

    public final SQLStmt deleteS39 = new SQLStmt("DELETE FROM S39;");

    public final SQLStmt deleteS40 = new SQLStmt("DELETE FROM S40;");

    public final SQLStmt deleteS41 = new SQLStmt("DELETE FROM S41;");

    public final SQLStmt deleteS42 = new SQLStmt("DELETE FROM S42;");

    public final SQLStmt deleteS43 = new SQLStmt("DELETE FROM S43;");

    public final SQLStmt deleteS44 = new SQLStmt("DELETE FROM S44;");

    public final SQLStmt deleteS45 = new SQLStmt("DELETE FROM S45;");

    public final SQLStmt deleteS46 = new SQLStmt("DELETE FROM S46;");

    public final SQLStmt deleteS47 = new SQLStmt("DELETE FROM S47;");

    public final SQLStmt deleteS48 = new SQLStmt("DELETE FROM S48;");

    public final SQLStmt deleteS49 = new SQLStmt("DELETE FROM S49;");

    public final SQLStmt deleteS50 = new SQLStmt("DELETE FROM S50;");


	public long run() {

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

        voltQueueSQL(insertS38);
        voltExecuteSQL();

        voltQueueSQL(insertS39);
        voltExecuteSQL();

        voltQueueSQL(insertS40);
        voltExecuteSQL();

        voltQueueSQL(insertS41);
        voltExecuteSQL();

        voltQueueSQL(insertS42);
        voltExecuteSQL();

        voltQueueSQL(insertS43);
        voltExecuteSQL();

        voltQueueSQL(insertS44);
        voltExecuteSQL();

        voltQueueSQL(insertS45);
        voltExecuteSQL();

        voltQueueSQL(insertS46);
        voltExecuteSQL();

        voltQueueSQL(insertS47);
        voltExecuteSQL();

        voltQueueSQL(insertS48);
        voltExecuteSQL();

        voltQueueSQL(insertS49);
        voltExecuteSQL();

        voltQueueSQL(insertS50);
        voltExecuteSQL();

        voltQueueSQL(insertS51);
        voltExecuteSQL();
        
        //delete
        voltQueueSQL(deleteS50);
        voltExecuteSQL();

        voltQueueSQL(deleteS49);
        voltExecuteSQL();

        voltQueueSQL(deleteS48);
        voltExecuteSQL();

        voltQueueSQL(deleteS47);
        voltExecuteSQL();

        voltQueueSQL(deleteS46);
        voltExecuteSQL();

        voltQueueSQL(deleteS45);
        voltExecuteSQL();

        voltQueueSQL(deleteS44);
        voltExecuteSQL();

        voltQueueSQL(deleteS43);
        voltExecuteSQL();

        voltQueueSQL(deleteS42);
        voltExecuteSQL();

        voltQueueSQL(deleteS41);
        voltExecuteSQL();
        
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


		return 0;
    }
}