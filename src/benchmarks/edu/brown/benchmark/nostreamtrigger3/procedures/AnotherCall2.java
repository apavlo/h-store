package edu.brown.benchmark.nostreamtrigger3.procedures;

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
		addTriggerTable("S15");
    }


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

    // delete statements

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


	public long run() {


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

		return 0;
    }
}