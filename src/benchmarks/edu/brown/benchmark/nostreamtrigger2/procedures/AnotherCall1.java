package edu.brown.benchmark.nostreamtrigger2.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class AnotherCall1 extends VoltProcedure {
    
    protected void toSetTriggerTableName()
    {   
        // set which stream will be used to trigger this frontend procedure
		addTriggerTable("S1");
    }

    public final SQLStmt insertS2 = new SQLStmt("INSERT INTO S2 (value) SELECT * FROM S1;");

    public final SQLStmt insertS3 = new SQLStmt("INSERT INTO S3 (value) SELECT * FROM S2;");

    public final SQLStmt insertS4 = new SQLStmt("INSERT INTO S4 (value) SELECT * FROM S3;");

    public final SQLStmt insertS5 = new SQLStmt("INSERT INTO S5 (value) SELECT * FROM S4;");

    public final SQLStmt insertS6 = new SQLStmt("INSERT INTO S6 (value) SELECT * FROM S5;");

    public final SQLStmt insertS7 = new SQLStmt("INSERT INTO S7 (value) SELECT * FROM S6;");

    public final SQLStmt insertS8 = new SQLStmt("INSERT INTO S8 (value) SELECT * FROM S7;");

    public final SQLStmt insertS9 = new SQLStmt("INSERT INTO S9 (value) SELECT * FROM S8;");

    public final SQLStmt insertS10 = new SQLStmt("INSERT INTO S10 (value) SELECT * FROM S9;");

    public final SQLStmt insertS11 = new SQLStmt("INSERT INTO S11 (value) SELECT * FROM S10;");

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

    public final SQLStmt insertS22 = new SQLStmt("INSERT INTO S22 (value) SELECT * FROM S21;");

    public final SQLStmt insertS23 = new SQLStmt("INSERT INTO S23 (value) SELECT * FROM S22;");

    public final SQLStmt insertS24 = new SQLStmt("INSERT INTO S24 (value) SELECT * FROM S23;");

    public final SQLStmt insertS25 = new SQLStmt("INSERT INTO S25 (value) SELECT * FROM S24;");

    public final SQLStmt insertS26 = new SQLStmt("INSERT INTO S26 (value) SELECT * FROM S25;");


    // delete statements
    public final SQLStmt deleteS1 = new SQLStmt("DELETE FROM S1;");

    public final SQLStmt deleteS2 = new SQLStmt("DELETE FROM S2;");

    public final SQLStmt deleteS3 = new SQLStmt("DELETE FROM S3;");

    public final SQLStmt deleteS4 = new SQLStmt("DELETE FROM S4;");

    public final SQLStmt deleteS5 = new SQLStmt("DELETE FROM S5;");

    public final SQLStmt deleteS6 = new SQLStmt("DELETE FROM S6;");

    public final SQLStmt deleteS7 = new SQLStmt("DELETE FROM S7;");

    public final SQLStmt deleteS8 = new SQLStmt("DELETE FROM S8;");

    public final SQLStmt deleteS9 = new SQLStmt("DELETE FROM S9;");

    public final SQLStmt deleteS10 = new SQLStmt("DELETE FROM S10;");
    // 1
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

    public final SQLStmt deleteS21 = new SQLStmt("DELETE FROM S21;");

    public final SQLStmt deleteS22 = new SQLStmt("DELETE FROM S22;");

    public final SQLStmt deleteS23 = new SQLStmt("DELETE FROM S23;");

    public final SQLStmt deleteS24 = new SQLStmt("DELETE FROM S24;");

    public final SQLStmt deleteS25 = new SQLStmt("DELETE FROM S25;");


	public long run() {

        voltQueueSQL(insertS2);
        voltExecuteSQL();

        voltQueueSQL(insertS3);
        voltExecuteSQL();

        voltQueueSQL(insertS4);
        voltExecuteSQL();

        voltQueueSQL(insertS5);
        voltExecuteSQL();

        voltQueueSQL(insertS6);
        voltExecuteSQL();

        voltQueueSQL(insertS7);
        voltExecuteSQL();

        voltQueueSQL(insertS8);
        voltExecuteSQL();

        voltQueueSQL(insertS9);
        voltExecuteSQL();

        voltQueueSQL(insertS10);
        voltExecuteSQL();

        voltQueueSQL(insertS11);
        voltExecuteSQL();

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

        
        //delete
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

        voltQueueSQL(deleteS14);
        voltExecuteSQL();

        voltQueueSQL(deleteS13);
        voltExecuteSQL();

        voltQueueSQL(deleteS12);
        voltExecuteSQL();

        voltQueueSQL(deleteS11);
        voltExecuteSQL();

        voltQueueSQL(deleteS10);
        voltExecuteSQL();

        voltQueueSQL(deleteS9);
        voltExecuteSQL();

        voltQueueSQL(deleteS8);
        voltExecuteSQL();

        voltQueueSQL(deleteS7);
        voltExecuteSQL();

        voltQueueSQL(deleteS6);
        voltExecuteSQL();

        voltQueueSQL(deleteS5);
        voltExecuteSQL();

        voltQueueSQL(deleteS4);
        voltExecuteSQL();

        voltQueueSQL(deleteS3);
        voltExecuteSQL();

        voltQueueSQL(deleteS2);
        voltExecuteSQL();

        voltQueueSQL(deleteS1);
        voltExecuteSQL();

		return 0;
    }
}