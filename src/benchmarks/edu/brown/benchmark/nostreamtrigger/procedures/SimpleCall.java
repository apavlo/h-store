package edu.brown.benchmark.nostreamtrigger.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo(singlePartition = true)
public class SimpleCall extends VoltProcedure {

    public final SQLStmt insertS1 = new SQLStmt("INSERT INTO S1 (value) VALUES (0);");

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

        voltQueueSQL(insertS1);
        voltExecuteSQL();

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
