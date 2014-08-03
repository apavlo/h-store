package edu.brown.benchmark.tm1.procedures;

import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;

public class InsertSubscriber extends VoltProcedure {
    private final Random rand = new Random();

    public SQLStmt insert = new SQLStmt(
        "INSERT INTO " + TM1Constants.TABLENAME_SUBSCRIBER +
        " VALUES (" +
            "?, " + // S_ID
            "?, " + // SUB_NBR
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + // BIT_#
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + // HEX_#
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + // BYTE2_#
            "?, " + // MSC_LOCATION
            "? " + // VLR_LOCATION
            ")"
    );

    public VoltTable[] run(long s_id, String sub_nbr) {
        Object args[] = new Object[34];
        args[0] = s_id;
        args[1] = sub_nbr;

        for (int i = 2; i <= 12; i++) {
            args[i] = 0; // BIT_#
            args[i + 10] = 1; // HEX_#
            args[i + 20] = 16; // BYTE2_#
        } // FOR

        args[32] = this.rand.nextInt();
        args[33] = this.rand.nextInt();

        voltQueueSQL(insert, args);

        return (voltExecuteSQL(true));
    }

}
