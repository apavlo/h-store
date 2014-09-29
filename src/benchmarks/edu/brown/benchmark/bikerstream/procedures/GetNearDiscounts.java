package edu.brown.benchmark.bikerstream.procedures;

import org.voltdb.*;

/**
 * Created by racoon on 7/22/14.
 */

public class GetNearDiscounts extends VoltProcedure {

    public final SQLStmt getNear = new SQLStmt(
            "SELECT * FROM NearByDiscounts WHERE user_id = ?"
    );

    public VoltTable run(long user_id){
        voltQueueSQL(getNear, user_id);
        return voltExecuteSQL(true)[0];
    }


}
