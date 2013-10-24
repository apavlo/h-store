package edu.brown.benchmark.streamtrigger.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;
import org.voltdb.StmtInfo;

@ProcInfo (
        singlePartition = true
    )
public class TestCall extends VoltProcedure {
    
     @StmtInfo(
            upsertable=true
        )
   public final SQLStmt insertVotesByPN = new SQLStmt(
            "INSERT INTO votes_by_phone_number (phone_number, num_votes) VALUES(?,?)"
        );
    
    public long run(int i, int j) {

        voltQueueSQL(insertVotesByPN, i, j);
        voltExecuteSQL(true);

        return 0;
    }
}
