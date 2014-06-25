package edu.brown.benchmark.recovery.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTrigger;
import org.voltdb.types.TimestampType;

@ProcInfo (
        singlePartition = true
    )
public class CountingTrigger extends VoltTrigger {
    
    @Override
    protected String toSetStreamName() {
        return "W1";
    }
    
    public final SQLStmt deleteCountingboard = new SQLStmt(
            "DELETE FROM T2;"
    );

    public final SQLStmt updateCountingboard = new SQLStmt(
            "INSERT INTO T2 (value) SELECT count(*) FROM W1 WHERE W1.value > 100;"
    );
}