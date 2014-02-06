package edu.brown.benchmark.simplewindowsstore.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
//import org.voltdb.VoltTable;
//import org.voltdb.types.TimestampType;
import java.util.Random;

@ProcInfo (
        singlePartition = true
    )
public class SimpleCall extends VoltProcedure {
	Random r = new Random();
	int currentTimestamp = 0;
	int tsCounter = 1;
	int tuplesPerTimestamp = 3;
    
    public final SQLStmt insertS1 = new SQLStmt(
            "INSERT INTO S1 (myvalue, time) VALUES (?,?);"
        );

    public long run() {

        voltQueueSQL(insertS1, r.nextInt(10), currentTimestamp);
        voltExecuteSQL();
        if(tsCounter == tuplesPerTimestamp)
        {
        	currentTimestamp++;
        	tsCounter = 1;
        }
        else
        	tsCounter++;

		return 0;
    }
}
