package edu.brown.benchmark.simplewindowhstore.procedures;

import org.voltdb.VoltProcedure;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import java.util.Random;


@ProcInfo (
        singlePartition = true
    )

public class AddNewValue extends VoltProcedure{
	Random r = new Random();
	int currentTimestamp = 0;
	int tsCounter = 1;
	int tuplesPerTimestamp = 3;
	int curTime = 0;
	//int nextVal = -5;
	final int slideSize = 1;
	final int windowSize = 100;
	
	public final SQLStmt insertStagingValue = new SQLStmt(
	   "INSERT INTO W_STAGING (myvalue, time) VALUES (?,?);"
    );
	
	public final SQLStmt stagingCount = new SQLStmt(
	   "SELECT COUNT(*), MAX(time) FROM W_STAGING;"
	);
	
	public final SQLStmt insertWindow = new SQLStmt(
	   "INSERT INTO W_ROWS (myvalue, time) SELECT * FROM W_STAGING;"
    );
	
	public final SQLStmt clearStaging = new SQLStmt(
	   "DELETE FROM W_STAGING;"
    );
	
	public final SQLStmt slideWindow = new SQLStmt(
	   "DELETE FROM W_ROWS WHERE time <= ?;"
    );
	
	public final SQLStmt insertResults = new SQLStmt(
	   "INSERT INTO AVG_FROM_WIN (time, valAvg) SELECT MAX(time), AVG(myvalue) FROM W_ROWS;"
    );
	
	public long run() {
        voltQueueSQL(insertStagingValue, r.nextInt(10), currentTimestamp);
		//voltQueueSQL(insertStagingValue, nextVal, currentTimestamp);
        voltQueueSQL(stagingCount);
        VoltTable validation[] = voltExecuteSQL();
        //nextVal = (int)(validation[1].fetchRow(0).getLong(0));
		
        if ((int)(validation[1].fetchRow(0).getLong(0)) == slideSize) {
        	curTime = (int)(validation[1].fetchRow(0).getLong(1));
            voltQueueSQL(insertWindow);
            voltQueueSQL(clearStaging);
            voltQueueSQL(slideWindow, curTime - windowSize);
            voltQueueSQL(insertResults);
            voltExecuteSQL(true);
        }
        
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
