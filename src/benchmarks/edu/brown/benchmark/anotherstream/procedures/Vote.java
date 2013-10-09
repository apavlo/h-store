package edu.brown.benchmark.anotherstream.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import edu.brown.benchmark.anotherstream.VoterConstants;

public class Vote extends VoltProcedure
{
    public final SQLStmt insertStmt = new SQLStmt("insert into votes_by_phone_number (phone_number, num_votes) SELECT * from S3;");
	
    public long run() {
		
            voltQueueSQL(insertStmt);
            voltExecuteSQL();
	 
         return VoterConstants.VOTE_SUCCESSFUL;
    }
}
