package edu.brown.benchmark.articles.procedures;
import org.voltdb.*;

@ProcInfo(
	    partitionParam = 0,
	    singlePartition = true
	)
public class GetComments extends VoltProcedure{
	 
    public final SQLStmt GetComments = new SQLStmt("SELECT * FROM COMMENTS WHERE C_A_ID = ? LIMIT 5");
//    public final SQLStmt GetUser = new SQLStmt("SELECT * FROM USERS JOIN COMMENTS ON USERS.U_ID = COMMENTS.U_ID WHERE A_ID = ? ");

    public VoltTable[] run(long a_id) {
        voltQueueSQL(GetComments, a_id);
        return voltExecuteSQL(true);
    }   

}
