package edu.brown.benchmark.users.procedures;
import org.voltdb.*;

@ProcInfo(
	    partitionParam = 0,
	    singlePartition = true
	)
public class GetArticle extends VoltProcedure{
	 
    public final SQLStmt GetArticle = new SQLStmt("SELECT * FROM ARTICLES WHERE A_ID = ? ");
//    public final SQLStmt GetUser = new SQLStmt("SELECT * FROM USERS JOIN COMMENTS ON USERS.U_ID = COMMENTS.U_ID WHERE A_ID = ? ");

    public VoltTable[] run(long a_id) {
    	System.out.println("Running procedure Get article "+a_id);
    	
    	voltQueueSQL(GetArticle, a_id);
        return voltExecuteSQL(true);
    }   

}
