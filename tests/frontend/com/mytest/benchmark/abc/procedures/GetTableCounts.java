/**
 * 
 */
package com.mytest.benchmark.abc.procedures;

import org.voltdb.*;

import edu.brown.benchmark.locality.LocalityConstants;

/**
 * @author mimosally
 *
 */
public class GetTableCounts extends VoltProcedure {
	public final SQLStmt COUNTA = new SQLStmt("SELECT COUNT(*) FROM TABLEA");
	public final SQLStmt GetB = new SQLStmt("SELECT B_ID FROM TABLEB WHERE B_A_ID=?");
	 public VoltTable[] run(long a_id) {
	        voltQueueSQL(COUNTA);
	        voltQueueSQL(GetB,a_id);
	        return (voltExecuteSQL());
	    }   


}
