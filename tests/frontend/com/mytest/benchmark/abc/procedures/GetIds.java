/**
 * 
 */
package com.mytest.benchmark.abc.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * @author mimosally
 *
 */
public class GetIds extends VoltProcedure {
	public final SQLStmt GETB = new SQLStmt("SELECT B_ID FROM TABLEB WHERE B_A_ID = ?");
	 public VoltTable[] run() {
	        voltQueueSQL(GETB);
	        return (voltExecuteSQL());
	    }   


}
