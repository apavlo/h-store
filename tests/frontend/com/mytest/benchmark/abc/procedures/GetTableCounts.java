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
	public final SQLStmt GetB = new SQLStmt(
			"SELECT A_ID FROM TABLEA WHERE A_NUM=?");

	public VoltTable[] run(long a_id) {
		voltQueueSQL(COUNTA);
		voltQueueSQL(GetB, a_id);
		return (voltExecuteSQL());
	}

}
