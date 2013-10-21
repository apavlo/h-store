
package edu.brown.benchmark.streamexample.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.VoltTrigger;

public class GetDataTrigger extends VoltTrigger {

	@Override
	protected String toSetStreamName() {
		return "SA";
	}
	
    public final SQLStmt GetA = new SQLStmt("SELECT * FROM SA ");

    //public final SQLStmt GetA = new SQLStmt("SELECT * FROM SA[ROWS 10 SLIDE 5] ");

    // I just added to make current framework happy, however I do not
    // think our trigger does need run method like this way. [hawk]
    public VoltTable[] run(long sa_id) {
        voltQueueSQL(GetA, sa_id);
        return (voltExecuteSQL());
    }
}
