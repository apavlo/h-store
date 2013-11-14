package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger8 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S8";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S9 (value) SELECT S8.value+1 FROM S8, votes_by_phone_number where S8.value=votes_by_phone_number.phone_number;"
    );
    
}
