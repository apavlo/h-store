package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger1 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S1";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S2 (value) SELECT S1.value+1 FROM S1, votes_by_phone_number where S1.value=votes_by_phone_number.phone_number;"
    );
    
}
