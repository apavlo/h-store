package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger6 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S6";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S7 (value) SELECT S6.value+1 FROM S6, votes_by_phone_number where S6.value=votes_by_phone_number.phone_number;"
    );
    
}
