package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger9 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S9";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S10 (value) SELECT S9.value+1 FROM S9, votes_by_phone_number where S9.value=votes_by_phone_number.phone_number;"
    );
    
}
