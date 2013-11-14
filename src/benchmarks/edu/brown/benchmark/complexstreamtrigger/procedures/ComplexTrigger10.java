package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger10 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S10";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S11 (value) SELECT S10.value+1 FROM S10, votes_by_phone_number where S10.value=votes_by_phone_number.phone_number;"
    );
    
}
