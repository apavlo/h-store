package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger5 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S5";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S6 (value) SELECT S5.value+1 FROM S5, votes_by_phone_number where S5.value=votes_by_phone_number.phone_number;"
    );
    
}
