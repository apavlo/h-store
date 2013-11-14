package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger4 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S4";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S5 (value) SELECT S4.value+1 FROM S4, votes_by_phone_number where S4.value=votes_by_phone_number.phone_number;"
    );
    
}
