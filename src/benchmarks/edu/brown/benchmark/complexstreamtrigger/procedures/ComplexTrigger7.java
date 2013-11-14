package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger7 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S7";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S8 (value) SELECT S7.value+1 FROM S7, votes_by_phone_number where S7.value=votes_by_phone_number.phone_number;"
    );
    
}
