package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger2 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S2";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S3 (value) SELECT S2.value+1 FROM S2, votes_by_phone_number where S2.value=votes_by_phone_number.phone_number;"
    );
    
}
