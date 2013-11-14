package edu.brown.benchmark.complexstreamtrigger.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class ComplexTrigger3 extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "S3";
    }

    public final SQLStmt insertS2 = new SQLStmt(
        "INSERT INTO S4 (value) SELECT S3.value+1 FROM S3, votes_by_phone_number where S3.value=votes_by_phone_number.phone_number;"
    );
    
}
