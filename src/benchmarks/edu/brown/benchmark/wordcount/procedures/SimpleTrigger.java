package edu.brown.benchmark.wordcount.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltTrigger;

public class SimpleTrigger extends VoltTrigger {

    @Override
    protected String toSetStreamName() {
        return "words";
    }

  
    @StmtInfo(
            upsertable=true
        )
    public final SQLStmt insertCountsStmt = 
        new SQLStmt(
                "INSERT INTO counts ( word, num ) SELECT counts.word, counts.num + 1 FROM counts, words WHERE counts.word=words.word;"
                );
    
}
