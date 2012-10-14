package edu.brown.hstore.estimators;

import java.util.Arrays;

import org.voltdb.catalog.Statement;
import org.voltdb.utils.Pair;

public class QueryEstimate extends Pair<Statement[], int[]> {
    
    public QueryEstimate(Statement[] statements, int[] counters) {
        super(statements, counters, false);
        assert(statements.length == counters.length);
    }
    
    @Override
    protected int computeHashCode() {
        return (Arrays.hashCode(this.getFirst()) * 31) +
                Arrays.hashCode(this.getSecond());
    }
    
    public int size() {
        return (this.getFirst().length);
    }
    public Statement getStatement(int offset) {
        return (this.getFirst()[offset]);
    }
    public int getStatementCounter(int offset) {
        return (this.getSecond()[offset]);
    }
    
    public String debug() {
        Statement statements[] = this.getFirst();
        int counters[] = this.getSecond();
        String ret = "";
        for (int i = 0; i < statements.length; i++) {
            if (i > 0) ret += "\n";
            ret += statements[i].fullName() + " / " + counters[i];
        }
        return (ret);
    }
    
}
