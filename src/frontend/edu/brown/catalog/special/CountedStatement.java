package edu.brown.catalog.special;

import org.voltdb.catalog.Statement;

public class CountedStatement {
    public final Statement statement;
    public final int counter;
    
    public CountedStatement(Statement statement, int counter) {
        assert(counter >= 0);
        assert(statement != null);
        this.statement = statement;
        this.counter = counter;
    }
    
    @Override
    public String toString() {
        return this.statement.fullName() + "/#" + this.counter;
    }
    
    @Override
    public int hashCode() {
        return (this.statement.hashCode() * 31) + this.counter;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CountedStatement) {
            CountedStatement other = (CountedStatement)obj;
            return (this.counter == other.counter &&
                    this.statement.equals(other.statement));
        }
        return (false);
    }
}