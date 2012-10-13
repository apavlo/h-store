package edu.brown.catalog.special;

import org.voltdb.catalog.Statement;

public class CountedStatement {
    public final Statement statement;
    public final int counter;
    
    public CountedStatement(Statement statement, int counter) {
        this.statement = statement;
        this.counter = counter;
    }
}