package edu.brown.hstore.txns;

import org.voltdb.catalog.Statement;

import edu.brown.utils.PartitionSet;

public class QueryInvocation {
    public final Statement stmt;
    public final int counter;
    public final PartitionSet partitions;
    public final int paramsHash;
    
    public QueryInvocation(Statement stmt, int counter, PartitionSet partitions, int paramsHash) {
        this.stmt = stmt;
        this.counter = counter;
        this.partitions = partitions;
        this.paramsHash = paramsHash;
    }
}