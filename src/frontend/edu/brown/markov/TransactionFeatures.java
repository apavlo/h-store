package edu.brown.markov;

import org.voltdb.catalog.*;

public class TransactionFeatures {

    private final Database catalog_db;
    
    public TransactionFeatures(Database catalog_db) {
        this.catalog_db = catalog_db;
    }
    
}
