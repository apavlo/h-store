package edu.brown.hstore.interfaces;
/*Indicates that a given SQLStmt may be deferred for a certain length of time.
*/
public @interface Deferred {
    int length();
}

