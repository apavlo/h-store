package edu.brown.interfaces;

/**
 * Special marker that indicates that a given SQLStmt may be
 * deferred for a certain length of time.
 * @author ambell
 */
public @interface Deferrable {
    int length();
}

