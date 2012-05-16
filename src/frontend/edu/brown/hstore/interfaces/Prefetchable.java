package edu.brown.hstore.interfaces;

/**
 * Special marker that indicates that a given SQLStmt may be
 * prefetched if it is part of a distributed transaction
 * @author pavlo
 * @author cjl6
 */
public @interface Prefetchable {

}
