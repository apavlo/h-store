package edu.brown.hstore.estimators;

public abstract class EstimatorUtil {

    /**
     * This marker is used to indicate that an Estimate is for the 
     * initial evaluation of a txn (i.e., before it has started running 
     * and submitted any query batches). 
     */
    public static final int INITIAL_ESTIMATE_BATCH = -1;
    
    /**
     * The value to use to indicate that a probability is null
     */
    public static final float NULL_MARKER = -1.0f;

}
