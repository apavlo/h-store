package org.voltdb;

import edu.brown.hstore.HStoreConstants;

/**
 * Special object that can be used to pass hints about
 * what a txn will do when the client submits the request.
 * @author pavlo
 */
public final class StoredProcedureInvocationHints {

    /**
     * Setting this value will tell the HStoreSite what partition to invoke this
     * transaction's control code on.
     */
    public int basePartition = HStoreConstants.NULL_PARTITION_ID;

    // FIXME public PartitionSet expectedPartitions = null;
    
}
