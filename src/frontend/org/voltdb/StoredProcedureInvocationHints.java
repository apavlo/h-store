package org.voltdb;

import edu.brown.hstore.HStoreConstants;

/**
 * Special object that can be used to pass hints about
 * what a txn will do when the client submits the request.
 * @author pavlo
 *
 */
public final class StoredProcedureInvocationHints {

    public int basePartition = HStoreConstants.NULL_PARTITION_ID;
    // FIXME public PartitionSet expectedPartitions = null;
    
}
