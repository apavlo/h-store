package org.voltdb.exceptions;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Thrown internally when we mispredicted a transaction as being single-partitioned
 * This will cause the transaction to get restarted as multi-partitioned 
 * @author pavlo
 */
public class MispredictionException extends SerializableException {
    private static final long serialVersionUID = 1L;
    
    /**
     * The transaction id that caused this exception
     */
    private long txn_id;
    /**
     * The partitions that the transaction was about to touch or had touched
     * before it was aborted
     */
    private final Set<Integer> partitions = new HashSet<Integer>();

    /**
     * Constructor
     * @param txn_id
     * @param partitions
     */
    public MispredictionException(long txn_id, Set<Integer> partitions) {
        this.txn_id = txn_id;
        this.partitions.addAll(partitions);
    }
    
    /**
     * Constructor for deserializing an exception returned from the EE.
     * @param exceptionBuffer
     */
    public MispredictionException(ByteBuffer exceptionBuffer) {
        super(exceptionBuffer);
        this.txn_id = exceptionBuffer.getLong();
    }
    
    /**
     * The transaction id that caused this exception
     * @return
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    
    /**
     * Partitions that this txn touched before it aborted
     * @return
     */
    public Set<Integer> getPartitions() {
        return partitions;
    }
    
    @Override
    public String getMessage() {
        return ("Mispredicted txn #" + this.txn_id + " " + this.partitions);
    }
    
}
