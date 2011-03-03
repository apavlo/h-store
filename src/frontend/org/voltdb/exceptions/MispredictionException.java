package org.voltdb.exceptions;

import java.nio.ByteBuffer;

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
     * 
     * @param txn_id
     */
    public MispredictionException(long txn_id) {
        this.txn_id = txn_id;
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
    
    @Override
    public String getMessage() {
        return ("Mispredicted txn #" + this.txn_id);
    }
    
}
