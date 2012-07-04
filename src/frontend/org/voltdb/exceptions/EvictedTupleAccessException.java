package org.voltdb.exceptions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Special exception that is thrown by the EE when as transaction
 * tries to access one or more tuples that have been evicted.
 * This is used with the anti-cache feature.
 */
public class EvictedTupleAccessException extends SerializableException {

    public static final long serialVersionUID = 0L;

    public final int[] table_ids;
    public final short[] block_ids;
    
    /**
     * 
     * @param buffer ByteBuffer containing a serialized representation of the exception.
     */
    public EvictedTupleAccessException(ByteBuffer buffer) {
        super(buffer);
        
        final int num_blocks = buffer.getShort();
        assert(num_blocks > 0);
        this.table_ids = new int[num_blocks];
        this.block_ids = new short[num_blocks];
        for (int i = 0; i < this.block_ids.length; i++) {
            this.table_ids[i] = (int)buffer.getShort();
            this.block_ids[i] = buffer.getShort();
        } // FOR
    }

    /**
     * Retrieve the tables ids that the txn tried to access that generated this exception.
     */
    public int[] getTableIds() {
        return (this.table_ids);
    }
    
    /**
     * Retrieve the block ids that the txn tried to access that generated this exception.
     */
    public short[] getBlockIds() {
        return (this.block_ids);
    }

    /**
     * Return the amount of storage necessary to store this exception
     */
    @Override
    protected int p_getSerializedSize() {
        // # of block_ids + 
        // (4 * # of block_ids)
        // The first 16-bits are the Table.relativeIndex
        // The second 16-bits are the blockId
        return (2 + (4 * this.block_ids.length));
    }

    /**
     * Serialize the five character SQLState to the provided ByteBuffer
     * @throws IOException
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        b.putShort((short)this.block_ids.length);
        for (int i = 0; i < this.block_ids.length; i++) {
            b.putShort((short)this.table_ids[i]);
            b.putShort(this.block_ids[i]);
        }
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.EvictedTupleAccessException;
    }
}
