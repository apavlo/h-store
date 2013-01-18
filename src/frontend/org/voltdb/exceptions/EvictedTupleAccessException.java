package org.voltdb.exceptions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

/**
 * Special exception that is thrown by the EE when as transaction
 * tries to access one or more tuples that have been evicted.
 * This is used with the anti-cache feature.
 */
public class EvictedTupleAccessException extends SerializableException {

    public static final long serialVersionUID = 0L;

    public final int table_id;
    public final short[] block_ids;
    
    /**
     * 
     * @param buffer ByteBuffer containing a serialized representation of the exception.
     */
    public EvictedTupleAccessException(ByteBuffer buffer) {
        super(buffer);
        
        this.table_id = buffer.getInt();
        final int num_blocks = buffer.getShort();
        assert(num_blocks > 0);
        this.block_ids = new short[num_blocks];
        for (int i = 0; i < this.block_ids.length; i++) {
            this.block_ids[i] = buffer.getShort();
        } // FOR
    }

    /**
     * Retrieve the Table that the txn tried to access that generated this exception.
     * @param catalog_db The current Database catalog handle
     */
    public Table getTable(Database catalog_db) {
        return catalog_db.getTables().values()[this.table_id-1];
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
        // 4 bytes for tableId
        // 2 bytes for # of block_ids 
        // (2 bytes * # of block_ids)
        return (4 + 2 + (2 * this.block_ids.length));
    }

    /**
     * Write out the internal state information for this Exception
     * @throws IOException
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        b.putInt(this.table_id);
        b.putShort((short)this.block_ids.length);
        for (int i = 0; i < this.block_ids.length; i++) {
            b.putShort(this.block_ids[i]);
        } // FOR
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.EvictedTupleAccessException;
    }
}
