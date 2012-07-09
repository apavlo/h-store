package org.voltdb.exceptions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

/**
 * Special exception that is thrown by the EE when somebody tries to have it
 * read in a block from the anti-cache database that doesn't exist
 * This is used with the anti-cache feature.
 */
public class UnknownBlockAccessException extends SerializableException {

    public static final long serialVersionUID = 0L;

    public final int table_id;
    public final short block_id;
    
    /**
     * 
     * @param buffer ByteBuffer containing a serialized representation of the exception.
     */
    public UnknownBlockAccessException(ByteBuffer buffer) {
        super(buffer);
        this.table_id = buffer.getInt();
        this.block_id = buffer.getShort();
    }

    /**
     * Retrieve the Table that the txn tried to access that generated this exception.
     * @param catalog_db The current Database catalog handle
     */
    public Table getTableId(Database catalog_db) {
        return catalog_db.getTables().values()[this.table_id];
    }
    
    /**
     * Retrieve the block ids that the txn tried to access that generated this exception.
     */
    public short getBlockId() {
        return (this.block_id);
    }

    /**
     * Return the amount of storage necessary to store this exception
     */
    @Override
    protected int p_getSerializedSize() {
        return (4 + 2);
    }

    /**
     * Write out the internal state information for this Exception
     * @throws IOException
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        b.putInt(this.table_id);
        b.putShort(this.block_id);
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.UnknownBlockAccessException;
    }
}
