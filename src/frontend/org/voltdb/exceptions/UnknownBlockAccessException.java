package org.voltdb.exceptions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

/**
 * Special exception that is thrown by the EE when somebody tries to have it
 * read in a block from the anti-cache database that doesn't exist
 * This is used with the anti-cache feature.
 */
public class UnknownBlockAccessException extends SerializableException {

    public static final long serialVersionUID = 0L;

    public final String table_name;
    public final short block_id;
    
    /**
     * 
     * @param buffer ByteBuffer containing a serialized representation of the exception.
     */
    public UnknownBlockAccessException(ByteBuffer buffer) {
        super(buffer);
        
        FastDeserializer fds = new FastDeserializer(buffer);
        String _table_name;
        short _block_id;
        try {
            _table_name = fds.readString();
            _block_id = fds.readShort();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        this.table_name = _table_name;
        this.block_id = _block_id;
    }

    /**
     * Retrieve the Table that the txn tried to access that generated this exception.
     * @param catalog_db The current Database catalog handle
     */
    public Table getTableId(Database catalog_db) {
        return catalog_db.getTables().getIgnoreCase(this.table_name);
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
        return (4 + this.table_name.length() + 2);
    }

    /**
     * Write out the internal state information for this Exception
     * @throws IOException
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        FastSerializer fs = new FastSerializer();
        try {
            fs.writeString(this.table_name);
            fs.writeShort(this.block_id);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        b.put(fs.getBuffer());
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.UnknownBlockAccessException;
    }
}
