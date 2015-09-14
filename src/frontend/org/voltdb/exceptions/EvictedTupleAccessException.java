package org.voltdb.exceptions;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.hstore.HStoreConstants;

/**
 * Special exception that is thrown by the EE when as transaction
 * tries to access one or more tuples that have been evicted.
 * This is used with the anti-cache feature.
 */
public class EvictedTupleAccessException extends SerializableException {

    public static final long serialVersionUID = 0L;

    public final int table_id;
    public final int[] block_ids;
    public final int[] tuple_offsets;
    public int partition_id;
    
    /**
     * 
     * @param buffer ByteBuffer containing a serialized representation of the exception.
     */
    public EvictedTupleAccessException(ByteBuffer buffer) {
        super(buffer);
        
        this.table_id = buffer.getInt();
        final int num_blocks = buffer.getInt();
        assert(num_blocks > 0) :
            "Unexpected non-negative block count '" + num_blocks + "'";
        this.block_ids = new int[num_blocks];
        this.tuple_offsets = new int[num_blocks];
        for (int i = 0; i < this.block_ids.length; i++) {
            this.block_ids[i] = buffer.getInt();
        } // FOR
        for (int i = 0; i < this.tuple_offsets.length; i++) {
            this.tuple_offsets[i] = buffer.getInt();
        } // FOR
        this.partition_id = buffer.getInt();
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
    public int[] getBlockIds() {
        return (this.block_ids);
    }
    
    /**
     * Retrieve the tuples ids that the txn tried to access that generated this exception.
     */
    public int[] getTupleOffsets() {
        return (this.tuple_offsets);
    }
    
    public int getPartitionId(){
    	return this.partition_id;
    }
    public void setPartitionId(int partition_id) {
        assert(this.partition_id == HStoreConstants.NULL_PARTITION_ID) :
            String.format("Trying to set %s.partition_id more than once [orig=%d / new=%d]",
                          EvictedTupleAccessException.class.getName(),
                          this.partition_id, partition_id);
        this.partition_id = partition_id;
    }

    /**
     * Return the amount of storage necessary to store this exception
     */
    @Override
    protected int p_getSerializedSize() {
        // 4 bytes for tableId
        // 2 bytes for # of block_ids 
        // (4 bytes * # of block_ids)
        // (4 bytes * # of tuple offsets)
    	// 4 bytes for partition id
        return (4 + 4 + (4 * this.block_ids.length) + (4 * this.tuple_offsets.length) + 4);
    }

    /**
     * Write out the internal state information for this Exception
     * @throws IOException
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        b.putInt(this.table_id);
        b.putInt(this.block_ids.length);
        for (int i = 0; i < this.block_ids.length; i++) {
            b.putInt(this.block_ids[i]);
        } // FOR
        
        for(int i = 0; i < this.tuple_offsets.length; i++) {
            b.putInt(this.tuple_offsets[i]); 
        }
        b.putInt(this.partition_id);
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.EvictedTupleAccessException;
    }
}
