package org.voltdb.exceptions;

import edu.brown.hstore.HStoreConstants;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EvictionPreparedTupleAccessException extends SerializableException {
    public static final long serialVersionUID = 0L;

    public final int tableId;
    public int partitionId;

    public EvictionPreparedTupleAccessException(ByteBuffer buffer) {
        super(buffer);
        tableId = buffer.getInt();
        partitionId = buffer.getInt();
    }

    public Table getTable(Database catalog_db) {
        return catalog_db.getTables().values()[tableId - 1];
    }

    public int getPartitionId(){
        return partitionId;
    }
    public void setPartitionId(int partitionId) {
        assert(this.partitionId == HStoreConstants.NULL_PARTITION_ID) :
                String.format("Trying to set %s.partition_id more than once [orig=%d / new=%d]",
                        EvictedTupleAccessException.class.getName(),
                        this.partitionId, partitionId);
        this.partitionId = partitionId;
    }

    @Override
    protected int p_getSerializedSize() {
        return 4 + 4;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) throws IOException {
        b.putInt(tableId);
        b.putInt(partitionId);
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.EvictionPreparedTupleAccessException;
    }
}
