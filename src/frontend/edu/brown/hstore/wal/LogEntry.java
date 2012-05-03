package edu.brown.hstore.wal;

import java.io.IOException;

import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.EstTime;

public class LogEntry implements FastSerializable {
    
    protected Long txnId;
    protected long timestamp;
    protected String procName;
    protected ParameterSet procParams;
    
    /** Set to true if we know that this entry has been written to disk */
    protected boolean flushed = false;

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        this.txnId = Long.valueOf(in.readLong());
        // TODO: Get other stuff!
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeLong(this.txnId.longValue());
        out.writeLong(EstTime.currentTimeMillis());
        
        // TODO: Remove this once we have the header stuff working...
        out.writeString(this.procName);
        /* If we have a header with a mapping to Procedure names to ProcIds, we can do this
        int procId = ts.getProcedure().getId();
        this.buffer.putInt(procId);
        */
        
        out.writeObject(this.procParams);
    }
} // CLASS
