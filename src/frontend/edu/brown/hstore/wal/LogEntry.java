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
            // TODO: We need to figure out how we want to read these entries
            // back in. I suppose we could just make a new LocalTransaction
            // entry each time. What we really should do is recreate
            // the StoredProcedureInvocation and then pass that into
            // the HStoreSite so that we can replay the transaction
            //File file = new File("filename");
            // Create a read-only memory-mapped file
            //FileChannel roChannel = new RandomAccessFile(file, "r").getChannel();

            //ByteBuffer readonlybuffer = roChannel.map(FileChannel.MapMode.READ_ONLY, 0, (int) roChannel.size());
            //long txn_id = in.readLong();
            //long time = in.readLong();
            //String procName = in.readString();
            //ParameterSet ps = (ParameterSet)in.readObject();
            /*FileWriter first = new FileWriter(new File("/ltmp/hstore/readbackin.log"), true);
            first.write("<" + txn_id + "><" + time + "><" + procName + "><" + "params" + "><COMMIT>");
            first.close();*/
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
