package edu.brown.hstore.wal;

import java.io.IOException;

import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.utils.Poolable;

public class LogEntry implements FastSerializable, Poolable {
    
    protected Long txnId;
    protected long timestamp;
    protected int procId;
    protected ParameterSet procParams;
    
    public LogEntry init(LocalTransaction ts) {
        this.txnId = ts.getTransactionId();
        assert(this.txnId != null);
        this.procId = ts.getProcedure().getId();
        this.procParams = ts.getProcedureParameters();
        return (this);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.txnId != null);
    }
    
    @Override
    public void finish() {
        this.txnId = null;
        this.timestamp = -1;
        this.procId = -1;
        this.procParams = null;
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        this.txnId = Long.valueOf(in.readLong());
        this.timestamp = in.readLong();
        this.procId = in.readInt();
        this.procParams = in.readObject(ParameterSet.class);
        
        //throw new RuntimeException("stupidInt : " + stupidInt + "txnId : " + txnId + " timestamp : " + timestamp + " procId : " + procId + " procParams : " + procParams.toString());
        
        // TODO: We need to figure out how we want to read these entries
        // back in. I suppose we could just make a new LocalTransaction
        // entry each time. What we really should do is recreate
        // the StoredProcedureInvocation and then pass that into
        // the HStoreSite so that we can replay the transaction
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeLong(this.txnId.longValue());
        out.writeLong(EstTime.currentTimeMillis());
        out.writeInt(this.procId);
        out.writeObject(this.procParams);
        
        //throw new RuntimeException("txnId : " + txnId + " timestamp : " + EstTime.currentTimeMillis() + " procId : " + procId + " procParams : " + procParams.toString());
    }
    
    public String toString() {
        return ("Txn #" + this.txnId + " / Proc #" + this.procId);
    }
} // CLASS
