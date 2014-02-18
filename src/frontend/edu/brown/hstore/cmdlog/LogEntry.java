/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/

package edu.brown.hstore.cmdlog;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;

/**
 * LogEntry class for command logging
 * @author mkirsch
 * @author pavlo
 */
public class LogEntry implements FastSerializable, Poolable {
    private static final Logger LOG = Logger.getLogger(CommandLogWriter.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private Long txnId;
    private long timestamp;
    private int procId;
    private ParameterSet procParams;
    
    /**
     * Initialization method.
     * Note that even though we take in a LocalTransaction handle, we will
     * only store its txnId and its procedure information
     * @param ts
     * @return
     */
    public LogEntry init(AbstractTransaction ts) {
        this.txnId = ts.getTransactionId();
        this.procId = ts.getProcedure().getId();
        this.procParams = ts.getProcedureParameters();
        assert(this.isInitialized()) : 
            "Unexpected uninitialized " + this.getClass().getSimpleName();
        return (this);
    }

    public LogEntry init(AbstractTransaction ts, Boolean type) {
        this.txnId = ts.getTransactionId();
        this.procId = ts.getProcedure().getId();
        this.procParams = ts.getProcedureParameters();
        assert(this.isInitialized()) : 
            "Unexpected uninitialized " + this.getClass().getSimpleName();
        return (this);
    }
    
    public Long getTransactionId() {
        return txnId;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public int getProcedureId() {
        return procId;
    }
    public ParameterSet getProcedureParams() {
        return procParams;
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
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert(this.isInitialized()) : 
            "Unexpected uninitialized " + this.getClass().getSimpleName();               
        out.writeLong(this.txnId.longValue());
        out.writeLong(EstTime.currentTimeMillis());
        out.writeInt(this.procId);
        out.writeObject(this.procParams);
    }
    
    public String toString() {
        return ("Txn #" + this.txnId + " / Proc #" + this.procId);
    }
} // CLASS