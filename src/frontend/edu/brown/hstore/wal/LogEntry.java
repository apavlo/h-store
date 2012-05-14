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

package edu.brown.hstore.wal;

import java.io.IOException;

import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.EstTime;

import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.utils.Poolable;

/**
 * LogEntry class for command logging
 * @author mkirsch
 * @author pavlo
 */
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
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeLong(this.txnId.longValue());
        out.writeLong(EstTime.currentTimeMillis());
        out.writeInt(this.procId);
        out.writeObject(this.procParams);
    }
    
    public String toString() {
        return ("Txn #" + this.txnId + " / Proc #" + this.procId);
    }
} // CLASS
