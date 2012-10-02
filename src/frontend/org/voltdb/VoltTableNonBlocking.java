package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import org.voltdb.messaging.FastSerializer;

import edu.brown.profilers.TransactionProfiler;

/**
 * Special VoltTable wrapper class for non-blocking query execution.
 * Any method invocation on these tables will cause the invoking thread 
 * to block until the real results are added. 
 * @author pavlo
 */
public class VoltTableNonBlocking extends VoltTable {

    private final Semaphore lock = new Semaphore(0);
    private final TransactionProfiler profiler;
    private VoltTable realTable = null;
    
    public VoltTableNonBlocking(TransactionProfiler profiler) {
        this.profiler = profiler;
    }
    
    private void block() {
        if (this.realTable == null) {
            if (this.profiler != null) this.profiler.markRemoteQueryAccess();
            this.lock.acquireUninterruptibly();
        }
    }
    
    public void setRealTable(VoltTable vt) {
        if (this.profiler != null) this.profiler.markRemoteQueryResult();
        this.realTable = vt;
        this.lock.release();
    }

    @Override
    public ByteBuffer getTableDataReference() {
        this.block();
        return this.realTable.getTableDataReference();
    }

    @Override
    public ByteBuffer getDirectDataReference() {
        this.block();
        return this.realTable.getDirectDataReference();
    }

    @Override
    public VoltTableRow cloneRow() {
        this.block();
        return this.realTable.cloneRow();
    }

    @Override
    public boolean hasColumn(String name) {
        this.block();
        return this.realTable.hasColumn(name);
    }

    @Override
    public String toString() {
        this.block();
        return this.realTable.toString();
    }

    @Override
    public String toString(boolean includeData) {
        this.block();
        return this.realTable.toString(includeData);
    }

    @Override
    public boolean hasSameContents(VoltTable other) {
        this.block();
        return this.realTable.hasSameContents(other);
    }

    @Override
    public boolean equals(Object o) {
        this.block();
        return this.realTable.equals(o);
    }

    @Override
    public int hashCode() {
        this.block();
        return this.realTable.hashCode();
    }

    @Override
    boolean testForUTF8Encoding(byte[] strbytes) {
        this.block();
        return this.realTable.testForUTF8Encoding(strbytes);
    }

    @Override
    public int getRowSize() {
        this.block();
        return this.realTable.getRowSize();
    }

    @Override
    public int getUnderlyingBufferSize() {
        this.block();
        return this.realTable.getUnderlyingBufferSize();
    }

    @Override
    public byte getStatusCode() {
        this.block();
        return this.realTable.getStatusCode();
    }

    @Override
    public void setStatusCode(byte code) {
        this.block();
        this.realTable.setStatusCode(code);
    }
    
    @Override
    public long asScalarLong() {
        this.block();
        return this.realTable.asScalarLong();
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        this.block();
        this.realTable.writeExternal(out);
    }

    @Override
    public void resetRowPosition() {
        this.block();
        this.realTable.resetRowPosition();
    }

    @Override
    public int getActiveRowIndex() {
        this.block();
        return this.realTable.getActiveRowIndex();
    }

    @Override
    public boolean advanceRow() {
        this.block();
        return this.realTable.advanceRow();
    }

    @Override
    public boolean advanceToRow(int rowIndex) {
        this.block();
        return this.realTable.advanceToRow(rowIndex);
    }

    @Override
    public BigDecimal getDecimalAsBigDecimal(String columnName) {
        this.block();
        return this.realTable.getDecimalAsBigDecimal(columnName);
    }

}
