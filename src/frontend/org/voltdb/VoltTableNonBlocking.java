package org.voltdb;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;

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
    
    /**
     * Block the current thread until the real VoltTable
     * has been added into this wrapper.
     */
    private void block() {
        if (this.profiler != null) this.profiler.markRemoteQueryAccess();
        if (this.realTable == null) {
            this.lock.acquireUninterruptibly();
        }
    }
    
    public void setRealTable(VoltTable vt) {
        if (this.profiler != null) this.profiler.markRemoteQueryResult();
        this.realTable = vt;
        this.lock.release();
    }

    @Override
    public int getRowCount() {
        this.block();
        return this.realTable.getRowCount();
    }

    @Override
    protected int getRowStart() {
        this.block();
        return this.realTable.getRowStart();
    }

    @Override
    public int getColumnCount() {
        this.block();
        return this.realTable.getColumnCount();
    }

    @Override
    public String getColumnName(int index) {
        this.block();
        return this.realTable.getColumnName(index);
    }

    @Override
    public VoltType getColumnType(int index) {
        this.block();
        return this.realTable.getColumnType(index);
    }

    @Override
    public int getColumnIndex(String name) {
        this.block();
        return this.realTable.getColumnIndex(name);
    }

    @Override
    public VoltTableRow fetchRow(int index) {
        this.block();
        return this.realTable.fetchRow(index);
    }

    @Override
    public VoltTableRow getRow() {
        this.block();
        return this.realTable.getRow();
    }

    @Override
    public Object[] getRowArray() {
        this.block();
        return this.realTable.getRowArray();
    }

    @Override
    public String[] getRowStringArray() {
        this.block();
        return this.realTable.getRowStringArray();
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

    @SuppressWarnings("deprecation")
    @Override
    public boolean equals(Object o) {
        this.block();
        return this.realTable.equals(o);
    }

    @SuppressWarnings("deprecation")
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

    @Override
    public Object get(int columnIndex) {
        this.block();
        return this.realTable.get(columnIndex);
    }

    @Override
    public Object get(int columnIndex, VoltType type) {
        this.block();
        return this.realTable.get(columnIndex, type);
    }

    @Override
    public Object get(String columnName, VoltType type) {
        this.block();
        return this.realTable.get(columnName, type);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        this.block();
        return this.realTable.getBoolean(columnIndex);
    }

    @Override
    public boolean getBoolean(String columnName) {
        this.block();
        return this.realTable.getBoolean(columnName);
    }

    @Override
    public long getLong(int columnIndex) {
        this.block();
        return this.realTable.getLong(columnIndex);
    }

    @Override
    public long getLong(String columnName) {
        this.block();
        return this.realTable.getLong(columnName);
    }

    @Override
    public boolean wasNull() {
        this.block();
        return this.realTable.wasNull();
    }

    @Override
    public double getDouble(int columnIndex) {
        this.block();
        return this.realTable.getDouble(columnIndex);
    }

    @Override
    public double getDouble(String columnName) {
        this.block();
        return this.realTable.getDouble(columnName);
    }

    @Override
    public String getString(int columnIndex) {
        this.block();
        return this.realTable.getString(columnIndex);
    }

    @Override
    public String getString(String columnName) {
        this.block();
        return this.realTable.getString(columnName);
    }

    @Override
    public byte[] getStringAsBytes(int columnIndex) {
        this.block();
        return this.realTable.getStringAsBytes(columnIndex);
    }

    @Override
    public byte[] getStringAsBytes(String columnName) {
        this.block();
        return this.realTable.getStringAsBytes(columnName);
    }

    @Override
    public long getTimestampAsLong(int columnIndex) {
        this.block();
        return this.realTable.getTimestampAsLong(columnIndex);
    }

    @Override
    public long getTimestampAsLong(String columnName) {
        this.block();
        return this.realTable.getTimestampAsLong(columnName);
    }

    @Override
    public TimestampType getTimestampAsTimestamp(int columnIndex) {
        this.block();
        return this.realTable.getTimestampAsTimestamp(columnIndex);
    }

    @Override
    public TimestampType getTimestampAsTimestamp(String columnName) {
        this.block();
        return this.realTable.getTimestampAsTimestamp(columnName);
    }

    @Override
    public BigDecimal getDecimalAsBigDecimal(int columnIndex) {
        this.block();
        return this.realTable.getDecimalAsBigDecimal(columnIndex);
    }
}
