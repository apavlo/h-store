/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.SpeculationType;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.StringUtil;

/**
 * Packages up the data to be sent back to the client as a stored
 * procedure response in one FastSerialziable object.
 *
 */
public class ClientResponseImpl implements FastSerializable, ClientResponse {
    private boolean setProperly = false;
    private Status status;
    private String statusString = null;
    private byte appStatus = Byte.MIN_VALUE;
    private String appStatusString = null;
    private VoltTable[] results = HStoreConstants.EMPTY_RESULT;

    private int clusterRoundTripTime = -1;
    private int clientRoundTripTime = -1;
    private SerializableException m_exception = null;
    
    // PAVLO
    private long txn_id;
    private boolean singlepartition = true;
    private int basePartition = -1;
    private int restartCounter = 0;
    private SpeculationType speculative = SpeculationType.NULL;
    private ClientResponseDebug debug = null;

    /** opaque data optionally provided by and returned to the client */
    private long clientHandle = -1;
    
    // ARIES
    private byte[] arieslogData = null;
    
    public ClientResponseImpl() {}

    /**
     * 
     * @param txn_id
     * @param client_handle
     * @param basePartition
     * @param status
     * @param results
     * @param statusString
     */
    public ClientResponseImpl(long txn_id, long client_handle, int basePartition, Status status, VoltTable[] results, String statusString) {
        this(txn_id, client_handle, basePartition, status, Byte.MIN_VALUE, null, results, statusString, null);
    }

    /**
     * And another....
     * @param status
     * @param results
     * @param extra
     * @param e
     */
    public ClientResponseImpl(long txn_id, long client_handle, int basePartition, Status status, VoltTable[] results, String statusString, SerializableException e) {
        this(txn_id, client_handle, basePartition, status, Byte.MIN_VALUE, null, results, statusString, e);
    }

    public ClientResponseImpl(long txn_id, long client_handle, int basePartition, Status status, byte appStatus, String appStatusString, VoltTable[] results, String statusString, SerializableException e) {
        this.init(txn_id, client_handle, basePartition, status, appStatus, appStatusString, results, statusString, e);
    }

    @Deprecated
    public void init(Long txn_id, long client_handle, int basePartition, Status status, byte appStatus, String appStatusString, VoltTable[] results, String statusString, SerializableException e) {
        this.txn_id = txn_id.longValue();
        this.clientHandle = client_handle;
        this.basePartition = basePartition;
        this.appStatus = appStatus;
        this.appStatusString = appStatusString;
        setResults(status, results, statusString, e);
    }
    
    public void init(LocalTransaction ts, Status status, VoltTable[] results, String statusString) {
        this.init(ts, status, Byte.MIN_VALUE, null, results, statusString, ts.getPendingError());
    }
    
    public void init(LocalTransaction ts,
                     Status status,
                     byte appStatus,
                     String appStatusString,
                     VoltTable[] results,
                     String statusString,
                     SerializableException e) {
        this.txn_id = ts.getTransactionId().longValue();
        this.clientHandle = ts.getClientHandle();
        this.basePartition = ts.getBasePartition();
        this.appStatus = appStatus;
        this.appStatusString = appStatusString;
        this.restartCounter = ts.getRestartCounter();
        this.singlepartition = ts.isPredictSinglePartition();
        this.speculative = ts.getSpeculationType();
        this.setResults(status, results, statusString, e);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.txn_id != -1);
    }
    
    @Override
    public void finish() {
        this.txn_id = -1;
        this.clientHandle = -1;
        this.status = null;
        this.results = null;
        this.restartCounter = 0;
    }
    
    private void setResults(Status status, VoltTable[] results, String statusString) {
        assert results != null;
        for (VoltTable result : results) {
            // null values are not permitted in results. If there is one, it will cause an
            // exception in writeExternal. This throws the exception sooner.
            assert result != null;
        }

        this.status = status;
        this.results = results;
        this.statusString = statusString;
        this.setProperly = true;
    }

    private void setResults(Status status, VoltTable[] results, String extra, SerializableException e) {
        m_exception = e;
        setResults(status, results, extra);
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    @Override
    public int getBasePartition() {
        return (this.basePartition);
    }

    public void setBasePartition(int val) {
        this.basePartition = val;
    }
    
    @Override
    public boolean isSinglePartition() {
        return singlepartition;
    }
    public void setSinglePartition(boolean val) {
        this.singlepartition = val;
    }
    
    @Override
    public Status getStatus() {
        return status;
    }

    public VoltTable[] getResults() {
        return results;
    }
    
    @Override
    public int getResultsSize() {
        int ret = 0;
        for (VoltTable vt : this.results) {
            ret += vt.getUnderlyingBufferSize();
        } // FOR
        return ret;
    }

    public String getStatusString() {
        return statusString;
    }

    public void setClientHandle(long aHandle) {
        clientHandle = aHandle;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    public SerializableException getException() {
        return m_exception;
    }

    @Override
    public long getTransactionId() {
        return (this.txn_id);
    }
    
    
    @Override
    public int getClusterRoundtrip() {
        return clusterRoundTripTime;
    }

    public void setClusterRoundtrip(int time) {
        clusterRoundTripTime = time;
    }

    @Override
    public int getClientRoundtrip() {
        return clientRoundTripTime;
    }

    public void setClientRoundtrip(int time) {
        clientRoundTripTime = time;
    }

    @Override
    public byte getAppStatus() {
        return appStatus;
    }

    @Override
    public String getAppStatusString() {
        return appStatusString;
    }
    
    @Override
    public int getRestartCounter() {
        return restartCounter;
    }
    
    public void setRestartCounter(int restarts) {
        restartCounter = restarts;
    }

    @Override
    public boolean isSpeculative() {
        return (this.speculative != SpeculationType.NULL);
    }
    
    // ----------------------------------------------------------------------------
    // SPECIAL DEBUG HANDLE
    // ----------------------------------------------------------------------------
    
    @Override
    public ClientResponseDebug getDebug() {
        return (this.debug);
    }
    @Override
    public boolean hasDebug() {
        return (this.debug != null);
    }
    public void setDebug(ClientResponseDebug debug) {
        this.debug = debug;
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        in.readByte();//Skip version byte   // 1 byte
        this.restartCounter = in.readByte();     // 1 byte
        this.txn_id = in.readLong();             // 8 bytes
        this.clientHandle = in.readLong();       // 8 bytes
        this.singlepartition = in.readBoolean(); // 1 byte
        this.basePartition = in.readInt();       // 4 bytes
        this.speculative = SpeculationType.get(in.readByte()); // 1 byte
        this.status = Status.valueOf(in.readByte()); // 1 byte
        
        byte presentFields = in.readByte(); // 1 byte
        if ((presentFields & (1 << 5)) != 0) {
            statusString = in.readString();
        } else {
            statusString = null;
        }
        appStatus = in.readByte();
        if ((presentFields & (1 << 7)) != 0) {
            appStatusString = in.readString();
        } else {
            appStatusString = null;
        }
        clusterRoundTripTime = in.readInt();
        if ((presentFields & (1 << 6)) != 0) {
            m_exception = SerializableException.deserializeFromBuffer(in.buffer());
        } else {
            m_exception = null;
        }
        results = (VoltTable[]) in.readArray(VoltTable.class);
        setProperly = true;
        
        if (in.readBoolean()) {
            this.debug = new ClientResponseDebug();
            this.debug.readExternal(in);
        }
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert setProperly;
        out.writeByte(0);//version
        out.writeByte(this.restartCounter);
        out.writeLong(this.txn_id);
        out.writeLong(this.clientHandle);
        out.writeBoolean(this.singlepartition);
        out.writeInt(this.basePartition);
        out.write((byte)this.speculative.ordinal());
        out.write((byte)this.status.ordinal());
        
        byte presentFields = 0;
        if (appStatusString != null) {
            presentFields |= 1 << 7;
        }
        if (m_exception != null) {
            presentFields |= 1 << 6;
        }
        if (statusString != null) {
            presentFields |= 1 << 5;
        }
        out.writeByte(presentFields);
        
        if (statusString != null) {
            out.writeString(statusString);
        }
        out.write(appStatus);
        if (appStatusString != null) {
            out.writeString(appStatusString);
        }
        out.writeInt(clusterRoundTripTime);
        if (m_exception != null) {
            final ByteBuffer b = ByteBuffer.allocate(m_exception.getSerializedSize());
            m_exception.serializeToBuffer(b);
            out.write(b.array());
        }
        out.writeArray(results);
        
        // DEBUG HANDLE
        out.writeBoolean(this.debug != null);
        if (this.debug != null) {
            this.debug.writeExternal(out);
        }
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("Status", this.status +
                        (this.statusString == null || this.statusString.isEmpty() ? "" : " / " + this.statusString));
        m.put("Handle", this.clientHandle);
        m.put("Restart Counter", this.restartCounter);
        m.put("Single-Partition", this.singlepartition);
        m.put("Speculative Execution", this.speculative);
        m.put("Base Partition", this.basePartition);
        m.put("Exception", m_exception);
        
        if (this.clientRoundTripTime > 0) {
            m.put("Client RoundTrip Time", this.clientRoundTripTime + " ms");
        }
        if (this.clusterRoundTripTime > 0) {
            m.put("Cluster RoundTrip Time", this.clusterRoundTripTime + " ms");
        }
        m.put("Debug", this.debug);
        
        Map<String, Object> inner = new LinkedHashMap<String, Object>();
        for (int i = 0; i < results.length; i++) {
            inner.put(String.format("[%d]", i), results[i].toString());
        }
        m.put("Results", inner);
        
        return String.format("ClientResponse[#%d]\n%s", this.txn_id, StringUtil.formatMaps(m));
    }

    // ARIES
    public void setAriesLogData(byte[] arieslogDataArray) {
        arieslogData = arieslogDataArray;
    }

    public boolean hasAriesLogData() {
        if (arieslogData == null || arieslogData.length == 0)
            return false;

        return true;
    }

    public byte[] getAriesLogData() {
        if (hasAriesLogData()) {
            return arieslogData;
        }

        return null;
    }
    
    // ----------------------------------------------------------------------------
    // SPECIAL BYTEBUFFER MODIFIERS
    // ----------------------------------------------------------------------------

    /**
     * Set the txn restart counter without deserializing it first
     * @param arr
     * @param flag
     */
    public static void setRestartCounter(ByteBuffer b, int val) {
        b.put(1, (byte)val); 
    }
    
    /**
     * Set the client handle without deserializing it first
     * @param b
     * @param handle
     */
    public static void setClientHandle(ByteBuffer b, long handle) {
        b.putLong(10, handle); // 1 + 1 + 8 = 10 
    }
    
    /**
     * Set the base partition for the client response without deserializing it
     * @param arr
     * @param flag
     */
    public static void setBasePartition(ByteBuffer b, int basePartition) {
        b.putInt(19, basePartition); // 1 + 1 + 8 + 8 + 1 = 19 
    }
    
    /**
     * Set the status without deserializing it first
     * @param arr
     * @param flag
     */
    public static void setStatus(ByteBuffer b, Status status) {
        b.put(24, (byte)status.ordinal()); // 1 + 1 + 8 + 8 + 1 + 1 + 4 = 24
    }
    
    // ----------------------------------------------------------------------------
}
