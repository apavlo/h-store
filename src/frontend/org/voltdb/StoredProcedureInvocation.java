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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.messaging.*;

import edu.brown.hstore.HStoreConstants;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 */
public class StoredProcedureInvocation implements FastSerializable {

    protected int procId = -1;
    protected String procName = null;
    protected ParameterSet params = null;
    protected ByteBuffer unserializedParams = null;

    /**
     * The number of times the txn for this request has been
     * restarted
     */
    protected int restartCounter = 0;
    
    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    protected long clientHandle = -1;
    
    /** Whether this invocation should be specifically executed at a particular partition **/
    protected int base_partition = HStoreConstants.NULL_PARTITION_ID;
    
    /** What partitions this invocation will touch **/
    @Deprecated
    protected Set<Integer> partitions = null;
    
    public StoredProcedureInvocation() {
        super();
    }
    
    public StoredProcedureInvocation(long handle, String procName, Object... parameters) {
        this(handle, -1, procName, parameters);
    }
    
    public StoredProcedureInvocation(long handle, int procId, String procName, Object... parameters) {
        super();
        this.clientHandle = handle;
        this.procId = procId;
        this.procName = procName;
        this.params = new ParameterSet();
        this.params.setParameters(parameters);
    }
    
    public StoredProcedureInvocation getShallowCopy() {
        StoredProcedureInvocation copy = new StoredProcedureInvocation();
        copy.clientHandle = this.clientHandle;
        copy.restartCounter = this.restartCounter;
        copy.params = this.params;
        copy.procName = this.procName;
        if (unserializedParams != null)
        {
            copy.unserializedParams = unserializedParams.duplicate();
        }
        else
        {
            copy.unserializedParams = null;
        }

        return copy;
    }
    
    public void setProcedureId(int procId) {
        this.procId = procId;
    }
    
    public void setProcName(String name) {
        this.procName = name;
    }

    public void setParams(Object... parameters) {
        // convert the params to the expected types
        params = new ParameterSet();
        params.setParameters(parameters);
        unserializedParams = null;
    }

    public int getProcId() {
        return procId;
    }
    
    public String getProcName() {
        return procName;
    }

    public ParameterSet getParams() {
        return params;
    }

    public void setClientHandle(int aHandle) {
        clientHandle = aHandle;
    }
    public long getClientHandle() {
        return clientHandle;
    }
    
    public boolean hasBasePartition() {
        return (this.base_partition != HStoreConstants.NULL_PARTITION_ID);
    }
    public int getBasePartition() {
        return (this.base_partition);
    }
    public void setBasePartition(int partition) {
        this.base_partition = partition;
    }
    
    public int getRestartCounter() {
        return (this.base_partition);
    }
    public void setRestartCounter(int count) {
        this.restartCounter = count;
    }
    
    @Deprecated
    public Set<Integer> getPartitions() {
        return (this.partitions);
    }
    @Deprecated
    public void addPartitions(Collection<Integer> partitions) {
        if (this.partitions == null) this.partitions = new HashSet<Integer>();
        this.partitions.addAll(partitions);
    }

    // ----------------------------------------------------------------------------
    // INTERNAL SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Build the ParameterSet embedded in this StoredProcedureInvocation
     * <B>Note:</B> This is the slow version because it will create a new
     * FastDeserializer every time.
     */
    public void buildParameterSet() {
        this.buildParameterSet(new FastDeserializer());
    }
    
    /**
     * If created from ClientInterface within a single host,
     * will still need to deserialize the parameter set. This
     * optimization is not performed for multi-site transactions
     * (which require concurrent access to the task).
     */
    public void buildParameterSet(FastDeserializer fds) {
       if (unserializedParams != null) {
           assert (params == null);
           try {
               fds.setBuffer(unserializedParams);
               params = fds.readObject(ParameterSet.class);
               unserializedParams = null;
           }
           catch (IOException ex) {
               throw new RuntimeException("Invalid ParameterSet in Stored Procedure Invocation.");
           }
       }
    }

    /** Read into an unserialized parameter buffer to extract a single parameter */
    Object getParameterAtIndex(int partitionIndex) {
        try {
            return ParameterSet.getParameterAtIndex(partitionIndex, unserializedParams);
        }
        catch (IOException ex) {
            throw new RuntimeException("Invalid partitionIndex", ex);
        }
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        this.restartCounter = (int)in.readShort();
        this.base_partition = (int)in.readShort();
        this.clientHandle = in.readLong();
        this.procId = in.readShort();
        this.procName = in.readString();
        
        // do not deserialize parameters in ClientInterface context
        this.unserializedParams = in.remainder();
        this.params = null;
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert(!((params == null) && (unserializedParams == null)));
        assert((params != null) || (unserializedParams != null));
        out.writeShort(this.restartCounter); // (2 bytes)
        out.writeShort(this.base_partition); // (2 bytes)
        out.writeLong(this.clientHandle);    // (8 bytes)
        out.writeShort(this.procId);         // (2 bytes)
        out.writeString(this.procName);
        
        if (this.params != null) {
            out.writeObject(this.params);
        } else if (this.unserializedParams != null) {
            out.write(this.unserializedParams.array(),
                      this.unserializedParams.position() + this.unserializedParams.arrayOffset(),
                      this.unserializedParams.remaining());
        }
    }
    

    @Override
    public String toString() {
        String extra = "";
        if (this.params != null) {
            extra += String.format("<%d Params>", this.params.toArray().length);
        }
        if (this.base_partition != HStoreConstants.NULL_PARTITION_ID) {
            if (extra.isEmpty() == false) extra += " / ";
            extra += String.format("<%d BasePartition>", this.base_partition);
        }
        return String.format("%s(%s) / %d", this.procName, extra, this.clientHandle);
    }

    public void getDumpContents(StringBuilder sb) {
        sb.append("Invocation: ").append(procName).append("(");
        if (params != null)
            for (Object o : params.toArray()) {
                sb.append(o.toString()).append(", ");
            }
        else
            sb.append("null");
        sb.append(")");
    }
    
    // ----------------------------------------------------------------------------
    // QUICK ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Mark a serialized byte array of a StoredProcedureInvocation as being redirected to
     * the given partition id without having to serialize it first.
     * @param partition
     * @param buffer ByteBuffer wrapper around a serialized StoredProcedureInvocation
     */
     public static void setBasePartition(int partition, ByteBuffer buffer) {
        buffer.putShort(2, (short)partition);
     }
     
     /**
      * Returns the restart counter embedded in this invocation
      * @param buffer ByteBuffer wrapper around a serialized StoredProcedureInvocation
      * @return
      */
     public static int getRestartCounter(ByteBuffer buffer) {
         return (buffer.getShort(0));
     }
    
    /**
     * Returns the base partition of this invocation
     * If the base partition is not set, the return value will be NULL_PARTITION_ID
     * @param buffer ByteBuffer wrapper around a serialized StoredProcedureInvocation
     * @return
     */
    public static int getBasePartition(ByteBuffer buffer) {
        return (buffer.getShort(2));
    }

    /**
     * Return the client handle from the serialized StoredProcedureInvocation without having to 
     * deserialize it first
     * @param buffer ByteBuffer wrapper around a serialized StoredProcedureInvocation
     * @return
     */
    public static long getClientHandle(ByteBuffer buffer) {
        buffer.rewind();
        return (buffer.getLong(4));
    }
    
    /**
     * Returns the procId encoded in this invocation
     * If the procId is not set, the return value will be -1 
     * @param buffer
     * @return
     */
    public static int getProcedureId(ByteBuffer buffer) {
        return (buffer.getShort(12));
    }
    
    /**
     * 
     * @param buffer ByteBuffer wrapper around a serialized StoredProcedureInvocation
     * @return
     */
    public static String getProcedureName(ByteBuffer buffer) {
        return getProcedureName(new FastDeserializer(buffer));
    }
    
    /**
     * Faster way of getting the Procedure name with an existing FastDeserializer
     * @param in
     * @return
     */
    public static String getProcedureName(FastDeserializer in) {
        in.buffer().position(14);
        try {
            return (in.readString());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Get a ByteBuffer that contains the serialized ParameterSet
     * for the ByteBuffer that contains a StoredProcedureInvocation 
     * @param buffer ByteBuffer wrapper around a serialized StoredProcedureInvocation
     * @return
     */
    public static ByteBuffer getParameterSet(ByteBuffer buffer) {
        seekToParameterSet(buffer);
        return buffer.slice();
    }
    
    public static void seekToParameterSet(ByteBuffer buffer) {
        // Skip to the procedure name
        buffer.position(14);
        int procNameLen = buffer.getInt();
        buffer.position(buffer.position() + procNameLen);
    }
    
}
