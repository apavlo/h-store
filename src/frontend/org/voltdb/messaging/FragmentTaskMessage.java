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

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.utils.DBBPool;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.WorkFragment;

/**
 * Message from a stored procedure coordinator to an execution site
 * which is participating in the transaction. This message specifies
 * which planfragment to run and with which parameters.
 *
 */
public class FragmentTaskMessage extends TransactionInfoBaseMessage
{
    private static final Logger LOG = Logger.getLogger(FragmentTaskMessage.class);
    
    public static final byte USER_PROC = 0;
    public static final byte SYS_PROC_PER_PARTITION = 1;
    public static final byte SYS_PROC_PER_SITE = 2;

    long[] m_fragmentIds = null;
    ByteBuffer[] m_parameterSets = null;
    int[] m_outputDepIds = null;
    ArrayList<?>[] m_inputDepIds = null;
    boolean m_isFinal = false;
    byte m_taskType = 0;
    boolean m_shouldUndo = false;
    boolean m_usingDtxn = false;
    int m_inputDepCount = 0;

    /** PAVLO **/
    // Whether we have real input dependencies
    int m_realInputDepCount = 0;
    // What SQLStmt in the batch these fragments belong to
    int[] m_fragmentStmtIndexes = null;
    // We have the ability to attach dependency results to our FragmentTask
    final Map<Integer, List<VoltTable>> m_attachedResults = new HashMap<Integer, List<VoltTable>>();
    /** PAVLO **/

    /** Empty constructor for de-serialization */
    public FragmentTaskMessage() {
        m_subject = Subject.DEFAULT.getId();
    }
    
    /** PAVLO **/
    public FragmentTaskMessage(int srcPartition,
            int destPartition,
            Long txnId,
            long clientHandle,
            boolean isReadOnly,
            long[] fragmentIds,
            int[] inputDepIds,
            int[] outputDepIds,
            ByteBuffer[] parameterSets,
            int[] fragmentStmtIndexes,
            boolean isFinal) {
        this(srcPartition, destPartition, txnId, clientHandle, isReadOnly, fragmentIds, outputDepIds, parameterSets, fragmentStmtIndexes, isFinal);
        if (inputDepIds != null) {
            for (int i = 0; i < inputDepIds.length; i++) {
                this.addInputDepId(i, inputDepIds[i]);
            }
        }
    }
    /** PAVLO **/

    /**
     *
     * @param srcPartition
     * @param destPartition
     * @param txnId
     * @param isReadOnly
     * @param fragmentIds
     * @param outputDepIds
     * @param parameterSets
     * @param isFinal
     */
    public FragmentTaskMessage(int srcPartition,
                        int destPartition,
                        Long txnId,
                        long clientHandle,
                        boolean isReadOnly,
                        long[] fragmentIds,
                        int[] outputDepIds,
                        ByteBuffer[] parameterSets,
                        int[] fragmentStmtIndexes,
                        boolean isFinal) {
        super(srcPartition, destPartition, txnId, clientHandle, isReadOnly);

        assert(fragmentIds != null);
        assert(parameterSets != null);
        assert(parameterSets.length == fragmentIds.length);

        m_fragmentIds = fragmentIds;
        m_outputDepIds = outputDepIds;
        m_parameterSets = parameterSets;
        m_fragmentStmtIndexes = fragmentStmtIndexes;
        m_isFinal = isFinal;
        m_subject = Subject.DEFAULT.getId();
        assert(selfCheck());
    }

    private boolean selfCheck() {
        for (ByteBuffer paramSet : m_parameterSets)
            if (paramSet == null)
                return false;
        return true;
    }

    /** PAVLO **/
    public long[] getFragmentIds() {
        return (m_fragmentIds);
    }
    public int[] getFragmentStmtIndexes() {
        return (m_fragmentStmtIndexes);
    }
    public void attachResults(int dependency_id, List<VoltTable> results) {
        LOG.debug("Attaching " + results.size() + " results for DependencyId " + dependency_id);
        if (!m_attachedResults.containsKey(dependency_id)) {
            m_attachedResults.put(dependency_id, results);
        } else {
            m_attachedResults.get(dependency_id).addAll(results);
        }
    }
    public boolean hasAttachedResults() {
        return (!m_attachedResults.isEmpty());
    }
    public Map<Integer, List<VoltTable>> getAttachedResults() {
        return (m_attachedResults);
    }
    public boolean hasInputDependencies() {
        return (m_realInputDepCount > 0);
    }
    public int getInputDependencyCount() {
        return (m_realInputDepCount);
    }
    public boolean hasOutputDependencies() {
        return (m_outputDepIds != null && m_outputDepIds.length > 0);
    }
    public int[] getOutputDependencyIds() {
        return m_outputDepIds;
    }
    /** PAVLO **/
    
    public void addInputDepId(int index, int depId) {
        if (m_inputDepIds == null)
            m_inputDepIds = new ArrayList<?>[m_fragmentIds.length];
        assert(index < m_fragmentIds.length);
        if (m_inputDepIds[index] == null)
            m_inputDepIds[index] = new ArrayList<Integer>();
        @SuppressWarnings("unchecked")
        ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[index];
        l.add(depId);
        m_inputDepCount++;
        if (depId != HStoreConstants.NULL_DEPENDENCY_ID) m_realInputDepCount++;
    }

    @SuppressWarnings("unchecked")
    public ArrayList<Integer> getInputDepIds(int index) {
        if (m_inputDepIds == null)
            return null;
        return (ArrayList<Integer>) m_inputDepIds[index];
    }

    public int getOnlyInputDepId(int index) {
        if (m_inputDepIds == null)
            return -1;
        @SuppressWarnings("unchecked")
        ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[index];
        if (l == null)
            return -1;
        assert(l.size() == 1);
        return l.get(0);
    }

    public int[] getAllUnorderedInputDepIds() {
        int[] retval = new int[m_inputDepCount];
        int i = 0;
        if (m_inputDepIds != null) {
            for (ArrayList<?> l : m_inputDepIds) {
                @SuppressWarnings("unchecked")
                ArrayList<Integer> l2 = (ArrayList<Integer>) l;
                for (int depId : l2) {
                    retval[i++] = depId;
                }
            }
        }
        assert(i == m_inputDepCount);
        return retval;
    }

    public void setFragmentTaskType(byte value) {
        m_taskType = value;
    }
    
    public void setShouldUndo(boolean value) {
        m_shouldUndo = value;
    }

    public void setUsingDtxnCoordinator(boolean value) {
        m_usingDtxn = value;
    }
    
    public boolean isFinalTask() {
        return m_isFinal;
    }

    public boolean isSysProcTask() {
        return (m_taskType != USER_PROC);
    }

    public byte getFragmentTaskType() {
        return m_taskType;
    }

    public boolean shouldUndo() {
        return m_shouldUndo;
    }

    public int getFragmentCount() {
        return (m_fragmentIds != null) ?
            m_fragmentIds.length : 0;
    }

    public int getFragmentId(int index) {
        return (int)m_fragmentIds[index];
    }
    
    public int getOutputDepId(int index) {
        return m_outputDepIds[index];
    }

    public ByteBuffer getParameterDataForFragment(int index) {
        return m_parameterSets[index].asReadOnlyBuffer();
    }
    
    @Override
    protected void flattenToBuffer(final DBBPool pool) {
        int msgsize = super.getMessageByteCount();

        // m_fragmentIds count (2)
        // m_outputDepIds count (2)
        // m_inputDepIds count (2)
        // m_usingDtxn (1)
        // m_isFinal (1)
        // m_taskType (1)
        // m_shouldUndo (1)
        // m_attachedResults count (2)

        msgsize += 2 + 2 + 2 + 1 + 1 + 1 + 1 + 2;
        if (m_fragmentIds != null) {
            msgsize += 8 * m_fragmentIds.length;
            msgsize += 4 * m_fragmentStmtIndexes.length;
            // each frag has one parameter set
            for (int i = 0; i < m_fragmentIds.length; i++)
                msgsize += 4 + m_parameterSets[i].remaining();
        }

        if (m_outputDepIds != null) {
            msgsize += 4 * m_outputDepIds.length;
        }
        if (m_inputDepIds != null) {
            for (int i = 0; i < m_inputDepIds.length; i++) {
                @SuppressWarnings("unchecked")
                ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[i];
                msgsize += 2 + (4 * l.size());
            }
            //msgsize += 4 * m_inputDepIds.length;
        }
        
        // Attached Dependencies
        Map<Integer, ByteBuffer> attachedBytes = new HashMap<Integer, ByteBuffer>();
        for (Integer d_id : m_attachedResults.keySet()) {
            msgsize += 4; // Dependency Id
            msgsize += 2; // # of tables

            try {
                FastSerializer fs = new FastSerializer();
                for (VoltTable vt : m_attachedResults.get(d_id)) {
                    fs.writeObject(vt);
                } // FOR
                ByteBuffer bytes = fs.getBuffer();
                msgsize += bytes.remaining();
                attachedBytes.put(d_id, bytes);
            } catch (IOException e) {
                LOG.fatal("Failed to serialize attached results for DependencyId " + d_id, e);
                assert(false);
            }
        } // FOR
        
        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(FRAGMENT_TASK_ID);

        super.writeToBuffer();

        if (m_fragmentIds == null) {
            m_buffer.putShort((short) 0);
        }
        else {
            m_buffer.putShort((short) m_fragmentIds.length);
            for (int i = 0; i < m_fragmentIds.length; i++) {
                m_buffer.putLong(m_fragmentIds[i]);
            }
            for (int i = 0; i < m_fragmentStmtIndexes.length; i++) {
                m_buffer.putInt(m_fragmentStmtIndexes[i]);
            }
            for (int i = 0; i < m_fragmentIds.length; i++) {
                m_buffer.putInt(m_parameterSets[i].remaining());
                //Duplicate because the parameter set might be used locally also
                m_buffer.put(m_parameterSets[i].duplicate());
            }
        }

        if (m_outputDepIds == null) {
            m_buffer.putShort((short) 0);
        }
        else {
            m_buffer.putShort((short) m_outputDepIds.length);
            for (int i = 0; i < m_outputDepIds.length; i++) {
                m_buffer.putInt(m_outputDepIds[i]);
            }
        }

        if (m_inputDepIds == null) {
            m_buffer.putShort((short) 0);
        }
        else {
            m_buffer.putShort((short) m_inputDepIds.length);
            for (int i = 0; i < m_inputDepIds.length; i++) {
                @SuppressWarnings("unchecked")
                ArrayList<Integer> l = (ArrayList<Integer>) m_inputDepIds[i];
                m_buffer.putShort((short) l.size());
                for (int depId : l)
                    m_buffer.putInt(depId);
            }
        }

        m_buffer.put(m_usingDtxn ? (byte) 1 : (byte) 0);
        m_buffer.put(m_isFinal ? (byte) 1 : (byte) 0);
        m_buffer.put(m_taskType);
        m_buffer.put(m_shouldUndo ? (byte) 1 : (byte) 0);
        
        if (m_attachedResults.isEmpty()) {
            m_buffer.putShort((short) 0);
        } else {
            m_buffer.putShort((short) m_attachedResults.size());
            for (Integer d_id : m_attachedResults.keySet()) {
                m_buffer.putInt(d_id);
                m_buffer.putShort((short)m_attachedResults.get(d_id).size());
                LOG.debug("DependencyId=" + d_id + ", ActualSize=" + attachedBytes.get(d_id).remaining());
                m_buffer.put(attachedBytes.get(d_id));
            } // FOR
        }

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();

        short fragCount = m_buffer.getShort();
        if (fragCount > 0) {
            m_fragmentIds = new long[fragCount];
            for (int i = 0; i < fragCount; i++)
                m_fragmentIds[i] = m_buffer.getLong();
            m_fragmentStmtIndexes = new int[fragCount];
            for (int i = 0; i < fragCount; i++)
                m_fragmentStmtIndexes[i] = m_buffer.getInt();
            
            m_parameterSets = new ByteBuffer[fragCount];
            for (int i = 0; i < fragCount; i++) {
                int paramsbytecount = m_buffer.getInt();
                m_parameterSets[i] = ByteBuffer.allocate(paramsbytecount);
                int cachedLimit = m_buffer.limit();
                m_buffer.limit(m_buffer.position() + m_parameterSets[i].remaining());
                m_parameterSets[i].put(m_buffer);
                m_parameterSets[i].flip();
                m_buffer.limit(cachedLimit);
            }
        }
        short expectedDepCount = m_buffer.getShort();
        if (expectedDepCount > 0) {
            m_outputDepIds = new int[expectedDepCount];
            for (int i = 0; i < expectedDepCount; i++)
                m_outputDepIds[i] = m_buffer.getInt();
        }

        short inputDepCount = m_buffer.getShort();
        if (inputDepCount > 0) {
            m_inputDepIds = new ArrayList<?>[inputDepCount];
            for (int i = 0; i < inputDepCount; i++) {
                short count = m_buffer.getShort();
                if (count > 0) {
                    ArrayList<Integer> l = new ArrayList<Integer>();
                    for (int j = 0; j < count; j++) {
                        l.add(m_buffer.getInt());
                        m_inputDepCount++;
                    }
                    m_inputDepIds[i] = l;
                    }
            }
        }

        m_usingDtxn = m_buffer.get() == 1;
        m_isFinal = m_buffer.get() == 1;
        m_taskType = m_buffer.get();
        m_shouldUndo = m_buffer.get() == 1;
        
        // Attached Results
        short attachedCount = m_buffer.getShort();
        if (attachedCount > 0) {
            for (int i = 0; i < attachedCount; i++) {
                int d_id = m_buffer.getInt();
                int count = m_buffer.getShort();
                
                List<VoltTable> attached = new ArrayList<VoltTable>();
                for (int ii = 0; ii < count; ii++) {
                    FastDeserializer fds = new FastDeserializer(m_buffer);
                    try {
                        attached.add(fds.readObject(VoltTable.class));
                    } catch (IOException e) {
                        e.printStackTrace();
                        assert(false);
                    }
                } // FOR
                m_attachedResults.put(d_id, attached);
            } // FOR
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_TASK (FROM ");
        sb.append(this.getSourcePartitionId());
        sb.append(" TO ");
        sb.append(this.getDestinationPartitionId());
        sb.append(") FOR TXN ");
        sb.append(this.getTxnId());
        sb.append(" [using_dtxn=").append(m_usingDtxn).append("]");

        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, COORD ");
        else
            sb.append("  WRITE, COORD ");
        sb.append(m_destPartition);

        if ((m_fragmentIds != null) && (m_fragmentIds.length > 0)) {
            sb.append("\n");
            sb.append("  FRAGMENT_IDS ");
            for (long id : m_fragmentIds)
                sb.append(id).append(", ");
            sb.setLength(sb.lastIndexOf(", "));
        }

        /** PAVLO **/
        sb.append("\n  INPUT_DEPENDENCY_IDS ");
        if ((m_inputDepIds != null) && (m_inputDepIds.length > 0)) {
            for (ArrayList<?> ids : m_inputDepIds)
                sb.append(ids).append(", ");
            sb.setLength(sb.lastIndexOf(", "));
        } else {
            sb.append("<none>");
        }
        sb.append("\n  OUTPUT_DEPENDENCY_IDS ");
        if ((m_outputDepIds != null) && (m_outputDepIds.length > 0)) {
            for (int id : m_outputDepIds)
                sb.append(id).append(", ");
            sb.setLength(sb.lastIndexOf(", "));
        } else {
            sb.append("<none>");
        }
        sb.append("\n  ATTACHED_RESULTS ");
        if (m_attachedResults.isEmpty()) {
            sb.append("<none>");
        } else {
            for (int id : m_attachedResults.keySet())
                sb.append(id).append("[# of tables=").append(m_attachedResults.get(id).size()).append("], ");
            sb.setLength(sb.lastIndexOf(", "));
        }
        /** PAVLO **/
        
//        if ((m_inputDepIds != null) && (m_inputDepIds.length > 0)) {
//            sb.append("\n");
//            sb.append("  DEPENDENCY_IDS ");
//            for (long id : m_fragmentIds)
//                sb.append(id).append(", ");
//            sb.setLength(sb.lastIndexOf(", "));
//        }

        if (m_isFinal)
            sb.append("\n  THIS IS THE FINAL TASK");

        if (m_taskType == USER_PROC)
        {
            sb.append("\n  THIS IS A USERPROC TASK");
        }
        else if (m_taskType == SYS_PROC_PER_PARTITION)
        {
            sb.append("\n  THIS IS A SYSPROC RUNNING PER PARTITION");
        }
        else if (m_taskType == SYS_PROC_PER_SITE)
        {
            sb.append("\n  THIS IS A SYSPROC TASK RUNNING PER EXECUTION SITE");
        }
        else
        {
            sb.append("\n  UNKNOWN FRAGMENT TASK TYPE");
        }

        if (m_shouldUndo)
            sb.append("\n  THIS IS AN UNDO REQUEST");

        if ((m_parameterSets != null) && (m_parameterSets.length > 0)) {
            for (ByteBuffer paramSetBytes : m_parameterSets) {
                FastDeserializer fds = new FastDeserializer(paramSetBytes.asReadOnlyBuffer());
                ParameterSet pset = null;
                try {
                    pset = fds.readObject(ParameterSet.class);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                assert(pset != null);
                sb.append("\n  ").append(pset.toString());

            }
        }
        sb.append("\n  HASHCODE: " + this.hashCode());

        return sb.toString() + "\n";
    }
    
    // ----------------------------------------------------------------------------
    // HACK: PROTOCOL BUFFER WRAPPER MODE!
    // ----------------------------------------------------------------------------

    @Deprecated
    private WorkFragment inner_work;
    
    @Deprecated
    public FragmentTaskMessage setWorkFragment(long txn_id, WorkFragment work) {
        this.setTransactionId(txn_id);
        this.inner_work = work;
        return (this);
    }
    
    @Deprecated
    public WorkFragment getWorkFragment() {
        return (this.inner_work);
    }
}
