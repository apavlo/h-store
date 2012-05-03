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

import org.voltdb.VoltTable;
import org.voltdb.debugstate.MailboxHistory.MessageState;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.utils.DBBPool;

/**
 * Message from an execution site which is participating in a transaction
 * to the stored procedure coordinator for that transaction. The contents
 * are the tables output by the plan fragments and a status code. In the
 * event of an error, a text message can be embedded in a table attached.
 *
 */
public class FragmentResponseMessage extends TransactionInfoBaseMessage {

    public static final byte NULL             = 0;
    public static final byte SUCCESS          = 1;
    public static final byte USER_ERROR       = 2;
    public static final byte UNEXPECTED_ERROR = 3;

    byte m_status;
    // default dirty to true until proven otherwise
    boolean m_dirty = true;
    boolean m_nullDependencies = false;
    short m_dependencyCount = 0;
    int[] m_dependencyIds = new int[50];
    VoltTable[] m_dependencies = new VoltTable[50];
    SerializableException m_exception;

    /** Empty constructor for de-serialization */
    FragmentResponseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public FragmentResponseMessage(FragmentTaskMessage task) {
        super(task.getDestinationPartitionId(), task.getSourcePartitionId(), task.getTxnId(), task.getClientHandle(), task.isReadOnly());
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     * If the status code is failure then an exception may be included.
     * @param status
     * @param e
     */
    public void setStatus(byte status, SerializableException e) {
        m_status = status;
        m_exception = e;
    }

    public void setDirtyFlag(boolean value) {
        m_dirty = value;
    }

    public void addDependency(int dependencyId, VoltTable table) {
        m_dependencyIds[m_dependencyCount] = dependencyId;
        m_dependencies[m_dependencyCount++] = table;
    }
    
    public void addDependency(int dependencyId) {
        this.addDependency(dependencyId, null);
        m_nullDependencies = true;
    }

    public Long getTxnId() {
        return m_txnId;
    }

    public byte getStatusCode() {
        return m_status;
    }
    
    public String getStatusCodeName() {
        if (m_status == SUCCESS) {
            return ("SUCCESS");
        } else if (m_status == NULL) {
            return ("NULL");
        } else if (m_status == USER_ERROR) {
            return ("USER_ERROR");
        } else if (m_status == UNEXPECTED_ERROR) {
            return ("UNEXPECTED_ERROR");
        }
        assert(false) : "Unknown FragmentResponseMessage status code '" + m_status + "'";
        return (null);
    }

    public boolean getDirtyFlag() {
        return m_dirty;
    }

    public int getTableCount() {
        return m_dependencyCount;
    }
    

    public int getTableDependencyIdAtIndex(int index) {
        return m_dependencyIds[index];
    }

    public VoltTable getTableAtIndex(int index) {
        return m_dependencies[index];
    }
    public boolean hasTableAtIndex(int index) {
        return (m_dependencies[index] != null);
    }

    public SerializableException getException() {
        return m_exception;
    }

    @Override
    protected void flattenToBuffer(final DBBPool pool) {
        int msgsize = super.getMessageByteCount();
        msgsize += 1 + // m_status 
                   1 + // m_nullDependencies
                   1 + // m_dirty
                   2;  // m_dependencyCount
        assert(m_exception == null || m_status != SUCCESS);

        if (m_exception != null) {
            msgsize += m_exception.getSerializedSize();
        } else {
            msgsize += 4;//Still serialize exception length 0
        }

        // stupid lame flattening of the tables
        ByteBuffer tableBytes = null;
        msgsize += 4 * m_dependencyCount;
        if (m_dependencyCount > 0 && m_nullDependencies == false) {

            FastSerializer fs = new FastSerializer();
            try {
                for (int i = 0; i < m_dependencyCount; i++)
                    fs.writeObject(m_dependencies[i]);
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            }
            tableBytes = fs.getBuffer();
            msgsize += tableBytes.remaining();
        }

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(FRAGMENT_RESPONSE_ID);

        super.writeToBuffer();
        
        m_buffer.put(m_status);
        m_buffer.put((byte) (m_nullDependencies ? 1 : 0));
        m_buffer.put((byte) (m_dirty ? 1 : 0));

        m_buffer.putShort(m_dependencyCount);
        for (int i = 0; i < m_dependencyCount; i++)
            m_buffer.putInt(m_dependencyIds[i]);
        
        if (tableBytes != null)
            m_buffer.put(tableBytes);
        if (m_exception != null) {
            try {
                m_exception.serializeToBuffer(m_buffer);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            m_buffer.putInt(0);
        }

        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer() {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();
        
        m_status = m_buffer.get();
        m_nullDependencies = (m_buffer.get() == 1);
        m_dirty = (m_buffer.get() == 1);
        
        m_dependencyCount = m_buffer.getShort();
        assert(m_dependencyCount <= 50);
        for (int i = 0; i < m_dependencyCount; i++)
            m_dependencyIds[i] = m_buffer.getInt();
        if (m_nullDependencies == false) {
            for (int i = 0; i < m_dependencyCount; i++) {
                FastDeserializer fds = new FastDeserializer(m_buffer);
                try {
                    m_dependencies[i] = fds.readObject(VoltTable.class);
                } catch (IOException e) {
                    e.printStackTrace();
                    assert(false);
                }
            }
        }
        m_exception = SerializableException.deserializeFromBuffer(m_buffer);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FRAGMENT_RESPONSE (FROM ");
        sb.append(this.getSourcePartitionId());
        sb.append(" TO ");
        sb.append(this.getDestinationPartitionId());
        sb.append(") FOR TXN ");
        sb.append(this.getTxnId());

        if (m_status == SUCCESS)
            sb.append("\n  SUCCESS");
        else if (m_status == UNEXPECTED_ERROR)
            sb.append("\n  UNEXPECTED_ERROR");
        else
            sb.append("\n  USER_ERROR");

        if (m_dirty)
            sb.append("\n  DIRTY");
        else
            sb.append("\n  PRISTINE");

        for (int i = 0; i < m_dependencyCount; i++) {
            sb.append("\n  DEP ").append(m_dependencyIds[i]);
            if (m_nullDependencies == false) {
                sb.append(" WITH ").append(m_dependencies[i].getRowCount()).append(" ROWS (");
                for (int j = 0; j < m_dependencies[i].getColumnCount(); j++) {
                    sb.append(m_dependencies[i].getColumnName(j)).append(", ");
                }
                sb.setLength(sb.lastIndexOf(", "));
                sb.append(")");
            }
        }

        return sb.toString();
    }

    @Override
    public MessageState getDumpContents() {
        MessageState ms = super.getDumpContents();
        ms.txnId = m_txnId;
        ms.fromSiteId = this.getSourcePartitionId();
        ms.toSiteId = this.getDestinationPartitionId();
        return ms;
    }
}
