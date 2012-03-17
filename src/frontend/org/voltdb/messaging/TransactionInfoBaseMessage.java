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

import org.voltdb.debugstate.MailboxHistory.MessageState;

/**
 * Message from an initiator to an execution site, informing the
 * site that it may be requested to do work for a multi-partition
 * transaction, and to reserve a slot in its ordered work queue
 * for this transaction.
 *
 */
public abstract class TransactionInfoBaseMessage extends VoltMessage {

    int m_srcPartition;
    int m_destPartition;
    Long m_txnId;
    long m_clientHandle;
    boolean m_isReadOnly;
    boolean m_sysproc = false;

    /** Empty constructor for de-serialization */
    TransactionInfoBaseMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    TransactionInfoBaseMessage(int srcPartition,
                                      int destPartition,
                                      Long txnId,
                                      long clientHandle,
                                      boolean isReadOnly) {
        m_srcPartition = srcPartition;
        m_destPartition = destPartition;
        m_txnId = txnId;
        m_clientHandle = clientHandle;
        m_isReadOnly = isReadOnly;
        m_subject = Subject.DEFAULT.getId();
    }
    
    public void setTransactionId(Long m_txnId) {
        this.m_txnId = m_txnId;
    }
    public void setClientHandle(long m_clientHandle) {
        this.m_clientHandle = m_clientHandle;
    }
    public void setSrcPartition(int m_srcPartition) {
        this.m_srcPartition = m_srcPartition;
    }
    public void setDestPartition(int m_destPartition) {
        this.m_destPartition = m_destPartition;
    }
    public void setReadOnly(boolean m_isReadOnly) {
        this.m_isReadOnly = m_isReadOnly;
    }
    

    public int getDestinationPartitionId() {
        return (int)m_destPartition;
    }
    
    public int getSourcePartitionId() {
        return m_srcPartition;
    }

    public Long getTxnId() {
        return m_txnId;
    }
    public void setTxnId(Long txnId) {
        m_txnId = txnId;
    }
    
    public boolean isSysProc() {
        return (m_sysproc);
    }
    public void setSysProc(boolean val) {
        m_sysproc = val; 
    }
    
    public long getClientHandle() {
        return m_clientHandle;
    }

    public boolean isReadOnly() {
        return m_isReadOnly;
    }

    public boolean isSinglePartition() {
        return false;
    }

    protected int getMessageByteCount() {
        return 4 + 4 + 8 + 8 + 1 + 1;
    }

    protected void writeToBuffer() {
        m_buffer.putInt(m_srcPartition);
        m_buffer.putInt(m_destPartition);
        m_buffer.putLong(m_txnId);
        m_buffer.putLong(m_clientHandle);
        m_buffer.put(m_sysproc ? (byte) 1 : (byte) 0);
        m_buffer.put(m_isReadOnly ? (byte) 1 : (byte) 0);
    }

    protected void readFromBuffer() {
        m_srcPartition = m_buffer.getInt();
        m_destPartition = m_buffer.getInt();
        m_txnId = m_buffer.getLong();
        m_clientHandle = m_buffer.getLong();
        m_sysproc = m_buffer.get() == 1;
        m_isReadOnly = m_buffer.get() == 1;
    }

    @Override
    public MessageState getDumpContents() {
        MessageState ms = super.getDumpContents();
        ms.fromSiteId = m_srcPartition;
        ms.txnId = m_txnId;
        return ms;
    }
}
