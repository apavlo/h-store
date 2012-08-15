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

import org.voltdb.utils.DBBPool;

import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.txns.AbstractTransaction;

/**
 * Message from an initiator to an execution site, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
@Deprecated
public class FinishTaskMessage extends TransactionInfoBaseMessage {

    AbstractTransaction ts;
    Hstoreservice.Status status;

    /** Empty constructor for de-serialization */
    FinishTaskMessage() {
        super();
    }

    // Use this one asshole!
    public FinishTaskMessage(AbstractTransaction ts, Hstoreservice.Status status) {
        super(ts.getBasePartition(), -1, ts.getTransactionId(), ts.getClientHandle(), false);
        this.ts = ts;
        this.status = status;
    }
    
    public Hstoreservice.Status getStatus() {
        return status;
    }
    public void setStatus(Hstoreservice.Status status) {
        this.status = status;
    }

    @Override
    public boolean isReadOnly() {
        return m_isReadOnly;
    }

    
    @Override
    protected void flattenToBuffer(final DBBPool pool) {
        return;
    }

    @Override
    protected void initFromBuffer() {
        return;
    }

    @Override
    public String toString() {
        return this.ts + " - " + this.status;
    }
}
