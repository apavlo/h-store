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

package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlanColumn.SortOrder;
import org.voltdb.planner.PlanColumn.Storage;

public class PlannerContext {
    private static final Logger LOG = Logger.getLogger(PlannerContext.class);

    /**
     * Generator for PlanColumn.m_guid
     */
    private AtomicInteger s_nextId = new AtomicInteger();

    /**
     * Global hash of PlanColumn guid to PlanColumn reference
     */
    private TreeMap<Integer, PlanColumn> s_columnPool = new TreeMap<Integer, PlanColumn>();
    
    private Map<Integer, AtomicInteger> s_columnCounters = new HashMap<Integer, AtomicInteger>();

    public PlanColumn getPlanColumn(AbstractExpression expression, String columnName) {
        return getPlanColumn(expression, columnName, SortOrder.kUnsorted, Storage.kTemporary);
    }

    /** Provide the common defaults */
    public synchronized PlanColumn getPlanColumn(AbstractExpression expression,
            String columnName,
            SortOrder sortOrder,
            Storage storage) {

        int guid = s_nextId.incrementAndGet();
        PlanColumn retval = new PlanColumn(guid, expression, columnName, sortOrder, storage);
        // in to the pool...
        assert(s_columnPool.get(guid) == null);
        s_columnPool.put(guid, retval);
        s_columnCounters.put(guid, new AtomicInteger(1));

        return retval;
    }
    
    public PlanColumn clonePlanColumn(PlanColumn orig) {
        PlanColumn clone = this.getPlanColumn(orig.m_expression,
                                              orig.displayName(),
                                              orig.getSortOrder(),
                                              orig.getStorage());
        return (clone);
    }
    
    public synchronized void add(int guid, PlanColumn col) {
        assert(!this.s_columnPool.containsKey(guid)) :
            "PlannerContext already contains entry for guid #" + guid + ": " + this.s_columnPool.get(guid);
        this.s_columnPool.put(guid, col);
    }
    
    /**
     * Retrieve a column instance by guid.
     */
    public PlanColumn get(int guid) {
        PlanColumn column = s_columnPool.get(guid);
//        assert(column != null) : "Failed to retrieve PlanColumn guid=" + guid;
        return column;
    }

    public synchronized void freeColumn(int guid) {
        PlanColumn pc = s_columnPool.remove(guid);
        LOG.info("REMOVED: " + pc);
    }
    
    @Override
    public String toString() {
        return this.s_columnPool.toString();
    }
    
    // PAVLO: Global singleton for us to use to get back the PlanColumns we need
    private static final PlannerContext m_singleton = new PlannerContext();
    public static PlannerContext singleton() {
        return (m_singleton);
    }
    
}
