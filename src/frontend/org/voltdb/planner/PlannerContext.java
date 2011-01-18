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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlanColumn.SortOrder;
import org.voltdb.planner.PlanColumn.Storage;

import edu.brown.utils.ThreadUtil;

public class PlannerContext {
    private static final Logger LOG = Logger.getLogger(PlannerContext.class);

    /**
     * Generator for PlanColumn.m_guid
     */
    private final AtomicInteger s_nextId = new AtomicInteger();

    /**
     * Global hash of PlanColumn guid to PlanColumn reference
     */
    private final TreeMap<Integer, PlanColumn> s_columnPool = new TreeMap<Integer, PlanColumn>();
    
    private transient final Map<Integer, PlanColumn> hashcode_col_xref = new HashMap<Integer, PlanColumn>();

    public PlanColumn getPlanColumn(AbstractExpression expression, String columnName) {
        return getPlanColumn(expression, columnName, SortOrder.kUnsorted, Storage.kTemporary);
    }
    
    /** Provide the common defaults */
    public synchronized PlanColumn getPlanColumn(AbstractExpression expression, String columnName, SortOrder sortOrder, Storage storage) {
        // Check if one already exists
        int hashCode = PlanColumn.computeHashCode(expression, columnName, sortOrder, storage);
        PlanColumn retval = hashcode_col_xref.get(hashCode);
        
        // We've never seen this one before, so we have to make a new one...
        if (retval == null) {
            int guid = s_nextId.incrementAndGet();
            retval = new PlanColumn(guid, expression, columnName, sortOrder, storage);
            if (s_columnPool.get(guid) != null) {
                PlanColumn orig = s_columnPool.get(guid); 
                LOG.warn(String.format("Trying to add PlanColumn GUID #%d more than once!\nORIG: %s\nNEW: %s", guid, orig, retval));
            }
            assert(s_columnPool.get(guid) == null);
            s_columnPool.put(guid, retval);
        }

        return retval;
    }

    /**
     * Internal Registration
     * HashCode -> PlanColumn
     */
    protected synchronized void registerPlanColumn(PlanColumn col) {
        int hashCode = col.hashCode();
        this.hashcode_col_xref.put(hashCode, col);
//        if (col.guid() > this.s_nextId)
//        this.s_nextId
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
        this.registerPlanColumn(col);
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
        // PlanColumn pc = s_columnPool.remove(guid);
        // LOG.info("REMOVED[" + guid + "]: " + pc);
    }
    
    public synchronized boolean hasColumn(int guid) {
        return (s_columnPool.containsKey(guid));
    }
    
    @Override
    public String toString() {
        return this.s_columnPool.toString();
    }
    
    public String debug() {
        StringBuilder sb = new StringBuilder();
        for (Entry<Integer, PlanColumn> e : this.s_columnPool.entrySet()) {
            sb.append(String.format("[%02d] %s\n", e.getKey(), e.getValue().toString()));
        }
        return (sb.toString());
    }
    
    // PAVLO: Global singleton for us to use to get back the PlanColumns we need
    private static final PlannerContext m_singleton = new PlannerContext();
    public static PlannerContext singleton() {
        return (m_singleton);
    }
    
}
