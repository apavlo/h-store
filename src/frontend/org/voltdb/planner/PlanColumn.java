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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;

import edu.brown.expressions.ExpressionUtil;

/**
 * A PlanColumn organizes all of the schema and meta-data about a column that is
 * required by the planner. Each plan node has an output column list of
 * PlanColumns specifying its output schema. Column value types are stored in
 * the column's associated expression.
 *
 * Once the AST objects generated from the parsed HSQL are processed into plan
 * nodes, all column references should be managed as PlanColumn GUIDs.
 *
 * Eventually, AbstractExpressions should reference column GUIDs allowing
 * walking the expression should lead to the GUIDs for the expression input
 * columns - those inputs are sometimes referred to as origin columns in
 * comments and variable names.
 */
public class PlanColumn
{

    public enum Members {
        GUID,
        TYPE,
        SIZE,
        NAME,
        INPUT_COLUMN_NAME, //For output columns, what was the name of the column in the input that it maps to
        INPUT_TABLE_NAME,
        SORT_ORDER,
        STORAGE,
        EXPRESSION;
    }

    public enum SortOrder {
        kAscending,
        kDescending,
        kUnsorted;
        
        protected static final Map<Integer, SortOrder> idx_lookup = new HashMap<Integer, SortOrder>();
        protected static final Map<String, SortOrder> name_lookup = new HashMap<String, SortOrder>();
        static {
            for (SortOrder vt : EnumSet.allOf(SortOrder.class)) {
                SortOrder.idx_lookup.put(vt.ordinal(), vt);
                SortOrder.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        public static SortOrder get(Integer idx) {
            return (SortOrder.idx_lookup.get(idx));
        }
        public static SortOrder get(String name) {
            return (SortOrder.name_lookup.get(name.toLowerCase().intern()));
        }
    };

    public enum Storage {
        kPartitioned,
        kReplicated,
        kTemporary,   // column in an intermediate table
        kUnknown;      // TODO: eliminate this and make all columns known
        
        protected static final Map<Integer, Storage> idx_lookup = new HashMap<Integer, Storage>();
        protected static final Map<String, Storage> name_lookup = new HashMap<String, Storage>();
        static {
            for (Storage vt : EnumSet.allOf(Storage.class)) {
                Storage.idx_lookup.put(vt.ordinal(), vt);
                Storage.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        public static Storage get(Integer idx) {
            return (Storage.idx_lookup.get(idx));
            }
        public static Storage get(String name) {
            return (Storage.name_lookup.get(name.toLowerCase().intern()));
        }
    };

    /**
     * Globally unique id identifying this column
     */
    private final int m_guid;

    /**
     * Columns may be derived from other columns by expressions. For example,
     * c = a + b. Or c = a + 1. Or c = a where (a < b). The creating expression,
     * if any, is given. Those expressions in turn contain information (in
     * TupleValueExpression nodes, e.g.) about "origin columns", from which
     * this column may be derived.
     */
    final AbstractExpression m_expression;

    /**
     * The sort order. This sort order may have been established by an ORDER BY
     * statement or as a result of the column's construction (scan of sorted
     * table column, for example).
     */
    final SortOrder m_sortOrder;

    /**
     * Partitioned, replicated or intermediate table column
     */
    final Storage m_storage;

    /**
     * Column's display name. If an output column, this will be the alias if an
     * alias was present in the SQL expression.
     */
    final String m_displayName;

    final int m_hashCode;
    
    //
    // Constructors
    //
    public PlanColumn(
            int guid,
            AbstractExpression expression,
            String columnName,
            SortOrder sortOrder,
            Storage storage)
    {
        // all members are final and immutable (by implementation)
        m_guid = guid;
        m_expression = expression;
        m_displayName = columnName;
        m_sortOrder = sortOrder;
        m_storage = storage;
        m_hashCode = computeHashCode(m_expression, m_displayName, m_sortOrder, m_storage);

        PlannerContext.singleton().registerPlanColumn(this);
        
        /* Breaks for adhoc deser code..
        if (expression instanceof TupleValueExpression) {
            assert(((TupleValueExpression)expression).getColumnAlias() != null);
            assert(((TupleValueExpression)expression).getColumnName() != null);
        } */
    }
    
    protected static int computeHashCode(AbstractExpression expression, String columnName, SortOrder sortOrder, Storage storage) {
        StringBuilder sb = new StringBuilder()
          .append(columnName).append("|")
          .append(sortOrder).append("|")
          .append(storage).append("|")
          .append(expression);
        return (sb.toString().hashCode());
    }
    
    @Override
    public int hashCode() {
        return (this.m_hashCode);
    }
    
    /**
     * Check if the two objects are equal in all regards except for the AbstractExpression
     * @param obj
     * @param ignore_exp
     * @return
     */
    public boolean equals(Object obj, boolean ignore_exp, boolean ignore_guid) {
        if (obj instanceof PlanColumn) {
            PlanColumn other = (PlanColumn)obj;
            if (ignore_guid == false && this.m_guid != other.m_guid) return (false);
            if (!this.m_displayName.equals(other.m_displayName)) return (false); 
            if (!this.m_sortOrder.equals(other.m_sortOrder)) return (false);
            /** DWU - ignore storage for now? Problematic in the future? **/
            //if (!this.m_storage.equals(other.m_storage)) return (false);
            if (ignore_exp == false && !ExpressionUtil.equals(this.m_expression, other.m_expression)) return (false);
            return (true);
        }
        return (false);
    }

    @Override
    public boolean equals(Object obj) {
        return (this.equals(obj, false, false));
    }
    
    //
    // Accessors: all return copies or immutable values
    //

    public String getDisplayName() {
        return new String(m_displayName);
    }

    public String originTableName() {
        TupleValueExpression tve = null;
        if ((m_expression instanceof TupleValueExpression) == true)
            tve = (TupleValueExpression)m_expression;
        else
            return null;

        if (tve.getTableName() != null)
            return new String(tve.getTableName());
        else
            return null;
    }

    public String originColumnName() {
        TupleValueExpression tve = null;
        if ((m_expression instanceof TupleValueExpression) == true)
            tve = (TupleValueExpression)m_expression;
        else
            return null;

        if (tve.getColumnAlias() != null)
            return new String(tve.getColumnAlias());
        else
            return null;
    }

    public int guid() {
        return m_guid;
    }

    public VoltType type() {
        return m_expression.getValueType();
    }

    public int width() {
        return m_expression.getValueSize();
    }
    
    public SortOrder getSortOrder() {
        return (this.m_sortOrder);
    }
    public Storage getStorage() {
        return (this.m_storage);
    }
    

    public AbstractExpression getExpression() {
        return m_expression;
    }
    
    @Override
    public String toString() {
        return ("PlanColumn(" + 
                "guid=" + guid() + ", " +
                "name=" + getDisplayName() + ", " +
                "type=" + type() + ", " +
                "size=" + width() + ", " +
                "sort=" + m_sortOrder + ", " +
                "storage=" + m_storage + ")");
    }

    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        stringer.object();
        stringer.key(Members.GUID.name()).value(guid());
        stringer.key(Members.NAME.name()).value(getDisplayName());
        stringer.key(Members.TYPE.name()).value(type().name());
        stringer.key(Members.SIZE.name()).value(width());
        stringer.key(Members.SORT_ORDER.name()).value(m_sortOrder.name());
        stringer.key(Members.STORAGE.name()).value(m_storage.name());
        
        if (originTableName() != null) {
            stringer.key(Members.INPUT_TABLE_NAME.name()).value(originTableName());
        }
        else
        {
            stringer.key(Members.INPUT_TABLE_NAME.name()).value("");
        }
        if (originColumnName() != null) {
            stringer.key(Members.INPUT_COLUMN_NAME.name()).value(originColumnName());
        }
        else
        {
            stringer.key(Members.INPUT_COLUMN_NAME.name()).value("");
        }
        if (m_expression != null) {
            stringer.key(Members.EXPRESSION.name());
            stringer.object();
            m_expression.toJSONString(stringer);
            stringer.endObject();
        }
        else
        {
            stringer.key(Members.EXPRESSION.name()).value("");
        }

        stringer.endObject();
    }
    
    public static PlanColumn fromJSONObject(JSONObject obj, Database db) throws JSONException {
        //
        // Get the global id and check whether we already have this guy
        //
        int guid = obj.getInt(Members.GUID.name());
        PlanColumn column = null;
        
        PlannerContext planner = PlannerContext.singleton();
        synchronized (planner) {
            column = planner.get(guid);
            if (column == null) {
                String columnName = obj.getString(Members.NAME.name());
                SortOrder sortOrder = SortOrder.get(obj.getString(Members.SORT_ORDER.name()));
                Storage storage = Storage.get(obj.getString(Members.STORAGE.name()));
    
                AbstractExpression expression = null;
                if (!obj.getString(Members.EXPRESSION.name()).isEmpty()) {
                    JSONObject jsonExpression = obj.getJSONObject(Members.EXPRESSION.name());
                    expression = AbstractExpression.fromJSONObject(jsonExpression, db);
                }
                column = new PlanColumn(guid, expression, columnName, sortOrder, storage);
                planner.add(guid, column);
            }
        } // SYCHRONIZED
        return (column);
    }
    
}
