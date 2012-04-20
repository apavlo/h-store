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

package org.voltdb.plannodes;

import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlanColumn.SortOrder;
import org.voltdb.planner.PlannerContext;
import org.voltdb.utils.CatalogUtil;

/**
 *
 */
public abstract class AbstractScanPlanNode extends AbstractPlanNode {

    public enum Members {
        PREDICATE,
        TARGET_TABLE_NAME;
    }

    protected AbstractExpression m_predicate;

    // The target table is the table that the plannode wants to perform some operation on.
    protected String m_targetTableName = "";
    protected String m_targetTableAlias = null;

    /**
     * @param id
     */
    protected AbstractScanPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }
    
    @Override
    public Object clone(boolean clone_children, boolean clone_inline) throws CloneNotSupportedException {
        AbstractScanPlanNode clone = (AbstractScanPlanNode)super.clone(clone_children, clone_inline);
        clone.m_predicate = (AbstractExpression)this.m_predicate.clone();
        return (clone);
    }

    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof AbstractScanPlanNode) == false) {
            return (false);
        }
        AbstractScanPlanNode other = (AbstractScanPlanNode)obj;
        if (this.m_targetTableName != null) {
            if (this.m_targetTableName.equals(other.m_targetTableName) == false) return (false);
        } else if (other.m_targetTableName != null) return (false);
        if (this.m_targetTableAlias != null) {
            if (this.m_targetTableAlias.equals(other.m_targetTableAlias) == false) return (false);
        } else if (other.m_targetTableAlias != null) return (false);
        if (this.m_predicate != null) {
            if (other.m_predicate == null) return (false);
            if (this.m_predicate.equals(other.m_predicate) == false) return (false);
        } else if (other.m_predicate != null) return (false);
        return super.equals(obj);
    }
    
    @Override
    public void validate() throws Exception {
        super.validate();
        // TargetTableId
        if (m_targetTableName == null) {
            throw new Exception("ERROR: TargetTableName is null for PlanNode '" + toString() + "'");
        }
        // Filter Expression
        // It is allowed to be null, but we need to check that it's valid
        if (m_predicate != null) {
            m_predicate.validate();
        }
    }

    /**
     * @return the target_table_name
     */
    public String getTargetTableName() {
        return m_targetTableName;
    }

    /**
     * @param name
     */
    public void setTargetTableName(String name) {
        m_targetTableName = name;
    }

    /**
     * @return the target_table_alias
     */
    public String getTargetTableAlias() {
        return m_targetTableAlias;
    }

    /**
     * @param alias
     */
    public void setTargetTableAlias(String alias) {
        m_targetTableAlias = alias;
    }

    /**
     * @return the predicate
     */
    public AbstractExpression getPredicate() {
        return m_predicate;
    }

    /**
     * @param predicate the predicate to set
     */
    public void setPredicate(AbstractExpression predicate) {
        m_predicate = predicate;
    }

    /**
     * Initialize output columns
     * @param db
     */
    @SuppressWarnings("unchecked")
    @Override
    protected ArrayList<Integer> createOutputColumns(Database db, ArrayList<Integer> input) {
        assert(m_children.isEmpty());

        // the planner gleefully re-calculates output columns multiple times
        // but this state should be idempotent w.r.t. to subsequent calls.
        if (m_outputColumns.isEmpty()) {

            CatalogMap<Column> cols =
                db.getTables().getIgnoreCase(m_targetTableName).getColumns();

            PlanColumn.Storage storage = PlanColumn.Storage.kPartitioned;
            if (db.getTables().getIgnoreCase(m_targetTableName).getIsreplicated())
                storage = PlanColumn.Storage.kReplicated;

            // construct the columns and store them in index-ordering
            PlanColumn orderedCols[] = new PlanColumn[cols.size()];

            // you don't strictly need to sort this, but it makes diff-ing easier
            for (Column col : CatalogUtil.getSortedCatalogItems(cols, "index")) {
                // must produce a tuple value expression for this column.
                TupleValueExpression tve = new TupleValueExpression();
                tve.setValueType(VoltType.get((byte)col.getType()));
                tve.setValueSize(col.getSize());
                tve.setColumnIndex(col.getIndex());
                tve.setTableName(m_targetTableName);
                tve.setColumnAlias(col.getTypeName());
                tve.setColumnName(col.getTypeName());

                orderedCols[col.getIndex()] =
                    m_context.getPlanColumn(tve, col.getTypeName(), SortOrder.kUnsorted, storage);
            }

            // populate column collection in index (output) order
            for (int i=0; i < orderedCols.length; ++i)
                m_outputColumns.add(orderedCols[i].guid());
        }

        return (ArrayList<Integer>)m_outputColumns.clone();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key(Members.PREDICATE.name()).value(m_predicate);
        stringer.key(Members.TARGET_TABLE_NAME.name()).value(m_targetTableName);
    }
    
    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        JSONObject predicateObject = null;
        if (!obj.isNull(Members.PREDICATE.name())) {
            try {
                predicateObject = obj.getJSONObject(Members.PREDICATE.name());
            } catch (JSONException e) {
                //okay for it not to be there.
            }
        }

        if (predicateObject != null) {
            m_predicate = AbstractExpression.fromJSONObject(predicateObject, db);
        }
        
        m_targetTableName = obj.getString(Members.TARGET_TABLE_NAME.name());
        if (db != null || m_targetTableName.equals("")) {
            Table table = db.getTables().get(m_targetTableName);
            if (table == null)
                throw new JSONException("Unable to retrieve catalog object for table '" + m_targetTableName + "'");
        } 
    }
}