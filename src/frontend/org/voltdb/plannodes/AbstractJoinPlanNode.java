/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
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

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.JoinType;

/**
 *
 */
public abstract class AbstractJoinPlanNode extends AbstractPlanNode {

    public enum Members {
        JOIN_TYPE,
        PREDICATE;
    }

    protected JoinType m_joinType = JoinType.INNER;
    protected AbstractExpression m_predicate;

    /**
     * @param id
     */
    protected AbstractJoinPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }

    @Override
    public Object clone(boolean clone_children, boolean clone_inline) throws CloneNotSupportedException {
        AbstractJoinPlanNode clone = (AbstractJoinPlanNode)super.clone(clone_children, clone_inline);
        if (this.m_predicate != null) {
            clone.m_predicate = (AbstractExpression)this.m_predicate.clone();
        }
        return (clone);
    }
    
    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof AbstractJoinPlanNode) == false) {
            return (false);
        }
        AbstractJoinPlanNode other = (AbstractJoinPlanNode)obj;
        if (this.m_joinType != other.m_joinType) return (false);
        if (this.m_predicate != null) {
            if (other.m_predicate == null) return (false);
            if (this.m_predicate.equals(other.m_predicate) == false) return (false);
        } else if (other.m_predicate != null) return (false);
        return super.equals(obj);
    }
    
    @Override
    public void validate() throws Exception {
        super.validate();

        if (m_predicate != null) {
            m_predicate.validate();
        }
    }

    /**
     * @return the join_type
     */
    public JoinType getJoinType() {
        return m_joinType;
    }

    /**
     * @param join_type the join_type to set
     */
    public void setJoinType(JoinType join_type) {
        m_joinType = join_type;
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

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.JOIN_TYPE.name()).value(m_joinType.toString());
        stringer.key(Members.PREDICATE.name()).value(m_predicate);
    }
    
    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        m_joinType = JoinType.valueOf(obj.getString(Members.JOIN_TYPE.name()));
        JSONObject predicateObject = null;
        if (!obj.isNull(Members.PREDICATE.name())) {
            try {
                predicateObject = obj.getJSONObject(Members.PREDICATE.name());
            } catch (JSONException e) {
                //okay for it not to be there
            }
        }
        if (predicateObject != null) {
            m_predicate = AbstractExpression.fromJSONObject(predicateObject, db);
        }
    }
}
