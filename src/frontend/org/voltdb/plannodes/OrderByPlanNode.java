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
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 *
 */
public class OrderByPlanNode extends AbstractPlanNode {

    public enum Members {
        SORT_COLUMNS,
        COLUMN_NAME,
        COLUMN_GUID,
        SORT_DIRECTION;
    }

    /**
     * Sort Columns Indexes
     * The column index in the table that we should sort on
     */
    private List<Integer> m_sortColumns = new ArrayList<Integer>();
    private List<Integer> m_sortColumnGuids = new ArrayList<Integer>();
    private List<String> m_sortColumnNames = new ArrayList<String>();
    private List<SortDirectionType> m_sortDirections = new Vector<SortDirectionType>();

    /**
     * @param id
     */
    public OrderByPlanNode(PlannerContext context, Integer id) {
        super(context, id);
    }
    
    @Override
    public Object clone(boolean clone_children, boolean clone_inline) throws CloneNotSupportedException {
        OrderByPlanNode clone = (OrderByPlanNode)super.clone(clone_children, clone_inline);
        
        clone.m_sortColumns = new ArrayList<Integer>(this.m_sortColumns);
        clone.m_sortColumnGuids = new ArrayList<Integer>(this.m_sortColumnGuids);
        clone.m_sortColumnNames = new ArrayList<String>(this.m_sortColumnNames);
        clone.m_sortDirections = new ArrayList<SortDirectionType>(this.m_sortDirections);
        
        return (clone);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.ORDERBY;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Make sure that they have the same # of columns and directions
        if (m_sortColumns.size() != m_sortDirections.size()) {
            throw new Exception("ERROR: PlanNode '" + toString() + "' has " +
                                "'" + m_sortColumns.size() + "' sort columns but " +
                                "'" + m_sortDirections.size() + "' sort directions");
        }

        // Make sure that none of the items are null
        for (int ctr = 0, cnt = m_sortColumns.size(); ctr < cnt; ctr++) {
            if (m_sortColumns.get(ctr) == null) {
                throw new Exception("ERROR: PlanNode '" + toString() + "' has a null " +
                                    "sort column index at position " + ctr);
            } else if (m_sortDirections.get(ctr) == null) {
                throw new Exception("ERROR: PlanNode '" + toString() + "' has a null " +
                                    "sort direction at position " + ctr);
            }
        }
    }

    /**
     * @return the sort_columns
     */
    public List<Integer> getSortColumns() {
        return m_sortColumns;
    }
    /**
     * @return the sort_column_guids
     */
    public List<Integer> getSortColumnGuids() {
        return m_sortColumnGuids;
    }
    /**
     * @return the sort_column_names
     */
    public List<String> getSortColumnNames() {
        return m_sortColumnNames;
    }
    /**
     * @return the sort_directions
     */
    public List<SortDirectionType> getSortDirections() {
        return m_sortDirections;
    }


    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_sortColumnNames.size() == m_sortDirections.size());
        stringer.key(Members.SORT_COLUMNS.name()).array();
        for (int ii = 0; ii < m_sortColumnNames.size(); ii++) {
            stringer.object();
            stringer.key(Members.COLUMN_NAME.name()).value(m_sortColumnNames.get(ii));
            stringer.key(Members.COLUMN_GUID.name()).value(m_sortColumnGuids.get(ii));
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirections.get(ii).toString());
            stringer.endObject();
        }
        stringer.endArray();
    }
    
    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        JSONArray sortColumns = obj.getJSONArray(Members.SORT_COLUMNS.name());
        for (int ii = 0; ii < sortColumns.length(); ii++) {
            JSONObject sortColumn = sortColumns.getJSONObject(ii);
            m_sortColumnNames.add(sortColumn.getString(Members.COLUMN_NAME.name()));
            m_sortColumnGuids.add(sortColumn.getInt(Members.COLUMN_GUID.name()));
            m_sortDirections.add(SortDirectionType.valueOf(sortColumn.getString(Members.SORT_DIRECTION.name())));
        }
    }

    /** adds a plan column to the list of output columns **/
    public void appendOutputColumn(PlanColumn column)
    {
        m_outputColumns.add(column.guid());
    }

    /** adds a sort column to the list of output columns **/
    public void appendSortColumn(PlanColumn column)
    {
        m_sortColumnGuids.add(column.guid());
    }

    /** clears the list of output columns **/
    public void clearOutputColumn()
    {
        m_outputColumns.clear();
    }

    /** clears the list of sort column GUIDS **/
    public void clearSortColumn()
    {
        m_sortColumnGuids.clear();
    }
}