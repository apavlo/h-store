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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

/**
 *
 */
public class PlanNodeList extends PlanNodeTree implements Comparable<PlanNodeList> {

    public enum Members {
        EXECUTE_LIST;
    }

    private final List<AbstractPlanNode> m_list = new ArrayList<AbstractPlanNode>();

    public PlanNodeList() {
        super();
    }

    public PlanNodeList(AbstractPlanNode root_node) {
        super(root_node);
        try {
            constructList();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<AbstractPlanNode> getExecutionList() {
        return m_list;
    }

    @Override
    public String toString() {
        String ret = "EXECUTE LIST: " + m_list.size() + " nodes\n";
        for (int ctr = 0, cnt = m_list.size(); ctr < cnt; ctr++) {
            ret += "   [" + ctr + "] " + m_list.get(ctr) + "\n";
        }
        ret += m_planNodes.get(0).toString();
        return ret;
    }

    public void constructList() throws Exception {
        // Create a counter for each node based on the # of children that it has
        // If any node has no children, put it in the execute list
        List<AbstractPlanNode> execute_list = new ArrayList<AbstractPlanNode>();
        Map<AbstractPlanNode, Integer> child_cnts = new HashMap<AbstractPlanNode, Integer>();
        for (AbstractPlanNode node : m_planNodes) {
            int num_of_children = node.getChildPlanNodeCount();
            if (num_of_children == 0) {
                execute_list.add(node);
            } else {
                child_cnts.put(node, num_of_children);
            }
        }
        // Now run through a simulation
        // Doing it this way maintains the nuances of the parent-child relationships
        m_list.clear();
        while (!execute_list.isEmpty()) {
            AbstractPlanNode node = execute_list.remove(0);
            // Add the node to our execution list
            m_list.add(node);
            // Then update all of this node's parents and reduce their wait counter by 1
            // If the counter is at zero, then we'll add it to end of our list
            for (int i = 0; i < node.getParentPlanNodeCount(); i++) {
                AbstractPlanNode parent = node.getParent(i);
                int remaining = child_cnts.get(parent) - 1;
                child_cnts.put(parent, remaining);
                if (remaining == 0) {
                    execute_list.add(parent);
                }
            }
        }

        // Important! Make sure that our list has the same number of entries in our tree
        if (m_list.size() != m_planNodes.size()) {
            throw new Exception("ERROR: The execution list has '" + m_list.size() + "' PlanNodes but our original tree has '" + m_planNodes.size() + "' PlanNode entries");
        }
        return;
    }

    @Override
    public int compareTo(PlanNodeList o) {
        if (m_list.size() != o.m_list.size()) return -1;

        int diff = getRootPlanNode().compareTo(o.getRootPlanNode());
        if (diff != 0) return diff;

        for (int i = 0; i < m_list.size(); i++) {
            diff = m_list.get(i).getId() - o.m_list.get(i).getId();
            if (diff != 0) return diff;
        }

        return 0;
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            super.toJSONString(stringer);

            stringer.key(Members.EXECUTE_LIST.name()).array();
            for (AbstractPlanNode node : m_list) {
                stringer.value(node.getPlanNodeId().intValue());
            }
            stringer.endArray(); //end execution list

            stringer.endObject(); //end PlanNodeList
        } catch (JSONException e) {
            // HACK ugly ugly to make the JSON handling
            // in QueryPlanner generate a JSONException for a plan we know
            // here that we can't serialize.  Making this method throw
            // JSONException pushes that exception deep into the bowels of
            // Volt with no good place to catch it and handle the error.
            // Consider this the coward's way out.
            throw new RuntimeException("Failed to serialize PlanNodeList", e);
//            return "This JSON error message is a lie";
        }
        return stringer.toString();
    }

    protected void loadFromJSONObject(JSONObject object, Database db) throws JSONException {
        super.loadFromJSONObject(object, db);
        JSONArray executeList = object.getJSONArray(Members.EXECUTE_LIST.name());
        for (int ii = 0; ii < executeList.length(); ii++) {
            Integer id = executeList.getInt(ii);
            m_list.add(m_idToNodeMap.get(id));
        }
    }
    
    /**
     * Does this plan node list generate an equivalent plan if passed through the
     * serialization mechanism in both directions? Used for testing only.
     */
    @Deprecated
    public boolean testJSONSerialization(Database db) {
        // serialize the plan list to a string
        String jsonValue = toJSONString();
        
        // deserialize the string of the original to a new list
        PlanNodeList other = new PlanNodeList();
        try {
            other.loadFromJSONObject(new JSONObject(jsonValue), db);
        } catch (JSONException e) {
            e.printStackTrace();
            return false;
        }
        
        // Try serializing the deserialized value and make
        // sure the two json strings are the same.
        String jsonValue2 = other.toJSONString();
        if (jsonValue.compareTo(jsonValue2) != 0) {
            System.out.println(jsonValue);
            System.out.println(jsonValue2);
            return false;
        }
        
        // Check if the deserialzied plan list is the same
        // according to the not-perfect compare interface
        // it supports. The previous test is probably better.
        return other.compareTo(this) == 0;
    }

    public String toDOTString(String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("digraph ").append(name).append(" {\n");

        for (AbstractPlanNode node : m_list) {
                sb.append(node.toDOTString());
        }

        sb.append("\n}\n");
        return sb.toString();
    }
}
