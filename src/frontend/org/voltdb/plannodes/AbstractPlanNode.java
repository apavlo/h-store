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

import java.util.*;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONStringer;
import org.json.JSONException;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.planner.PlanAssembler;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.PlannerContext;
import org.voltdb.planner.StatsField;
import org.voltdb.types.*;

import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ClassUtil;

public abstract class AbstractPlanNode implements JSONString, Cloneable, Comparable<AbstractPlanNode> {

    public enum Members {
        ID,
        PLAN_NODE_TYPE,
        INLINE_NODES,
        CHILDREN_IDS,
        PARENT_IDS,
        OUTPUT_COLUMNS,
        IS_INLINE,
    }

    private int m_id = -1;
    protected List<AbstractPlanNode> m_children = new ArrayList<AbstractPlanNode>();
    protected List<AbstractPlanNode> m_parents = new ArrayList<AbstractPlanNode>();
    protected HashSet<AbstractPlanNode> m_dominators = new HashSet<AbstractPlanNode>();

    // PAVLO: We need this figure out how to reconstruct the tree
    protected List<Integer> m_childrenIds = new ArrayList<Integer>();
    protected List<Integer> m_parentIds = new ArrayList<Integer>();
    
    // TODO: planner accesses this data directly. Should be protected.
    protected ArrayList<Integer> m_outputColumns = new ArrayList<Integer>();
    protected List<ScalarValueHints> m_outputColumnHints = new ArrayList<ScalarValueHints>();
    protected long m_estimatedOutputTupleCount = 0;

    /**
     * Some PlanNodes can take advantage of inline PlanNodes to perform
     * certain additional tasks while performing their main operation, rather than
     * having to re-read tuples from intermediate results
     */
    protected Map<PlanNodeType, AbstractPlanNode> m_inlineNodes = new HashMap<PlanNodeType, AbstractPlanNode>();
    protected boolean m_isInline = false;

    protected final PlannerContext m_context;

    /**
     * Instantiates a new plan node.
     *
     * @param id the id
     */
    protected AbstractPlanNode(PlannerContext context, int id) {
        assert(context != null);
        assert(id != 0);
        m_context = context;
        m_id = id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AbstractPlanNode) {
            AbstractPlanNode other = (AbstractPlanNode)obj;
            return (m_id == other.m_id &&
                    m_isInline == other.m_isInline &&
                    this.getPlanNodeType() == other.getPlanNodeType() &&
                    this.getOutputColumnGUIDs().equals(other.getOutputColumnGUIDs()));
        }
        return (false);
    }
    
    protected final int getId() {
        return (m_id);
    }
    
    @Override
    public final Object clone() throws CloneNotSupportedException {
        return (this.clone(true, true));
    }
    
    public Object clone(boolean clone_children, boolean clone_inline) throws CloneNotSupportedException {
        AbstractPlanNode clone = (AbstractPlanNode)super.clone();
        clone.overrideId(PlanAssembler.getNextPlanNodeId());
        
        clone.m_children = new ArrayList<AbstractPlanNode>();
        clone.m_parents = new ArrayList<AbstractPlanNode>();
        clone.m_dominators = new HashSet<AbstractPlanNode>(m_dominators);
        clone.m_childrenIds = new ArrayList<Integer>();
        clone.m_outputColumns = new ArrayList<Integer>(m_outputColumns);
        clone.m_outputColumnHints = new ArrayList<ScalarValueHints>(m_outputColumnHints);
        clone.m_inlineNodes = new HashMap<PlanNodeType, AbstractPlanNode>();
        
        // Clone Children
        if (clone_children) {
//            clone.m_children.clear();
//            clone.m_childrenIds.clear();
            for (AbstractPlanNode child_node : this.m_children) {
                AbstractPlanNode child_clone = (AbstractPlanNode)child_node.clone(clone_inline, clone_children);
                child_clone.m_parents.clear();
                child_clone.m_parentIds.clear();
                child_clone.m_parents.add(clone);
                child_clone.m_parentIds.add(clone.m_id);
                clone.m_children.add(child_clone);
                clone.m_childrenIds.add(child_clone.m_id);
            } // FOR
        }
        
        // Clone Inlines
        if (clone_inline) {
//            clone.m_inlineNodes.clear();
            for (Entry<PlanNodeType, AbstractPlanNode> e : this.m_inlineNodes.entrySet()) {
                AbstractPlanNode inline_clone = (AbstractPlanNode)e.getValue().clone(clone_inline, clone_children);
                clone.m_inlineNodes.put(e.getKey(), inline_clone);
            } // FOR
        }
        
        return (clone);
    }
    
    public void overrideId(int newId) {
        m_id = newId;
    }

    /**
     * Create a PlanNode that clones the configuration information but
     * is not inserted in the plan graph and has a unique plan node id.
     */
    protected void produceCopyForTransformation(AbstractPlanNode copy) {
        for (Integer colGuid : m_outputColumns) {
            copy.m_outputColumns.add(colGuid);
        }
        copy.m_outputColumnHints.addAll(m_outputColumnHints);
        copy.m_estimatedOutputTupleCount = m_estimatedOutputTupleCount;

        // clone is not yet implemented for every node.
        assert(m_inlineNodes.size() == 0);
        assert(m_isInline == false);

        // the api requires the copy is not (yet) connected
        assert (copy.m_parents.size() == 0);
        assert (copy.m_children.size() == 0);
    }


    public abstract PlanNodeType getPlanNodeType();

    public void setOutputColumns(Collection<Integer> col_guids) {
        this.m_outputColumns.clear();
        this.m_outputColumns.addAll(col_guids);
    }
    
    public boolean updateOutputColumns(Database db) {
        //System.out.println("updateOutputColumns Node type: " + this.getPlanNodeType() + " # of inline nodes: " + this.getInlinePlanNodes().size());

        ArrayList<Integer> childCols = new ArrayList<Integer>();
        for (AbstractPlanNode child : m_children) {
            boolean result = child.updateOutputColumns(db);
            assert(result);
            
            // print child inline columns
//            for (Integer out : child.m_outputColumns)
//            {
//              System.out.println(m_context.get(out).displayName());
//            }
            
            childCols.addAll(child.m_outputColumns);
        }

        ArrayList<Integer> new_output_cols = new ArrayList<Integer>();
        new_output_cols = createOutputColumns(db, childCols);
        for (AbstractPlanNode child : m_inlineNodes.values()) {
            if (child instanceof IndexScanPlanNode)
                continue;
            new_output_cols = child.createOutputColumns(db, new_output_cols);
        }

        // Before we wipe out the old column list, free any PlanColumns that
        // aren't getting reused
        for (Integer col : m_outputColumns)
        {
            if (!new_output_cols.contains(col))
            {
                m_context.freeColumn(col);
            }
        }

        m_outputColumns = new_output_cols;

        return true;
    }

    /** By default, a plan node does not alter its input schema */
    @SuppressWarnings("unchecked")
    protected ArrayList<Integer> createOutputColumns(Database db, ArrayList<Integer> input) {
        return (ArrayList<Integer>)input.clone();
    }

    /**
     * Get number of output columns for this node
     * @return
     */
    public int getOutputColumnGUIDCount() {
        return (this.m_outputColumns.size());
    }
    
    /**
     * Return the PlanColumn GUID at the given offset
     * @param idx
     * @return
     */
    public int getOutputColumnGUID(int idx) {
        return (this.m_outputColumns.get(idx));
    }
    
    /**
     * Get the list of the Output PlanColumn GUIDs
     * @return
     */
    public List<Integer> getOutputColumnGUIDs() {
        return (this.m_outputColumns);
    }
    
    public PlanColumn findMatchingOutputColumn(String tableName,
                                               String columnName,
                                               String columnAlias)
    {
        boolean found = false;
        PlanColumn retval = null;
        for (Integer colguid : m_outputColumns) {
            PlanColumn plancol = m_context.get(colguid);
            if ((plancol.originTableName().equals(tableName)) &&
                ((plancol.originColumnName().equals(columnName)) ||
                 (plancol.originColumnName().equals(columnAlias))))
            {
                found = true;
                retval = plancol;
                break;
            }
        }
        if (!found) {
            assert(found) : "Found no candidate output column.";
            throw new RuntimeException("Found no candidate output column.");
        }
        return retval;
    }

    public void validate() throws Exception {
        //
        // Make sure our children have us listed as their parents
        //
        for (AbstractPlanNode child : m_children) {
            if (!child.m_parents.contains(this)) {
                throw new Exception("ERROR: The child PlanNode '" + child.toString() + "' does not " +
                                    "have its parent PlanNode '" + toString() + "' in its parents list");
            }
            child.validate();
        }
        //
        // Inline PlanNodes
        //
        if (!m_inlineNodes.isEmpty()) {
            for (AbstractPlanNode node : m_inlineNodes.values()) {
                //
                // Make sure that we're not attached to some kind of tree somewhere...
                //
                if (!node.m_children.isEmpty()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' has children inside of PlanNode '" + this + "'");
                } else if (!node.m_parents.isEmpty()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' has parents inside of PlanNode '" + this + "'");
                } else if (!node.isInline()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' was not marked as inline for PlanNode '" + this + "'");
                } else if (!node.getInlinePlanNodes().isEmpty()) {
                    throw new Exception("ERROR: The inline PlanNode '" + node + "' has its own inline PlanNodes inside of PlanNode '" + this + "'");
                }
                node.validate();
            }
        }
    }

    @Override
    public final String toString() {
        return String.format("%s[#%02d]", getPlanNodeType().toString(), m_id);
    }

    public boolean computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        assert(estimates != null);

        m_outputColumnHints.clear();
        m_estimatedOutputTupleCount = 0;

        // recursively compute and collect stats from children
        for (AbstractPlanNode child : m_children) {
            boolean result = child.computeEstimatesRecursively(stats, cluster, db, estimates, paramHints);
            assert(result);
            m_outputColumnHints.addAll(child.m_outputColumnHints);
            m_estimatedOutputTupleCount += child.m_estimatedOutputTupleCount;

            stats.incrementStatistic(0, StatsField.TUPLES_READ, m_estimatedOutputTupleCount);
        }

        return true;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public Integer getPlanNodeId() {
        return m_id;
    }

    /**
     * Add a plan node as a child of this node and link this node as it's parent.
     * @param child The node to add.
     */
    public void addAndLinkChild(AbstractPlanNode child) {
        m_children.add(child);
        child.m_parents.add(this);
    }

    /** Remove child from this node.
     * @param child to remove.
     */
    public void unlinkChild(AbstractPlanNode child) {
        m_children.remove(child);
        child.m_parents.remove(this);
    }

    /**
     * Gets the children.
     * @return the children
     */
    public int getChildPlanNodeCount() {
        return m_children.size();
    }

    /**
     * @param index
     * @return The child node of this node at a given index or null if none exists.
     */
    public AbstractPlanNode getChild(int index) {
        return m_children.get(index);
    }
    
    /**
     * Gets all of the children of this node
     * @return
     */
    public List<AbstractPlanNode> getChildren() {
        return (Collections.unmodifiableList(m_children));
    }

    public void clearChildren() {
        m_children.clear();
        m_childrenIds.clear();
    }

    public boolean hasChild(AbstractPlanNode receive) {
        return m_children.contains(receive);
    }

    /**
     * Gets the number of parents.
     * @return the parents
     */
    public int getParentPlanNodeCount() {
        return m_parents.size();
    }

    public AbstractPlanNode getParent(int index) {
        return m_parents.get(index);
    }
    
    /**
     * Gets all of the parents of this node
     * @return
     */
    public List<AbstractPlanNode> getParents() {
        return (Collections.unmodifiableList(m_parents));
    }

    public void clearParents() {
        m_parents.clear();
        m_parentIds.clear();
    }

    public void removeFromGraph() {
       for (AbstractPlanNode parent : m_parents)
           parent.m_children.remove(this);
       for (AbstractPlanNode child : m_children)
           child.m_parents.remove(this);

       m_parents.clear();
       m_children.clear();
    }

    /** Interject the provided node between this node and this node's current children */
    public void addIntermediary(AbstractPlanNode node) {

        // transfer this node's children to node
        Iterator<AbstractPlanNode> it = m_children.iterator();
        while (it.hasNext()) {
            AbstractPlanNode child = it.next();
            it.remove();                          // remove this.child from m_children
            assert(child.getParentPlanNodeCount() == 1) :
                String.format("Expected %s to have only one parent but it has %s", child, child.getParents()); 
            child.clearParents();                 // and reset child's parents list
            node.addAndLinkChild(child);          // set node.child and child.parent
        }

        // and add node to this node's children
        assert(m_children.size() == 0);
        addAndLinkChild(node);
    }

    /**
     * @return The map of inlined nodes.
     */
    public Map<PlanNodeType, AbstractPlanNode> getInlinePlanNodes() {
        return m_inlineNodes;
    }
    
    public int getInlinePlanNodeCount() {
        return (m_inlineNodes.size());
    }

    /**
     * @param node
     */
    public void addInlinePlanNode(AbstractPlanNode node) {
        node.m_isInline = true;
        m_inlineNodes.put(node.getPlanNodeType(), node);
        node.m_children.clear();
        node.m_parents.clear();
    }

    /**
     *
     * @param type
     */
    public void removeInlinePlanNode(PlanNodeType type) {
        if (m_inlineNodes.containsKey(type)) {
            m_inlineNodes.remove(type);
        }
    }

    /**
     *
     * @param type
     * @return An inlined node of the given type or null if none.
     */
    @SuppressWarnings("unchecked")
    public <T extends AbstractPlanNode> T getInlinePlanNode(PlanNodeType type) {
        return (T)m_inlineNodes.get(type);
    }
    
    /**
     * Return all of the inline AbstractPlanNodes with the same class
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends AbstractPlanNode> Collection<T> getInlinePlanNodes(Class<T> clazz) {
        Set<T> ret = new HashSet<T>(); 
        for (AbstractPlanNode inline : this.m_inlineNodes.values()) {
            if (ClassUtil.getSuperClasses(inline.getClass()).contains(clazz)) {
                ret.add((T)inline);
            }
        } // FOR
        return (ret);
    }

    /**
     *
     * @return Is this node inlined in another node.
     */
    public Boolean isInline() {
        return m_isInline;
    }


    /**
     * @return the dominator list for a node
     */
    public HashSet<AbstractPlanNode> getDominators() {
        return m_dominators;
    }

    /**
    *   Initialize a hashset for each node containing that node's dominators
    *   (the set of predecessors that *always* precede this node in a traversal
    *   of the plan-graph in reverse-execution order (from root to leaves)).
    */
    public void calculateDominators() {
        HashSet<AbstractPlanNode> visited = new HashSet<AbstractPlanNode>();
        calculateDominators_recurse(visited);
    }

    private void calculateDominators_recurse(HashSet<AbstractPlanNode> visited) {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }

        visited.add(this);
        m_dominators.clear();
        m_dominators.add(this);

        // find nodes that are in every parent's dominator set.

        HashMap<AbstractPlanNode, Integer> union = new HashMap<AbstractPlanNode, Integer>();
        for (AbstractPlanNode n : m_parents) {
            for (AbstractPlanNode d : n.getDominators()) {
                if (union.containsKey(d))
                    union.put(d, union.get(d) + 1);
                else
                    union.put(d, 1);
            }
        }

        for (AbstractPlanNode pd : union.keySet() ) {
            if (union.get(pd) == m_parents.size())
                m_dominators.add(pd);
        }

        for (AbstractPlanNode n : m_children)
            n.calculateDominators_recurse(visited);
    }

    /**
     * @param type plan node type to search for
     * @return a list of nodes that are eventual successors of this node of the desired type
     */
    public List<AbstractPlanNode> findAllNodesOfType(PlanNodeType type) {
        HashSet<AbstractPlanNode> visited = new HashSet<AbstractPlanNode>();
        ArrayList<AbstractPlanNode> collected = new ArrayList<AbstractPlanNode>();
        findAllNodesOfType_recurse(type, collected, visited);
        return collected;
    }

    public void findAllNodesOfType_recurse(PlanNodeType type,ArrayList<AbstractPlanNode> collected,
        HashSet<AbstractPlanNode> visited)
    {
        if (visited.contains(this)) {
            assert(false): "do not expect loops in plangraph.";
            return;
        }
        visited.add(this);
        if (getPlanNodeType() == type)
            collected.add(this);

        for (AbstractPlanNode n : m_children)
            n.findAllNodesOfType_recurse(type, collected, visited);
    }

    public void freeColumns(Set<Integer> skip) {
        Collection<Integer> guids = PlanNodeUtil.getAllPlanColumnGuids(this);
        guids.removeAll(skip);
        for (Integer guid : guids) {
            m_context.freeColumn(guid);
        } // FOR
    }

    @Override
    public int compareTo(AbstractPlanNode other) {
        int diff = 0;

        // compare child nodes
        HashMap<Integer, AbstractPlanNode> nodesById = new HashMap<Integer, AbstractPlanNode>();
        for (AbstractPlanNode node : m_children)
            nodesById.put(node.getPlanNodeId(), node);
        for (AbstractPlanNode node : other.m_children) {
            AbstractPlanNode myNode = nodesById.get(node.getPlanNodeId());
            diff = myNode.compareTo(node);
            if (diff != 0) return diff;
        }

        // compare inline nodes
        HashMap<Integer, Entry<PlanNodeType, AbstractPlanNode>> inlineNodesById =
               new HashMap<Integer, Entry<PlanNodeType, AbstractPlanNode>>();
        for (Entry<PlanNodeType, AbstractPlanNode> e : m_inlineNodes.entrySet())
            inlineNodesById.put(e.getValue().getPlanNodeId(), e);
        for (Entry<PlanNodeType, AbstractPlanNode> e : other.m_inlineNodes.entrySet()) {
            Entry<PlanNodeType, AbstractPlanNode> myE = inlineNodesById.get(e.getValue().getPlanNodeId());
            if (myE.getKey() != e.getKey()) return -1;

            diff = myE.getValue().compareTo(e.getValue());
            if (diff != 0) return diff;
        }

        diff = m_id - other.m_id;
        return diff;
    }

    // produce a file that can imported into graphviz for easier visualization
    public String toDOTString() {
        StringBuilder sb = new StringBuilder();

        // id [label=id: value-type <value-type-attributes>];
        // id -> child_id;
        // id -> child_id;

        sb.append(m_id).append(" [label=\"").append(m_id).append(": ").append(getPlanNodeType()).append("\" ");
        sb.append(getValueTypeDotString(this));
        sb.append("];\n");
        for (AbstractPlanNode node : m_inlineNodes.values()) {
            sb.append(m_id).append(" -> ").append(node.getPlanNodeId().intValue()).append(";\n");
            sb.append(node.toDOTString());
        }
        for (AbstractPlanNode node : m_children) {
           sb.append(m_id).append(" -> ").append(node.getPlanNodeId().intValue()).append(";\n");
        }

        return sb.toString();
    }

    // maybe not worth polluting
    private String getValueTypeDotString(AbstractPlanNode pn) {
        PlanNodeType pnt = pn.getPlanNodeType();
        if (pn.isInline()) {
            return "fontcolor=\"white\" style=\"filled\" fillcolor=\"red\"";
        }
        if (pnt == PlanNodeType.SEND || pnt == PlanNodeType.RECEIVE) {
            return "fontcolor=\"white\" style=\"filled\" fillcolor=\"black\"";
        }
        return "";
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try
        {
            stringer.object();
            toJSONString(stringer);
            stringer.endObject();
        }
        catch (JSONException e)
        {
            throw new RuntimeException("Failed to serialize " + this, e);
//            System.exit(-1);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.ID.name()).value(m_id);
        stringer.key(Members.PLAN_NODE_TYPE.name()).value(getPlanNodeType().toString());

        stringer.key(Members.IS_INLINE.name()).value(m_isInline);
        stringer.key(Members.INLINE_NODES.name()).array();
        PlanNodeType types[] = new PlanNodeType[m_inlineNodes.size()];
        int i = 0;
        for (PlanNodeType type : m_inlineNodes.keySet()) {
            types[i++] = type;
        }
        Arrays.sort(types);
        for (PlanNodeType type : types) {
            AbstractPlanNode node = m_inlineNodes.get(type);
            assert(node != null);
            assert(node instanceof JSONString);
            stringer.value(node);
        }
        /*for (Map.Entry<PlanNodeType, AbstractPlanNode> entry : m_inlineNodes.entrySet()) {
            assert (entry.getValue() instanceof JSONString);
            stringer.value(entry.getValue());
        }*/
        stringer.endArray();
        stringer.key(Members.CHILDREN_IDS.name()).array();
        for (AbstractPlanNode node : m_children) {
            stringer.value(node.getPlanNodeId().intValue());
        }
        stringer.endArray().key(Members.PARENT_IDS.name()).array();
        for (AbstractPlanNode node : m_parents) {
            stringer.value(node.getPlanNodeId().intValue());
        }
        stringer.endArray(); //end inlineNodes

        stringer.key(Members.OUTPUT_COLUMNS.name());
        stringer.array();
        for (int col = 0; col < m_outputColumns.size(); col++) {
            PlanColumn column = m_context.get(m_outputColumns.get(col));
            column.toJSONString(stringer);
        }
        stringer.endArray();
    }
    
    abstract protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException;
    
    public static AbstractPlanNode fromJSONObject(JSONObject obj, Database db) throws JSONException {
        PlanNodeType pnt = PlanNodeType.valueOf(obj.getString(Members.PLAN_NODE_TYPE.name()));
        
        AbstractPlanNode node = null;
        try {
            node = (AbstractPlanNode)ClassUtil.newInstance(pnt.getPlanNodeClass(),
                                                  new Object[]{ PlannerContext.singleton(), 1 },
                                                  new Class[]{  PlannerContext.class, Integer.class });
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        node.m_id = obj.getInt(Members.ID.name());
        node.m_isInline = obj.getBoolean(Members.IS_INLINE.name());
        
        JSONArray inlineNodes = obj.getJSONArray(Members.INLINE_NODES.name());
        for (int ii = 0; ii < inlineNodes.length(); ii++) {
            JSONObject inobj = inlineNodes.getJSONObject(ii);
            AbstractPlanNode inlineNode = AbstractPlanNode.fromJSONObject(inobj, db);
            node.m_inlineNodes.put(inlineNode.getPlanNodeType(), inlineNode);
        }
        
        JSONArray childrenIds = obj.getJSONArray(Members.CHILDREN_IDS.name());
        for (int ii = 0; ii < childrenIds.length(); ii++) {
            node.m_childrenIds.add(childrenIds.getInt(ii));
        }
        
        JSONArray parentIds = obj.getJSONArray(Members.PARENT_IDS.name());
        for (int ii = 0; ii < parentIds.length(); ii++) {
            node.m_parentIds.add(parentIds.getInt(ii));
        }
        
        JSONArray outputColumns = obj.getJSONArray(Members.OUTPUT_COLUMNS.name());
        for (int ii = 0; ii < outputColumns.length(); ii++) {
            JSONObject jsonObject = outputColumns.getJSONObject(ii);
            PlanColumn column = PlanColumn.fromJSONObject(jsonObject, db);
            assert(column != null);
            node.m_outputColumns.add(column.guid());
//            System.err.println(String.format("[%02d] %s => %s", ii, node, column));
        }
        
        node.loadFromJSONObject(obj, db);
        
        return node;
    }
}
