package edu.brown.plannodes;

import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.utils.AbstractTreeWalker;

/**
 * @author pavlo
 */
public abstract class PlanNodeTreeWalker extends AbstractTreeWalker<AbstractPlanNode> {

    private final boolean include_inline;
    private final boolean reverse;

    /**
     * If include_line is true, the walker will visit each node's inline nodes
     * after visiting the parent
     * 
     * @param include_inline
     */
    public PlanNodeTreeWalker(boolean include_inline, boolean reverse) {
        super();
        this.include_inline = include_inline;
        this.reverse = reverse;
    }

    public PlanNodeTreeWalker(boolean include_inline) {
        this(include_inline, false);
    }

    public PlanNodeTreeWalker() {
        this(false);
    }

    /**
     * Depth first traversal
     * 
     * @param node
     */
    protected void populate_children(PlanNodeTreeWalker.Children<AbstractPlanNode> children, AbstractPlanNode node) {
        if (this.reverse) {
            for (int ctr = 0, cnt = node.getParentPlanNodeCount(); ctr < cnt; ctr++) {
                children.addAfter(node.getParent(ctr));
            }
        } else {
            for (int ctr = 0, cnt = node.getChildPlanNodeCount(); ctr < cnt; ctr++) {
                children.addBefore(node.getChild(ctr));
            }
        }
        if (this.include_inline)
            children.addAfter(node.getInlinePlanNodes().values());
        return;
    }
}
