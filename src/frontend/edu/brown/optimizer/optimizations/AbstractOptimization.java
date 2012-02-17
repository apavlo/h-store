package edu.brown.optimizer.optimizations;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.utils.Pair;

import edu.brown.optimizer.PlanOptimizerState;

public abstract class AbstractOptimization {

    protected final PlanOptimizerState state;

    public AbstractOptimization(PlanOptimizerState state) {
        this.state = state;
    }

    /**
     * Perform the optimization on the given PlanNode tree Returns a pair
     * containing the new root of the tree a boolean flag that signals whether
     * the tree was modified or not
     * 
     * @param rootNode
     * @return
     */
    public abstract Pair<Boolean, AbstractPlanNode> optimize(final AbstractPlanNode rootNode);

}
