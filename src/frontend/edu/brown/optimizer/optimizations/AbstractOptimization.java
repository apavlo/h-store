package edu.brown.optimizer.optimizations;

import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.optimizer.PlanOptimizerState;

public abstract class AbstractOptimization {

    protected final PlanOptimizerState state;
    
    public AbstractOptimization(PlanOptimizerState state) {
        this.state = state;
    }
    
    public abstract AbstractPlanNode optimize(AbstractPlanNode rootNode);
    
}
