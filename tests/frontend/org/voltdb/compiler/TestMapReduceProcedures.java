package org.voltdb.compiler;

import org.junit.Test;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.mapreduce.procedures.MockMapReduce;
import edu.brown.optimizer.BasePlanOptimizerTestCase;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ProjectType;

public class TestMapReduceProcedures extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.MAPREDUCE);
    }
    
    /**
     * testPlanner
     */
    @Test
    public void testPlanner() throws Exception {
        Procedure catalog_proc = this.getProcedure(MockMapReduce.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, catalog_proc.getMapinputquery());
        
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        System.err.println(PlanNodeUtil.debug(root));
        
        BasePlanOptimizerTestCase.validate(root);
        
    }
    
    
}
