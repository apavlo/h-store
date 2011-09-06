package org.voltdb.planner;

import org.junit.Test;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.plannodes.PlanNodeUtil;

public class TestPlanOptimizer4 extends BasePlanOptimizerTestCase {

    AbstractProjectBuilder pb = new PlanOptimizerTestProjectBuilder("planopt4") {
        {
            this.addStmtProcedure("SingleSelect", "SELECT A_ID FROM TABLEA");
            this.addStmtProcedure("TwoTableJoin", "SELECT B_ID, B_A_ID, B_VALUE0, C_ID, C_VALUE0 " +
                                                  "  FROM TABLEB, TABLEC " +
                                                  " WHERE B_A_ID = ? AND B_ID = ? " +
                                                  "   AND B_A_ID = C_B_A_ID AND B_ID = C_B_ID  " +
                                                  " ORDER BY B_VALUE1 ASC LIMIT 25");
        }
    };

    
    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }

    /**
     * testSingleSelect
     */
    @Test
    public void testSingleSelect() throws Exception {   
        Procedure catalog_proc = this.getProcedure("SingleSelect");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        System.err.println(PlanNodeUtil.debug(root));
//        validateNodeColumnOffsets(root);
    }
     
    /**
     * testTwoTableJoin
     */
    @Test
    public void testTwoTableJoin() throws Exception {   
        Procedure catalog_proc = this.getProcedure("TwoTableJoin");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");

        // Grab the root node of the multi-partition query plan tree for this
        // Statement
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        System.err.println(PlanNodeUtil.debug(root));
//        validateNodeColumnOffsets(root);
    }
    
}
