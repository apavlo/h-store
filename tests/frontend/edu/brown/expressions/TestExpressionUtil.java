package edu.brown.expressions;

import org.voltdb.catalog.*;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.*;

import edu.brown.BaseTestCase;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestExpressionUtil extends BaseTestCase {

    public static final String TARGET_PROC = "DeleteCallForwarding";
    public static final String TARGET_STMT = "query";
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }
    
    /**
     * testDebug
     */
    public void testDebug() throws Exception {
        // Just make sure this doesn't throw an Exception
        Procedure catalog_proc = this.getProcedure(TARGET_PROC);
        Statement catalog_stmt = catalog_proc.getStatements().get(TARGET_STMT);
        assertNotNull(catalog_stmt);
        
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
        assertNotNull(root);
        //System.err.println(PlanNodeUtil.debug(root));
        IndexScanPlanNode scan_node = CollectionUtil.getFirst(PlanNodeUtil.getPlanNodes(root, IndexScanPlanNode.class));
        assertNotNull(scan_node);
        
        AbstractExpression exp = scan_node.getEndExpression();
        assertNotNull(exp);
        String debug = ExpressionUtil.debug(exp);
        assertNotNull(debug);
        assertFalse(debug.isEmpty());
    }
    
    
    
}
