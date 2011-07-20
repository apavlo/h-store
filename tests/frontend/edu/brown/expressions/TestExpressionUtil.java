package edu.brown.expressions;

import java.util.Set;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.catalog.*;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.NullValueExpression;
import org.voltdb.plannodes.*;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestExpressionUtil extends BaseTestCase {

    public static final String TARGET_PROC = DeleteCallForwarding.class.getSimpleName();
    public static final String TARGET_STMT = "query";

    private Procedure catalog_proc;
    private Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.catalog_proc = this.getProcedure(TARGET_PROC);
        this.catalog_stmt = this.getStatement(this.catalog_proc, TARGET_STMT);
    }
    
    /**
     * testEquals
     */
    @Test
    public void testEquals() throws Exception {
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        assertNotNull(root);
        
        Set<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, AbstractScanPlanNode.class);
        assertNotNull(scan_nodes);
        assertEquals(1, scan_nodes.size());
        AbstractScanPlanNode scan_node = CollectionUtil.getFirst(scan_nodes);
        assertNotNull(scan_node);
        
        Set<AbstractExpression> exps = PlanNodeUtil.getExpressions(scan_node);
        assertNotNull(exps);
        assertFalse(exps.isEmpty());
        AbstractExpression exp = CollectionUtil.getFirst(exps);
        assertNotNull(exp);
        
        // Clone the mofo and make sure equals() returns true!
        String json = exp.toJSONString();
        assertFalse(json.isEmpty());
        AbstractExpression clone = AbstractExpression.fromJSONObject(new JSONObject(json), catalog_db);
        assertNotNull(clone);
        assert(ExpressionUtil.equals(exp, clone));

        // Change one of the branches. This should now return false!
        clone.setRight(new NullValueExpression());
        assertFalse(ExpressionUtil.equals(exp, clone));
        
        // Remove both the branch on both sids. This should return true!
        exp.setRight(null);
        clone.setRight(null);
        assert(ExpressionUtil.equals(exp, clone));
    }
    
    /**
     * testDebug
     */
    public void testDebug() throws Exception {
        // Just make sure this doesn't throw an Exception
        AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
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
