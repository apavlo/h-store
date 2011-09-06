package org.voltdb.planner;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.NullValueExpression;
import org.voltdb.planner.PlanColumn.SortOrder;
import org.voltdb.planner.PlanColumn.Storage;

import edu.brown.expressions.ExpressionUtil;

/**
 * @author pavlo
 */
public class TestPlannerContext extends TestCase {

    private final PlannerContext context = PlannerContext.singleton();
    
    @Test
    public void testDuplicateColumns() {
        AbstractExpression expression = new NullValueExpression();
        String columnName = "TABLEA.A_ID";
        SortOrder sortOrder = SortOrder.kUnsorted;
        Storage storage = Storage.kTemporary;
        
        PlanColumn col0 = context.getPlanColumn(expression, columnName, sortOrder, storage);
        assertNotNull(col0);
//        System.err.println(col0 + " ==> " + col0.hashCode());
        
        PlanColumn col1 = context.getPlanColumn(expression, columnName, sortOrder, storage);
        assertNotNull(col1);
//        System.err.println(col1 + " ==> " + col1.hashCode());
        assertEquals(col0.hashCode(), col1.hashCode());
        assertEquals(col0.getDisplayName(), col1.getDisplayName());
        assertEquals(col0.getSortOrder(), col1.getSortOrder());
        assertEquals(col0.getStorage(), col1.getStorage());
        assert(ExpressionUtil.equals(col0.getExpression(), col1.getExpression()));
        assertEquals(col0, col1);
        
        expression.setLeft(new NullValueExpression());
        PlanColumn col2 = context.getPlanColumn(expression, columnName, sortOrder, storage);
        assertNotNull(col2);
//        System.err.println(col2 + " ==> " + col2.hashCode());
        assertNotSame(col0.hashCode(), col2.hashCode());
        assertEquals(col0.getDisplayName(), col2.getDisplayName());
        assertEquals(col0.getSortOrder(), col2.getSortOrder());
        assertEquals(col0.getStorage(), col2.getStorage());
        assert(ExpressionUtil.equals(col0.getExpression(), col2.getExpression()));
        assertFalse(col0.equals(col2));
        
    }
    
}
