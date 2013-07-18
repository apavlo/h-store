package edu.brown.plannodes;

import java.util.*;

import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.*;

import edu.brown.BaseTestCase;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 *
 */
public class TestPlanNodeUtil2 extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
    }
    
    /**
     * testIsRangeQuery
     */
    public void testIsRangeQuery() throws Exception {
        Statement stmts[] = null;
        
        // Ranges
        stmts = new Statement[] {
            this.getStatement(slev.class, "GetStockCount"),
            this.getStatement(delivery.class, "getNewOrder"),
        };
        for (Statement stmt : stmts) {
            AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(stmt, true);
            assertNotNull(stmt.fullName(), root);
            assertTrue(stmt.fullName(), PlanNodeUtil.isRangeQuery(root));
        } // FOR
        
        // Not ranges
        stmts = new Statement[] {
            this.getStatement(slev.class, "GetOId"),
            this.getStatement(delivery.class, "getCId"),
        };
        for (Statement stmt : stmts) {
            AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(stmt, true);
            assertNotNull(stmt.fullName(), root);
            assertFalse(stmt.fullName(), PlanNodeUtil.isRangeQuery(root));
        } // FOR
    }
    
    /**
     * testGetPlanNodeTreeForPlanFragment
     */
    public void testGetPlanNodeTreeForPlanFragment() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            assertNotNull(catalog_stmt);
            Set<PlanFragment> fragments = new HashSet<PlanFragment>();
            fragments.addAll(catalog_stmt.getFragments());
            fragments.addAll(catalog_stmt.getMs_fragments());
            for (PlanFragment catalog_frag : fragments) {
                AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
                assertNotNull(root);
            } // FOR
        } // FOR
    }
    
    /**
     * testGetSortedPlanFragments
     */
    public void testGetSortedPlanFragments() throws Exception {
        Procedure catalog_proc = this.getProcedure(slev.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetStockCount");
        
        List<PlanFragment> unsorted = Arrays.asList(catalog_stmt.getMs_fragments().values());
        assertEquals(catalog_stmt.getMs_fragments().size(), unsorted.size());
        Collections.shuffle(unsorted);
        
        List<PlanFragment> sorted = PlanNodeUtil.getSortedPlanFragments(catalog_stmt, false);
        assertNotNull(sorted);
        assertEquals(catalog_stmt.getMs_fragments().size(), sorted.size());
        
        Integer last_id = null;
        for (PlanFragment catalog_frag : sorted) {
            AbstractPlanNode root = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
            assertNotNull(root);
            int id = root.getPlanNodeId();
            if (last_id != null) assert(last_id > id) : "Unexpected execution order [" + last_id + " < " + id + "]";
            last_id = id;
        } // FOR
    }
    
    /**
     * testGetColumnForStmtParameterSelect
     */
    public void testGetColumnForStmtParameterSelect() throws Exception {
        Procedure catalog_proc = this.getProcedure("neworder");
        Statement catalog_stmt = this.getStatement(catalog_proc, "getWarehouseTaxRate");
        StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(0);
        assertNotNull(catalog_stmt_param);
        
        Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_stmt_param);
        assertNotNull(catalog_col);
        Column expected = this.getTable("WAREHOUSE").getColumns().get("W_ID");
        assertEquals(expected, catalog_col);
        
        // Make sure the cache works
        catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_stmt_param);
        assertEquals(expected, catalog_col);
    }

    /**
     * testGetColumnForStmtParameterUpdate
     */
    public void testGetColumnForStmtParameterUpdate() throws Exception {
        Procedure catalog_proc = this.getProcedure("neworder");
        Statement catalog_stmt = this.getStatement(catalog_proc, "incrementNextOrderId");
        StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(0);
        assertNotNull(catalog_stmt_param);
        
        Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_stmt_param);
        assertNotNull(catalog_col);
        Column expected = this.getTable("DISTRICT").getColumns().get("D_NEXT_O_ID");
        assertEquals(expected, catalog_col);
    }
    
    /**
     * testDeserializeMultiSiteStatement
     */
    public void testDeserializeMultiSiteStatement() throws Exception {
        Procedure catalog_proc = this.getProcedure("SelectAll");
        Statement catalog_stmt = this.getStatement(catalog_proc, "history");
        
        // Pass the Statement off to get deserialized
        // We will inspect it to make sure that it has at least one scan node and a result
        AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root_node);
        List<Class<? extends AbstractPlanNode>> expected = new ArrayList<Class<? extends AbstractPlanNode>>();
        expected.add(SendPlanNode.class);
        expected.add(ProjectionPlanNode.class);
        expected.add(ReceivePlanNode.class);
        expected.add(SendPlanNode.class);
        expected.add(SeqScanPlanNode.class);
        final List<Class<? extends AbstractPlanNode>> found = new ArrayList<Class<? extends AbstractPlanNode>>(); 
        
        // System.out.println(PlanNodeUtil.debug(root_node));
        new PlanNodeTreeWalker() {
            @Override
            protected void callback(AbstractPlanNode element) {
                // Nothing...
            }
            @Override
            protected void callback_before(AbstractPlanNode element) {
                found.add(element.getClass());
            }
        }.traverse(root_node);
        assertFalse(found.isEmpty());
        assertEquals(expected.size(), found.size());
        assertTrue(found.containsAll(expected));
    }
    
    /**
     * testDeserializeSingleSiteStatement
     */
    public void testDeserializeSingleSiteStatement() throws Exception {
        Procedure catalog_proc = this.getProcedure("SelectAll");
        Statement catalog_stmt = this.getStatement(catalog_proc, "history");
        
        // Pass the Statement off to get deserialized
        // We will inspect it to make sure that it has at least one scan node and a result
        AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root_node);
        List<Class<? extends AbstractPlanNode>> expected = new ArrayList<Class<? extends AbstractPlanNode>>();
        expected.add(SendPlanNode.class);
        expected.add(SeqScanPlanNode.class);
        final List<Class<? extends AbstractPlanNode>> found = new ArrayList<Class<? extends AbstractPlanNode>>(); 
        
        new PlanNodeTreeWalker() {
            @Override
            protected void callback(AbstractPlanNode element) {
                // Nothing...
            }
            @Override
            protected void callback_before(AbstractPlanNode element) {
                found.add(element.getClass());
            }
        }.traverse(root_node);
        assertFalse(found.isEmpty());
        assertEquals(expected.size(), found.size());
        assertTrue(found.containsAll(expected));
    }
    
    /**
     * testDeserializeSingleSiteStatement
     */
    public void testDeserializePlanFragment() throws Exception {
        Procedure catalog_proc = this.getProcedure("SelectAll");
        Statement catalog_stmt = this.getStatement(catalog_proc, "history");
        PlanFragment catalog_frag = CollectionUtil.first(catalog_stmt.getFragments());
        assertNotNull(catalog_frag);
        
        // Pass the Fragment off to get deserialized
        for (int i = 0; i < 4; i++) {
            AbstractPlanNode root_node = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
            assertNotNull(root_node);
        
            List<Class<? extends AbstractPlanNode>> expected = new ArrayList<Class<? extends AbstractPlanNode>>();
            expected.add(SendPlanNode.class);
            expected.add(SeqScanPlanNode.class);
            final List<Class<? extends AbstractPlanNode>> found = new ArrayList<Class<? extends AbstractPlanNode>>(); 
            
            new PlanNodeTreeWalker() {
                @Override
                protected void callback(AbstractPlanNode element) {
                    // Nothing...
                }
                @Override
                protected void callback_before(AbstractPlanNode element) {
                    found.add(element.getClass());
                }
            }.traverse(root_node);
            assertFalse(found.isEmpty());
            assertEquals(expected.size(), found.size());
            assertTrue(found.containsAll(expected));
        }
    }
    
}
