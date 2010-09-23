package edu.brown.catalog;

import java.util.*;

import org.voltdb.catalog.*;
import org.voltdb.plannodes.*;

import edu.brown.BaseTestCase;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 *
 */
public class TestQueryPlanUtil extends BaseTestCase {

    protected static final Set<String> CHECK_FIELDS_EXCLUDE = new HashSet<String>();
    static {
        CHECK_FIELDS_EXCLUDE.add("procparameter");
        CHECK_FIELDS_EXCLUDE.add("partitioncolumn");
    };
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
    }
    
    /**
     * testGetSortedPlanFragments
     */
    public void testGetSortedPlanFragments() throws Exception {
        Procedure catalog_proc = this.getProcedure("slev");
        Statement catalog_stmt = catalog_proc.getStatements().get("GetStockCount");
        assertNotNull(catalog_stmt);
        
        List<PlanFragment> unsorted = Arrays.asList(catalog_stmt.getMs_fragments().values());
        assertEquals(catalog_stmt.getMs_fragments().size(), unsorted.size());
        Collections.shuffle(unsorted);
        
        List<PlanFragment> sorted = QueryPlanUtil.getSortedPlanFragments(unsorted);
        assertNotNull(sorted);
        assertEquals(catalog_stmt.getMs_fragments().size(), sorted.size());
        
        Integer last_id = null;
        for (PlanFragment catalog_frag : sorted) {
            AbstractPlanNode root = QueryPlanUtil.deserializePlanFragment(catalog_frag);
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
        Statement catalog_stmt = catalog_proc.getStatements().get("getWarehouseTaxRate");
        assertNotNull(catalog_stmt);
        StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(0);
        assertNotNull(catalog_stmt_param);
        
        Column catalog_col = QueryPlanUtil.getColumnForStmtParameter(catalog_stmt_param);
        assertNotNull(catalog_col);
        Column expected = this.getTable("WAREHOUSE").getColumns().get("W_ID");
        assertEquals(expected, catalog_col);
        
        // Make sure the cache works
        catalog_col = QueryPlanUtil.getColumnForStmtParameter(catalog_stmt_param);
        assertEquals(expected, catalog_col);
    }

    /**
     * testGetColumnForStmtParameterUpdate
     */
    public void testGetColumnForStmtParameterUpdate() throws Exception {
        Procedure catalog_proc = this.getProcedure("neworder");
        Statement catalog_stmt = catalog_proc.getStatements().get("incrementNextOrderId");
        assertNotNull(catalog_stmt);
        StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(0);
        assertNotNull(catalog_stmt_param);
        
        Column catalog_col = QueryPlanUtil.getColumnForStmtParameter(catalog_stmt_param);
        assertNotNull(catalog_col);
        Column expected = this.getTable("DISTRICT").getColumns().get("D_NEXT_O_ID");
        assertEquals(expected, catalog_col);
    }

    
    /**
     * testDeserializeMultiSiteStatement
     */
    public void testDeserializeMultiSiteStatement() throws Exception {
        Procedure catalog_proc = catalog_db.getProcedures().get("SelectAll");
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get("history");
        assertNotNull(catalog_stmt);
        
        // Pass the Statement off to get deserialized
        // We will inspect it to make sure that it has at least one scan node and a result
        AbstractPlanNode root_node = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
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
        Procedure catalog_proc = catalog_db.getProcedures().get("SelectAll");
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get("history");
        assertNotNull(catalog_stmt);
        
        //
        // Pass the Statement off to get deserialized
        // We will inspect it to make sure that it has at least one scan node and a result
        //
        AbstractPlanNode root_node = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
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
