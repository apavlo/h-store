package edu.brown.plannodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.GetTableCounts;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestPlanNodeUtil extends BaseTestCase {

    protected Procedure catalog_proc;
    protected Statement catalog_stmt;
    protected Table catalog_tbl;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        catalog_proc = this.getProcedure(GetTableCounts.class);
        
        catalog_stmt = catalog_proc.getStatements().get("AccessInfoCount");
        assertNotNull(catalog_stmt);

        catalog_tbl = catalog_db.getTables().get(TM1Constants.TABLENAME_ACCESS_INFO);
        assertNotNull(catalog_tbl);
    }
    
    /**
     * testClone
     */
    @Test
    public void testClone() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetData");
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        
        AbstractPlanNode clone = (AbstractPlanNode)root.clone();
        assertNotNull(clone);
//        assertEquals(root, clone);
        assertFalse(root == clone);
        
        List<AbstractPlanNode> list0 = new PlanNodeList(root).getExecutionList();
        List<AbstractPlanNode> list1 = new PlanNodeList(clone).getExecutionList();
        assertEquals(list0.size(), list1.size());
        
        for (int i = 0, cnt = list0.size(); i < cnt; i++) {
            AbstractPlanNode node0 = list0.get(i);
            assertNotNull(node0);
            AbstractPlanNode node1 = list1.get(i);
            assertNotNull(node1);
            
            // Compare!
            assertFalse(node0 == node1);
            assertEquals(node0.getChildPlanNodeCount(), node1.getChildPlanNodeCount());
            assertEquals(node0.getInlinePlanNodeCount(), node1.getInlinePlanNodeCount());
            assertEquals(node0.getOutputColumnGUIDCount(), node1.getOutputColumnGUIDCount());
            
            List<AbstractExpression> exps0 = new ArrayList<AbstractExpression>(PlanNodeUtil.getExpressionsForPlanNode(node0));
            List<AbstractExpression> exps1 = new ArrayList<AbstractExpression>(PlanNodeUtil.getExpressionsForPlanNode(node1));
            assertEquals(exps0.size(), exps1.size());
            for (int j = 0; j < exps0.size(); j++) {
                AbstractExpression exp0 = exps0.get(j);
                assertNotNull(exp0);
                AbstractExpression exp1 = exps1.get(j);
                assertNotNull(exp1);
//                assertFalse(exp0 == exp1);
                assertEquals(exp0, exp1);
            } // FOR (exps)
        } // FOR (nodes)
    }
    
    /**
     * testGetScanExpressionTypes
     */
    @Test
    public void testGetScanExpressionTypes() throws Exception {
        ExpressionType expected[] = {
            ExpressionType.COMPARE_EQUAL,
            ExpressionType.VALUE_PARAMETER,
            ExpressionType.VALUE_TUPLE,
        };
        
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "update");

        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        Collection<ExpressionType> result = PlanNodeUtil.getScanExpressionTypes(catalog_db, root);
        assertNotNull(result);
        assertFalse(result.isEmpty());
        for (ExpressionType e : expected) {
            assert(result.contains(e)) : "Missing " + e;
        }
    }
    
    /**
     * testGetOutputColumns
     */
    @Test
    public void testGetOutputColumns() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetTableCounts.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "CallForwardingCount");
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING);
        Column expected[] = { 
            this.getColumn(catalog_tbl, "S_ID")
        };
        
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root);
        
        Collection<Column> columns = PlanNodeUtil.getOutputColumnsForPlanNode(catalog_db, root.getChild(0));
        assertNotNull(columns);
//        System.err.print(catalog_stmt.fullName() + ": " + CatalogUtil.debug(columns));
//        System.err.println(PlanNodeUtil.debug(root));
        assertEquals(catalog_stmt.fullName(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert(columns.contains(expected[i])) : "Missing column " + CatalogUtil.getDisplayName(expected[i]);
        } // FOR

    }
    
    /**
     * testGetOutputColumns
     */
    @Test
    public void testGetUpdatedColumns() throws Exception {
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "update");
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column expected[] = { 
            this.getColumn(catalog_tbl, "VLR_LOCATION")
        };
        
        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        assertNotNull(root);
        IndexScanPlanNode idx_node = CollectionUtil.first(PlanNodeUtil.getPlanNodes(root, IndexScanPlanNode.class));
        assertNotNull(idx_node);
        
        Collection<Column> columns = PlanNodeUtil.getUpdatedColumnsForPlanNode(catalog_db, idx_node);
        assertNotNull(columns);
//        System.err.print(catalog_stmt.fullName() + ": " + CatalogUtil.debug(columns));
//        System.err.println(PlanNodeUtil.debug(root));
        assertEquals(catalog_stmt.fullName(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert(columns.contains(expected[i])) : "Missing column " + CatalogUtil.getDisplayName(expected[i]);
        } // FOR
    }
    
    /**
     * testGetPlanNodes
     */
    public void testGetPlanNodes() throws Exception {
        PlannerContext cntxt = new PlannerContext();
        AbstractPlanNode root_node = new ProjectionPlanNode(cntxt, 1);
        root_node.addAndLinkChild(new SeqScanPlanNode(cntxt, 2));
        
        Collection<SeqScanPlanNode> found0 = PlanNodeUtil.getPlanNodes(root_node, SeqScanPlanNode.class);
        assertFalse(found0.isEmpty());
        
        Collection<AbstractScanPlanNode> found1 = PlanNodeUtil.getPlanNodes(root_node, AbstractScanPlanNode.class);
        assertFalse(found1.isEmpty());
    }
    
    /**
     * testGetTableReferences
     */
    public void testGetTableReferences() throws Exception {
        AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root_node);
        
        Collection<AbstractPlanNode> found = PlanNodeUtil.getPlanNodesReferencingTable(root_node, catalog_tbl);
        assertEquals(1, found.size());
        AbstractPlanNode node = CollectionUtil.first(found);
        assertNotNull(node);
        assertTrue(node instanceof AbstractScanPlanNode);
    }
    
}