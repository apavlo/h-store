package edu.brown.utils;

import java.util.*;

import org.junit.Test;
import org.voltdb.catalog.*;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.*;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetTableCounts;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.plannodes.PlanNodeUtil;

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

        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
        assertNotNull(root);
        Set<ExpressionType> result = PlanNodeUtil.getScanExpressionTypes(catalog_db, root);
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
        Procedure catalog_proc = this.getProcedure(UpdateLocation.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "update");
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column expected[] = { 
            this.getColumn(catalog_tbl, "VLR_LOCATION")
        };
        
        AbstractPlanNode root = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
        assertNotNull(root);
        IndexScanPlanNode idx_node = CollectionUtil.getFirst(PlanNodeUtil.getPlanNodes(root, IndexScanPlanNode.class));
        assertNotNull(idx_node);
        
        Set<Column> columns = PlanNodeUtil.getOutputColumns(catalog_db, idx_node);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
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
        
        Set<SeqScanPlanNode> found0 = PlanNodeUtil.getPlanNodes(root_node, SeqScanPlanNode.class);
        assertFalse(found0.isEmpty());
        
        Set<AbstractScanPlanNode> found1 = PlanNodeUtil.getPlanNodes(root_node, AbstractScanPlanNode.class);
        assertFalse(found1.isEmpty());
    }
    
    /**
     * testGetTableReferences
     */
    public void testGetTableReferences() throws Exception {
        AbstractPlanNode root_node = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
        assertNotNull(root_node);
        
        Set<AbstractPlanNode> found = PlanNodeUtil.getNodesReferencingTable(root_node, catalog_tbl);
        assertEquals(1, found.size());
        AbstractPlanNode node = CollectionUtil.getFirst(found);
        assertNotNull(node);
        assertTrue(node instanceof AbstractScanPlanNode);
    }
    
}