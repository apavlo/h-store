package edu.brown.optimizer;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;

/**
 * @author sw47
 */
public class TestPlanOptimizerUtil extends BasePlanOptimizerTestCase {

    AbstractProjectBuilder pb = new PlanOptimizerTestProjectBuilder("planopt1") {
        {
            this.addStmtProcedure("SingleProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
            // this.addStmtProcedure("JoinProjection",
            // "SELECT TABLEA.A_VALUE0 FROM TABLEA, TABLEB WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
        }
    };

    PlanOptimizerState state;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
        this.state = new PlanOptimizerState(catalog_db, PlannerContext.singleton());
    }

    /**
     * Check the columns A_ID and A_VALUE0 are there in in that particular order
     * + they both belong to Table A.
     * 
     * @param tableCols
     */
    private void checkTableColumns(Map<Table, SortedSet<Column>> tableCols) {
        assert (tableCols.size() == 1) : " ERROR: more than 1 entry in tableCols";
        Table t = CollectionUtil.first(tableCols.keySet());
        assert (t != null);
        assert (t.getName().equals("TABLEA")) : "table " + t.getName() + " does not match TABLEA";
        Set<Column> cols = CollectionUtil.first(tableCols.values());
        assert (cols.size() == 2) : "Size: " + cols.size() + " doesn't match 2";
        assert (((Column) cols.toArray()[0]).getName().equals("A_ID")) : " first column: " + ((Column) cols.toArray()[0]).getName() + " doesn't match: " + " A_ID";
        assert (((Column) cols.toArray()[1]).getName().equals("A_VALUE0")) : "second column: " + ((Column) cols.toArray()[1]).getName() + " doesn't match: " + " A_VALUE0";
    }

    private void checkPlanNodeColumns(Map<AbstractPlanNode, Set<Column>> planNodeCols) {
        System.out.println("planNodeCols: " + planNodeCols);
    }

    /**
     * Checks a column gets properly added to tableColumns in the correct order
     */
    public void testAddTableColumn() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        
        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode rootNode = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        PlanOptimizerUtil.populateTableNodeInfo(state, rootNode);
        
        // check two hashmaps contain what we expect
        checkPlanNodeColumns(state.planNodeColumns);
        checkTableColumns(state.tableColumns);
        
        // add column A_VALUE5
        state.addTableColumn(catalog_db.getTables().get("TABLEA").getColumns().get("A_VALUE5"));
        assert (state.tableColumns.size() == 1) : " ERROR: more than 1 entry in tableCols";
        Table t = CollectionUtil.first(state.tableColumns.keySet());
        assert (t != null);
        assert (t.getName().equals("TABLEA")) : "table " + t.getName() + " does not match TABLEA";
        Set<Column> cols = CollectionUtil.first(state.tableColumns.values());
        assert (cols.size() == 3) : "Size: " + cols.size() + " doesn't match 3";
        assert (((Column) cols.toArray()[0]).getName().equals("A_ID")) : " first column: " + ((Column) cols.toArray()[0]).getName() + " doesn't match: " + " A_ID";
        assert (((Column) cols.toArray()[1]).getName().equals("A_VALUE0")) : "second column: " + ((Column) cols.toArray()[1]).getName() + " doesn't match: " + " A_VALUE0";
        assert (((Column) cols.toArray()[2]).getName().equals("A_VALUE5")) : "third column: " + ((Column) cols.toArray()[2]).getName() + " doesn't match: " + " A_VALUE5";
        
        // add column A_VALUE3
        state.addTableColumn(catalog_db.getTables().get("TABLEA").getColumns().get("A_VALUE3"));
        
        // expect the order to be A_ID, A_VALUE0, A_VALUE3, A_VALUE5
        Table t2 = CollectionUtil.first(state.tableColumns.keySet());
        assert (t2 != null);
        assert (t2.getName().equals("TABLEA")) : "table " + t2.getName() + " does not match TABLEA";
        Set<Column> cols2 = CollectionUtil.first(state.tableColumns.values());
        assert (cols2.size() == 4) : "Size: " + cols2.size() + " doesn't match 4";
        assert (((Column) cols2.toArray()[0]).getName().equals("A_ID")) : " first column: " + ((Column) cols2.toArray()[0]).getName() + " doesn't match: " + " A_ID";
        assert (((Column) cols2.toArray()[1]).getName().equals("A_VALUE0")) : "second column: " + ((Column) cols2.toArray()[1]).getName() + " doesn't match: " + " A_VALUE0";
        assert (((Column) cols2.toArray()[2]).getName().equals("A_VALUE3")) : "third column: " + ((Column) cols2.toArray()[2]).getName() + " doesn't match: " + " A_VALUE3";
        assert (((Column) cols2.toArray()[3]).getName().equals("A_VALUE5")) : "third column: " + ((Column) cols2.toArray()[3]).getName() + " doesn't match: " + " A_VALUE5";
    }

    /**
     * testPopulateTableNodeInfo
     */
    public void testPopulateTableNodeInfo() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        
        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode rootNode = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        PlanOptimizerUtil.populateTableNodeInfo(state, rootNode);
        
        // check two hashmaps contain what we expect
        checkPlanNodeColumns(state.planNodeColumns);
        checkTableColumns(state.tableColumns);
    }

    /**
     * Checks a column gets properly added to tableColumns in the correct order
     */
    public void testExtractColumnInfo() throws Exception {
        Procedure catalog_proc = this.getProcedure("SingleProjection");
        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
        
        // Grab the root node of the multi-partition query plan tree for this Statement
        AbstractPlanNode rootNode = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        PlanOptimizerUtil.populateTableNodeInfo(state, rootNode);
        
        // check two hashmaps contain what we expect
        checkPlanNodeColumns(state.planNodeColumns);
        checkTableColumns(state.tableColumns);
        System.out.println("Root Node: " + rootNode + " output columns: " + rootNode.getOutputColumnGUIDs() + " child type: " + rootNode.getChild(0).getPlanNodeType());
        // walk the tree and call extractColumnInfo on a rootNode
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // if (trace) LOG.trace(PlanNodeUtil.debugNode(element));
                // call function to build the data structure that maps all nodes
                // to the columns they affect
                try {
                    if (this.getDepth() != 0) {
                        PlanOptimizerUtil.extractColumnInfo(state, element, false);
                        this.stop();
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Failed to extract column information for " + element, ex);
                }
            }
        }.traverse(rootNode);

        // plan_opt.propagateProjections(rootNode);
        // check planNodeColumns + tableColumns
        System.out.println("planNodeColumns: " + state.planNodeColumns);
        System.out.println("tableColumns: " + state.tableColumns);
        System.out.println("column_guid_xref: " + state.column_guid_xref);
        System.out.println("guid_column_xref: " + state.guid_column_xref);
        System.out.println("orig_node_output: " + state.orig_node_output);
    }
    
    
    /**
     * testExtractColumnInfo
     */
//    @Test
//    public void testExtractColumnInfo() throws Exception {
//        Procedure catalog_proc = this.getProcedure("DistinctCount");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//        
//        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
//        assertNotNull(root);
//        PlanOptimizerState state = new PlanOptimizerState(catalog_db, PlannerContext.singleton());
//        
//        Collection<SeqScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root, SeqScanPlanNode.class);
//        SeqScanPlanNode scan_node = CollectionUtil.first(scan_nodes);
//        assertNotNull(scan_node);
//        
//        // Use the PlanOptimizerUtil to compute the referenced columns for this node
//        PlanOptimizerUtil.populateTableNodeInfo(state, root);
//        
//        // Then make sure it has the right references
//        Table catalog_tbl = this.getTable(scan_node.getTargetTableName());
//        Set<Column> expected = new HashSet<Column>();
//        for (Integer guid : scan_node.getOutputColumnGUIDs()) {
//            PlanColumn pc = state.plannerContext.get(guid);
//            assertNotNull(pc);
//            
//            Column catalog_col = catalog_tbl.getColumns().getIgnoreCase(pc.getDisplayName());
//            assertNotNull(pc.toString(), catalog_col);
//            expected.add(catalog_col);
//        } // FOR
//        
//        Collection<Column> actual = state.getPlanNodeColumns(scan_node);
//        assertNotNull(actual);
//        assertEquals(expected.size(), actual.size());
//        assertTrue(expected.containsAll(actual));
//    }

}