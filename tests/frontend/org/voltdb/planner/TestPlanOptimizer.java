package org.voltdb.planner;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.regressionsuites.matviewprocs.AddThing;

import edu.brown.BaseTestCase;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.utils.CollectionUtil;

/**
 * 
 * @author sw47
 */
public class TestPlanOptimizer extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestPlanOptimizer.class);
    private VoltProjectBuilder pb = new VoltProjectBuilder("test-planopt") {
        {
            File schema = new File(TestPlanOptimizations2.class.getResource("testopt-ddl.sql").getFile());
            assert(schema.exists()) : "Schema: " + schema;
            this.addSchema(schema.getAbsolutePath());
            
            this.addPartitionInfo("TABLEA", "A_ID");
            this.addPartitionInfo("TABLEB", "B_A_ID");
            
            this.addStmtProcedure("SingleProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA WHERE TABLEA.A_ID = ?");
            //this.addStmtProcedure("JoinProjection", "SELECT TABLEA.A_VALUE0 FROM TABLEA, TABLEB WHERE TABLEA.A_ID = ? AND TABLEA.A_ID = TABLEB.B_A_ID");
        }
    };
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(pb);
    }
    
//    /**
//     * testpopulateTableNodeInfo
//     */
//    @Test
//    public void testpopulateTableNodeInfo() throws Exception {
//        PlannerContext m_context = PlannerContext.singleton(); // new PlannerContext();
//        Procedure catalog_proc = this.getProcedure("SingleProjection");
//        Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//        // Grab the root node of the multi-partition query plan tree for this Statement 
//        AbstractPlanNode rootNode = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//        PlanOptimizer plan_opt = new PlanOptimizer(m_context, catalog_db);
//        plan_opt.populateTableNodeInfo(rootNode);
//        // check two hashmaps contain what we expect
//        checkPlanNodeColumns(plan_opt.planNodeColumns);
//        checkTableColumns(plan_opt.tableColumns);
//   }
    /**
     * Check the columns A_ID and A_VALUE0 are there in in that particular order +
     * they both belong to Table A.
     * @param tableCols
     */
    private void checkTableColumns(Map<Table, SortedSet<Column>> tableCols) {
       assert(tableCols.size() == 1) : " ERROR: more than 1 entry in tableCols";
       Table t = CollectionUtil.getFirst(tableCols.keySet());
       assert(t != null);
       assert(t.getName().equals("TABLEA")) : "table " + t.getName() + " does not match TABLEA";
       Set<Column> cols = CollectionUtil.getFirst(tableCols.values());
       assert(cols.size() == 2) : "Size: " + cols.size() + " doesn't match 2";
       assert(((Column)cols.toArray()[0]).getName().equals("A_ID")) : " first column: " + ((Column)cols.toArray()[0]).getName() + " doesn't match: " + " A_ID";
       assert(((Column)cols.toArray()[1]).getName().equals("A_VALUE0")) : "second column: " + ((Column)cols.toArray()[1]).getName() + " doesn't match: " + " A_VALUE0";
   }
   private void checkPlanNodeColumns(Map<AbstractPlanNode, Set<Column>> planNodeCols) {
       LOG.debug("planNodeCols: " + planNodeCols);
   }

//   /**
//    * Checks a column gets properly added to tableColumns in the correct order
//    * @throws Exception
//    */
//   public void testaddTableColumn() throws Exception {
//       PlannerContext m_context = PlannerContext.singleton(); // new PlannerContext();
//       Procedure catalog_proc = this.getProcedure("SingleProjection");
//       Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
//       // Grab the root node of the multi-partition query plan tree for this Statement 
//       AbstractPlanNode rootNode = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
//       PlanOptimizer plan_opt = new PlanOptimizer(m_context, catalog_db);
//       plan_opt.populateTableNodeInfo(rootNode);
//       // check two hashmaps contain what we expect
//       checkPlanNodeColumns(plan_opt.planNodeColumns);
//       checkTableColumns(plan_opt.tableColumns);
//       // add column A_VALUE5
//       plan_opt.addTableColumn(catalog_db.getTables().get("TABLEA").getColumns().get("A_VALUE5"));
//       assert(plan_opt.tableColumns.size() == 1) : " ERROR: more than 1 entry in tableCols";
//       Table t = CollectionUtil.getFirst(plan_opt.tableColumns.keySet());
//       assert(t != null);
//       assert(t.getName().equals("TABLEA")) : "table " + t.getName() + " does not match TABLEA";
//       Set<Column> cols = CollectionUtil.getFirst(plan_opt.tableColumns.values());
//       assert(cols.size() == 3) : "Size: " + cols.size() + " doesn't match 3";
//       assert(((Column)cols.toArray()[0]).getName().equals("A_ID")) : " first column: " + ((Column)cols.toArray()[0]).getName() + " doesn't match: " + " A_ID";
//       assert(((Column)cols.toArray()[1]).getName().equals("A_VALUE0")) : "second column: " + ((Column)cols.toArray()[1]).getName() + " doesn't match: " + " A_VALUE0";
//       assert(((Column)cols.toArray()[2]).getName().equals("A_VALUE5")) : "third column: " + ((Column)cols.toArray()[2]).getName() + " doesn't match: " + " A_VALUE5";
//       // add column A_VALUE3
//       plan_opt.addTableColumn(catalog_db.getTables().get("TABLEA").getColumns().get("A_VALUE3"));
//       // expect the order to be A_ID, A_VALUE0, A_VALUE3, A_VALUE5
//       Table t2 = CollectionUtil.getFirst(plan_opt.tableColumns.keySet());
//       assert(t2 != null);
//       assert(t2.getName().equals("TABLEA")) : "table " + t2.getName() + " does not match TABLEA";
//       Set<Column> cols2 = CollectionUtil.getFirst(plan_opt.tableColumns.values());
//       assert(cols2.size() == 4) : "Size: " + cols2.size() + " doesn't match 4";
//       assert(((Column)cols2.toArray()[0]).getName().equals("A_ID")) : " first column: " + ((Column)cols2.toArray()[0]).getName() + " doesn't match: " + " A_ID";
//       assert(((Column)cols2.toArray()[1]).getName().equals("A_VALUE0")) : "second column: " + ((Column)cols2.toArray()[1]).getName() + " doesn't match: " + " A_VALUE0";
//       assert(((Column)cols2.toArray()[2]).getName().equals("A_VALUE3")) : "third column: " + ((Column)cols2.toArray()[2]).getName() + " doesn't match: " + " A_VALUE3";
//       assert(((Column)cols2.toArray()[3]).getName().equals("A_VALUE5")) : "third column: " + ((Column)cols2.toArray()[3]).getName() + " doesn't match: " + " A_VALUE5";
//   }
   
   /**
    * Checks a column gets properly added to tableColumns in the correct order
    * @throws Exception
    */
   public void testextractColumnInfo() throws Exception {
       PlannerContext m_context = PlannerContext.singleton(); // new PlannerContext();
       Procedure catalog_proc = this.getProcedure("SingleProjection");
       Statement catalog_stmt = this.getStatement(catalog_proc, "sql");
       // Grab the root node of the multi-partition query plan tree for this Statement 
       AbstractPlanNode rootNode = QueryPlanUtil.deserializeStatement(catalog_stmt, false);
       final PlanOptimizer plan_opt = new PlanOptimizer(m_context, catalog_db);
       plan_opt.populateTableNodeInfo(rootNode);
       // check two hashmaps contain what we expect
       //checkPlanNodeColumns(plan_opt.planNodeColumns);
       checkTableColumns(plan_opt.tableColumns);
       System.out.println("Root Node: " + rootNode + " output columns: " + rootNode.m_outputColumns + " child type: " + rootNode.getChild(0).getPlanNodeType());
       // walk the tree and call extractColumnInfo on a rootNode
       new PlanNodeTreeWalker(true) {
           @Override
           protected void callback(AbstractPlanNode element) {
//             if (trace) LOG.trace(PlanNodeUtil.debugNode(element));
               // call function to build the data structure that maps all nodes to the columns they affect
               try {
                   if (this.getDepth() != 0) {
                       plan_opt.extractColumnInfo(element, false);
                       this.stop();
                   }
               } catch (Exception ex) {
                   // LOG.fatal(PlanNodeUtil.debug(rootNode));
                   LOG.fatal("Failed to extract column information for " + element, ex);
                   System.exit(1);
               }
           }
       }.traverse(rootNode);
       
       //plan_opt.propagateProjections(rootNode);
       // check planNodeColumns + tableColumns
       LOG.debug("planNodeColumns: " + plan_opt.planNodeColumns);
       LOG.debug("tableColumns: " + plan_opt.tableColumns);
       LOG.debug("column_guid_xref: " + plan_opt.column_guid_xref);
       LOG.debug("guid_column_xref: " + plan_opt.guid_column_xref);
       LOG.debug("orig_node_output: " + plan_opt.orig_node_output);
   }

}