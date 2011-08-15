package org.voltdb.planner;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestVerticalPartitionPlanner extends BaseTestCase {

    static final int NUM_PARTITIONS = 10;
    
    static final String REWRITE_SQLS[] = new String[]{
        "SELECT S.S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " AS S WHERE S.SUB_NBR = ?",
        "SELECT " + TM1Constants.TABLENAME_SUBSCRIBER + ".S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " WHERE " + TM1Constants.TABLENAME_SUBSCRIBER + ".SUB_NBR = ?",
        "SELECT " + TM1Constants.TABLENAME_SUBSCRIBER + "2.S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " AS " + TM1Constants.TABLENAME_SUBSCRIBER + "2  WHERE " + TM1Constants.TABLENAME_SUBSCRIBER + "2.SUB_NBR = ?",
//        "SELECT " + TM1Constants.TABLENAME_SUBSCRIBER + ".S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " AS S WHERE S.SUB_NBR = ?",
//        "SELECT " + TM1Constants.TABLENAME_SUBSCRIBER + ".S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " AS S WHERE S.SUB_NBR = ?",
    };
    
    static final Map<String, String> SQLS = new HashMap<String, String>();
    static {
        SQLS.put("TestSingleTable", "SELECT S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " WHERE SUB_NBR = ?");
        SQLS.put("TestInvalid", "SELECT S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " WHERE VLR_LOCATION = ?");
        for (int i = 0; i < REWRITE_SQLS.length; i++) {
            SQLS.put(String.format("TestRewrite%d", i), REWRITE_SQLS[i]);
        } // FOR
    }
    
    
    private class MockProjectBuilder extends AbstractProjectBuilder {

        public MockProjectBuilder() {
            super("tm1-vp", MockProjectBuilder.class, null, null);
            File schema = getDDLPath(ProjectType.TM1);
            assert (schema.exists()) : "Schema: " + schema;
            this.addSchema(schema.getAbsolutePath());

            // Partition everything on S_ID
            for (String tableName : TM1Constants.TABLENAMES) {
                this.addPartitionInfo(tableName, "S_ID");
            } // FOR
            
            // Add Vertical Partition
            this.addVerticalPartitionInfo(TM1Constants.TABLENAME_SUBSCRIBER, "S_ID", "SUB_NBR");
            
            // Single Query Procedures
            for (Entry<String, String> e : SQLS.entrySet()) {
                this.addStmtProcedure(e.getKey(), e.getValue());
            } // FOR
            
            this.setEnableVerticalPartitionOptimizations(false);
        }
    }
    
    VerticalPartitionPlanner vp_planner; 
    
    @Override
    protected void setUp() throws Exception {
        MockProjectBuilder pb = new MockProjectBuilder();
        super.setUp(pb);
        this.addPartitions(NUM_PARTITIONS);
        assert(catalog_db.getProcedures().size() > 0);
        vp_planner = new VerticalPartitionPlanner(catalog_db);
    }
    
    /**
     * testInvalidQuery
     */
    public void testInvalidQuery() throws Exception {
        // Check to make sure that we don't try to update a query when we don't have
        // any vertical partitions defined
        Procedure catalog_proc = this.getProcedure("TestInvalid");
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        boolean ret = vp_planner.process(catalog_stmt);
        assertFalse(ret);
    }
    
    /**
     * testRewriteSQL
     */
    public void testRewriteSQL() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER); 
        Table view_tbl = CatalogUtil.getVerticalPartition(catalog_tbl).getDest();
        Map<Table, Table> m = new HashMap<Table, Table>();
        m.put(catalog_tbl, view_tbl);
        
        for (int i = 0; i < REWRITE_SQLS.length; i++) {
            String procName = String.format("TestRewrite%d", i);
            Procedure catalog_proc = this.getProcedure(procName);
            Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        
            String orig_sql = catalog_stmt.getSqltext();
            String new_sql = vp_planner.rewriteSQL(catalog_stmt, m);
            assertNotNull(new_sql);
            assertFalse(new_sql.isEmpty());
            System.err.println(String.format("BEFORE: %s\n" +
                                             "AFTER:  %s\n\n",
                                             orig_sql, new_sql));
            assertFalse(orig_sql.equals(new_sql));
        } // FOR
    }
    
    /**
     * testSingleColumnQuery
     */
    public void testSingleColumnQuery() throws Exception {
        Procedure catalog_proc = this.getProcedure("TestSingleTable");
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        Object params[] = new Object[]{ "ABC" };
        
        Integer base_partition = new Random().nextInt(NUM_PARTITIONS);
        
        // Double check that this is always a distributed query
        AbstractPlanNode orig = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, false);
        assertNotNull(orig);
        Set<Integer> orig_partitions = p_estimator.getAllPartitions(catalog_stmt, params, base_partition);
        assertNotNull(orig_partitions);
        assertEquals(NUM_PARTITIONS, orig_partitions.size());
        
        // Now create the optimized query plan that uses the vertical partition
        boolean ret = vp_planner.optimizeStatement(catalog_stmt);
        assert(ret);
        p_estimator.clear();
        Set<Integer> new_partitions = p_estimator.getAllPartitions(catalog_stmt, params, base_partition);
        assertNotNull(new_partitions);
//        System.err.println("NEW PARTITIONS: " + new_partitions);
        assertEquals(1, new_partitions.size());
        assertEquals(base_partition, CollectionUtil.first(new_partitions));
    }
    
    /**
     * testOptimizeDatabase
     */
    public void testOptimizeDatabase() throws Exception {
        Collection<Statement> updated = vp_planner.optimizeDatabase();
        assertNotNull(updated);
        assertFalse(updated.isEmpty());

        for (Statement catalog_stmt : updated) {
            System.err.println(catalog_stmt.fullName());
        }
        assertEquals(REWRITE_SQLS.length + 1, updated.size());
    }
    
}
