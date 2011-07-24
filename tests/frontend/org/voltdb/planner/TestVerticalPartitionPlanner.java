package org.voltdb.planner;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestVerticalPartitionPlanner extends BaseTestCase {

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
            this.addStmtProcedure("Test1", "SELECT S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " WHERE SUB_NBR = ?");
            this.addStmtProcedure("Test2", "SELECT S_ID FROM " + TM1Constants.TABLENAME_SUBSCRIBER + " WHERE VLR_LOCATION = ?");
            
        }
    }
    
    MockProjectBuilder pb;
    
    @Override
    protected void setUp() throws Exception {
        pb = new MockProjectBuilder();
        super.setUp(pb);
        assert(catalog_db.getProcedures().size() > 0);
    }
    
    /**
     * testInvalidQuery
     */
    public void testInvalidQuery() throws Exception {
        // Check to make sure that we don't try to update a query when we don't have
        // any vertical partitions defined
        Procedure catalog_proc = this.getProcedure("Test2");
        Statement catalog_stmt = CollectionUtil.getFirst(catalog_proc.getStatements());
        VerticalPartitionPlanner vp_planner = new VerticalPartitionPlanner(PlannerContext.singleton(), catalog_db);
        boolean ret = vp_planner.process(catalog_stmt);
        assertFalse(ret);
    }
    
    /**
     * testSingleColumnQuery
     */
    public void testSingleColumnQuery() throws Exception {
        // Create a vertical partition for SUBSCRIBER that includes SUB_NBR and S_ID
        // This should be enough to allow our query to use it
//        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
//        Collection<Column> vp_cols = CollectionUtil.addAll(new ArrayList<Column>(),
//                                                           this.getColumn(catalog_tbl, "S_ID"),
//                                                           this.getColumn(catalog_tbl, "SUB_NBR"));
//        MaterializedViewInfo vp = CatalogUtil.addVerticalPartition(catalog_tbl, vp_cols);
//        assertNotNull(vp);
        
        Procedure catalog_proc = this.getProcedure("Test1");
        Statement catalog_stmt = CollectionUtil.getFirst(catalog_proc.getStatements());
        VerticalPartitionPlanner vp_planner = new VerticalPartitionPlanner(PlannerContext.singleton(), catalog_db);
        boolean ret = vp_planner.process(catalog_stmt);
        assert(ret);
        
    }
    
}
