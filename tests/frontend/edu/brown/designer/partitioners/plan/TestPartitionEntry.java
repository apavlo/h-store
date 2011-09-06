package edu.brown.designer.partitioners.plan;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.utils.ProjectType;

public class TestPartitionEntry extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
    }
    
    /**
     * testTableEntrySerialization
     */
    @Test
    public void testTableEntrySerialization() throws Exception {
        Table catalog_tbl = this.getTable("DISTRICT");
        Column catalog_col = this.getColumn(catalog_tbl, "D_W_ID");
        
        Table parent_tbl = this.getTable("WAREHOUSE");
        Column parent_col = this.getColumn(parent_tbl, "W_ID");
        
        TableEntry entries[] = {
            new TableEntry(PartitionMethodType.REPLICATION, null, null, null),
            new TableEntry(PartitionMethodType.HASH, catalog_col, null, null),
            new TableEntry(PartitionMethodType.MAP, catalog_col, parent_tbl, parent_col),
            new TableEntry(PartitionMethodType.NONE, null, null, null),
        };
        
        for (TableEntry te : entries) {
            assertNotNull(te);
            String json = te.toJSONString();
            assertNotNull(json);
            assertFalse(json.isEmpty());
            
            JSONObject jsonObject = new JSONObject(json);
            TableEntry copy = new TableEntry();
            copy.fromJSON(jsonObject, catalog_db);
            
            assertEquals(te.getAttribute(), copy.getAttribute());
            assertEquals(te.getMethod(), copy.getMethod());
            assertEquals(te.getParent(), copy.getParent());
            assertEquals(te.getParentAttribute(), copy.getParentAttribute());
            
//            System.err.println(JSONUtil.format(json) + "\n");
        } // FOR
    }
    
    /**
     * testProcedureEntrySerialization
     */
    @Test
    public void testProcedureEntrySerialization() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        ProcParameter catalog_params[] = {
            this.getProcParameter(catalog_proc, 0),
            this.getProcParameter(catalog_proc, 1),
        };
        
        ProcedureEntry entries[] = {
            new ProcedureEntry(PartitionMethodType.HASH, catalog_params[0], null),
            new ProcedureEntry(PartitionMethodType.HASH, catalog_params[0], true),
            new ProcedureEntry(PartitionMethodType.HASH, MultiProcParameter.get(catalog_params), false),
            new ProcedureEntry(PartitionMethodType.NONE, null, null),
        };
        
        for (ProcedureEntry te : entries) {
            assertNotNull(te);
            String json = te.toJSONString();
            assertNotNull(json);
            assertFalse(json.isEmpty());
            
            JSONObject jsonObject = new JSONObject(json);
            ProcedureEntry copy = new ProcedureEntry();
            copy.fromJSON(jsonObject, catalog_db);
            
            assertEquals(te.getAttribute(), copy.getAttribute());
            assertEquals(te.getMethod(), copy.getMethod());
            assertEquals(te.isSinglePartition(), copy.isSinglePartition());
            
//            System.err.println(JSONUtil.format(json) + "\n");
        } // FOR
    }
}