package edu.brown.designer.partitioners;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.catalog.*;
import org.voltdb.types.PartitionMethodType;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestPartitionEntry extends BaseTestCase {

    protected static Table catalog_tbl = null;
    protected static PartitionEntry entry = null;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        if (catalog_tbl == null) {
            catalog_tbl = this.getTable("DISTRICT");
            Column catalog_col = catalog_tbl.getColumns().get("D_W_ID");
            assertNotNull(catalog_col);
            
            Table parent_tbl = catalog_db.getTables().get("WAREHOUSE");
            assertNotNull(parent_tbl);
            Column parent_col = parent_tbl.getColumns().get("W_ID");
            assertNotNull(parent_col);
            
            entry = new PartitionEntry(PartitionMethodType.HASH, catalog_col);
            entry.setParent(parent_tbl);
            entry.setParentAttribute(parent_col);
        }
    }
    
    /**
     * testToJSON
     */
    @Test
    public void testToJSON() throws Exception {
        String json = entry.toJSONString();
        assertNotNull(json);
        
        // Check to make sure our properties got serialized too
        for (PartitionEntry.Members element : PartitionEntry.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }
    
    /**
     * testFromJSONReplication
     */
    @Test
    public void testFromJSONReplication() throws Exception {
        entry = new PartitionEntry(PartitionMethodType.REPLICATION);
        String json = entry.toJSONString();
        assertNotNull(json);

        JSONObject jsonObject = new JSONObject(json);
        PartitionEntry copy = new PartitionEntry();
        copy.fromJSON(jsonObject, catalog_db);
        
        assertEquals(entry.getMethod(), copy.getMethod());
        assertNull(copy.getAttribute());
        assertNull(copy.getParent());
        assertNull(copy.getParentAttribute());
    }

    /**
     * testFromJSON
     */
    @Test
    public void testFromJSON() throws Exception {
        String json = entry.toJSONString();
        assertNotNull(json);

        JSONObject jsonObject = new JSONObject(json);
        PartitionEntry copy = new PartitionEntry();
        copy.fromJSON(jsonObject, catalog_db);
        
        assertEquals(entry.getAttribute(), copy.getAttribute());
        assertEquals(entry.getMethod(), copy.getMethod());
        assertEquals(entry.getParent(), copy.getParent());
        assertEquals(entry.getParentAttribute(), copy.getParentAttribute());
    }
    
}