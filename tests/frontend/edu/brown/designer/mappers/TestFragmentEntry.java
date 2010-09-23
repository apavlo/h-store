package edu.brown.designer.mappers;

import org.json.JSONObject;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogKey;
import edu.brown.utils.ProjectType;

public class TestFragmentEntry extends BaseTestCase {

    protected static Table catalog_tbl = null;
    protected static FragmentEntry fragment = null;
    
    @Override
    protected void setUp() throws Exception {
        setUp(ProjectType.TPCC);
        if (catalog_tbl == null) {
            catalog_tbl = this.getTable("DISTRICT");
            fragment = new FragmentEntry(CatalogKey.createKey(catalog_tbl), 99);
            fragment.setEstimatedHeat(100l);
            fragment.setEstimatedSize(200l);
        }
    }
    
    public void testToJSONString() throws Exception {
        String json = fragment.toJSONString();
        assertNotNull(json);
        
        // Check to make sure our properties got serialized too
        for (FragmentEntry.Members element : FragmentEntry.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }

    public void testFromJSONString() throws Exception {
        String json = fragment.toJSONString();
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        
        FragmentEntry copy = new FragmentEntry();
        copy.fromJSONObject(jsonObject, catalog_db);
        
        assertEquals(fragment.getTableKey(), copy.getTableKey());
        assertEquals(fragment.getHashKey(), copy.getHashKey());
        assertEquals(fragment.getEstimatedSize(), copy.getEstimatedSize());
        assertEquals(fragment.getEstimatedHeat(), copy.getEstimatedHeat());
    }
}