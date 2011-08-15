package edu.brown.designer.mappers;

import org.json.JSONObject;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;

public class TestSiteEntry extends TestFragmentEntry {

    protected static Host catalog_host = null;
    protected static SiteEntry site = null;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (catalog_host == null) {
            Cluster catalog_cluster = CatalogUtil.getCluster(catalog_db);
            catalog_host = CollectionUtil.first(catalog_cluster.getHosts());
            
            site = new SiteEntry(99);
            site.add(fragment);
            site.setHost(catalog_host);
        }
    }
    
    public void testToJSONString() throws Exception {
        String json = site.toJSONString();
        assertNotNull(json);
        
        // Check to make sure our properties got serialized too
        for (SiteEntry.Members element : SiteEntry.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }

    public void testFromJSONString() throws Exception {
        String json = site.toJSONString();
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        
        SiteEntry copy = new SiteEntry();
        copy.fromJSONObject(jsonObject, catalog_db);
        
        assertEquals(site.getId(), copy.getId());
        assertEquals(site.getHost(catalog_db), copy.getHost(catalog_db));
        assertEquals(site.getFragments().size(), copy.getFragments().size());
    }
}