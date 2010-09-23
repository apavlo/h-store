package edu.brown.statistics;

import java.lang.reflect.Field;
import java.util.*;

import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestQueryStatistics extends BaseTestCase {
    
    protected static Catalog catalog;
    protected static Database catalog_db;
    protected static Table catalog_tbl;
    protected static Procedure catalog_proc;
    protected static Statement catalog_stmt;
    protected static QueryStatistics stats;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        if (stats == null) {
            catalog_tbl = this.getTable("WAREHOUSE");
            catalog_proc = this.getProcedure("neworder");
            catalog_stmt = catalog_proc.getStatements().get("getWarehouseTaxRate");
            
            stats = new QueryStatistics(catalog_stmt);
            assertNotNull(stats);
        }
    }

    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = stats.toJSONString();
        assertNotNull(json);
        for (QueryStatistics.Members element : QueryStatistics.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }
    
    /**
     * testFromJSONString
     */
    public void testFromJSONString() throws Exception {
        String json = stats.toJSONString();
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        QueryStatistics copy = new QueryStatistics(catalog_stmt);
        copy.fromJSONObject(jsonObject, catalog_db);
        for (QueryStatistics.Members element : QueryStatistics.Members.values()) {
            String field_name = element.toString().toLowerCase();
            Field field = QueryStatistics.class.getDeclaredField(field_name);
            assertNotNull(field);
            
            Object orig_value = field.get(stats);
            Object copy_value = field.get(copy);
            
            if (orig_value instanceof SortedMap) {
//                SortedMap orig_map = (SortedMap)orig_value;
//                SortedMap copy_map = (SortedMap)copy_value;
//                for (Object key : orig_map.keySet()) {
//                    assertTrue(copy_map.containsKey(key));
//                    assertEquals(orig_map.get(key), copy_map.get(key));
//                } // FOR
            } else if (orig_value == null) {
                assertNull(copy_value);
            } else {
                assertEquals(orig_value, copy_value);
            }
        } // FOR
    }
}
