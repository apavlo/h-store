package edu.brown.statistics;

import java.lang.reflect.Field;
import java.util.SortedMap;

import org.json.JSONObject;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

public class TestProcedureStatistics extends BaseTestCase {
    
    protected static Procedure catalog_proc;
    protected static ProcedureStatistics stats;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        if (stats == null) {
            catalog_proc = this.getProcedure("neworder");
            stats = new ProcedureStatistics(catalog_proc);
            assertNotNull(stats);
        }
    }

    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = stats.toJSONString();
        assertNotNull(json);
        for (ProcedureStatistics.Members element : ProcedureStatistics.Members.values()) {
            System.out.println(element); System.out.flush();
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
        ProcedureStatistics copy = new ProcedureStatistics(catalog_proc);
        copy.fromJSONObject(jsonObject, catalog_db);
        for (ProcedureStatistics.Members element : ProcedureStatistics.Members.values()) {
            String field_name = element.toString().toLowerCase();
            Field field = ProcedureStatistics.class.getDeclaredField(field_name);
            assertNotNull(field);
            System.out.println(field_name); System.out.flush();
            
            Object orig_value = field.get(stats);
            Object copy_value = field.get(copy);
            
            if (orig_value instanceof SortedMap) {
                SortedMap<?, ?> orig_map = (SortedMap<?, ?>)orig_value;
                SortedMap<?, ?> copy_map = (SortedMap<?, ?>)copy_value;
                for (Object key : orig_map.keySet()) {
                    assertTrue("Missing Key: " + key + "\n" + StringUtil.formatMaps(copy_map), copy_map.containsKey(key));
                    System.out.println("\t" + key);
                    System.out.flush();
                    assertEquals(orig_map.get(key), copy_map.get(key));
                } // FOR
            } else {
                assertEquals(orig_value, copy_value);
            }
        } // FOR
    }
}
