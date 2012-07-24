package edu.brown.statistics;

import java.io.File;
import java.lang.reflect.Field;
import java.util.SortedMap;

import org.json.JSONObject;
import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestTableStatistics extends BaseTestCase {
    
    protected static Table catalog_tbl;
    protected static TableStatistics stats;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        if (stats== null) {
            catalog_tbl = this.getTable("ACCESS_INFO");
            
            stats = new TableStatistics(catalog_tbl);
            assertNotNull(stats);
            
            long temp = 0l;
            stats.tuple_size_total = ++temp;
            stats.tuple_size_min = ++temp;
            stats.tuple_size_max = ++temp;
            stats.tuple_size_avg = ++temp;
            stats.tuple_count_total = ++temp;
        }
    }
    
    /**
     * testLoad
     */
    public void testLoad() throws Exception {
        File f = this.getStatsFile(ProjectType.TM1);
        assertNotNull(f);
        WorkloadStatistics wstats = new WorkloadStatistics(catalog_db);
        wstats.load(f, catalog_db);
        
        for (Table catalog_tbl : catalog_db.getTables()) {
            TableStatistics tstats = wstats.getTableStatistics(catalog_tbl);
            assertNotNull(tstats);
            assert(tstats.tuple_count_total > 0) : String.format("%s.tuple_count_total", catalog_tbl.getName());
//            System.err.println(String.format("%s.tuple_count_total = %d", catalog_tbl.getName(), tstats.tuple_count_total));
            assert(tstats.tuple_size_total > 0) : String.format("%s.tuple_size_total", catalog_tbl.getName());
//            System.err.println(String.format("%s.tuple_size_total = %d", catalog_tbl.getName(), tstats.tuple_size_total));
        } // FOR
    }

    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = stats.toJSONString();
        assertNotNull(json);
        for (TableStatistics.Members element : TableStatistics.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }
    
    /**
     * testFromJSONString
     */
    @SuppressWarnings("unchecked")
    public void testFromJSONString() throws Exception {
        String json = stats.toJSONString();
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        TableStatistics copy = new TableStatistics(catalog_tbl);
        copy.fromJSONObject(jsonObject, catalog_db);
        for (TableStatistics.Members element : TableStatistics.Members.values()) {
            String field_name = element.toString().toLowerCase();
            Field field = TableStatistics.class.getDeclaredField(field_name);
            assertNotNull(field);
            
            Object orig_value = field.get(stats);
            Object copy_value = field.get(copy);
            
            if (orig_value instanceof SortedMap) {
                SortedMap orig_map = (SortedMap)orig_value;
                SortedMap copy_map = (SortedMap)copy_value;
                for (Object key : orig_map.keySet()) {
                    assertTrue(copy_map.containsKey(key));
                    
                    Object orig_map_value = orig_map.get(key);
                    if (! (orig_map_value instanceof ColumnStatistics)) {
                        assertEquals(orig_map_value, copy_map.get(key));
                    }
                } // FOR
            } else {
                assertEquals(orig_value, copy_value);
            }
        } // FOR
    }
}
