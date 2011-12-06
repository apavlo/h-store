package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections15.map.ListOrderedMap;

import junit.framework.TestCase;

public class TestStringUtil extends TestCase {

    Random rand = new Random();
    
    /**
     * testFormatMaps
     */
    public void testFormatMaps() throws Exception {
        String key[] = { "This", "is", "a", "key" };
        
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put(StringUtil.join(" ", key), rand.nextDouble());
        m.put(StringUtil.join("\n", key), rand.nextBoolean());
        m.put(Double.toString(rand.nextDouble()), StringUtil.join(" ", key));
        m.put(Boolean.toString(rand.nextBoolean()), StringUtil.join("\n", key));
        m.put("XXX" + StringUtil.join("\n", key), StringUtil.join("\n", key));
        
        String formatted = StringUtil.formatMaps(m);
        assertNotNull(formatted);
        assertFalse(formatted.isEmpty());
        System.out.println(formatted);
    }
    
    /**
     * testHeader
     */
    public void testHeader() throws Exception {
        String msg = "THIS IS A TEST";
        String title = StringUtil.header(msg);
        assertNotNull(title);
        System.out.println(title);
        assert(title.length() > msg.length());
    }
    
    /**
     * testColumns
     */
    @SuppressWarnings("unchecked")
    public void testColumns() throws Exception {
        for (int num_cols = 1; num_cols < 5; num_cols++) {
            List<Integer> lists[] = new List[num_cols];
            int max_length = 0;
            String strs[] = new String[num_cols];
            for (int i = 0; i < strs.length; i++) {
                lists[i] = new ArrayList<Integer>();
                int size = rand.nextInt(30) + 1;
                for (int j = 0; j < size; j++) {
                    int value = rand.nextInt() + 1;
                    lists[i].add(value);
                }
                strs[i] = StringUtil.join("\n", lists[i]);
                max_length = Math.max(max_length, lists[i].size());
            } // FOR
            
            String columns = StringUtil.columns(strs);
            assertNotNull(columns);
            assertFalse(columns.isEmpty());
            System.out.println(columns);
            
            String lines[] = StringUtil.splitLines(columns);
            assertEquals(max_length, lines.length);
            for (int i = 0; i < strs.length; i++) {
                for (Integer value : lists[i]) {
                    assert(columns.contains(value.toString())) : "Missing " + value;
                }
            }
        }
    }
    
    /**
     * testMD5sum
     */
    public void testMD5sum() throws Exception {
        // Use Linux's md5sum to compute these strings
        String expected[][] = {
            { "H-Store",                    "531f6eb53d9fbfb3791ce62216ebd451" },
            { "NewSQL\ngets\nyou\ngirls",   "c9e783020aeb4ceb79d4d382e5fc15be" },
            { "Parallel OLTP!",             "c0a67d90aad4a0cf1e6d11964b6cdc40" },
        };
        
        for (int i = 0; i < expected.length; i++) {
            String actual = StringUtil.md5sum(expected[i][0]);
            assertEquals(expected[i][0], expected[i][1], actual);
        } // FOR
        
    }
    
}
