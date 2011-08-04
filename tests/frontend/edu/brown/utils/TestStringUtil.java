package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

public class TestStringUtil extends TestCase {

    /**
     * testColumns
     */
    @SuppressWarnings("unchecked")
    public void testColumns() throws Exception {
        Random rand = new Random();
        for (int num_cols = 1; num_cols < 5; num_cols++) {
            List<Integer> lists[] = new List[num_cols];
            int max_length = 0;
            String strs[] = new String[num_cols];
            for (int i = 0; i < strs.length; i++) {
                lists[i] = new ArrayList<Integer>();
                int size = rand.nextInt(30);
                for (int j = 0; j < size; j++) {
                    int value = rand.nextInt();
                    lists[i].add(value);
                }
                strs[i] = StringUtil.join("\n", lists[i]);
                max_length = Math.max(max_length, lists[i].size());
            } // FOR
            
            String columns = StringUtil.columns(strs);
            assertNotNull(columns);
            assertFalse(columns.isEmpty());
            System.err.println(columns);
            
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
