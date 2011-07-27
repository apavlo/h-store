package edu.brown.utils;

import junit.framework.TestCase;

public class TestStringUtil extends TestCase {

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
