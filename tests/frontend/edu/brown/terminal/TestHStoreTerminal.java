package edu.brown.terminal;

import java.util.List;

import org.junit.Test;
import org.voltdb.types.TimestampType;

import junit.framework.TestCase;

public class TestHStoreTerminal extends TestCase {
    
    /**
     * testExtractParams
     */
    @Test
    public void testExtractParams() throws Exception {
        Object params[] = {
            12345,
            123.45,
            new TimestampType(),
            "This is a string",
        };
        
        StringBuilder paramStr = new StringBuilder();
        for (Object obj : params) {
            if (obj instanceof String || obj instanceof TimestampType) paramStr.append('"');
            paramStr.append(obj);
            if (obj instanceof String || obj instanceof TimestampType) paramStr.append('"');
            paramStr.append(" ");
        } // FOR
        System.err.println("ORIG: " + paramStr);
        
        // Just check that we get the same number of parameters back
        List<String> extracted = HStoreTerminal.extractParams(paramStr.toString());
        System.err.println("EXTRACTED: " + extracted);
        assertEquals(params.length, extracted.size());
    }
    
}
