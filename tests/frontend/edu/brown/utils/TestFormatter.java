package edu.brown.utils;

import junit.framework.TestCase;

public class TestFormatter extends TestCase {

    static final String SQL = 
            "SELECT B_NAME "
            + "from TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY "
            + "where TR_S_SYMB = S_SYMB and " + "S_CO_ID = CO_ID and "
            + "CO_IN_ID = IN_ID and " + "SC_ID = IN_SC_ID and "
            + "B_NAME = ? and "
            + " SC_NAME = ? " + "group by B_NAME ";
    
    public void testFormat() throws Exception {
        SQLFormatter f = new SQLFormatter(SQL);
        String formatted = f.format();
        assertNotNull(formatted);
        assertFalse(formatted.isEmpty());
        System.err.println(formatted);
        
        // Just check that we get the same thing if we upper case everything first
        String copy = new String(SQL);
        assertEquals(SQL.length(), copy.length());
        f = new SQLFormatter(copy);
        String copyFormatted = f.format();
        assertNotNull(copyFormatted);
        assertFalse(copyFormatted.isEmpty());
        assertTrue(formatted.equalsIgnoreCase(copyFormatted));
        
        System.err.print(StringUtil.SINGLE_LINE);
        System.err.println(copyFormatted);
    }
    
}
