package edu.brown.utils;

import org.junit.Test;
import junit.framework.TestCase;

/**
 * @author pavlo
 */
public class TestEnumUtil extends TestCase {

    enum CosbyKids {
        FAT_ALBERT_JACKSON,
        MUSHMOUTH,
        DUMB_DONALD,
        BILL_COSBY,
        RUSSELL_COSBY,
        WEIRD_HAROLD,
        RUDY_DAVIS,
        BUCKY,
    };
    
    /**
     * testGetName
     */
    @Test
    public void testGetName() {
        CosbyKids target = CosbyKids.BILL_COSBY;
        CosbyKids get = EnumUtil.get(CosbyKids.values(), "BILL_COSBY");
        assertNotNull(get);
        assertEquals(target, get);
        
        get = EnumUtil.get(CosbyKids.values(), "EVAN_JONES");
        assertNull(get);
        assertNotSame(target, get);
    }
    
    /**
     * testGetIndex
     */
    @Test
    public void testGetIndex() {
        CosbyKids target = CosbyKids.BILL_COSBY;
        CosbyKids get = EnumUtil.get(CosbyKids.values(), 3);
        assertNotNull(get);
        assertEquals(target, get);
        
        get = EnumUtil.get(CosbyKids.values(), -1);
        assertNull(get);
        assertNotSame(target, get);
    }
}