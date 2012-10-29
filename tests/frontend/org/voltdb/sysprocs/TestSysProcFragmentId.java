package org.voltdb.sysprocs;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

public class TestSysProcFragmentId extends TestCase {

    /**
     * testUniqueIds
     */
    public void testUniqueIds() throws Exception {
        Class<SysProcFragmentId> clazz = SysProcFragmentId.class;
        Map<Integer, String> ids = new HashMap<Integer, String>();
        for (Field f : clazz.getDeclaredFields()) {
           String f_name = f.getName(); 
           int id = f.getInt(null);
           assertTrue(f_name, id >= 0);
           
           assertFalse(String.format("Duplicate id %d for %s <-> %s", id, f_name, ids.get(id)),
                       ids.containsKey(id));
           ids.put(id, f_name);
        } // FOR
        assertFalse(ids.isEmpty());
    }
    
}
