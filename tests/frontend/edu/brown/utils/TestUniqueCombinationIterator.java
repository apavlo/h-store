package edu.brown.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.junit.Test;

import edu.brown.logging.LoggerUtil;

/**
 * 
 * @author pavlo
 */
public class TestUniqueCombinationIterator extends TestCase {

    private static final int BASE_COMBO_SIZE = 1; 
    private static final SortedMap<Integer, Integer> EXPECTED_NUM_COMBOS = new TreeMap<Integer, Integer>();
    static {
        // These values were calculated using the Python script found here:
        // http://code.activestate.com/recipes/190465/
        EXPECTED_NUM_COMBOS.put(1, 10);
        EXPECTED_NUM_COMBOS.put(2, 45);
        EXPECTED_NUM_COMBOS.put(3, 120);
        EXPECTED_NUM_COMBOS.put(4, 210);
        EXPECTED_NUM_COMBOS.put(5, 252);
        EXPECTED_NUM_COMBOS.put(6, 210);
        EXPECTED_NUM_COMBOS.put(7, 120);
        EXPECTED_NUM_COMBOS.put(8, 45);
        EXPECTED_NUM_COMBOS.put(9, 10);
    }

    private static final int NUM_LETTERS = 10;
    private static final List<String> LETTERS = new ArrayList<String>();
    static {
        for (int i = 0; i < NUM_LETTERS; i++) {
            LETTERS.add(String.valueOf((char)(i + 97)));
        }
        LoggerUtil.setupLogging();
    }
    
    /**
     * testIterator
     */
    @Test
    public void testIterator() {
        for (int combo_size = BASE_COMBO_SIZE; combo_size <= EXPECTED_NUM_COMBOS.lastKey(); combo_size++) {
            UniqueCombinationIterator<String> it = new UniqueCombinationIterator<String>(LETTERS, combo_size);
            boolean debug = false; // combo_size == 4;
            
            int i = 0;
            Set<Set<String>> seen = new HashSet<Set<String>>();
            while (it.hasNext()) {
                Set<String> s = it.next();
                // System.err.println(it);
                if (debug) System.err.println(String.format("[%03d] %s", i, s));
                i+=1;
    //            System.err.println();
                
                assertNotNull(s);
                assertEquals(combo_size, s.size());
                assertFalse("Duplicate: " + s, seen.contains(s));
                seen.add(s);
            } // WHILE
            assert(i > 0);
            assertEquals("ComboSize="+combo_size, EXPECTED_NUM_COMBOS.get(combo_size).intValue(), i);
        } // FOR
    }
    
}
