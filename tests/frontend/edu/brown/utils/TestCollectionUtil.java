package edu.brown.utils;

import junit.framework.TestCase;
import java.util.*;

public class TestCollectionUtil extends TestCase {
    
    private final Random rand = new Random();
    
    /**
     * testAddAll
     */
    public void testAddAll() {
        int cnt = rand.nextInt(50) + 1; 
        List<Integer> l = new ArrayList<Integer>();
        Integer a[] = new Integer[cnt];
        for (int i = 0; i < cnt; i++) {
            int next = rand.nextInt(); 
            l.add(next);
            a[i] = next;
        } // FOR
        
        Collection<Integer> c = CollectionUtil.addAll(new HashSet<Integer>(), l);
        assertEquals(l.size(), c.size());
        assert(c.containsAll(l));
        
        c = CollectionUtil.addAll(new HashSet<Integer>(), a);
        assertEquals(l.size(), c.size());
        assert(c.containsAll(l));
    }
    
    /**
     * testSortByValues
     */
    public void testSortByValues() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("b", 2000);
        map.put("a", 1000);
        map.put("c", 4000);
        map.put("d", 3000);
        
        Map<String, Integer> sorted = CollectionUtil.sortByValues(map);
        assertNotNull(sorted);
        assertEquals(map.size(), sorted.size());
        assert(map.keySet().containsAll(sorted.keySet()));
        assert(map.values().containsAll(sorted.values()));

        List<String> list = new ArrayList<String>(sorted.keySet());
        assertEquals("a", list.get(0));
        Collections.reverse(list);
        assertEquals("c", list.get(0));
    }
    
    /**
     * testGetGreatest
     */
    public void testGetGreatest() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 4);
        map.put("d", 3);
        String key = CollectionUtil.getGreatest(map);
        assertEquals("c", key);
    }
    
    /**
     * testGetFirst
     */
    public void testGetFirst() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        String key = CollectionUtil.getFirst(list);
        assertEquals("a", key);
    }
}
