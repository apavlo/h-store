package edu.brown.graphs;

import java.util.*;

import junit.framework.TestCase;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.*;

public class TestAbstractGraphElement extends TestCase {

    public static class TestElement extends AbstractGraphElement {
        public enum Members {
            TEST_LIST,
            TEST_SET,
            TEST_MAP,
            TEST_LONG,
            TEST_STRING,
        }
        
        public List<Long> test_list = new ArrayList<Long>();
        public Set<Long> test_set = new HashSet<Long>();
        public Map<Long, String> test_map = new HashMap<Long, String>();
        public Long test_long;
        public String test_string;
        
        public TestElement(JSONObject jsonObject) throws Exception {
            super();
            this.fromJSON(jsonObject, null);
        }
        
        public TestElement(Random rand) {
            super();
            this.test_long = rand.nextLong();
            this.test_string = "You smell bad ==> " + Integer.toString(rand.nextInt());
            
            for (int i = 0; i < 10; i++) {
                test_list.add(rand.nextLong());
                test_set.add(rand.nextLong());
                
                long key = rand.nextLong();
                String val = "VALUE["+ key + "]";
                test_map.put(key, val);
            } // FOR
        }
        
        @Override
        protected void toJSONStringImpl(JSONStringer stringer) throws JSONException {
            this.fieldsToJSONString(stringer, TestElement.class, Members.values());
        }
        
        @Override
        protected void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException {
            this.fieldsFromJSONObject(object, catalog_db, TestElement.class, Members.values());
        }
    } // END CLASS
    
    private Random rand = new Random(0);
    private TestElement element = new TestElement(this.rand);
    
    /**
     * testFieldsToJSONString
     */
    public void testFieldsToJSONString() throws Exception {
        String contents = this.element.toJSONString();
        
        for (TestElement.Members member : TestElement.Members.values()) {
            assertTrue(contents.contains(member.name()));
        } // FOR
        
        for (Long val : this.element.test_list) {
            assertTrue(contents.contains(val.toString()));
        }
        for (Long val : this.element.test_set) {
            assertTrue(contents.contains(val.toString()));
        }
        for (Long key : this.element.test_map.keySet()) {
            assertTrue(contents.contains(key.toString()));
            assertTrue(contents.contains(element.test_map.get(key)));
        }
    }
    
    /**
     * testFieldsFromJSONObject
     */
    public void testFieldsFromJSONObject() throws Exception {
        String contents = this.element.toJSONString();
        
        TestElement clone = new TestElement(new JSONObject(contents));
        
        assertEquals(this.element.test_long, clone.test_long);
        assertEquals(this.element.test_string, clone.test_string);
        
        assertEquals(this.element.test_list.size(), clone.test_list.size());
        for (int i = 0; i < this.element.test_list.size(); i++) {
            assertEquals(this.element.test_list.get(i).intValue(), clone.test_list.get(i).intValue()); 
        }
        
        assertEquals(this.element.test_set.size(), clone.test_set.size());
        for (Long val : this.element.test_set) {
            assertTrue(clone.test_set.contains(val));
        }
        
        assertEquals(this.element.test_map.size(), clone.test_map.size());
        for (Long key : this.element.test_map.keySet()) {
            assertTrue(clone.test_map.containsKey(key));
            assertEquals(this.element.test_map.get(key), clone.test_map.get(key)); 
        }
    }
    
}
