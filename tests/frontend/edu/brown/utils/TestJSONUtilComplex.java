package edu.brown.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.json.*;
import org.voltdb.catalog.Database;

import edu.brown.BaseTestCase;

public class TestJSONUtilComplex extends BaseTestCase {
    
    private static final int NUM_ELEMENTS = 5;

    public static class TestObject implements JSONSerializable {
        public enum Members {
            LIST_SET,
            LIST_MAP,
            LIST_LIST,
            SET_SET,
            SET_MAP,
            SET_LIST,
            MAP_SET,
            MAP_MAP,
            MAP_LIST,
        }
        
        public static final Set<Members> LISTS = new ListOrderedSet<Members>();
        public static final Set<Members> SETS = new ListOrderedSet<Members>();
        public static final Set<Members> MAPS = new ListOrderedSet<Members>();
        
        static {
            for (Members e : Members.values()) {
                String name = e.name();
                if (name.startsWith("LIST")) {
                    LISTS.add(e);
                } else if (name.startsWith("SET")) {
                    SETS.add(e);
                } else if (name.startsWith("MAP")) {
                    MAPS.add(e);
                } else {
                    assert(false) : "Unexpected Member '" + e + "'";
                }
            } // FOR
            assert(!LISTS.isEmpty()) : "No list members selected";
            assert(!SETS.isEmpty()) : "No set members selected";
            assert(!MAPS.isEmpty()) : "No map members selected";
        }
        
        // --------------------------------------------------------------------------------
        // Data Members
        // --------------------------------------------------------------------------------

        // Lists
        public List<Set<Integer>> list_set = new ArrayList<Set<Integer>>();
        public List<Map<Integer, Integer>> list_map = new ArrayList<Map<Integer, Integer>>();
        public List<List<Integer>> list_list = new ArrayList<List<Integer>>();

        // Sets
        public Set<Set<Integer>> set_set = new ListOrderedSet<Set<Integer>>();
        public Set<Map<Integer, Integer>> set_map = new ListOrderedSet<Map<Integer, Integer>>();
        public Set<List<Integer>> set_list = new ListOrderedSet<List<Integer>>();

        // Maps
        public Map<Integer, Set<Integer>> map_set = new HashMap<Integer, Set<Integer>>();
        public Map<Integer, Map<Integer, Integer>> map_map = new HashMap<Integer, Map<Integer, Integer>>();
        public Map<Integer, List<Integer>> map_list = new HashMap<Integer, List<Integer>>();
        
        // --------------------------------------------------------------------------------
        // JSONSerializable Methods
        // --------------------------------------------------------------------------------
        
        @Override
        public void load(File input_path, Database catalog_db) throws IOException {
            JSONUtil.load(this, catalog_db, input_path);
        }
        @Override
        public void save(File output_path) throws IOException {
            JSONUtil.save(this, output_path);
        }
        @Override
        public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
            this.fromJSON(json_object, catalog_db, Arrays.asList(Members.values()));
        }
        @Override
        public void toJSON(JSONStringer stringer) throws JSONException {
            this.toJSON(stringer, Arrays.asList(Members.values()));
        }
        @Override
        public String toJSONString() {
            return (this.toJSONString(Arrays.asList(Members.values())));
        }
        
        public void fromJSON(JSONObject json_object, Database catalog_db, Collection<Members> members) throws JSONException {
            Members members_arr[] = new Members[members.size()];
            members.toArray(members_arr);
            JSONUtil.fieldsFromJSON(json_object, catalog_db, this, TestObject.class, members_arr);
        }
        public void toJSON(JSONStringer stringer, Collection<Members> members) throws JSONException {
            Members members_arr[] = new Members[members.size()];
            members.toArray(members_arr);
            JSONUtil.fieldsToJSON(stringer, this, TestObject.class, members_arr);
        }
        public String toJSONString(Collection<Members> members) {
            JSONStringer stringer = new JSONStringer();
            try {
                stringer.object();
                this.toJSON(stringer, members);
                stringer.endObject();
            } catch (JSONException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            return (stringer.toString());
        }
    }
    
    private final Random rand = new Random();
    private TestObject obj;
    
    @Override
    protected void setUp() throws Exception {
        this.obj = new TestObject();
        for (int i = 0; i < NUM_ELEMENTS; i++) {
            this.obj.list_list.add(new ArrayList<Integer>());
            this.obj.list_set.add(new HashSet<Integer>());
            this.obj.list_map.add(new HashMap<Integer, Integer>());
            
            this.obj.set_list.add(new ArrayList<Integer>());
            this.obj.set_set.add(new HashSet<Integer>());
            this.obj.set_map.add(new HashMap<Integer, Integer>());
            
            this.obj.map_list.put(i, new ArrayList<Integer>());
            this.obj.map_set.put(i, new HashSet<Integer>());
            this.obj.map_map.put(i, new HashMap<Integer, Integer>());

            for (int ii = 0, cnt = rand.nextInt(NUM_ELEMENTS) + 1; ii < cnt; ii++) {
                CollectionUtil.last(this.obj.list_list).add(rand.nextInt());
                CollectionUtil.last(this.obj.list_set).add(rand.nextInt());
                CollectionUtil.last(this.obj.list_map).put(rand.nextInt(), rand.nextInt());
                
                CollectionUtil.last(this.obj.set_list).add(rand.nextInt());
                CollectionUtil.last(this.obj.set_set).add(rand.nextInt());
                CollectionUtil.last(this.obj.set_map).put(rand.nextInt(), rand.nextInt());
                
                this.obj.map_list.get(i).add(rand.nextInt());
                this.obj.map_set.get(i).add(rand.nextInt());
                this.obj.map_map.get(i).put(rand.nextInt(), rand.nextInt());
            } // FOR
        } // FOR
    }
    
    private JSONObject toJSONObject(TestObject orig, Collection<TestObject.Members> members) throws Exception {
        String json_string = orig.toJSONString(members);
        assert(json_string.length() > 0);
        JSONObject json_object = new JSONObject(json_string);
        assertNotNull(json_object);
        // if (members.size() == 2) System.err.println(json_object.toString(1));
        assertEquals(members.size(), json_object.length());
        return (json_object);
    }
    
    private TestObject clone(TestObject orig, Collection<TestObject.Members> members) throws Exception {
        assert(!members.isEmpty());
        JSONObject json_object = new JSONObject(orig.toJSONString());
//        System.err.println(json_object.toString(1));
        TestObject clone = new TestObject();
        clone.fromJSON(json_object, catalog_db, members);
        return (clone);
    }
    
    private void compareInnerObjects(TestObject.Members e, Object o0, Object o1) throws Exception {
        // MAP
        if (e.name().endsWith("MAP")) {
            Map<?,?> inner0 = (Map<?,?>)o0;
            Map<?,?> inner1 = (Map<?,?>)o1;
            assertEquals(inner0.keySet(), inner1.keySet());
            
            for (Object inner_key : inner0.keySet()) {
                Object value0 = inner0.get(inner_key);
                Object value1 = inner1.get(inner_key);
                assertEquals(value0, value1);
            } // FOR
            
        // LIST + SET
        } else {
            Collection<?> inner0 = (Collection<?>)o0;
            Collection<?> inner1 = (Collection<?>)o1;
            assertEquals(inner0.size(), inner1.size());
            assert(inner0.containsAll(inner1));
            assert(inner1.containsAll(inner0));
        }
    }

    
    // --------------------------------------------------------------------------------
    // Test Cases
    // --------------------------------------------------------------------------------
    
    /**
     * testListFieldsToJSON
     */
    public void testListFieldsToJSON() throws Exception {
        JSONObject json_object = this.toJSONObject(obj, TestObject.LISTS);
        
        for (TestObject.Members e : TestObject.LISTS) {
            String json_key = e.name();
            assert(json_object.has(json_key));
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Collection<?> collection = (Collection<?>)field.get(obj);
            List<String> collection_strings = new ArrayList<String>();
            for (Object outer : collection) {
                if (outer instanceof Map) {
                    Map<?,?> m = (Map<?,?>)outer;
                    for (Object temp : m.entrySet()) {
                        Entry<?, ?> entry = (Entry<?, ?>)temp;
                        collection_strings.add(entry.getKey().toString());
                        collection_strings.add(entry.getValue().toString());
                    } // FOR
                } else {
                    for (Object o : (Collection<?>)outer) {
                        collection_strings.add(o.toString());    
                    } // FOR
                }
            } // FOR
            
            JSONArray json_array = json_object.getJSONArray(json_key);
            assertEquals(collection.size(), json_array.length());
            for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
                // MAP
                if (e.name().endsWith("MAP")) {
                    JSONObject inner_object = json_array.getJSONObject(i);
                    for (String key : JSONObject.getNames(inner_object)) {
                        assert(collection_strings.contains(key)) :
                            "Missing key '" + key + "' from field " + e + ": " + collection_strings;
                        String value = inner_object.getString(key);
                        assertNotNull(value);
                        assertFalse(value.isEmpty());
                        assert(collection_strings.contains(key)) :
                            "Missing value '" + value + "' for key '" + key + "' in field " + e + ": " + collection_strings;
                    } // FOR
                    
                // SET + LIST
                } else {
                    JSONArray inner_array = json_array.getJSONArray(i);
                    for (int ii = 0, cnt2 = inner_array.length(); ii < cnt2; ii++) {
                        String value = inner_array.getString(ii);
                        assertNotNull(value);
                        assertFalse(value.isEmpty());
                        assert(collection_strings.contains(value)) :
                            "Missing element '" + value + "' from field " + e + ": " + collection_strings;
                    } // FOR
                }
            } // FOR
        } // FOR
    }
    
    /**
     * testListFieldsFromJSON
     */
    public void testListFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.LISTS);
        for (TestObject.Members e : TestObject.LISTS) {
            String json_key = e.name();
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            List<?> collection0 = (List<?>)field.get(obj);
            List<?> collection1 = (List<?>)field.get(clone);
            assertEquals(collection0.size(), collection1.size());
            for (int i = 0, cnt = collection0.size(); i < cnt; i++) {
                compareInnerObjects(e, collection0.get(i), collection1.get(i));
            } // FOR
        } // FOR
    }
    
    /**
     * testSetFieldsToJSON
     */
    public void testSetFieldsToJSON() throws Exception {
        JSONObject json_object = this.toJSONObject(obj, TestObject.SETS);
        
        for (TestObject.Members e : TestObject.SETS) {
            String json_key = e.name();
            assert(json_object.has(json_key));
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Collection<?> collection = (Collection<?>)field.get(obj);
            List<String> collection_strings = new ArrayList<String>();
            for (Object outer : collection) {
                if (outer instanceof Map) {
                    Map<?,?> m = (Map<?,?>)outer;
                    for (Object temp : m.entrySet()) {
                        Entry<?, ?> entry = (Entry<?, ?>)temp;
                        collection_strings.add(entry.getKey().toString());
                        collection_strings.add(entry.getValue().toString());
                    } // FOR
                } else {
                    for (Object o : (Collection<?>)outer) {
                        collection_strings.add(o.toString());    
                    } // FOR
                }
            } // FOR
            
            JSONArray json_array = json_object.getJSONArray(json_key);
            assertEquals(collection.size(), json_array.length());
            for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
                // MAP
                if (e.name().endsWith("MAP")) {
                    JSONObject inner_object = json_array.getJSONObject(i);
                    for (String key : JSONObject.getNames(inner_object)) {
                        assert(collection_strings.contains(key)) :
                            "Missing key '" + key + "' from field " + e + ": " + collection_strings;
                        String value = inner_object.getString(key);
                        assertNotNull(value);
                        assertFalse(value.isEmpty());
                        assert(collection_strings.contains(key)) :
                            "Missing value '" + value + "' for key '" + key + "' in field " + e + ": " + collection_strings;
                    } // FOR
                    
                // SET + LIST
                } else {
                    JSONArray inner_array = json_array.getJSONArray(i);
                    for (int ii = 0, cnt2 = inner_array.length(); ii < cnt2; ii++) {
                        String value = inner_array.getString(ii);
                        assertNotNull(value);
                        assertFalse(value.isEmpty());
                        assert(collection_strings.contains(value)) :
                            "Missing element '" + value + "' from field " + e + ": " + collection_strings;
                    } // FOR
                }
            } // FOR
        } // FOR
    }
    
    /**
     * testSetFieldsFromJSON
     */
    public void testSetFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.SETS);
        for (TestObject.Members e : TestObject.SETS) {
            String json_key = e.name();
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Set<?> collection0 = (Set<?>)field.get(obj);
            Set<?> collection1 = (Set<?>)field.get(clone);
            assertEquals(collection0.size(), collection1.size());
            for (int i = 0, cnt = collection0.size(); i < cnt; i++) {
                compareInnerObjects(e, CollectionUtil.get(collection0, i), CollectionUtil.get(collection1, i));
            } // FOR
        } // FOR
    }
    
    /**
     * testMapFieldsToJSON
     */
    public void testMapFieldsToJSON() throws Exception {
        JSONObject json_object = this.toJSONObject(obj, TestObject.MAPS);
        for (TestObject.Members e : TestObject.MAPS) {
            String json_key = e.name();
            assert(json_object.has(json_key));
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Map<?,?> map = (Map<?,?>)field.get(obj);
            List<String> key_strings = new ArrayList<String>();
            List<String> val_strings = new ArrayList<String>();
            for (Object key : map.keySet()) {
                key_strings.add(key.toString());

                Object val = map.get(key);
                if (val instanceof Map) {
                    Map<?,?> m = (Map<?,?>)val;
                    for (Object temp : m.entrySet()) {
                        Entry<?, ?> entry = (Entry<?, ?>)temp;
                        key_strings.add(entry.getKey().toString());
                        val_strings.add(entry.getValue().toString());
                    } // FOR
                } else {
                    for (Object o : (Collection<?>)val) {
                        val_strings.add(o.toString());    
                    } // FOR
                }
            } // FOR
            
            JSONObject json_inner_obj = json_object.getJSONObject(json_key);
            for (String json_inner_key : JSONObject.getNames(json_inner_obj)) {
                assert(!json_inner_key.isEmpty());
                if (e.name().endsWith("MAP")) {
                    JSONObject json_map_obj = json_inner_obj.getJSONObject(json_inner_key);
                    for (String key : JSONObject.getNames(json_map_obj)) {
                        assert(key_strings.contains(key)) :
                            "Missing key '" + key + "' from field " + e + ": " + key_strings;
                        String value = json_map_obj.getString(key);
                        assertNotNull(value);
                        assertFalse(value.isEmpty());
                        assert(val_strings.contains(value)) :
                            "Missing value '" + value + "' for key '" + key + "' in field " + e + ": " + val_strings;
                    } // FOR
                } else {
                    JSONArray inner_array = json_inner_obj.getJSONArray(json_inner_key);
                    for (int ii = 0, cnt2 = inner_array.length(); ii < cnt2; ii++) {
                        String value = inner_array.getString(ii);
                        assertNotNull(value);
                        assertFalse(value.isEmpty());
                        assert(val_strings.contains(value)) :
                            "Missing element '" + value + "' from field " + e + ": " + val_strings;
                    } // FOR
                }
            } // WHILE
        } // FOR
    }
    
    
    /**
     * testMapFieldsFromJSON
     */
    public void testMapFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.MAPS);
        for (TestObject.Members e : TestObject.MAPS) {
            String json_key = e.name();
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Map<?,?> map0 = (Map<?,?>)field.get(obj);
            assertNotNull(map0);
            Map<?,?> map1 = (Map<?,?>)field.get(clone);
            assertNotNull(map1);
            assertEquals(map0.size(), map1.size());
            assertEquals(map0.keySet(), map1.keySet());

            for (Object key : map0.keySet()) {
                compareInnerObjects(e, map0.get(key), map1.get(key));
            } // FOR
        } // FOR
    }
}
