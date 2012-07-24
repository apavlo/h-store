package edu.brown.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.json.*;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogKey;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.utils.TestJSONUtil.TestObject.TestEnum;

public class TestJSONUtil extends BaseTestCase {

    public static class TestObject implements JSONSerializable {
        public enum Members {
            DATA_INT,
            DATA_INT_OBJ,
            DATA_LONG,
            DATA_LONG_OBJ,
            DATA_DOUBLE,
            DATA_DOUBLE_OBJ,
            DATA_BOOLEAN,
            DATA_BOOLEAN_OBJ,
            DATA_ENUM,
            LIST_INT,
            LIST_LONG,
            LIST_DOUBLE,
            LIST_BOOLEAN,
            LIST_CATALOG,
            LIST_ENUM,
            LIST_NULL,
            SET_INT,
            SET_LONG,
            SET_DOUBLE,
            SET_BOOLEAN,
            SET_CATALOG,
            SET_NULL,
            MAP_INT,
            MAP_LONG,
            MAP_DOUBLE,
            MAP_STRING,
            MAP_CATALOG,
            MAP_NULL,
            SPECIAL_CLASS,
            SPECIAL_CATALOG,
        }
        
        public static final Set<Members> PRIMITIVES = new ListOrderedSet<Members>();
        public static final Set<Members> LISTS = new ListOrderedSet<Members>();
        public static final Set<Members> MAPS = new ListOrderedSet<Members>();
        public static final Set<Members> SPECIALS = new ListOrderedSet<Members>();
        
        static {
            for (Members e : Members.values()) {
                String name = e.name();
                if (name.startsWith("DATA")) {
                    PRIMITIVES.add(e);
                } else if (name.startsWith("LIST") || name.startsWith("SET")) {
                    LISTS.add(e);
                } else if (name.startsWith("MAP")) {
                    MAPS.add(e);
                } else if (name.startsWith("SPECIAL")) {
                    SPECIALS.add(e);
                } else {
                    assert(false) : "Unexpected Member '" + e + "'";
                }
            } // FOR
            assert(!PRIMITIVES.isEmpty()) : "No primitive members selected";
            assert(!LISTS.isEmpty()) : "No list members selected";
            assert(!MAPS.isEmpty()) : "No map members selected";
            assert(!SPECIALS.isEmpty()) : "No map members selected";
        }
        
        // --------------------------------------------------------------------------------
        // Data Members
        // --------------------------------------------------------------------------------
        
        // Enum
        public enum TestEnum {
            CLUBS,
            HEARTS,
            DIAMONDS,
            SPADES,
        }
        
        // Primitives
        public int data_int;
        public Integer data_int_obj;
        public long data_long;
        public Long data_long_obj;
        public double data_double;
        public Double data_double_obj;
        public boolean data_boolean;
        public Boolean data_boolean_obj;
        public TestEnum data_enum;
        
        // Lists
        public List<Integer> list_int = new ArrayList<Integer>();
        public List<Long> list_long = new ArrayList<Long>();
        public List<Double> list_double = new ArrayList<Double>();
        public List<Boolean> list_boolean = new ArrayList<Boolean>();
        public List<Table> list_catalog = new ArrayList<Table>();
        public List<TestEnum> list_enum = new ArrayList<TestEnum>();
        public List<String> list_null = new ArrayList<String>();
        public Set<Integer> set_int = new HashSet<Integer>();
        public Set<Long> set_long = new HashSet<Long>();
        public Set<Double> set_double = new HashSet<Double>();
        public Set<Boolean> set_boolean = new HashSet<Boolean>();
        public Set<Table> set_catalog = new HashSet<Table>();
        public Set<TestEnum> set_enum = new HashSet<TestEnum>();
        public Set<String> set_null = new HashSet<String>();

        // Maps
        public Map<Integer, String> map_int = new HashMap<Integer, String>();
        public Map<Long, String> map_long = new HashMap<Long, String>();
        public Map<Double, String> map_double = new HashMap<Double, String>();
        public Map<String, String> map_string = new HashMap<String, String>();
        public Map<Table, String> map_catalog = new HashMap<Table, String>();
        public Map<TestEnum, String> map_enum = new HashMap<TestEnum, String>();
        public Map<String, String> map_null = new HashMap<String, String>();

        // Specials
        public Class<? extends AbstractHasher> special_class;
        public Procedure special_catalog;
        
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
    
    private final Random rand = new Random(0);
    private TestObject obj;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.obj = new TestObject();
        this.obj.data_int = rand.nextInt();
        this.obj.data_int_obj = new Integer(rand.nextInt());
        this.obj.data_long = rand.nextLong();
        this.obj.data_long_obj = new Long(rand.nextLong());
        this.obj.data_double = rand.nextDouble();
        this.obj.data_double_obj = new Double(rand.nextDouble());
        this.obj.data_boolean = rand.nextBoolean();
        this.obj.data_boolean_obj = new Boolean(rand.nextBoolean());
        this.obj.data_enum = TestEnum.values()[rand.nextInt(TestEnum.values().length)];
        
        List<Table> tables = CollectionUtil.list(catalog_db.getTables());
        for (int i = 0, cnt = rand.nextInt(20) + 1; i < cnt; i++) {
            this.obj.list_int.add(rand.nextInt());
            this.obj.list_long.add(rand.nextLong());
            this.obj.list_double.add(rand.nextDouble());
            this.obj.list_boolean.add(rand.nextBoolean());
            this.obj.list_catalog.add(CollectionUtil.random(tables));
            this.obj.list_enum.add(TestEnum.values()[rand.nextInt(TestEnum.values().length)]);
            this.obj.list_null.add(i % 2 == 0 ? VoltTypeUtil.getRandomValue(VoltType.STRING).toString() : null);
         
            this.obj.set_int.add(rand.nextInt());
            this.obj.set_long.add(rand.nextLong());
            this.obj.set_double.add(rand.nextDouble());
            this.obj.set_boolean.add(rand.nextBoolean());
            this.obj.set_catalog.add(CollectionUtil.random(tables));
            this.obj.set_enum.add(TestEnum.values()[rand.nextInt(TestEnum.values().length)]);
            this.obj.set_null.add(i % 2 == 0 ? VoltTypeUtil.getRandomValue(VoltType.STRING).toString() : null);
            
            this.obj.map_int.put(rand.nextInt(), VoltTypeUtil.getRandomValue(VoltType.STRING).toString());
            this.obj.map_long.put(rand.nextLong(), VoltTypeUtil.getRandomValue(VoltType.STRING).toString());
            this.obj.map_double.put(rand.nextDouble(), VoltTypeUtil.getRandomValue(VoltType.STRING).toString());
            this.obj.map_string.put(VoltTypeUtil.getRandomValue(VoltType.STRING).toString(), VoltTypeUtil.getRandomValue(VoltType.STRING).toString());
            this.obj.map_catalog.put(CollectionUtil.random(tables), VoltTypeUtil.getRandomValue(VoltType.STRING).toString());
            this.obj.map_enum.put(TestEnum.values()[rand.nextInt(TestEnum.values().length)], VoltTypeUtil.getRandomValue(VoltType.STRING).toString());
            this.obj.map_null.put(VoltTypeUtil.getRandomValue(VoltType.STRING).toString(),
                                  i % 2 == 0 ? VoltTypeUtil.getRandomValue(VoltType.STRING).toString() : null);
        } // FOR
        
        this.obj.special_class = DefaultHasher.class;
        this.obj.special_catalog = this.getProcedure("GetNewDestination");
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
        TestObject clone = new TestObject();
        clone.fromJSON(json_object, catalog_db, members);
        return (clone);
    }
    
    // --------------------------------------------------------------------------------
    // Test Cases
    // --------------------------------------------------------------------------------
    
    /**
     * testPrimitiveFieldsToJSON
     */
    public void testPrimitiveFieldsToJSON() throws Exception {
        JSONObject json_object = this.toJSONObject(obj, TestObject.PRIMITIVES);
        for (TestObject.Members e : TestObject.PRIMITIVES) {
            String key = e.name();
            assert(json_object.has(key));
            String value = json_object.getString(key);
            assert(value.length() > 0);
            Field field = TestObject.class.getField(key.toLowerCase());
            assertNotNull(field);
            assertEquals(field.get(obj).toString(), value);
        } // FOR
    }
    
    /**
     * testPrimitiveFieldsFromJSON
     */
    public void testPrimitiveFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.PRIMITIVES);
        for (TestObject.Members e : TestObject.PRIMITIVES) {
            String key = e.name();
            Field field = TestObject.class.getField(key.toLowerCase());
            assertNotNull(field);
            assertEquals(field.get(obj), field.get(clone));
        } // FOR
    }
    
    /**
     * testListFieldsToJSON
     */
    @SuppressWarnings("unchecked")
    public void testListFieldsToJSON() throws Exception {
        JSONObject json_object = this.toJSONObject(obj, TestObject.LISTS);
        
        for (TestObject.Members e : TestObject.LISTS) {
            String json_key = e.name();
            assert(json_object.has(json_key));
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Collection collection = (Collection)field.get(obj);
            List<String> collection_strings = new ArrayList<String>();
            for (Object o : collection) {
                if (o instanceof CatalogType) {
                    collection_strings.add(CatalogKey.createKey((CatalogType)o));
                } else {
                    collection_strings.add(o != null ? o.toString() : "null");
                }
            } // FOR
            
            JSONArray json_array = json_object.getJSONArray(json_key);
            assertEquals(collection.size(), json_array.length());
            for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
                String value = json_array.getString(i);
                assert(collection_strings.contains(value)) :
                    "Missing element '" + value + "' from field " + e + ": " + collection_strings;
            } // FOR
        } // FOR
    }
    
    /**
     * testListFieldsFromJSON
     */
    @SuppressWarnings("unchecked")
    public void testListFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.LISTS);
        for (TestObject.Members e : TestObject.LISTS) {
            String json_key = e.name();
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Collection collection0 = (Collection)field.get(obj);
            Collection collection1 = (Collection)field.get(clone);
            assertEquals(collection0.size(), collection1.size());
            assert(collection0.containsAll(collection1));
        } // FOR
    }
    
    /**
     * testMapsToJSON
     */
    public void testMapsToJSON() throws Exception {
//        JSONObject json_object = this.toJSONObject(obj, TestObject.MAPS);
        for (TestObject.Members e : TestObject.MAPS) {
            String json_key = e.name();
//            assert(json_object.has(json_key));
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            Map<?,?> map = (Map<?,?>)field.get(obj);
            
            String json_string = JSONUtil.toJSONString(map);
            JSONObject json_object = new JSONObject(json_string);
            assertNotNull(json_object);
//            System.err.println(field.getName() + " -> " + json_string);
            
            List<String> key_strings = new ArrayList<String>();
            List<String> val_strings = new ArrayList<String>();
            for (Object key : map.keySet()) {
                Object val = map.get(key);
                if (key instanceof CatalogType) {
                    key_strings.add(CatalogKey.createKey((CatalogType)key));
                } else {
                    key_strings.add(key != null ? key.toString() : "null");    
                }
                val_strings.add(val != null ? val.toString() : "null");
            } // FOR
            assertEquals(map.size(), key_strings.size());
            assertEquals(map.size(), val_strings.size());
            
//            JSONObject json_inner_obj = json_object.getJSONObject(json_key);
            Iterator<String> json_keys_it = json_object.keys();
            while (json_keys_it.hasNext()) {
                String json_inner_key = json_keys_it.next();
                assert(!json_inner_key.isEmpty());
                String json_inner_val = json_object.getString(json_inner_key);
                
                assert(key_strings.contains(json_inner_key));
                int idx = key_strings.indexOf(json_inner_key);
                assertEquals(val_strings.get(idx), json_inner_val);
            } // WHILE
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
                Object val = map.get(key);
                if (key instanceof CatalogType) {
                    key_strings.add(CatalogKey.createKey((CatalogType)key));
                } else {
                    key_strings.add(key != null ? key.toString() : "null");    
                }
                val_strings.add(val != null ? val.toString() : "null");
            } // FOR
            assertEquals(map.size(), key_strings.size());
            assertEquals(map.size(), val_strings.size());
            
            JSONObject json_inner_obj = json_object.getJSONObject(json_key);
            Iterator<String> json_keys_it = json_inner_obj.keys();
            while (json_keys_it.hasNext()) {
                String json_inner_key = json_keys_it.next();
                assert(!json_inner_key.isEmpty());
                String json_inner_val = json_inner_obj.getString(json_inner_key);
                
                assert(key_strings.contains(json_inner_key));
                int idx = key_strings.indexOf(json_inner_key);
                assertEquals(val_strings.get(idx), json_inner_val);
            } // WHILE
        } // FOR
    }
    
    /**
     * testMapFieldsFromJSON
     */
    @SuppressWarnings("unchecked")
    public void testMapFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.MAPS);
        for (TestObject.Members e : TestObject.MAPS) {
            String json_key = e.name();
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            
            Map m0 = (Map)field.get(obj);
            Map m1 = (Map)field.get(clone);
            assertEquals(m0.size(), m1.size());
            assert(m0.keySet().containsAll(m1.keySet()));
            assert(m1.keySet().containsAll(m0.keySet()));
            for (Object key : m0.keySet()) {
                Object v0 = m0.get(key);
                Object v1 = m1.get(key);
                final String debug = String.format("%s-%s: %s (%s) <=> %s (%s)", json_key, 
                                                   (key != null ? key.toString() : key),
                                                   (v0 != null ? v0.toString() : v0), (v0 != null ? v0.getClass().getSimpleName() : null),
                                                   (v1 != null ? v1.toString() : v1), (v1 != null ? v1.getClass().getSimpleName() : null));
                try {
                    assertEquals(debug, v0, v1);
                } catch (AssertionError ex) {
                    System.err.println(JSONUtil.format(obj));
                    throw ex;
                }

            } // FOR
        } // FOR
    }
    
    /**
     * testSpecialFieldsToJSON
     */
    @SuppressWarnings("unchecked")
    public void testSpecialFieldsToJSON() throws Exception {
        JSONObject json_object = this.toJSONObject(obj, TestObject.SPECIALS);
        for (TestObject.Members e : TestObject.SPECIALS) {
            String json_key = e.name();
            assert(json_object.has(json_key));
            String json_value = json_object.getString(json_key);
            assert(json_value.length() > 0);
            Field field = TestObject.class.getField(json_key.toLowerCase());
            assertNotNull(field);
            Object field_value = field.get(obj); 
            Class<?> field_class = field.getType();
            
            switch (e) {
                case SPECIAL_CLASS:
                    assertEquals(((Class<?>)field_value).getName(), json_value);
                    break;
                case SPECIAL_CATALOG:
                    assertEquals(((CatalogType)field_value).getPath(), CatalogKey.getFromKey(catalog_db, json_value, (Class<? extends CatalogType>)field_class).getPath());
                    break;
                default:
                    assert(false) : "Unexpected field '" + json_key + "'";
            } // SWITCH
        } // FOR
    }
    
    /**
     * testSpecialFieldsFromJSON
     */
    public void testSpecialFieldsFromJSON() throws Exception {
        TestObject clone = this.clone(obj, TestObject.SPECIALS);
        for (TestObject.Members e : TestObject.SPECIALS) {
            String key = e.name();
            Field field = TestObject.class.getField(key.toLowerCase());
            assertNotNull(field);
            assertEquals(field.get(obj), field.get(clone));
        } // FOR
    }
}
