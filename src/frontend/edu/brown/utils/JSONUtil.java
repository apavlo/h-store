package edu.brown.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.*;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogKey;

/**
 * @author pavlo
 */
public abstract class JSONUtil {
    private static final Logger LOG = Logger.getLogger(JSONUtil.class.getName());
    
    private static final String JSON_CLASS_SUFFIX = "_class";
    
    /**
     * JSON Pretty Print
     * @param json
     * @return
     * @throws JSONException
     */
    public static String format(String json) {
        try {
            return (JSONUtil.format(new JSONObject(json)));
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * JSON Pretty Print
     * @param <T>
     * @param object
     * @return
     */
    public static <T extends JSONSerializable> String format(T object) {
        JSONStringer stringer = new JSONStringer();
        try {
            if (object instanceof JSONObject) return ((JSONObject)object).toString(2);
            stringer.object();
            object.toJSON(stringer);
            stringer.endObject();
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        return (JSONUtil.format(stringer.toString()));
    }
    
    public static String format(JSONObject o) {
        try {
            return o.toString(1);
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * 
     * @param <T>
     * @param object
     * @return
     */
    public static <T extends JSONSerializable> String toJSONString(T object) {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            object.toJSON(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return (stringer.toString());
    }
    
    /**
     * Write the contents of a JSONSerializable object out to a file on the local disk
     * @param <T>
     * @param object
     * @param output_path
     * @throws IOException
     */
    public static <T extends JSONSerializable> void save(T object, String output_path) throws IOException {
        final String className = object.getClass().getSimpleName();
        LOG.debug("Writing out contents of " + className + " to '" + output_path + "'");
        try {
            String json = object.toJSONString();
            FileUtil.writeStringToFile(new File(output_path), format(json));
        } catch (Exception ex) {
            LOG.error("Failed to serialize the " + className + " file '" + output_path + "'", ex);
            throw new IOException(ex);
        }
    }
    
    /**
     * Load in a JSONSerialable stored in a file
     * @param <T>
     * @param object
     * @param output_path
     * @throws Exception
     */
    public static <T extends JSONSerializable> void load(T object, Database catalog_db, String input_path) throws IOException {
        final String className = object.getClass().getSimpleName();
        LOG.debug("Loading in serialized " + className + " from '" + input_path + "'");
        String contents = FileUtil.readFile(input_path);
        if (contents.isEmpty()) {
            throw new IOException("The " + className + " file '" + input_path + "' is empty");
        }
        try {
            object.fromJSON(new JSONObject(contents), catalog_db);
        } catch (Exception ex) {
            LOG.error("Failed to deserialize the " + className + " from file '" + input_path + "'", ex);
            throw new IOException(ex);
        }
        LOG.debug("The loading of the " + className + " is complete");
    }
    
    /**
     * For a given Enum, write out the contents of the corresponding field to the JSONObject
     * We assume that the given object has matching fields that correspond to the Enum members, except
     * that their names are lower case.
     * @param <E>
     * @param stringer
     * @param members
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    public static <E extends Enum<?>, T> void fieldsToJSON(JSONStringer stringer, T object, Class<? extends T> base_class, E members[]) throws JSONException {
        LOG.debug("Serializing out " + members.length + " elements for " + base_class.getSimpleName());
        for (E element : members) {
            String json_key = element.name();
            try {
                Field field_handle = base_class.getDeclaredField(json_key.toLowerCase());
                Class<?> field_class = field_handle.getType();
                Object field_value = field_handle.get(object);
                
                // Null
                if (field_value == null) {
                    stringer.key(json_key);
                    writeFieldValue(stringer, field_class, field_value);
                // Maps
                } else if (field_value instanceof Map) {
                    stringer.key(json_key);
                    writeFieldValue(stringer, field_class, field_value);
                // Everything else
                } else {
                    stringer.key(json_key);
                    writeFieldValue(stringer, field_class, field_value);
                    addClassForField(stringer, json_key, field_class, field_value);
                }
            } catch (Exception ex) {
                LOG.fatal("Failed to serialize field '" + element + "'", ex);
                System.exit(1);
            }
        } // FOR
    }
    
    /**
     * 
     * @param stringer
     * @param field_class
     * @param field_value
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    public static void writeFieldValue(JSONStringer stringer, Class<?> field_class, Object field_value) throws JSONException {
        // Null
        if (field_value == null) {
            LOG.debug("writeNullFieldValue(" + field_class + ", " + field_value + ")");
            stringer.value(null);
            
        // Collections
        } else if (ClassUtil.getInterfaces(field_class).contains(Collection.class)) {
            LOG.debug("writeCollectionFieldValue(" + field_class + ", " + field_value + ")");
            stringer.array();
            for (Object value : (Collection<?>)field_value) {
                if (value == null) {
                    stringer.value(null);
                } else {
                    writeFieldValue(stringer, value.getClass(), value);
                }
            } // FOR
            stringer.endArray();
            
        // Maps
        } else if (field_value instanceof Map) {
            LOG.debug("writeMapFieldValue(" + field_class + ", " + field_value + ")");
            stringer.object();
            for (Entry<?, ?> e : ((Map<?, ?>)field_value).entrySet()) {
                // We can handle null keys
                String key_value = null;
                if (e.getKey() != null) {
                    // The key can't be a raw CatalogType because CatalogKey won't know how to 
                    // deserialize it on the other side
                    Class<?> key_class = e.getKey().getClass();
                    if (key_class.equals(CatalogType.class))
                        throw new JSONException("CatalogType is not allowed to be the map key");
                    key_value = makePrimitiveValue(key_class, e.getKey()).toString();
                }
                stringer.key(key_value);
                
                // We can also handle null values. Where is your god now???
                if (e.getValue() == null) {
                    stringer.value(null);
                } else {
                    writeFieldValue(stringer, e.getValue().getClass(), e.getValue());
                }
            } // FOR
            stringer.endObject();
            
        // Primitive
        } else {
            LOG.debug("writePrimitiveFieldValue(" + field_class + ", " + field_value + ")");
            stringer.value(makePrimitiveValue(field_class, field_value));
        }
        return;
    }
    
    /**
     * Read data from the given JSONObject and populate the given Map
     * @param json_object
     * @param catalog_db
     * @param map
     * @param inner_classes
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected static void readMapField(final JSONObject json_object, final Database catalog_db, final Map map, final Stack<Class> inner_classes) throws Exception {
        Class key_class = inner_classes.pop();
        Class val_class = inner_classes.pop();
        Set<Class<?>> val_interfaces = ClassUtil.getInterfaces(val_class);
        
        assert(json_object != null);
        Iterator<String> it = json_object.keys();
        while (it.hasNext()) {
            String json_key = it.next();
            final Stack<Class> next_inner_classes = new Stack<Class>();
            next_inner_classes.addAll(inner_classes);
            assert(next_inner_classes.equals(inner_classes));
            
            // KEY
            Object key = JSONUtil.getPrimitiveValue(json_key, key_class, catalog_db);
            
            // VALUE
            Object object = null;
            if (json_object.isNull(json_key)) {
                // Nothing...
            } else if (val_interfaces.contains(List.class)) {
                object = new ArrayList();
                readCollectionField(json_object.getJSONArray(json_key), catalog_db, (Collection)object, next_inner_classes);
            } else if (val_interfaces.contains(Set.class)) {
                object = new HashSet();
                readCollectionField(json_object.getJSONArray(json_key), catalog_db, (Collection)object, next_inner_classes);
            } else if (val_interfaces.contains(Map.class)) {
                object = new HashMap();
                readMapField(json_object.getJSONObject(json_key), catalog_db, (Map)object, next_inner_classes);
            } else {
                String json_string = json_object.getString(json_key);
                try {
                    object = JSONUtil.getPrimitiveValue(json_string, val_class, catalog_db);
                } catch (Exception ex) {
                    System.err.println("val_interfaces: " + val_interfaces);
                    LOG.error("Failed to deserialize value '" + json_string + "' from inner map key '" + json_key + "'");
                    throw ex;
                }
            }
            map.put(key, object);
        }
    }
    
    /**
     * Read data from the given JSONArray and populate the given Collection
     * @param json_array
     * @param catalog_db
     * @param collection
     * @param inner_classes
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected static void readCollectionField(final JSONArray json_array, final Database catalog_db, final Collection collection, final Stack<Class> inner_classes) throws Exception {
        // We need to figure out what the inner type of the collection is
        // If it's a Collection or a Map, then we need to instantiate it before 
        // we can call readFieldValue() again for it.
        Class inner_class = inner_classes.pop();
        Set<Class<?>> inner_interfaces = ClassUtil.getInterfaces(inner_class);
        
        for (int i = 0, cnt = json_array.length(); i < cnt; i++) {
            final Stack<Class> next_inner_classes = new Stack<Class>();
            next_inner_classes.addAll(inner_classes);
            assert(next_inner_classes.equals(inner_classes));
            Object value = null;
            
            // Null
            if (json_array.isNull(i)) {
                value = null;
            // Lists
            } else if (inner_interfaces.contains(List.class)) {
                value = new ArrayList();
                readCollectionField(json_array.getJSONArray(i), catalog_db, (Collection)value, next_inner_classes);
            // Sets
            } else if (inner_interfaces.contains(Set.class)) {
                value = new HashSet();
                readCollectionField(json_array.getJSONArray(i), catalog_db, (Collection)value, next_inner_classes);
            // Maps
            } else if (inner_interfaces.contains(Map.class)) {
                value = new HashMap();
                readMapField(json_array.getJSONObject(i), catalog_db, (Map)value, next_inner_classes);
            // Values
            } else {
                String json_string = json_array.getString(i);
                value = JSONUtil.getPrimitiveValue(json_string, inner_class, catalog_db);
            }
            collection.add(value);
        } // FOR
        return;
    }
    
    /**
     * 
     * @param json_object
     * @param catalog_db
     * @param json_key
     * @param field_handle
     * @param object
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static void readFieldValue(final JSONObject json_object, final Database catalog_db, final String json_key, Field field_handle, Object object) throws Exception {
        assert(json_object.has(json_key)) : "No entry exists for '" + json_key + "'";
        final boolean debug = LOG.isDebugEnabled();
        Class<?> field_class = field_handle.getType();
        Object field_object = field_handle.get(object);
        // String field_name = field_handle.getName();
        
        // Null
        if (json_object.isNull(json_key)) {
            if (debug) LOG.debug("Field " + json_key + " is null");
            field_handle.set(object, null);
            
        // Collections
        } else if (ClassUtil.getInterfaces(field_class).contains(Collection.class)) {
            if (debug) LOG.debug("Field " + json_key + " is a collection");
            assert(field_object != null);
            Stack<Class> inner_classes = new Stack<Class>();
            inner_classes.addAll(ClassUtil.getGenericTypes(field_handle));
            Collections.reverse(inner_classes);
            
            JSONArray json_inner =json_object.getJSONArray(json_key); 
            if (json_inner == null) throw new JSONException("No array exists for '" + json_key + "'");
            readCollectionField(json_inner, catalog_db, (Collection)field_object, inner_classes);

        // Maps
        } else if (field_object instanceof Map) {
            if (debug) LOG.debug("Field " + json_key + " is a map");
            assert(field_object != null);
            Stack<Class> inner_classes = new Stack<Class>();
            inner_classes.addAll(ClassUtil.getGenericTypes(field_handle));
            Collections.reverse(inner_classes);
            
            JSONObject json_inner = json_object.getJSONObject(json_key);
            if (json_inner == null) throw new JSONException("No object exists for '" + json_key + "'");
            readMapField(json_inner, catalog_db, (Map)field_object, inner_classes);
            
        // Everything else...
        } else {
            Class explicit_field_class = JSONUtil.getClassForField(json_object, json_key);
            if (explicit_field_class != null) {
                field_class = explicit_field_class;
                if (debug) LOG.debug("Found explict field class " + field_class.getSimpleName() + " for " + json_key);
            }
            if (debug) LOG.debug("Field " + json_key + " is primitive type " + field_class.getSimpleName());
            Object value = JSONUtil.getPrimitiveValue(json_object.getString(json_key), field_class, catalog_db);
            field_handle.set(object, value);
            if (debug) LOG.debug("Set field " + json_key + " to '" + value + "'");
        }
    }
    
    /**
     * For the given enum, load in the values from the JSON object into the current object
     * @param <E>
     * @param json_object
     * @param catalog_db
     * @param members
     * @throws JSONException
     */
    public static <E extends Enum<?>, T> void fieldsFromJSON(JSONObject json_object, Database catalog_db, T object, Class<? extends T> base_class, E...members) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, object, base_class, false, members);
    }
    
    /**
     * 
     * @param <E>
     * @param <T>
     * @param json_object
     * @param catalog_db
     * @param object
     * @param base_class
     * @param ignore_missing
     * @param members
     * @throws JSONException
     */
    public static <E extends Enum<?>, T> void fieldsFromJSON(JSONObject json_object, Database catalog_db, T object, Class<? extends T> base_class, boolean ignore_missing, E...members) throws JSONException {
        final boolean debug= LOG.isDebugEnabled(); 
        for (E element : members) {
            String json_key = element.name();
            String field_name = element.toString().toLowerCase();
            Field field_handle = null;
            if (debug) LOG.debug("Retreiving value for field '" + json_key + "'");
            
            if (!json_object.has(json_key)) {
                String msg = "JSONObject for " + base_class.getSimpleName() + " does not have key '" + json_key + "': " + CollectionUtil.toList(json_object.keys()); 
                if (ignore_missing) {
                    if (debug) LOG.warn(msg);
                    continue;
                } else {
                    throw new JSONException(msg);    
                }
            }
            
            try {
                field_handle = base_class.getDeclaredField(field_name);
                readFieldValue(json_object, catalog_db, json_key, field_handle, object);
            } catch (Exception ex) {
                // System.err.println(field_class + ": " + ClassUtil.getSuperClasses(field_class));
                String msg = "Unable to deserialize field '" + json_key + "' from " + base_class.getSimpleName();
                LOG.error(msg, ex);
                throw new JSONException(ex);
            }
        } // FOR
    }

    /**
     * For some fields we will also store the class of the value
     * @param stringer
     * @param json_key
     * @param field_class
     * @throws JSONException
     */
    private static void addClassForField(JSONStringer stringer, String json_key, Class<?> field_class, Object field_value) throws JSONException {
        // If the field_class is just a CatalogType, then store the true class of the object
        // so that we can deserialize it on the other side
        if (field_class.equals(CatalogType.class)) {
            stringer.key(json_key + JSON_CLASS_SUFFIX).value(makePrimitiveValue(Class.class, field_value.getClass()));
        }
    }
    
    /**
     * Return the class of a field if it was stored in the JSONObject along with the value
     * If there is no class information, then this will return null 
     * @param json_object
     * @param json_key
     * @return
     * @throws JSONException
     */
    private static Class<?> getClassForField(JSONObject json_object, String json_key) throws JSONException {
        Class<?> field_class = null;
        // Check whether we also stored the class
        if (json_object.has(json_key + JSON_CLASS_SUFFIX)) {
            try {
                field_class = ClassUtil.getClass(json_object.getString(json_key + JSON_CLASS_SUFFIX));
            } catch (Exception ex) {
                LOG.error("Failed to include class for field '" + json_key + "'", ex);
                throw new JSONException(ex);
            }
        }
        return (field_class);
        
    }
    
    /**
     * Return the proper serialization string for the given value
     * @param field_name
     * @param field_class
     * @param field_value
     * @return
     */
    private static Object makePrimitiveValue(Class<?> field_class, Object field_value) {
        Object value = null;
        
        // Class
        if (field_class.equals(Class.class)) {
            value = ((Class<?>)field_value).getName();
        // JSONSerializable
        } else if (ClassUtil.getInterfaces(field_class).contains(JSONSerializable.class)) {
            // Just return the value back. The JSON library will take care of it
//            System.err.println(field_class + ": " + field_value);
            value = field_value;
        // VoltDB Catalog
        } else if (ClassUtil.getSuperClasses(field_class).contains(CatalogType.class)) {
            value = CatalogKey.createKey((CatalogType)field_value);
        // Everything else
        } else {
            value = field_value.toString();
        }
        return (value);
    }

    
    /**
     * For the given JSON string, figure out what kind of object it is and return it
     * @param json_value
     * @param field_class
     * @param catalog_db
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private static Object getPrimitiveValue(String json_value, Class<?> field_class, Database catalog_db) throws Exception {
        Object value = null;

        // VoltDB Catalog Object
        if (ClassUtil.getSuperClasses(field_class).contains(CatalogType.class)) {
            value = CatalogKey.getFromKey(catalog_db, json_value, (Class<? extends CatalogType>)field_class);
            if (value == null) throw new JSONException("Failed to get catalog object from '" + json_value + "'");
        // Class
        } else if (field_class.equals(Class.class)) {
            value = ClassUtil.getClass(json_value);
            if (value == null) throw new JSONException("Failed to get class from '" + json_value + "'");
        // Enum
        } else if (field_class.isEnum()) {
            if (field_class.equals(VoltType.class)) {
                json_value = json_value.replace("VoltType.", "");
            }
            for (Object o : field_class.getEnumConstants()) {
                Enum<?> e = (Enum<?>)o;
                if (json_value.equals(e.name())) return (e);
            } // FOR
            throw new JSONException("Invalid enum value '" + json_value + "': " + Arrays.toString(field_class.getEnumConstants()));
        // Boolean
        } else if (field_class.equals(Boolean.class) || field_class.equals(boolean.class)) {
            // We have to use field_class.equals() because the value may be null
            value = Boolean.parseBoolean(json_value);
        // Integer
        } else if (field_class.equals(Integer.class) || field_class.equals(int.class)) {
            value = Integer.parseInt(json_value);
        // Float
        } else if (field_class.equals(Float.class) || field_class.equals(float.class)) {
            value = Float.parseFloat(json_value);
        // JSONSerializable
        } else if (ClassUtil.getInterfaces(field_class).contains(JSONSerializable.class)) {
            value = ClassUtil.newInstance(field_class, null, null);
            ((JSONSerializable)value).fromJSON(new JSONObject(json_value), catalog_db);
        // Everything else
        } else {
            // LOG.debug(json_value + " -> " + field_class);
            VoltType volt_type = VoltType.typeFromClass(field_class);
            value = VoltTypeUtil.getObjectFromString(volt_type, json_value);
        }
        return (value);
    }
}
