package edu.mit.hstore;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;


public class TestHStoreConf extends BaseTestCase {

    private static final Map<String, Object> properties = new ListOrderedMap<String, Object>();
    static {
        properties.put("site.markov_path_caching", false);               // Boolean
        properties.put("site.markov_path_caching_threshold", 0.91919);   // Double
        properties.put("site.helper_initial_delay", 19999);              // Long
    }
    
    private HStoreConf hstore_conf;
    
    @Override
    protected void setUp() throws Exception {
        assert(HStoreConf.isInitialized());
        hstore_conf = HStoreConf.singleton();
    }
    
    /**
     * testMakeHTML
     */
    public void testMakeHTML() throws Exception {
        String contents = hstore_conf.makeHTML();
        assertNotNull(contents);
        System.err.println(contents);
    }
    
    /**
     * 
     */
    public void testMakeConfig() throws Exception {
        String contents = hstore_conf.makeConfig(true, true);
        assertNotNull(contents);
        Class<?> confClass = hstore_conf.site.getClass();
//        for (Field f : confClass.getFields()) {
//            String key = String.format("site.%s", f.getName());
//            assert(contents.contains(key)) : "Missing " + key;
//        } // FOR
//        System.out.println(contents);
        
    }
    
    /**
     * testLoadFromFile
     */
    public void testLoadFromFile() throws Exception {
        // First make sure that the values aren't the same
        Class<?> confClass = hstore_conf.site.getClass();
        for (String k : properties.keySet()) {
            Field field = confClass.getField(k.split("\\.")[1]);
            assertNotSame(k, properties.get(k), field.get(hstore_conf.site));
        } // FOR
        
        // Then create a config file
        String contents = "";
        for (Entry<String, Object> e : properties.entrySet()) {
            contents += String.format("%s = %s\n", e.getKey(), e.getValue()); 
        } // FOR
        File f = FileUtil.writeStringToTempFile(contents, "properties", true);
        
        // And load it in. Now the values should be what we expect them to be
        hstore_conf.loadFromFile(f);
        for (String k : properties.keySet()) {
            Field field = confClass.getField(k.split("\\.")[1]);
            assertEquals(k, properties.get(k), field.get(hstore_conf.site));
        } // FOR
    }
    
}
