package edu.mit.hstore;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.utils.FileUtil;

import junit.framework.TestCase;

public class TestHStoreConf extends TestCase {

    private static final Map<String, Object> properties = new ListOrderedMap<String, Object>();
    static {
        properties.put("markov_path_caching", false);               // Boolean
        properties.put("markov_path_caching_threshold", 0.91919);   // Double
        properties.put("helper_initial_delay", 19999);              // Long
    }
    
    private HStoreConf hstore_conf;
    
    @Override
    protected void setUp() throws Exception {
        hstore_conf = HStoreConf.init(null, null);
        
    }
    
    /**
     * testLoadFromFile
     */
    public void testLoadFromFile() throws Exception {
        // First make sure that the values aren't the same
        Class<?> confClass = hstore_conf.getClass();
        for (String k : properties.keySet()) {
            Field field = confClass.getField(k);
            assertNotSame(k, properties.get(k), field.get(hstore_conf));
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
            Field field = confClass.getField(k);
            assertEquals(k, properties.get(k), field.get(hstore_conf));
        } // FOR
    }
    
}
