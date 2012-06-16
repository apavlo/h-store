package edu.brown.hstore.conf;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.BaseTestCase;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;


public class TestHStoreConf extends BaseTestCase {

    private static final Map<String, Object> properties = new ListOrderedMap<String, Object>();
    static {
        properties.put("site.markov_path_caching", false);              // Boolean
        properties.put("site.markov_path_caching_threshold", 0.91919);  // Double
        properties.put("site.memory", 19999);                           // Long
    }
    
    private HStoreConf hstore_conf;
    private final String groups[] = { "global", "client", "site" };
    
    @Override
    protected void setUp() throws Exception {
        assert(HStoreConf.isInitialized());
        hstore_conf = HStoreConf.singleton();
    }
    
    /**
     * testMakeHTML
     */
    public void testMakeIndexHTML() throws Exception {
        for (String prefix : groups) {
            String contents = HStoreConfUtil.makeIndexHTML(hstore_conf, prefix);
            assertNotNull(contents);
            System.err.println(contents);
        } // FOR
        System.err.println(HStoreConfUtil.navigationLink);
        
        System.err.println(StringUtil.DOUBLE_LINE);
    }
    
    /**
     * testMakeHTML
     */
    public void testMakeHTML() throws Exception {
        for (String prefix : groups) {
            String contents = HStoreConfUtil.makeHTML(hstore_conf, prefix);
            assertNotNull(contents);
            assertFalse(contents.isEmpty());
            System.err.println(contents);
            System.err.println(StringUtil.DOUBLE_LINE);
        }
        System.err.println(HStoreConfUtil.navigationLink);
    }
    
    /**
     * testBuildXML
     */
    public void testBuildXML() throws Exception {
        for (String prefix : groups) {
            String contents = HStoreConfUtil.makeBuildXML(hstore_conf, prefix);
            assertNotNull(contents);
            assertFalse(contents.isEmpty());
            System.err.println(contents);
        }
    }
    
    /**
     * 
     */
    public void testMakeConfig() throws Exception {
        String contents = HStoreConfUtil.makeConfig(hstore_conf, true);
        assertNotNull(contents);
        Class<?> confClass = hstore_conf.site.getClass();
//        for (Field f : confClass.getFields()) {
//            String key = String.format("site.%s", f.getName());
//            assert(contents.contains(key)) : "Missing " + key;
//        } // FOR
        System.err.println(contents);
        System.err.println(StringUtil.DOUBLE_LINE);
        
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
    
    /**
     * testIsConfParameterValid
     */
    public void testIsConfParameterValid() {
        String valid[] = {
            "site.planner_caching",
            "client.txnrate",
            "global.temp_dir"
        };
        for (String name : valid) {
            assertTrue("Unexpected '" + name + "'", HStoreConf.isConfParameter(name));
        } // FOR
    }
    
    /**
     * testIsConfParameterInvalid
     */
    public void testIsConfParameterInvalid() {
        String invalid[] = {
            "evanjones.canadian",
            "site.squirrels",
            "siteplanner_caching"
        };
        for (String name : invalid) {
            assertFalse("Unexpected '" + name + "'", HStoreConf.isConfParameter(name));
        } // FOR
    }
    
}
