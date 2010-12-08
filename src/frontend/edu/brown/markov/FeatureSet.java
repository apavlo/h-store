package edu.brown.markov;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.catalog.Procedure;

import edu.brown.utils.ClassUtil;
import edu.brown.utils.StringUtil;
import edu.brown.workload.TransactionTrace;

public class FeatureSet {
    
    protected enum Type {
        NUMERIC,
        STRING,
        RANGE,
        BOOLEAN,
    }
    
    
    protected final HashMap<String, Vector<Object>> txn_values = new HashMap<String, Vector<Object>>();
    protected final ListOrderedMap<String, Type> attributes = new ListOrderedMap<String, Type>();
    protected final Map<String, Set<String>> attribute_ranges = new HashMap<String, Set<String>>();
    
    public FeatureSet() {
        // Nothing for now...
    }
    
    public List<String> getFeatures() {
        return (this.attributes.asList());
    }
    
    public Type getFeatureType(String key) {
        return (this.attributes.get(key));
    }

    public void addFeature(TransactionTrace txn, String key, Object val) {
        this.addFeature(txn, key, val, null);
    }

    
    public void addFeature(TransactionTrace txn, String key, Object val, Type type) {
        String txn_id = txn.getTransactionId();
        
        // Add the attribute if it's new
        if (!this.attributes.containsKey(key)) {
            // Figure out what type it is
            if (type == null) {
                Class<?> valClass = val.getClass();
                if (valClass.equals(Boolean.class) || valClass.equals(boolean.class)) {
                    type = Type.BOOLEAN;
                } else if (ClassUtil.getSuperClasses(valClass).contains(Number.class)) {
                    type = Type.NUMERIC;
                } else if (val instanceof String) {
                    type = Type.STRING;
                } else {
                    type = Type.RANGE;
                }
            }
            this.attributes.put(key, type);
        }
        
        // Store ranges if needed
        if (type == Type.RANGE) {
            if (!this.attribute_ranges.containsKey(key)) this.attribute_ranges.put(key, new HashSet<String>());
            this.attribute_ranges.get(key).add(val.toString());
        }
        
        int idx = this.attributes.indexOf(key);
        if (!this.txn_values.containsKey(txn_id)) {
            this.txn_values.put(txn_id, new Vector<Object>(this.attributes.size()));
        }
        this.txn_values.get(txn_id).setSize(this.attributes.size());
        this.txn_values.get(txn_id).set(idx, val);
    }
    
    
    public void load(String path) {
        
        
    }
    
    /**
     * Write out the data set to a file
     * @param path
     * @throws IOException
     */
    public void save(String path, String name) throws IOException {
        FileWriter out = new FileWriter(path);
        out.write(String.format("@relation %s\n", name));
        
        // Attributes
        for (Entry<String, Type> e : this.attributes.entrySet()) {
            String include = "";
            if (e.getValue() == Type.RANGE) {
                String add = " {";
                for (String v : this.attribute_ranges.get(e.getKey())) {
                    include += add + v;
                    add = ", ";
                } // FOR
                include += "}";
            }
            out.write(String.format("@attribute %s%s\n", e.getKey(), include));
        } // FOR
        
        // Values
        out.write("@data\n");
        for (Vector<Object> values : this.txn_values.values()) {
            out.write(StringUtil.join(",", values) + "\n");
        } // FOR
        out.close();
        
    }

}
