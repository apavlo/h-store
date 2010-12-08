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
import org.apache.log4j.Logger;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import edu.brown.utils.ClassUtil;
import edu.brown.workload.TransactionTrace;

public class FeatureSet {
    private static final Logger LOG = Logger.getLogger(FeatureSet.class);
    
    protected enum Type {
        NUMERIC,
        STRING,
        RANGE,
        BOOLEAN,
    }

    /**
     * The row values for each txn record
     */
    protected final HashMap<String, Vector<Object>> txn_values = new HashMap<String, Vector<Object>>();
    
    /**
     * The list of attributes that each txn should have
     */
    protected final ListOrderedMap<String, Type> attributes = new ListOrderedMap<String, Type>();
    
    /**
     * For RANGE types, the list of values that it could have 
     */
    protected final Map<String, Set<String>> attribute_ranges = new HashMap<String, Set<String>>();
    
    /**
     * Constructor
     */
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

    /**
     * 
     * @param txn
     * @param key
     * @param val
     * @param type
     */
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
            LOG.debug("Adding new attribute " + key + " [" + type + "]");
            this.attributes.put(key, type);
        }
        
        // Store ranges if needed
        if (type == Type.RANGE || type == Type.BOOLEAN) {
            if (!this.attribute_ranges.containsKey(key)) {
                this.attribute_ranges.put(key, new HashSet<String>());
                if (type == Type.BOOLEAN) {
                    this.attribute_ranges.get(key).add(Boolean.toString(true));
                    this.attribute_ranges.get(key).add(Boolean.toString(false));
                }
            }
            this.attribute_ranges.get(key).add(val.toString());
        }
        
        int idx = this.attributes.indexOf(key);
        if (!this.txn_values.containsKey(txn_id)) {
            this.txn_values.put(txn_id, new Vector<Object>(this.attributes.size()));
        }
        this.txn_values.get(txn_id).setSize(this.attributes.size());
        this.txn_values.get(txn_id).set(idx, val);
    }
    
    protected List<Object> getFeatures(String txn_id) {
        return (this.txn_values.get(txn_id));
    }

    protected List<Object> getFeatures(TransactionTrace txn_trace) {
        return (this.getFeatures(txn_trace.getTransactionId()));
    }

    
    public void load(String path) {
        
        
    }
    
    /**
     * Write out the data set to a file
     * @param path
     * @throws IOException
     */
    public void save(String path, String name) throws IOException {
        LOG.debug("Writing FeatureSet contents to '" + path + "'");
        
        // Attributes
        FastVector attrs = new FastVector();
        for (Entry<String, Type> e : this.attributes.entrySet()) {
            Attribute a = null;
            
            if (e.getValue() == Type.RANGE) {
                FastVector range_values = new FastVector();
                for (String v : this.attribute_ranges.get(e.getKey())) {
                    range_values.addElement(v);
                } // FOR
                a = new Attribute(e.getKey(), range_values);
            } else {
                a = new Attribute(e.getKey());    
            }
            attrs.addElement(a);
        } // FOR

        Instances data = new Instances(name, attrs, 0);
        
        // Values
        for (Vector<Object> values : this.txn_values.values()) {
            double vals[] = new double[data.numAttributes()];
            for (int i = 0; i < vals.length; i++) {
                Object value = values.get(i);
                Type type = this.attributes.getValue(i);
                
                if (value == null) {
                    vals[i] = Instance.missingValue();
                } else {
                    switch (type) {
                        case NUMERIC:
                            vals[i] = ((Number)value).doubleValue();
                            break;
                        case STRING:
                            vals[i] = data.attribute(i).addStringValue(value.toString());
                            break;
                        case BOOLEAN:
                            vals[i] = data.attribute(i).indexOfValue(Boolean.toString((Boolean)value));
                            break;
                        case RANGE:
                            vals[i] = data.attribute(i).indexOfValue(value.toString());
                            break;
                        default:
                            assert(false) : "Unexpected attribute type " + type;
                    } // SWITCH
                }
                
            }
        } // FOR
        FileWriter out = new FileWriter(path);
    }
    
}
