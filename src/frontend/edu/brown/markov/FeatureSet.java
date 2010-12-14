package edu.brown.markov;

import java.util.*;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ClassUtil;
import edu.brown.workload.TransactionTrace;

public class FeatureSet {
    private static final Logger LOG = Logger.getLogger(FeatureSet.class);
    
    public enum Type {
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
    protected int last_num_attributes = 0;
    
    /**
     * Attribute Value Histograms
     */
    protected final Map<String, Histogram> attribute_histograms = new HashMap<String, Histogram>();
    
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

    
    protected List<Object> getFeatures(String txn_id) {
        return (this.txn_values.get(txn_id));
    }

    protected List<Object> getFeatures(TransactionTrace txn_trace) {
        return (this.getFeatures(txn_trace.getTransactionId()));
    }
    
    /**
     * 
     * @param txn
     * @param key
     * @param val
     * @param type
     */
    public synchronized void addFeature(TransactionTrace txn, String key, Object val, Type type) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
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
            if (debug) LOG.debug("Adding new attribute " + key + " [" + type + "]");
            this.attributes.put(key, type);
            this.attribute_histograms.put(key, new Histogram());
        }
        
        // Always store the values in a histogram so we can normalize them later on
        try {
            this.attribute_histograms.get(key).put(val);
        } catch (Exception ex) {
            LOG.error("\n" + this.attribute_histograms.get(key));
            LOG.error("Invalid value '" + val + "' for attribute '" + key + "'", ex);
            System.exit(1);
        }

        // Now add the values into this txn's feature vector
        int idx = this.attributes.indexOf(key);
        int num_attributes = this.attributes.size();
        Vector<Object> values = this.txn_values.get(txn_id); 
        if (values == null) {
            if (trace) LOG.trace("Creating new feature vector for " + txn_id);
            values = new Vector<Object>(num_attributes);
            values.setSize(num_attributes);
            this.txn_values.put(txn_id, values);
        }
        if (num_attributes != this.last_num_attributes) {
            assert(num_attributes > this.last_num_attributes);
            for (Vector<Object> v : this.txn_values.values()) {
                v.setSize(num_attributes);
            } // FOR
            this.last_num_attributes = num_attributes;
            if (trace) LOG.trace("Increased FeatureSet size to " + this.last_num_attributes + " attributes");
        }
        this.txn_values.get(txn_id).set(idx, val);
        if (trace) LOG.trace(txn_id + ": " + key + " => " + val);
    }

    /**
     * Export the FeatureSet to a Weka Instances
     * @param name
     * @return
     */
    public Instances export(String name) {
        return (this.export(name, false, this.attributes.keySet()));
    }

    public Instances export(String name, boolean normalize) {
        return (this.export(name, normalize, this.attributes.keySet()));
    }
    
    public Instances export(String name, boolean normalize, Collection<String> prefix_include) {
        final boolean debug = LOG.isDebugEnabled();
        
        // Figure out what attributes we want to export
        SortedSet<String> export_attrs = new TreeSet<String>();
        for (String key : this.attributes.keySet()) {
            boolean include = false;
            for (String prefix : prefix_include) {
                if (key.startsWith(prefix)) {
                    include = true;
                    break;
                }
            } // FOR
            if (include) export_attrs.add(key);
        } // FOR
        if (debug) LOG.debug("# of Attributes to Export: " + export_attrs.size());
        
        List<SortedMap<Object, Double>> normalized_values = null;
        if (normalize) {
            if (debug) LOG.debug("Normalizing values!");
            normalized_values = new ArrayList<SortedMap<Object,Double>>();
            for (String key : export_attrs) {
                normalized_values.add(this.attribute_histograms.get(key).normalize()); 
            } // FOR
        }
        
        // Attributes
        FastVector attrs = new FastVector();
        for (String key : export_attrs) {
            Type type = this.attributes.get(key);
            Attribute a = null;

            // Normalized values will always just be numeric
            if (normalize) {
                a = new Attribute(key);
                
            // Otherwise we can play games with ranges and strings
            } else {
                switch (type) {
                    case RANGE:
                    case BOOLEAN: {
                        FastVector range_values = new FastVector();
                        for (Object v : this.attribute_histograms.get(key).values()) {
                            range_values.addElement(v.toString());
                        } // FOR
                        a = new Attribute(key, range_values);
                        break;
                    }
                    case STRING:
                        a = new Attribute(key, (FastVector)null);
                        break;
                    default:
                        a = new Attribute(key);
                } // SWITCH
            }
            attrs.addElement(a);
        } // FOR
        assert(attrs.size() == export_attrs.size());

        Instances data = new Instances(name, attrs, 0);
        
        // Instance Values
        for (Vector<Object> values : this.txn_values.values()) {
            double instance[] = new double[data.numAttributes()];
            int i = 0;
            for (String key : export_attrs) {
                int attr_idx = this.attributes.indexOf(key);
                Object value = values.get(attr_idx);
                Type type = this.attributes.getValue(attr_idx);

                // Null => Missing Value Placeholder
                if (value == null) {
                    instance[i] = Instance.missingValue();
                // Normalized
                } else if (normalize) {
                    assert(normalized_values != null);
                    instance[i] = normalized_values.get(i).get(value);
                // Actual Values
                } else {
                    switch (type) {
                        case NUMERIC:
                            instance[i] = ((Number)value).doubleValue();
                            break;
                        case STRING:
                            instance[i] = data.attribute(i).addStringValue(value.toString());
                            break;
                        case BOOLEAN:
                            instance[i] = data.attribute(i).indexOfValue(Boolean.toString((Boolean)value));
                            break;
                        case RANGE:
                            instance[i] = data.attribute(i).indexOfValue(value.toString());
                            break;
                        default:
                            assert(false) : "Unexpected attribute type " + type;
                    } // SWITCH
                }
                i += 1;
            } // FOR
            data.add(new Instance(1.0, instance));
        } // FOR
        
        return (data);
    }
}
