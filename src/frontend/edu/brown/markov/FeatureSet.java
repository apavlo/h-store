package edu.brown.markov;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.utils.VoltTypeUtil;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.features.AbstractFeature;
import edu.brown.markov.features.BasePartitionFeature;
import edu.brown.markov.features.FeatureUtil;
import edu.brown.markov.features.TransactionIdFeature;
import edu.brown.statistics.HistogramUtil;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.workload.TransactionTrace;

public class FeatureSet implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(FeatureSet.class);
    private final static LoggerBoolean debug = new LoggerBoolean();
    private final static LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public enum Members {
        TXN_VALUES,
        ATTRIBUTES,
        LAST_NUM_ATTRIBUTES,
        ATTRIBUTE_HISTOGRAMS,
        ATTRIBUTE_TYPES,
    }
    
    public enum Type {
        NUMERIC,
        STRING,
        RANGE,
        BOOLEAN,
    }

    /**
     * The row values for each txn record
     */
    public final Map<Long, Vector<Object>> txn_values = new ListOrderedMap<Long, Vector<Object>>();
    
    /**
     * The list of attributes that each txn should have
     */
    public final ListOrderedMap<String, Type> attributes = new ListOrderedMap<String, Type>();
    public int last_num_attributes = 0;
    
    /**
     * Attribute Value Histograms
     */
    public final Map<String, ObjectHistogram> attribute_histograms = new HashMap<String, ObjectHistogram>();
    
    /**
     * 
     */
    public final Map<String, VoltType> attribute_types = new HashMap<String, VoltType>();
    
    /**
     * Constructor
     */
    public FeatureSet() {
        // Nothing for now...
    }

    /**
     * Total number of attributes stored in this FeatureSet
     */
    public int getAttributeCount() {
        return (this.attributes.size());
    }
    
    /**
     * Total number of transactions that we have extracted features for
     */
    public int getTransactionCount() {
        return (this.txn_values.size());
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
     * Returns true if the given key has been included in the set of attributes
     * @param key
     * @return
     */
    public boolean hasFeature(String key) {
        return (this.attributes.containsKey(key));
    }

    /**
     * Return the index in the list of attributes for the given key
     * @param key
     * @return
     */
    public Integer getFeatureIndex(String key) {
        int idx = this.attributes.indexOf(key); 
        return (idx != -1 ? idx : null);
    }

    /**
     * Return the indexes of all the features for the given prefix
     * @param feature_class
     * @return
     */
    public Set<Integer> getFeatureIndexes(Class<? extends AbstractFeature> feature_class) {
        Set<Integer> ret = new HashSet<Integer>();
        String prefix = FeatureUtil.getFeatureKeyPrefix(feature_class);
        for (int i = 0, cnt = this.attributes.size(); i < cnt; i++) {
            String key = this.attributes.get(i);
            if (key.startsWith(prefix)) ret.add(i); 
        } // FOR
        return (ret);
    }
    
    public List<Object> getFeatureValues(Long txn_id) {
        return (this.txn_values.get(txn_id));
    }

    public List<Object> getFeatureValues(TransactionTrace txn_trace) {
        return (this.getFeatureValues(txn_trace.getTransactionId()));
    }

    @SuppressWarnings("unchecked")
    public <T> T getFeatureValue(Long txn_id, String key) {
        int idx = this.attributes.indexOf(key);
        return ((T)this.txn_values.get(txn_id).get(idx));
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getFeatureValue(TransactionTrace txn_trace, String key) {
        return ((T)this.getFeatureValue(txn_trace.getTransactionId(), key));
    }
    
    /**
     * 
     * @param txn
     * @param key
     * @param val
     * @param type
     */
    public synchronized void addFeature(TransactionTrace txn, String key, Object val, Type type) {
        long txn_id = txn.getTransactionId();
        
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
            if (debug.val) LOG.debug("Adding new attribute " + key + " [" + type + "]");
            this.attributes.put(key, type);
            this.attribute_histograms.put(key, new ObjectHistogram());
            this.attribute_types.put(key, VoltType.NULL);
        }
        // HACK
        if (val != null && (val.getClass().equals(int.class) || val.getClass().equals(Integer.class))) {
            val = new Long((Integer)val);
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
            if (trace.val) LOG.trace("Creating new feature vector for " + txn_id);
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
            if (trace.val) LOG.trace("Increased FeatureSet size to " + this.last_num_attributes + " attributes");
        }
        this.txn_values.get(txn_id).set(idx, val);
        
        if (val != null && this.attribute_types.get(key) == VoltType.NULL) {
            this.attribute_types.put(key, VoltType.typeFromClass(val.getClass()));
        }
        
        if (trace.val) LOG.trace(txn_id + ": " + key + " => " + val);
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
    
    /**
     * Export this FeatureSet to a Weka Instances data set
     * @param name
     * @param normalize
     * @param prefix_include
     * @return
     */
    @SuppressWarnings("unchecked")
    public Instances export(String name, boolean normalize, Collection<String> prefix_include) {
        // Figure out what attributes we want to export
        Set<String> export_attrs = new ListOrderedSet<String>();
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
        if (debug.val) LOG.debug("# of Attributes to Export: " + export_attrs.size());
        
        List<Map<Object, Double>> normalized_values = null;
        Set<String> normalize_ignore = new HashSet<String>();
        if (normalize) {
            normalize_ignore.add(FeatureUtil.getFeatureKeyPrefix(TransactionIdFeature.class));
            normalize_ignore.add(FeatureUtil.getFeatureKeyPrefix(BasePartitionFeature.class));
            
            if (debug.val) LOG.debug("Normalizing values!");
            normalized_values = new ArrayList<Map<Object,Double>>();
            for (String key : export_attrs) {
                if (normalize_ignore.contains(key) == false) {
                    normalized_values.add(HistogramUtil.normalize(this.attribute_histograms.get(key)));
                } else {
                    normalized_values.add(null);
                }
            } // FOR
        }
        
        // Attributes
        FastVector attrs = new FastVector();
        for (String key : export_attrs) {
            Type type = this.attributes.get(key);
            Attribute a = null;
            boolean normalize_attr = (normalize && normalize_ignore.contains(key) == false);

            // Normalized values will always just be numeric
            if (normalize_attr) {
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
                boolean normalize_attr = (normalize && normalize_ignore.contains(key) == false);

                // Null => Missing Value Placeholder
                if (value == null) {
                    instance[i] = Instance.missingValue();
                // Normalized
                } else if (normalize_attr) {
                    assert(normalized_values != null);
                    try {
                        instance[i] = normalized_values.get(i).get(value);
                    } catch (Exception ex) {
                        System.err.println(normalized_values.get(i));
                        LOG.fatal("Failed to get normalized value '" + value + "' for Attribute '" + key + "'");
                        throw new RuntimeException(ex);
                    }
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
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, FeatureSet.class, FeatureSet.Members.values());
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // First deserialize all of the fields except for TXN_VALUES
        Set<Members> fields = CollectionUtil.getAllExcluding(FeatureSet.Members.values(), FeatureSet.Members.TXN_VALUES);
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, FeatureSet.class, fields.toArray(new Members[0]));
        
        // Then we have reconstruct this mofo ourselves because the object types are implicit
        JSONObject inner_obj = json_object.getJSONObject(Members.TXN_VALUES.name());
        assert(inner_obj != null);
        Iterator<String> it = inner_obj.keys();
        while (it.hasNext()) {
            String inner_key = it.next();
            long txn_id = Long.valueOf(inner_key);
            this.txn_values.put(txn_id, new Vector<Object>());
            
            JSONArray inner_arr = inner_obj.getJSONArray(inner_key);
            for (int i = 0, cnt = inner_arr.length(); i < cnt; i++) {
                Object val = null;
                if (inner_arr.isNull(i) == false) {
                    String json_val = inner_arr.getString(i);
                    String attr_key = this.attributes.get(i);
                    Type attr_type = this.attributes.getValue(i);
                    try {
                        switch (attr_type) {
                            case NUMERIC:
                                val = VoltTypeUtil.getObjectFromString(this.attribute_types.get(attr_key), json_val);
                                break;
                            case BOOLEAN:
                                val = Boolean.valueOf(json_val);
                                break;
                            case RANGE: {
                                // Get the value type from the histogram
                                ObjectHistogram h = this.attribute_histograms.get(attr_key);
                                assert(h != null);
                                VoltType volt_type = h.getEstimatedType();
                                assert(volt_type != VoltType.INVALID);
                                val = VoltTypeUtil.getObjectFromString(volt_type, json_val);
                                break;
                            }
                            case STRING:
                                val = json_val;
                                break;
                            default:
                                assert(false) : "Unexpected Type: " + attr_type;
                        } // SWITCH
                    } catch (Exception ex) {
                        LOG.fatal("Failed to deserialize TXN_VALUES-" + inner_key + "-" + i);
                        throw new JSONException(ex);
                    }
                }
                this.txn_values.get(txn_id).add(val);
            } // FOR
        } // WHILE
    }
}
