package edu.brown.markov;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogKey;
import edu.brown.hashing.AbstractHasher;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * Convenience wrapper for a collection of Procedure-based MarkovGraphs that are split on some unique id 
 * <Id> -> <Procedure> -> <MarkovGraph> 
 * @author pavlo
 */
public class MarkovGraphsContainer implements JSONSerializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(MarkovGraphsContainer.class);

    public enum Members {
        MARKOVS,
        FEATURES,
        GLOBAL,
        CLASSNAME,
        ;
    }
    
    protected AbstractHasher hasher;
    
    private boolean global;
    
    /**
     * 
     */
    private final SortedMap<Integer, Map<Procedure, MarkovGraph>> markovs = new TreeMap<Integer, Map<Procedure, MarkovGraph>>();
    
    /**
     * 
     */
    private final Map<Procedure, List<String>> features = new HashMap<Procedure, List<String>>();
    
    
    /**
     * The procedures that we actually want to load. If this is null, then we will load everything
     */
    private final Set<Procedure> load_procedures;
    
    // -----------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------

    /**
     * Base Constructor
     * @param global
     * @param procedures
     */
    public MarkovGraphsContainer(boolean global, Collection<Procedure> procedures) {
        this.global = global;
        if (procedures != null) {
            this.load_procedures = new HashSet<Procedure>();
            this.load_procedures.addAll(procedures);
        } else {
            this.load_procedures = null;
        }
    }
    
    /**
     * 
     * @param global
     */
    public MarkovGraphsContainer(boolean global) {
        this(global, null);
    }

    /**
     * 
     * @param procedures
     */
    public MarkovGraphsContainer(Collection<Procedure> procedures) {
        this(false, procedures);
    }
    
    public MarkovGraphsContainer() {
        this(false);
    }
    
    // -----------------------------------------------------------------
    // PSEUDO-MAP METHODS
    // -----------------------------------------------------------------
    
    public void setHasher(AbstractHasher hasher) {
        this.hasher = hasher;
    }
    
    public boolean isGlobal() {
        return global;
    }
    
    
    public void clear() {
        this.markovs.clear();
        this.features.clear();
    }
    
    public MarkovGraph get(Integer id, Procedure catalog_proc) {
        Map<Procedure, MarkovGraph> inner = this.markovs.get(id);
        return (inner != null ? inner.get(catalog_proc) : null);
    }
    
    /**
     * Get or create the MarkovGraph for the given id/procedure pair
     * If initialize is set to true, then when we have to create the graph we will call initialize() 
     * @param id
     * @param catalog_proc
     * @param initialize
     * @return
     */
    public MarkovGraph getOrCreate(Integer id, Procedure catalog_proc, boolean initialize) {
        MarkovGraph markov = this.get(id, catalog_proc);
        if (markov == null) {
            markov = new MarkovGraph(catalog_proc);
            this.put(id, markov);
            if (initialize) markov.initialize();
        }
        return (markov);
    }
    
    /**
     * Get or create the MarkovGraph for the given id+catalog_proc
     * Does not automatically initialize a new graph by default
     * @param id
     * @param catalog_proc
     * @return
     */
    public MarkovGraph getOrCreate(Integer id, Procedure catalog_proc) {
        return (this.getOrCreate(id, catalog_proc, false));
    }
    
    public void put(Integer id, MarkovGraph markov) {
        Map<Procedure, MarkovGraph> inner = this.markovs.get(id);
        if (inner == null) {
            inner = new HashMap<Procedure, MarkovGraph>();
            this.markovs.put(id, inner);
        }
        inner.put(markov.getProcedure(), markov);
    }
    
    /**
     * 
     * @param txn_id
     * @param base_partition
     * @param params
     * @param catalog_proc
     * @return
     */
    public MarkovGraph getFromParams(long txn_id, int base_partition, Object params[], Procedure catalog_proc) {
        assert(catalog_proc != null);
        MarkovGraph m = null;
        if (this.global) {
            m = this.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID, catalog_proc);
        } else {
            List<String> proc_features = this.features.get(catalog_proc);
            if (proc_features == null) {
                m = this.getOrCreate(base_partition, catalog_proc, true);
//                m = this.get(base_partition, catalog_proc);
            } else {
                assert(false) : "To be implemented!";
            }
        }
        if (m == null) {
            LOG.warn(String.format("Failed to find MarkovGraph for %s txn #%d [base_partition=%d, params=%s]",
                                   catalog_proc.getName(), txn_id, base_partition, Arrays.toString(params)));
            LOG.warn("MarkovGraphsContainer Dump:\n" + this);
        }
        
        return (m);
    }
    
    
    public void setFeatureKeys(Procedure catalog_proc, List<String> keys) {
        List<String> inner = this.features.get(catalog_proc);
        if (inner == null) {
            inner = new ArrayList<String>();
            this.features.put(catalog_proc, inner);
        }
        inner.addAll(keys);
    }
    
    public List<String> getFeatureKeys(Procedure catalog_proc) {
        return (this.features.get(catalog_proc));
    }
    
    /**
     * Invoke MarkovGraph.calculateProbabilities() for all of the graphs stored within this container 
     */
    public void calculateProbabilities() {
        for (Map<Procedure, MarkovGraph> inner : this.markovs.values()) {
            for (Entry<Procedure, MarkovGraph> e : inner.entrySet()) {
                MarkovGraph m = e.getValue();
                m.calculateProbabilities();
                boolean is_valid = m.isValid();
                if (is_valid == false) {
                    try {
                        String dump = "/tmp/" + e.getKey().getName() + ".markovs"; 
                        m.save(dump);
                        System.err.println("DUMP: " + dump);
                        System.err.println("GRAPHVIZ: " + MarkovUtil.exportGraphviz(e.getValue(), false, null).writeToTempFile(e.getKey()));
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
                assert(is_valid) : "Failed to calculate probabilities for " + e.getKey();
            } // FOR
        } // FOR
    }
    
    protected Map<Procedure, MarkovGraph> getAll(Integer id) {
        return (this.markovs.get(id));
    }
    
    public Map<Integer, MarkovGraph> getAll(Procedure catalog_proc) {
        Map<Integer, MarkovGraph> ret = new HashMap<Integer, MarkovGraph>();
        for (Integer id : this.markovs.keySet()) {
            MarkovGraph m = this.markovs.get(id).get(catalog_proc);
            if (m != null) ret.put(id, m);
        } // FOR
        return (ret);
    }
    
    /**
     * Get all the MarkovGraphs contained within this object
     * @return
     */
    public Set<MarkovGraph> getAll() {
        Set<MarkovGraph> ret = new HashSet<MarkovGraph>();
        for (Integer id : this.markovs.keySet()) {
            Map<Procedure, MarkovGraph> m = this.markovs.get(id);
            if (m != null && m.isEmpty() == false) ret.addAll(m.values());
        } // FOR
        return (ret);
    }
    
    public void copy(MarkovGraphsContainer other) {
        this.features.putAll(other.features);
        this.markovs.putAll(other.markovs);
    }
    
    protected Set<Integer> keySet() {
        return this.markovs.keySet();
    }
    
    protected Set<Entry<Integer, Map<Procedure, MarkovGraph>>> entrySet() {
        return this.markovs.entrySet();
    }
    
    public int size() {
        return (this.markovs.size());
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
        int num_ids = this.markovs.size();
        Map maps[] = new Map[num_ids+1];
        int i = 0;
        
        maps[i] = new ListOrderedMap<String, Object>();
        maps[i].put("Number of Ids", num_ids);
        maps[i].put("Global", this.global);
        
        for (Integer id : this.markovs.keySet()) {
            Map<Procedure, MarkovGraph> m = this.markovs.get(id);
            
            i += 1;
            maps[i] = new ListOrderedMap<String, Object>();
            maps[i].put("ID", id);
            maps[i].put("Number of Procedures", m.size());
            for (Entry<Procedure, MarkovGraph> e : m.entrySet()) {
                maps[i].put("   " + e.getKey().getName(), e.getValue().getVertexCount());
            } // FOR
        } // FOR
        
        return StringUtil.formatMaps(maps);
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        // CLASSNAME
        stringer.key(Members.CLASSNAME.name()).value(this.getClass().getCanonicalName());
        
        // IS GLOBAL
        stringer.key(Members.GLOBAL.name()).value(this.global);
        
        // FEATURE KEYS
        stringer.key(Members.FEATURES.name()).object();
        for (Procedure catalog_proc : this.features.keySet()) {
            stringer.key(CatalogKey.createKey(catalog_proc)).array();
            for (String feature_key : this.features.get(catalog_proc)) {
                stringer.value(feature_key);
            } // FOR (feature)
            stringer.endArray();
        } // FOR (procedure)
        stringer.endObject();

        // MARKOV GRAPHS
        stringer.key(Members.MARKOVS.name()).object();
        for (Integer id : this.markovs.keySet()) {
            // Roll through each id and create a new JSONObject per id
            LOG.debug("Serializing " + this.markovs.get(id).size() + " graphs for id " + id);
            stringer.key(id.toString()).object();
            for (Entry<Procedure, MarkovGraph> e : this.markovs.get(id).entrySet()) {
                // Optimization: Hex-encode all of the MarkovGraphs so that we don't get crushed
                // when trying to read them all back at once when we create the JSONObject
                try {
                    FastSerializer fs = new FastSerializer(false, false); // C++ needs little-endian
                    fs.write(e.getValue().toJSONString().getBytes());
                    String hexString = fs.getHexEncodedBytes();
                    stringer.key(CatalogKey.createKey(e.getKey())).value(hexString);
                } catch (Exception ex) {
                    String msg = String.format("Failed to serialize %s MarkovGraph for Id %d", e.getKey(), id);
                    LOG.fatal(msg);
                    throw new JSONException(ex);
                }
            } // FOR
            stringer.endObject();
        } // FOR
        stringer.endObject();
    }

    @Override
    public void fromJSON(JSONObject json_object, final Database catalog_db) throws JSONException {
        final boolean d = LOG.isDebugEnabled(); 
        
        // IS GLOBAL
        this.global = json_object.getBoolean(Members.GLOBAL.name());
        
        // FEATURE KEYS
        JSONObject json_inner = json_object.getJSONObject(Members.FEATURES.name());
        for (String proc_key : CollectionUtil.wrapIterator(json_inner.keys())) {
            Procedure catalog_proc = CatalogKey.getFromKey(catalog_db, proc_key, Procedure.class);
            assert(catalog_proc != null);
            
            JSONArray json_arr = json_inner.getJSONArray(proc_key);
            List<String> feature_keys = new ArrayList<String>(); 
            for (int i = 0, cnt = json_arr.length(); i < cnt; i++) {
                feature_keys.add(json_arr.getString(i));
            } // FOR
            this.features.put(catalog_proc, feature_keys);
        } // FOR (proc key)
        
        // MARKOV GRAPHS
        json_inner = json_object.getJSONObject(Members.MARKOVS.name());
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (String id_key : CollectionUtil.wrapIterator(json_inner.keys())) {
            final Integer id = Integer.valueOf(id_key);
            // HACK
            if (id.equals(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) this.global = true;
        
            final JSONObject json_procs = json_inner.getJSONObject(id_key);
            assert(json_procs != null);
            
            for (final String proc_key : CollectionUtil.wrapIterator(json_procs.keys())) {
                final Procedure catalog_proc = CatalogKey.getFromKey(catalog_db, proc_key, Procedure.class);
                assert(catalog_proc != null);
                if (this.load_procedures != null && this.load_procedures.contains(catalog_proc) == false) {
                    if (d) LOG.debug(String.format("Skipping MarkovGraph [id=%d, proc=%s]", id, catalog_proc.getName()));
                    continue;
                }
                
                runnables.add(new Runnable() {
                    @Override
                    public void run() {
                        if (d) LOG.debug(String.format("Loading MarkovGraph [id=%d, proc=%s]", id, catalog_proc.getName()));
                        try {
                            JSONObject json_graph = new JSONObject(Encoder.hexDecodeToString(json_procs.getString(proc_key)));
                            MarkovGraph markov = new MarkovGraph(catalog_proc);
                            markov.fromJSON(json_graph, catalog_db);
                            MarkovGraphsContainer.this.put(id, markov);
                            markov.buildCache();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
            } // FOR (proc key)
        } // FOR (id key)
        if (d) LOG.debug(String.format("Going to wait for %d MarkovGraphs to load", runnables.size())); 
        ThreadUtil.runGlobalPool(runnables);
    }
}
