package edu.brown.markov.containers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogKey;
import edu.brown.graphs.exceptions.InvalidGraphElementException;
import edu.brown.hashing.AbstractHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * Convenience wrapper for a collection of Procedure-based MarkovGraphs that are split on some unique id 
 * <Id> -> <Procedure> -> <MarkovGraph> 
 * @author pavlo
 */
public class MarkovGraphsContainer implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(MarkovGraphsContainer.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public enum Members {
        MARKOVS,
        CLASSNAME,
        ;
    }
    
    protected AbstractHasher hasher;
    
    /**
     * 
     */
    private final Map<Integer, Map<Procedure, MarkovGraph>> markovs = Collections.synchronizedMap(new TreeMap<Integer, Map<Procedure, MarkovGraph>>());
    
    /**
     * The procedures that we actually want to load. If this is null, then we will load everything
     */
    private final Set<Procedure> load_procedures;
    
    // -----------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------

    /**
     * Base Constructor
     * @param procedures
     */
    public MarkovGraphsContainer(Collection<Procedure> procedures) {
        if (procedures != null) {
            this.load_procedures = new HashSet<Procedure>();
            this.load_procedures.addAll(procedures);
        } else {
            this.load_procedures = null;
        }
    }
    
    public MarkovGraphsContainer() {
        this(null);
    }
    
    // -----------------------------------------------------------------
    // UTILITY METHODS
    // -----------------------------------------------------------------
    
    public MarkovGraph getFromGraphId(int id) {
        for (MarkovGraph m : this.getAll()) {
            if (m.getGraphId() == id) return (m);
        } // FOR
        return (null);
    }

    public AbstractHasher getHasher() {
        return (this.hasher);
    }
    public void setHasher(AbstractHasher hasher) {
        this.hasher = hasher;
    }
    public boolean isGlobal() {
        return (false);
    }

    // -----------------------------------------------------------------
    // PSEUDO-MAP METHODS
    // -----------------------------------------------------------------
    
    public void clear() {
        this.markovs.clear();
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
            synchronized (this) {
                markov = this.get(id, catalog_proc);
                if (markov == null) {
                    if (debug.val)
                        LOG.warn(String.format("Creating a new %s MarkovGraph for id %d",
                                 catalog_proc.getName(), id));
                    markov = new MarkovGraph(catalog_proc);
                    if (initialize) markov.initialize();
                    this.put(id, markov);
                }
            } // SYNCH
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
        assert(id != null) : "Invalid id";
        Map<Procedure, MarkovGraph> inner = this.markovs.get(id);
        if (inner == null) {
            synchronized (this.markovs) {
                inner = this.markovs.get(id);
                if (inner == null) {
                    inner = new ConcurrentHashMap<Procedure, MarkovGraph>();
                }
            } // SYNCH
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
    public MarkovGraph getFromParams(Long txn_id, int base_partition, Object params[], Procedure catalog_proc) {
        assert(catalog_proc != null);
        MarkovGraph m = this.getOrCreate(base_partition, catalog_proc, true);
        if (m == null) {
            LOG.warn(String.format("Failed to find MarkovGraph for %s txn #%d [base_partition=%d, params=%s]",
                                   catalog_proc.getName(), txn_id, base_partition, Arrays.toString(params)));
            LOG.warn("MarkovGraphsContainer Dump:\n" + this);
        }
        
        return (m);
    }

    /**
     * Invoke MarkovGraph.calculateProbabilities() for all of the graphs stored within this container 
     */
    public void calculateProbabilities(PartitionSet partitions) {
        for (Map<Procedure, MarkovGraph> inner : this.markovs.values()) {
            for (Entry<Procedure, MarkovGraph> e : inner.entrySet()) {
                MarkovGraph m = e.getValue();
                m.calculateProbabilities(partitions);
                boolean is_valid = m.isValid();
                if (is_valid == false) {
                    try {
                        File dump = new File("/tmp/" + e.getKey().getName() + ".markovs"); 
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
        this.markovs.putAll(other.markovs);
    }
    
    public Set<Integer> keySet() {
        return this.markovs.keySet();
    }
    
    public Set<Entry<Integer, Map<Procedure, MarkovGraph>>> entrySet() {
        return this.markovs.entrySet();
    }
    
    public int size() {
        return (this.markovs.size());
    }
    
    public int totalSize() {
        int total = 0;
        for (Integer id : this.markovs.keySet()) {
            Map<Procedure, MarkovGraph> m = this.markovs.get(id);
            if (m != null) total += m.size();
        } // FOR
        return (total);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public String toString() {
        int num_ids = this.markovs.size();
        Map<String, Object> maps[] = (Map<String, Object>[])new Map<?, ?>[num_ids+1];
        int i = 0;
        
        maps[i] = new ListOrderedMap<String, Object>();
        maps[i].put("Number of Ids", num_ids);
        
        for (Integer id : this.markovs.keySet()) {
            Map<Procedure, MarkovGraph> m = this.markovs.get(id);
            
            maps[++i] = new ListOrderedMap<String, Object>();
            maps[i].put("ID", "#" + id);
            maps[i].put("Number of Procedures", m.size());
            for (Entry<Procedure, MarkovGraph> e : m.entrySet()) {
                MarkovGraph markov = e.getValue();
                String val = String.format("[Vertices=%d, Recomputed=%d, Accuracy=%.4f]",
                                           markov.getVertexCount(), markov.getRecomputeCount(), markov.getAccuracyRatio());
                maps[i].put("   " + e.getKey().getName(), val);
            } // FOR
        } // FOR
        
        return StringUtil.formatMaps(maps);
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
        // CLASSNAME
        stringer.key(Members.CLASSNAME.name()).value(this.getClass().getCanonicalName());
        
        // MARKOV GRAPHS
        stringer.key(Members.MARKOVS.name()).object();
        for (Integer id : this.markovs.keySet()) {
            // Roll through each id and create a new JSONObject per id
            if (debug.val)
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
        // MARKOV GRAPHS
        JSONObject json_inner = json_object.getJSONObject(Members.MARKOVS.name());
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (String id_key : CollectionUtil.iterable(json_inner.keys())) {
            final Integer id = Integer.valueOf(id_key);
            final JSONObject json_procs = json_inner.getJSONObject(id_key);
            assert(json_procs != null);
            
            for (final String proc_key : CollectionUtil.iterable(json_procs.keys())) {
                final Procedure catalog_proc = CatalogKey.getFromKey(catalog_db, proc_key, Procedure.class);
                assert(catalog_proc != null);
                if (this.load_procedures != null && this.load_procedures.contains(catalog_proc) == false) {
                    if (debug.val) LOG.debug(String.format("Skipping MarkovGraph [id=%d, proc=%s]", id, catalog_proc.getName()));
                    continue;
                }
                
                runnables.add(new Runnable() {
                    @Override
                    public void run() {
                        if (trace.val) LOG.trace(String.format("Loading MarkovGraph [id=%d, proc=%s]",
                                                                 id, catalog_proc.getName()));
                        JSONObject json_graph = null;
                        try {
                            json_graph = new JSONObject(Encoder.hexDecodeToString(json_procs.getString(proc_key)));
                            MarkovGraph markov = new MarkovGraph(catalog_proc);
                            markov.fromJSON(json_graph, catalog_db);
                            MarkovGraphsContainer.this.put(id, markov);
                            markov.buildCache();
                        } catch (Throwable ex) {
                            throw new RuntimeException("Failed to load MarkovGraph " + id + " for " + catalog_proc.getName(), ex);
                        }
                    }
                });
            } // FOR (proc key)
        } // FOR (id key)
        if (debug.val) LOG.debug(String.format("Going to wait for %d MarkovGraphs to load", runnables.size())); 
        ThreadUtil.runGlobalPool(runnables);
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG,
                     ArgumentsParser.PARAM_MARKOV);
        
        Map<Integer, MarkovGraphsContainer> all_markovs = MarkovUtil.load(args.catalogContext,
                                                                          args.getFileParam(ArgumentsParser.PARAM_MARKOV));
        int cnt_invalid = 0;
        int cnt_total = 0;
        boolean save = true;
        for (Integer p : all_markovs.keySet()) {
            MarkovGraphsContainer m = all_markovs.get(p);
            LOG.info(String.format("[%s] Validating %d MarkovGraphs for partition %d", m.getClass().getSimpleName(), m.size(), p));
            
            for (Integer id : m.keySet()) {
                for (MarkovGraph markov : m.getAll(id).values()) {
                    boolean dump = false;
                    String before = MarkovUtil.exportGraphviz(markov, true, false, true, null).export(markov.getProcedure().getName());
                    try {
                        markov.calculateProbabilities(args.catalogContext.getAllPartitionIds());
                        markov.validate();
                        if (markov.getGraphId() == 10014) dump = true;
                    } catch (InvalidGraphElementException ex) {
                        cnt_invalid++;
                        LOG.error(String.format("[%d] %-16s - %s", markov.getGraphId(), markov.getProcedure().getName(), ex.getMessage()));
                        dump = true;
                        throw ex;
                    } finally {
                        if (dump) {
                            LOG.warn("BEFORE DUMPED: " + FileUtil.writeStringToFile("/tmp/before.dot", before));
                            LOG.warn("AFTER DUMPED: " + MarkovUtil.exportGraphviz(markov, true, false, true, null).writeToTempFile(markov.getProcedure()));
                        }
                    }
                    cnt_total++;
                }
            } // FOR
        }
        LOG.info("VALID: " + (cnt_total - cnt_invalid) + " / "+ cnt_total);
        if (save && cnt_invalid == 0) {
            MarkovGraphsContainerUtil.save(all_markovs, args.getFileParam(ArgumentsParser.PARAM_MARKOV));
        }
    }
}
