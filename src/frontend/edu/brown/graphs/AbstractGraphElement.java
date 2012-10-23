package edu.brown.graphs;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.utils.NotImplementedException;

import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public abstract class AbstractGraphElement implements JSONSerializable, Comparable<AbstractGraphElement> {
//    private static final Logger LOG = Logger.getLogger(AbstractGraphElement.class.getName());
    
    private Map<IGraph<?, ?>, Map<String, Object>> attributes;
    private Long element_id;
    private transient boolean enable_verbose = false;
    private static final AtomicLong NEXT_ELEMENT_ID = new AtomicLong(1000); 
    
    public AbstractGraphElement() {
        // Nothing...
    }
    
    /**
     * Copies the attributes from all the graphs from another vertex into
     * the attribute set for this vertex
     * @param graph
     * @param copy
     */
    public AbstractGraphElement(IGraph<?, ?> graph, AbstractGraphElement copy) {
        this();
        if (copy != null) {
            for (IGraph<?, ?> copy_graph : copy.attributes.keySet()) {
                for (String key : copy.getAttributes(copy_graph)) {
                    Object value = copy.getAttribute(copy_graph, key);
                    this.setAttribute(graph, key, value);
                } // FOR
            } // FOR
        }
    }
    
    private Map<IGraph<?, ?>, Map<String, Object>> lazyAttributeAllocation() {
        if (this.attributes == null) {
            synchronized (this) {
                if (this.attributes == null) {
                    this.attributes = new HashMap<IGraph<?, ?>, Map<String,Object>>();            
                }
            } // SYNCH
        }
        return (this.attributes);
    }
    
    @Override
    public int compareTo(AbstractGraphElement o) {
        if (o != null) {
            Long id0 = this.getElementId();
            Long id1 = o.getElementId();
            return (id0.compareTo(id1));
        }
        return -1;
    }
    
    private long computeElementId() {
        return (NEXT_ELEMENT_ID.getAndIncrement());
    }
    
    public Long getElementId() {
        if (this.element_id == null) {
            synchronized (this) {
                if (this.element_id == null) this.element_id = computeElementId();
            } // SYNCH
        }
        return this.element_id;
    }
    

    public Set<String> getAttributes(IGraph<?, ?> graph) {
        this.lazyAttributeAllocation();
        Set<String> ret = null;
        if (this.attributes.containsKey(graph)) {
            ret = this.attributes.get(graph).keySet();
        }
        return (ret);
    }
    
    public Map<IGraph<?, ?>, Map<String, Object>> getAllAttributeValues() {
        this.lazyAttributeAllocation();
        return (this.attributes);
    }
    
    public Map<String, Object> getAttributeValues(IGraph<?, ?> graph) {
        this.lazyAttributeAllocation();
        return (this.attributes.get(graph));
    }
    
    public boolean hasAttribute(IGraph<?, ?> graph, String key) {
        this.lazyAttributeAllocation();
        return (this.attributes.containsKey(graph) && this.attributes.get(graph).containsKey(key));
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(IGraph<?, ?> graph, String key) {
        this.lazyAttributeAllocation();
        return (this.attributes.containsKey(graph) ? (T)this.attributes.get(graph).get(key) : null);
    }
    
    @SuppressWarnings("unchecked")
    public <T, E extends Enum<?>> T getAttribute(IGraph<?, ?> graph, E e) {
        return ((T)this.getAttribute(graph, e.name()));
    }
    
    public <T> void setAttribute(IGraph<?, ?> graph, String key, T value) {
        this.lazyAttributeAllocation();
        if (!this.attributes.containsKey(graph)) {
            this.attributes.put(graph, new HashMap<String, Object>());
        }
        this.attributes.get(graph).put(key, value);
    }
    
    public <T, E extends Enum<?>> void setAttribute(IGraph<?, ?> graph, E key, T value) {
        this.setAttribute(graph, key.name(), value);
    }
    
    public void copyAttributes(IGraph<?, ?> graph0, IGraph<?, ?> graph1) {
        this.lazyAttributeAllocation();
        for (String key : this.getAttributes(graph0)) {
            this.setAttribute(graph1, key, this.getAttribute(graph0, key));
        } // FOR
    }
    
    /**
     * Enable verbose output when toString() is called
     * @param enableVerbose
     */
    public void setVerbose(boolean enableVerbose) {
        this.enable_verbose = enableVerbose;
    }
    
    public boolean getVerbose() {
        return (this.enable_verbose);
    }
    
    public String debug() {
        this.lazyAttributeAllocation();
        String ret = this.getClass().getSimpleName() + "{" + this.toString() + "}\n";
        for (IGraph<?, ?> graph : this.attributes.keySet()) {
            ret += this.debug(graph);
        } // FOR
        return (ret);
    }
    
    public String debug(IGraph<?, ?> graph) {
        this.lazyAttributeAllocation();
        String ret = StringUtil.SPACER + graph + "\n";
        for (String key : this.attributes.get(graph).keySet()) {
            ret += StringUtil.SPACER + StringUtil.SPACER + key + ": " + this.attributes.get(graph).get(key) + "\n";
        } // FOR
        return (ret);
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    protected abstract void toJSONStringImpl(JSONStringer stringer) throws JSONException;
    protected abstract void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException;
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        this.toJSONStringImpl(stringer);
        stringer.key("ELEMENT_ID").value(this.getElementId());
    }
    
    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        throw new NotImplementedException("Cannot load a " + this.getClass().getSimpleName() + " from a file");
    }
    
    @Override
    public void save(File output_path) throws IOException {
        throw new NotImplementedException("Cannot save a " + this.getClass().getSimpleName() + " to a file");
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
    protected <E extends Enum<?>> void fieldsToJSONString(JSONStringer stringer, Class<? extends AbstractGraphElement> base_class, E members[]) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, base_class, members);
    }
    
    @Override
    public void fromJSON(JSONObject object, Database catalog_db) throws JSONException {
        this.element_id = object.getLong("ELEMENT_ID");
        NEXT_ELEMENT_ID.set(this.element_id);
        this.fromJSONObjectImpl(object, catalog_db);
    }
    
    /**
     * For the given enum, load in the values from the JSON object into the current object
     * @param <E>
     * @param object
     * @param catalog_db
     * @param members
     * @throws JSONException
     */
    protected <E extends Enum<?>> void fieldsFromJSONObject(JSONObject object, Database catalog_db, Class<? extends AbstractGraphElement> base_class, E members[]) throws JSONException {
        JSONUtil.fieldsFromJSON(object, catalog_db, this, base_class, members);
    }
}