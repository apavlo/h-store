package edu.brown.graphs;

import java.io.IOException;
import java.util.*;

import org.apache.log4j.Logger;
import org.json.*;
import org.voltdb.catalog.Database;
import org.voltdb.utils.NotImplementedException;

import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public abstract class AbstractGraphElement implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(AbstractGraphElement.class.getName());
    public static final String DEBUG_SPACER = "  ";
    
    private final Map<IGraph<?, ?>, Map<String, Object>> attributes = new HashMap<IGraph<?, ?>, Map<String,Object>>();
    private Long element_id;

    public AbstractGraphElement() {
        this.element_id = (long)super.hashCode();
    }
    
    public Long getElementId() {
        return element_id;
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
    
    public Set<String> getAttributes(IGraph<?, ?> graph) {
        Set<String> ret = null;
        if (this.attributes.containsKey(graph)) {
            ret = this.attributes.get(graph).keySet();
        }
        return (ret);
    }
    
    public Map<IGraph<?, ?>, Map<String, Object>> getAllAttributeValues() {
        return (this.attributes);
    }
    
    public Map<String, Object> getAttributeValues(IGraph<?, ?> graph) {
        return (this.attributes.get(graph));
    }
    
    public boolean hasAttribute(IGraph<?, ?> graph, String key) {
        return (this.attributes.containsKey(graph) && this.attributes.get(graph).containsKey(key));
    }
    
    public Object getAttribute(IGraph<?, ?> graph, String key) {
        return (this.attributes.containsKey(graph) ? this.attributes.get(graph).get(key) : null);
    }
    
    public void setAttribute(IGraph<?, ?> graph, String key, Object value) {
        if (!this.attributes.containsKey(graph)) {
            this.attributes.put(graph, new HashMap<String, Object>());
        }
        this.attributes.get(graph).put(key, value);
    }
    
    public void copyAttributes(IGraph<?, ?> graph0, IGraph<?, ?> graph1) {
        for (String key : this.getAttributes(graph0)) {
            this.setAttribute(graph1, key, this.getAttribute(graph0, key));
        } // FOR
    }
    
    public String debug() {
        String ret = this.getClass().getSimpleName() + "{" + this.toString() + "}\n";
        for (IGraph<?, ?> graph : this.attributes.keySet()) {
            ret += this.debug(graph);
        } // FOR
        return (ret);
    }
    
    public String debug(IGraph<?, ?> graph) {
        String ret = DEBUG_SPACER + graph + "\n";
        for (String key : this.attributes.get(graph).keySet()) {
            ret += DEBUG_SPACER + DEBUG_SPACER + key + ": " + this.attributes.get(graph).get(key) + "\n";
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
    public void load(String input_path, Database catalog_db) throws IOException {
        throw new NotImplementedException("Cannot load a " + this.getClass().getSimpleName() + " from a file");
    }
    
    @Override
    public void save(String output_path) throws IOException {
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
    @SuppressWarnings("unchecked")
    protected <E extends Enum<?>> void fieldsFromJSONObject(JSONObject object, Database catalog_db, Class<? extends AbstractGraphElement> base_class, E members[]) throws JSONException {
        JSONUtil.fieldsFromJSON(object, catalog_db, this, base_class, members);
    }
}