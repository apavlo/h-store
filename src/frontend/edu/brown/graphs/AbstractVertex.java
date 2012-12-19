package edu.brown.graphs;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogKey;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.graph.DelegateForest;

/**
 * 
 * @author pavlo
 *
 */
public class AbstractVertex extends AbstractGraphElement {
    public enum Members {
        CATALOG_KEY,
        CATALOG_CLASS,
    }
    
    public String catalog_key;
    public Class<? extends CatalogType> catalog_class;
    protected transient CatalogType catalog_item;

    /**
     * Constructor
     */
    public AbstractVertex() {
        super();
    }
    
    public AbstractVertex(CatalogType catalog_item) {
        assert(catalog_item != null);
        this.catalog_item = catalog_item;
        this.catalog_key = CatalogKey.createKey(this.catalog_item);
        this.catalog_class = this.catalog_item.getClass();
    }
    
    /**
     * Copy constructor
     * @param graph
     * @param copy
     */
    public AbstractVertex(IGraph<? extends AbstractVertex, ? extends AbstractEdge> graph, AbstractVertex copy) {
        super(graph, copy);
        this.catalog_item = copy.catalog_item;
        this.catalog_key = CatalogKey.createKey(this.catalog_item);
    }
    
    @SuppressWarnings("unchecked")
    public <T extends CatalogType> T getCatalogItem() {
        assert(this.catalog_item != null) : "The catalog item object is null for " + this.catalog_key;
        return ((T)catalog_item);
    }
    
    public String getCatalogItemName() {
        return (CatalogKey.getNameFromKey(this.catalog_key));
    }
    
    public String getCatalogKey() {
        return (this.catalog_key);
    }
    
    @Override
    public String toString() {
        return (this.catalog_item.getName());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public String debug(IGraph<?, ?> graph) {
        String ret = super.debug(graph);
        if (graph instanceof DelegateForest) {
            ret += StringUtil.SPACER + StringUtil.SPACER + "PARENT: " + ((DelegateForest)graph).getParent(this) + "\n";
        }
        return (ret);
    }
    
//    @Override
//    public boolean equals(Object obj) {
//        return this.catalog_item.equals(obj);
//    }
    
    @Override
    public int hashCode() {
        return this.catalog_item.hashCode();
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    protected void toJSONStringImpl(JSONStringer stringer) throws JSONException {
        this.fieldsToJSONString(stringer, AbstractVertex.class, Members.values());
    }
    
    @Override
    protected void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException {
        this.fieldsFromJSONObject(object, catalog_db, AbstractVertex.class, Members.values());
        this.catalog_item = CatalogKey.getFromKey(catalog_db, this.catalog_key, this.catalog_class);
    }
} // END CLASS