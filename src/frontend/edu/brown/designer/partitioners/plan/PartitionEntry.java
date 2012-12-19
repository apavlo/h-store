package edu.brown.designer.partitioners.plan;

import java.io.File;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.types.PartitionMethodType;

import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * 
 */
public abstract class PartitionEntry<T extends CatalogType> implements JSONSerializable {

    /**
     * 
     */
    public PartitionMethodType method;

    /**
     * 
     */
    public T attribute;

    /**
     * Empty Constructor for Deserialization
     */
    public PartitionEntry() {
        // Nothing to see, move along...
    }

    /**
     * Full Constructor
     * 
     * @param method
     * @param attributes
     * @param mapping
     * @param parent
     */
    public PartitionEntry(PartitionMethodType method, T attribute) {
        this.method = method;
        this.attribute = attribute;
    }

    /**
     * @param method
     */
    public PartitionEntry(PartitionMethodType method) {
        this(method, null);
    }

    /**
     * @return the method
     */
    public PartitionMethodType getMethod() {
        return this.method;
    }

    public void setMethod(PartitionMethodType method) {
        this.method = method;
    }

    /**
     * @return
     */
    @SuppressWarnings("unchecked")
    public <U extends CatalogType> U getAttribute() {
        return ((U) this.attribute);
    }

    /**
     * @param attribute
     */
    public void setAttribute(T attribute) {
        this.attribute = attribute;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof PartitionEntry<?>)) {
            return (false);
        }
        PartitionEntry<?> other = (PartitionEntry<?>) obj;

        // Method
        if (this.method != other.method) {
            return (false);
        }
        // Attribute
        if (this.attribute == null) {
            if (other.attribute != null)
                return (false);
        } else if (!this.attribute.equals(other.attribute)) {
            return (false);
        }

        return (true);
    }

    @Override
    public String toString() {
        return (this.getClass().getName() + "{" + this.toString(",") + "}");
    }

    public String toString(String delimiter) {
        String ret = "";
        ret += "method=" + this.method + delimiter;
        ret += "attribute=" + this.attribute;
        return (ret);
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

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
        JSONUtil.fieldsToJSON(stringer, this, PartitionEntry.class, JSONUtil.getSerializableFields(this.getClass()));
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // Set<PartitionEntry.Members> members =
        // CollectionUtil.getAllExcluding(PartitionEntry.Members.values(),
        // PartitionEntry.Members.METHOD);
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, PartitionEntry.class, true, JSONUtil.getSerializableFields(this.getClass()));
        // this.method =
        // PartitionMethodType.get(json_object.getString(Members.METHOD.name()));
    }
} // END CLASS
