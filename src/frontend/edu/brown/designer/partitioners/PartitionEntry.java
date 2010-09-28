package edu.brown.designer.partitioners;

import java.io.IOException;
import java.util.Set;

import org.apache.log4j.Logger;

import org.json.*;
import org.voltdb.catalog.*;
import org.voltdb.types.PartitionMethodType;

import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * 
 */
public class PartitionEntry implements JSONSerializable {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(PartitionEntry.class);
    
    public enum Members {
        METHOD,
        ATTRIBUTE,
        PARENT,
        PARENT_ATTRIBUTE,
        SINGLE_PARTITION,
    }
    
    public PartitionMethodType method;
    public CatalogType attribute;
    
    // Table Information
    public Column parent_attribute;
    public Table parent;
    
    // Procedure Information
    public Boolean single_partition;
    
    /**
     * Empty Constructor for Deserialization
     */
    public PartitionEntry() {
        // Nothing to see, move along...
    }

    /**
     * Full Constructor
     * @param method
     * @param attributes
     * @param mapping
     * @param parent
     */
    public PartitionEntry(PartitionMethodType method, CatalogType attribute, Table parent, Column parent_attribute) {
        this.method = method;
        this.attribute = attribute;
        this.parent = parent;
        this.parent_attribute = parent_attribute;
        
        // Sanity checks
        switch (this.method) {
            case MAP:
                assert(this.parent != null);
                assert(this.parent_attribute != null);
            case HASH:
                assert(this.attribute != null) : this.method + " - Missing attribute";
                assert(this.parent == null) : this.method + " - Unexpected attribute: " + this.parent;
                assert(this.parent_attribute == null) : this.method + " - Unexpected parent attribute: " + this.parent_attribute;
                break;
            case REPLICATION:
            case NONE:
                assert(this.attribute == null) : this.method + " - Unexpected attribute: " + this.attribute;
                assert(this.parent == null) : this.method + " - Unexpected attribute: " + this.parent;
                assert(this.parent_attribute == null) : this.method + " - Unexpected parent attribute: " + this.parent_attribute;
                break;
            default:
                assert(false) : "Unexpected PartitionMethodType => " + this.method;
        } // SWITCH
    }
    
    /**
     * 
     * @param method
     */
    public PartitionEntry(PartitionMethodType method) {
        this(method, null, null, null);
    }
    
    /**
     * 
     * @param method
     * @param attributes
     */
    public PartitionEntry(PartitionMethodType method, CatalogType attribute) {
        this(method, attribute, null, null);
    }

    /**
     * Should the table for this entry be replicated? 
     * @return
     */
    public boolean isReplicated() {
        return (this.method == PartitionMethodType.REPLICATION);
    }
    
    /**
     * Is the procedure this entry guaranteed to be single-partition?
     * @return
     */
    public Boolean isSinglePartition() {
        return single_partition;
    }
    public void setSinglePartition(boolean singlePartition) {
        this.single_partition = singlePartition;
    }
    
    /**
     * @return the parent
     */
    public Table getParent() {
        return this.parent;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(Table parent) {
        this.parent = parent;
    }

    /**
     * @return the method
     */
    public PartitionMethodType getMethod() {
        return this.method;
    }

    /**
     * 
     * @param method
     */
    public void setMethod(PartitionMethodType method) {
        this.method = method;
    }
    
    /**
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public <U extends CatalogType> U getAttribute() {
        return ((U)this.attribute);
    }
    
    /**
     * 
     * @param attribute
     */
    public void setAttribute(CatalogType attribute) {
        this.attribute = attribute;
    }
    
    /**
     * 
     * @return
     */
    public CatalogType getParentAttribute() {
        return this.parent_attribute;
    }
    
    /**
     * 
     * @param parent_attribute
     */
    public void setParentAttribute(Column parent_attribute) {
        this.parent_attribute = parent_attribute;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionEntry)) {
            return (false);
        }
        PartitionEntry other = (PartitionEntry)obj;
        
        // Method
        if (this.method != other.method) {
            return (false);
        } 
        // Attribute
        if (this.attribute == null) {
            if (other.attribute != null) return (false);
        } else if (!this.attribute.equals(other.attribute)) {
            return (false);
        }
        // Parent Attribute
        if (this.parent_attribute == null) {
            if (other.parent_attribute != null) return (false);
        } else if (!this.parent_attribute.equals(other.parent_attribute)) {
            return (false);
        }
        // Parent
        if (this.parent == null) {
            if (other.parent != null) return (false);
        } else if (!this.parent.equals(other.parent)) {
            return (false);
        }
        // SinglePartition
        if (this.single_partition == null) {
            if (other.single_partition != null) return (false);
        } else if (!this.single_partition.equals(other.single_partition)) {
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
        ret += "attribute=" + this.attribute + delimiter;
        ret += "parent=" + this.parent + delimiter;
        ret += "parent_attribute=" + this.parent_attribute + delimiter;
        ret += "single_partition=" + this.single_partition;
        return (ret);
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

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
        JSONUtil.fieldsToJSON(stringer, this, PartitionEntry.class, PartitionEntry.Members.values());
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        Set<PartitionEntry.Members> members = CollectionUtil.getAllExcluding(PartitionEntry.Members.values(), PartitionEntry.Members.METHOD);
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, PartitionEntry.class, true, members.toArray(new Members[0]));
        this.method = PartitionMethodType.get(json_object.getString(Members.METHOD.name()));
    }
} // END CLASS
