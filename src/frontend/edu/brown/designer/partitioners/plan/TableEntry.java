package edu.brown.designer.partitioners.plan;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;

import edu.brown.catalog.special.ReplicatedColumn;

public class TableEntry extends PartitionEntry<Column> {

    // Table Information
    public Column parent_attribute;
    public Table parent;

    public TableEntry() {
        // For serialization
    }

    /**
     * Full Constructor
     * 
     * @param method
     * @param attributes
     * @param mapping
     * @param parent
     */
    public TableEntry(PartitionMethodType method, Column catalog_col, Table parent, Column parent_attribute) {
        super(method, catalog_col);
        this.parent = parent;
        this.parent_attribute = parent_attribute;

        // Sanity checks
        switch (this.method) {
            case MAP:
                assert (this.parent != null);
                assert (this.parent_attribute != null);
                break;
            case HASH:
                assert (this.attribute != null) : this.method + " - Missing attribute";
                assert (this.parent == null) : this.method + " - Unexpected attribute: " + this.parent;
                assert (this.parent_attribute == null) : this.method + " - Unexpected parent attribute: " + this.parent_attribute;
                break;
            case REPLICATION:
                if (this.attribute instanceof ReplicatedColumn)
                    this.attribute = null;
            case NONE:
                assert (this.attribute == null) : this.method + " - Unexpected attribute: " + this.attribute;
                assert (this.parent == null) : this.method + " - Unexpected attribute: " + this.parent;
                assert (this.parent_attribute == null) : this.method + " - Unexpected parent attribute: " + this.parent_attribute;
                break;
            default:
                assert (false) : "Unexpected PartitionMethodType => " + this.method;
        } // SWITCH
    }

    public TableEntry(PartitionMethodType method, Column catalog_col) {
        this(method, catalog_col, null, null);
    }

    /**
     * Should the table for this entry be replicated?
     * 
     * @return
     */
    public boolean isReplicated() {
        return (this.method == PartitionMethodType.REPLICATION);
    }

    /**
     * @return the parent
     */
    public Table getParent() {
        return this.parent;
    }

    /**
     * @param parent
     *            the parent to set
     */
    public void setParent(Table parent) {
        this.parent = parent;
    }

    /**
     * @return
     */
    public CatalogType getParentAttribute() {
        return this.parent_attribute;
    }

    /**
     * @param parent_attribute
     */
    public void setParentAttribute(Column parent_attribute) {
        this.parent_attribute = parent_attribute;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof TableEntry)) {
            return (false);
        }
        TableEntry other = (TableEntry) obj;

        // Parent Attribute
        if (this.parent_attribute == null) {
            if (other.parent_attribute != null)
                return (false);
        } else if (!this.parent_attribute.equals(other.parent_attribute)) {
            return (false);
        }
        // Parent
        if (this.parent == null) {
            if (other.parent != null)
                return (false);
        } else if (!this.parent.equals(other.parent)) {
            return (false);
        }

        return (super.equals(other));
    }
}
