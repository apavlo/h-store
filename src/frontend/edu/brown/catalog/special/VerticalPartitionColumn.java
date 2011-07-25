package edu.brown.catalog.special;

import java.util.Collection;

import org.voltdb.catalog.Column;

import edu.brown.utils.StringUtil;

public class VerticalPartitionColumn extends MultiColumn {
    public static final String PREFIX = "*VerticalPartitionColumn*"; 
    
    /**
     * THIS SHOULD NOT BE CALLED DIRECTLY
     * Use VerticalPartitionColumn.get()
     * @param attributes
     */
    public VerticalPartitionColumn(Collection<MultiColumn> attributes) {
        super((Collection<? extends Column>)attributes);
        
        // There should only be two elements
        // The first one is the horizontal partitioning parameter(s) 
        // The second one is the vertical partitioning parameter(s)
        assert(attributes.size() == 2) : "Total # of Attributes = " + this.getSize() + ": " + StringUtil.join(",", this);
    }
    
    public static VerticalPartitionColumn get(MultiColumn hp_cols, MultiColumn vp_cols) {
        return InnerMultiAttributeCatalogType.get(VerticalPartitionColumn.class, (Column)hp_cols, (Column)vp_cols);
    }
    
    @Override
    public String getPrefix() {
        return (PREFIX);
    }

    /**
     * Return the Horizontal Partitioning Columns
     * @return
     */
    public Collection<Column> getHorizontalPartitionColumns() {
        return ((MultiColumn)this.get(0)).getAttributes();
    }

    /**
     * Return the Vertical Partitioning Columns
     * @return
     */
    public Collection<Column> getVerticalPartitionColumns() {
        return ((MultiColumn)this.get(1)).getAttributes();
    }
}
