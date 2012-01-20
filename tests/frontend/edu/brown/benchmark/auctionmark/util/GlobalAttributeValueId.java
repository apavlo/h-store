package edu.brown.benchmark.auctionmark.util;

import edu.brown.utils.CompositeId;

public class GlobalAttributeValueId extends CompositeId {

    private static final int COMPOSITE_BITS[] = {
        32, // GROUP_ATTRIBUTE_ID
        8,  // ID
    };
    
    private long group_attribute_id;
    private int id;
    
    public GlobalAttributeValueId(long group_attribute_id, int id) {
        this.group_attribute_id = group_attribute_id;
        this.id = id;
    }
    
    public GlobalAttributeValueId(GlobalAttributeGroupId group_attribute_id, int id) {
        this(group_attribute_id.encode(), id);
    }
    
    public GlobalAttributeValueId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (super.encode(COMPOSITE_BITS));
    }

    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, COMPOSITE_BITS);
        this.group_attribute_id = (int)values[0];
        this.id = (int)values[1];
    }

    @Override
    public long[] toArray() {
        return (new long[]{ this.group_attribute_id, this.id });
    }
    
    public GlobalAttributeGroupId getGroupAttributeId() {
        return new GlobalAttributeGroupId(this.group_attribute_id);
    }
    
    protected int getId() {
        return (this.id);
    }
}
