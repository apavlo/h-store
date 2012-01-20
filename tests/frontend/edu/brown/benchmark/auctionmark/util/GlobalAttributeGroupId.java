package edu.brown.benchmark.auctionmark.util;

import edu.brown.utils.CompositeId;

public class GlobalAttributeGroupId extends CompositeId {

    private static final int COMPOSITE_BITS[] = {
        16, // CATEGORY
        8,  // ID
        8   // COUNT
    };
    
    private int category_id;
    private int id;
    private int count;
    
    public GlobalAttributeGroupId(int category_id, int id, int count) {
        this.category_id = category_id;
        this.id = id;
        this.count = count;
    }
    
    public GlobalAttributeGroupId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (this.encode(COMPOSITE_BITS));
    }

    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, COMPOSITE_BITS);
        this.category_id = (int)values[0];
        this.id = (int)values[1];
        this.count = (int)values[2];
    }

    @Override
    public long[] toArray() {
        return (new long[]{ this.category_id, this.id, this.count });
    }
    
    public int getCategoryId() {
        return (this.category_id);
    }
    protected int getId() {
        return (this.id);
    }
    public int getCount() {
        return (this.count);
    }
}
