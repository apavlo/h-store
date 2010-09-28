package edu.brown.benchmark.auctionmark.util;

/**
 * Composite Item Id
 * First 48-bits are the USER.U_ID
 * Last 16-bits are the item counter for this particular user
 * @author pavlo
 */
public class ItemId {

    private static final long BASE_ID_MASK = 281474976710655l; // 2^48-1
    private static final int USER_ID_OFFSET = 48;
    
    private final long user_id;
    private final int item_ctr;
    private final Long encoded;
    
    public ItemId(long user_id, int item_ctr) {
        this.user_id = user_id;
        this.item_ctr = item_ctr;
        this.encoded = ItemId.encode(new long[]{this.user_id, this.item_ctr});
    }
    
    public ItemId(long composite_id) {
        long values[] = ItemId.decode(composite_id);
        this.user_id = values[0];
        this.item_ctr = (int)values[1];
        this.encoded = composite_id;
    }
    
    /**
     * Return the user id portion of this ItemId
     * @return the user_id
     */
    public long getUserId() {
        return (this.user_id);
    }

    /**
     * Return the item counter id for this user in the ItemId
     * @return the item_ctr
     */
    public int getItemCtr() {
        return (this.item_ctr);
    }
    
    public long encode() {
        return (this.encoded);
    }

    protected static long encode(long...values) {
        assert(values.length == 2);
        return (values[0] | values[1]<<USER_ID_OFFSET);
    }
    
    protected static long[] decode(long composite_id) {
        long values[] = { composite_id & BASE_ID_MASK,
                          composite_id>>USER_ID_OFFSET };
        return (values);
    }
    
    @Override
    public String toString() {
        return ("ItemId<" + this.getItemCtr() + "-" + this.getUserId() + ">");
    }
    
    @Override
    public int hashCode() {
        return (this.encoded.hashCode());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ItemId) {
            ItemId o = (ItemId)obj;
            return (this.user_id == o.user_id &&
                    this.item_ctr == o.item_ctr);
        }
        return (false);
    }
}