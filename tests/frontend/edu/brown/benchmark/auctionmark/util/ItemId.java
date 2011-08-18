package edu.brown.benchmark.auctionmark.util;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

/**
 * Composite Item Id
 * First 48-bits are the seller's USER.U_ID
 * Last 16-bits are the item counter for this particular user
 * @author pavlo
 */
public class ItemId extends CompositeId {

    private static final long BASE_VALUE_MASK = 281474976710655l; // 2^48-1
    private static final int VALUE_OFFSET = 48;
    
    
    private static final long ITEM_ID_MASK = 0x0FFFFFFFFFFFFFFFl; 
    
    public static long getUniqueElementId(long item_id, int idx) {
        return ((long) idx << 60) | (item_id & ITEM_ID_MASK);
    }
    
    private UserId seller_id;
    private int item_ctr;
    
    /**
     * We can keep a transient copy of the current price of this item in here
     * It's just easier this way
     */
    private transient Double currentPrice = null;
    
    public ItemId() {
        // For serialization
    }
    
    public ItemId(UserId seller_id, int item_ctr) {
        this.seller_id = seller_id;
        this.item_ctr = item_ctr;
    }
    
    public ItemId(long seller_id, int item_ctr) {
        this(new UserId(seller_id), item_ctr);
    }
    
    public ItemId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (this.encode(BASE_VALUE_MASK, VALUE_OFFSET));
    }
    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, new long[3], BASE_VALUE_MASK, VALUE_OFFSET);
        this.seller_id = new UserId(values[0]);
        this.item_ctr = (int)values[1];
    }
    @Override
    public long[] toArray() {
        return (new long[]{ this.seller_id.encode(), this.item_ctr });
    }
    
    /**
     * Return the user id portion of this ItemId
     * @return the user_id
     */
    public UserId getSellerId() {
        return (this.seller_id);
    }

    /**
     * Return the item counter id for this user in the ItemId
     * @return the item_ctr
     */
    public int getItemCtr() {
        return (this.item_ctr);
    }
    
    public boolean hasCurrentPrice() {
        return (this.currentPrice != null);
    }
    public double getCurrentPrice() {
        return this.currentPrice;
    }
    public void setCurrentPrice(double price) {
        this.currentPrice = price;
    }
    
    @Override
    public String toString() {
        return ("ItemId<" + this.getItemCtr() + "-" + this.getSellerId() + ">");
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ItemId) {
            ItemId o = (ItemId)obj;
            return (this.seller_id == o.seller_id &&
                    this.item_ctr == o.item_ctr);
        }
        return (false);
    }

    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        super.toJSON(stringer);
        stringer.key("PRICE").value(this.currentPrice);
    }
    
    @Override
    public void fromJSON(JSONObject jsonObject, Database catalogDb) throws JSONException {
        super.fromJSON(jsonObject, catalogDb);
        if (jsonObject.has("PRICE") && jsonObject.isNull("PRICE") == false) {
            this.currentPrice = jsonObject.getDouble("PRICE");
        }
    }
}