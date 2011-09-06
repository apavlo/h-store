package edu.brown.benchmark.auctionmark.util;

import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class ItemInfo implements JSONSerializable, Comparable<ItemInfo> {
    public ItemId itemId;
    public Float currentPrice;
    public TimestampType endDate;
    public long numBids = 0;
    public long status = AuctionMarkConstants.ITEM_STATUS_OPEN;
    
    public ItemInfo(ItemId id, Double currentPrice, TimestampType endDate, int numBids) {
        this.itemId = id;
        this.currentPrice = (currentPrice != null ? currentPrice.floatValue() : null);
        this.endDate = endDate;
        this.numBids = numBids;
    }

    public ItemInfo() {
        // For serialization
    }
    
    public ItemId getItemId() {
        return (this.itemId);
    }
    public UserId getSellerId() {
        return (this.itemId.getSellerId());
    }
    public boolean hasCurrentPrice() {
        return (this.currentPrice != null);
    }
    public Float getCurrentPrice() {
        return currentPrice;
    }
    public boolean hasEndDate() {
        return (this.endDate != null);
    }
    public TimestampType getEndDate() {
        return endDate;
    }
    
    @Override
    public int compareTo(ItemInfo o) {
        return this.itemId.compareTo(o.itemId);
    }
    @Override
    public String toString() {
        return this.itemId.toString();
    }
    @Override
    public int hashCode() {
        return this.itemId.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ItemInfo) {
            return (this.itemId.equals(((ItemInfo)obj).itemId));
        }
        return (false);
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        
    }
    @Override
    public void save(String output_path) throws IOException {
        
    }
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, ItemInfo.class, JSONUtil.getSerializableFields(ItemInfo.class));
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, ItemInfo.class, true, JSONUtil.getSerializableFields(ItemInfo.class));
    }
}