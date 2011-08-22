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
    public ItemId id;
    public Float currentPrice;
    public TimestampType endDate;
    public long numBids = 0;
    public long status = AuctionMarkConstants.ITEM_STATUS_OPEN;
    
    public ItemInfo(ItemId id, Double currentPrice, TimestampType endDate, int numBids) {
        this.id = id;
        this.currentPrice = (currentPrice != null ? currentPrice.floatValue() : null);
        this.endDate = endDate;
        this.numBids = numBids;
    }

    public ItemInfo() {
        // For serialization
    }
    
    public ItemId getItemId() {
        return (this.id);
    }
    public UserId getSellerId() {
        return (this.id.getSellerId());
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
        return this.id.compareTo(o.id);
    }
    @Override
    public String toString() {
        return this.id.toString();
    }
    @Override
    public int hashCode() {
        return this.id.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ItemInfo) {
            return (this.id.equals(((ItemInfo)obj).id));
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