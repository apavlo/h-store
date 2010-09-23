package edu.brown.benchmark.ebay.model;

import java.util.Date;

import org.voltdb.types.TimestampType;

public class ItemInfo implements Cloneable {
    public short num_bids;
    public short num_images;
    public short num_attributes;
    public short num_comments;
    public short num_watches;
    public boolean still_available;
    public TimestampType startDate;
    public TimestampType endDate;
    public TimestampType purchaseDate;
    public float initialPrice;
    public float currentPrice;
    public Long seller_id;
    public Long last_bidder_id; // if null, then no bidder

    public ItemInfo() {
        this.num_bids = 0;
        this.num_images = 0;
        this.num_attributes = 0;
        this.num_comments = 0;
        this.num_watches = 0;
        this.still_available = true;
        this.startDate = null;
        this.endDate = null;
        this.purchaseDate = null;
        this.initialPrice = 0;
        this.currentPrice = 0;
        this.seller_id = -1l;
        this.last_bidder_id = -1l;
    }

    @Override
    public ItemInfo clone() {
        ItemInfo ret = new ItemInfo();
        ret.num_bids = this.num_bids;
        ret.num_images = this.num_images;
        ret.num_attributes = this.num_attributes;
        ret.num_comments = this.num_comments;
        ret.num_watches = this.num_watches;
        ret.still_available = this.still_available;
        ret.startDate = this.startDate;
        ret.endDate = this.endDate;
        ret.purchaseDate = this.purchaseDate;
        ret.initialPrice = this.initialPrice;
        ret.currentPrice = this.currentPrice;
        ret.seller_id = this.seller_id;
        ret.last_bidder_id = this.last_bidder_id;
        return (ret);
    }
}