package edu.brown.benchmark.ebay.model;

import org.voltdb.types.TimestampType;

public class Bid implements Cloneable {
    public long id;
    public long itemId;
    public long sellerId;
    public long bidderId;
    public float maxBid;
    public TimestampType createDate;
    public TimestampType updateDate;
    public boolean won;

    public Bid() {
        this.id = -1;
        this.itemId = -1;
        this.sellerId = -1;
        this.bidderId = -1;
        this.maxBid = 0;
        this.createDate = null;
        this.updateDate = null;
        this.won = false;
    }

    @Override
    public Bid clone() {
        Bid ret = new Bid();
        ret.id = this.id;
        ret.itemId = this.itemId;
        ret.sellerId = this.sellerId;
        ret.bidderId = this.bidderId;
        ret.maxBid = this.maxBid;
        ret.createDate = this.createDate;
        ret.updateDate = this.updateDate;
        ret.won = this.won;
        return ret;
    }
}
