package edu.brown.benchmark.auctionmark.util;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.TimestampType;

import edu.brown.utils.CollectionUtil;

public class ItemInfo implements Cloneable {
    public final ItemId id;
    public final List<Bid> bids = new ArrayList<Bid>();
    
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
    public UserId sellerId;
    public UserId lastBidderId; // if null, then no bidder

    public ItemInfo(ItemId id) {
        this.id = id;
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
        this.sellerId = null;
        this.lastBidderId = null;
    }

    public Bid getNextBid(long id) {
        Bid b = new Bid(id);
        this.bids.add(b);
        assert(this.bids.size() <= this.num_bids);
        return (b);
    }
    
    public Bid getLastBid() {
        return (CollectionUtil.last(this.bids));
    }
    
    @Override
    public ItemInfo clone() {
        ItemInfo ret = null;
        try {
            ret = (ItemInfo)super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone " + this.getClass().getSimpleName(), ex);
        }
        return (ret);
    }
    
    public class Bid implements Cloneable {
        public final long id;
        public UserId bidderId;
        public float maxBid;
        public TimestampType createDate;
        public TimestampType updateDate;
        public boolean won;

        private Bid(long id) {
            this.id = id;
            this.bidderId = null;
            this.maxBid = 0;
            this.createDate = null;
            this.updateDate = null;
            this.won = false;
        }

        public ItemInfo getItemInfo() {
            return (ItemInfo.this);
        }
        
        @Override
        public Bid clone() {
            Bid ret = null;
            try {
                ret = (Bid)super.clone();
            } catch (CloneNotSupportedException ex) {
                throw new RuntimeException("Failed to clone " + this.getClass().getSimpleName(), ex);
            }
            return (ret);
        }
    }
}