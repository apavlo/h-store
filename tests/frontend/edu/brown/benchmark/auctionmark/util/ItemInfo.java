package edu.brown.benchmark.auctionmark.util;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.TimestampType;

import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;

public class ItemInfo implements Cloneable {
    public final ItemId id;
    private final List<Bid> bids = new ArrayList<Bid>();
    private Histogram<UserId> bidderHistogram = new Histogram<UserId>();
    
    public short numBids;
    public short numImages;
    public short numAttributes;
    public short numComments;
    public short num_watches;
    public boolean still_available;
    public TimestampType start_date;
    public TimestampType end_date;
    public TimestampType purchaseDate;
    public float initialPrice;
    public float currentPrice;
    public UserId sellerId;
    public UserId lastBidderId; // if null, then no bidder

    public ItemInfo(ItemId id) {
        this.id = id;
        this.numBids = 0;
        this.numImages = 0;
        this.numAttributes = 0;
        this.numComments = 0;
        this.num_watches = 0;
        this.still_available = true;
        this.start_date = null;
        this.end_date = null;
        this.purchaseDate = null;
        this.initialPrice = 0;
        this.currentPrice = 0;
        this.sellerId = null;
        this.lastBidderId = null;
    }

    public Bid getNextBid(long id, UserId bidder_id) {
        assert(bidder_id != null);
        Bid b = new Bid(id, bidder_id);
        this.bids.add(b);
        assert(this.bids.size() <= this.numBids);
        this.bidderHistogram.put(bidder_id);
        return (b);
    }
    
    public Bid getLastBid() {
        return (CollectionUtil.last(this.bids));
    }
    
    public Histogram<UserId> getBidderHistogram() {
        return bidderHistogram;
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
        public final UserId bidderId;
        public float maxBid;
        public TimestampType createDate;
        public TimestampType updateDate;

        private Bid(long id, UserId bidderId) {
            this.id = id;
            this.bidderId = bidderId;
            this.maxBid = 0;
            this.createDate = null;
            this.updateDate = null;
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