/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.auctionmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.DefaultHasher;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution;
import edu.brown.rand.WrappingRandomDistribution;
import edu.brown.rand.RandomDistribution.DiscreteRNG;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public class AuctionMarkBenchmarkProfile implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(AuctionMarkBaseClient.class);

    public List<Long> user_ids;

    public enum Members {
        SCALE_FACTOR, TABLE_SIZES, ITEM_CATEGORY_HISTOGRAM, USER_IDS, USER_AVAILABLE_ITEMS, USER_WAIT_FOR_PURCHASE_ITEMS, USER_COMPLETE_ITEMS, ITEM_BID_MAP, ITEM_BUYER_MAP, GAG_GAV_MAP, GAG_GAV_HISTOGRAM
    };

    /**
     * Data Scale Factor
     */
    public long scale_factor;

    /**
     * Map from table names to the number of tuples we inserted during loading
     */
    public SortedMap<String, Long> table_sizes = new TreeMap<String, Long>();

    /**
     * Histogram for number of items per category (stored as category_id)
     */
    public Histogram item_category_histogram = new Histogram();

    /**
     * Three status types for an item 1. Available (The auction of this item is
     * still open) 2. Wait for purchase - The auction of this item is still
     * open. There is a bid winner and the bid winner has not purchased the
     * item. 3. Complete (The auction is closed and (There is no bid winner or
     * The bid winner has already purchased the itme))
     */
    public Histogram user_available_items_histogram;
    public Histogram user_wait_for_purchase_items_histogram;
    public Histogram user_complete_items_histogram;

    public Map<Long, List<Long>> user_available_items;
    public Map<Long, List<Long>> user_wait_for_purchase_items;
    public Map<Long, List<Long>> user_complete_items;
    public Map<Long, Long> item_bid_map;
    public Map<Long, Long> item_buyer_map;

    // Map from global attribute group to list of global attribute value
    public Map<Long, List<Long>> gag_gav_map;
    public Histogram gag_gav_histogram;

    // Map from user ID to total number of items that user sell
    // public Map<Long, Integer> user_total_items = new ConcurrentHashMap<Long,
    // Integer>();

    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public AuctionMarkBenchmarkProfile() {

        // Initialize table sizes
        for (String tableName : AuctionMarkConstants.TABLENAMES) {
            this.table_sizes.put(tableName, 0l);
        }

        // _lastUserId = this.getTableSize(AuctionMarkConstants.TABLENAME_USER);

        LOG.debug("AuctionMarkBenchmarkProfile :: constructor");

        this.user_ids = new ArrayList<Long>();

        this.user_available_items = new ConcurrentHashMap<Long, List<Long>>();
        this.user_available_items_histogram = new Histogram();

        this.user_wait_for_purchase_items = new ConcurrentHashMap<Long, List<Long>>();
        this.user_wait_for_purchase_items_histogram = new Histogram();

        this.user_complete_items = new ConcurrentHashMap<Long, List<Long>>();
        this.user_complete_items_histogram = new Histogram();

        this.item_bid_map = new ConcurrentHashMap<Long, Long>();
        this.item_buyer_map = new ConcurrentHashMap<Long, Long>();

        this.gag_gav_map = new ConcurrentHashMap<Long, List<Long>>();
        this.gag_gav_histogram = new Histogram();
    }

    // -----------------------------------------------------------------
    // SKEWING METHODS
    // -----------------------------------------------------------------

    private Integer current_tick = -1;
    private Integer window_total = null;
    private Integer window_size = null;
    private final Histogram window_histogram = new Histogram();
    private final Vector<Integer> window_partitions = new Vector<Integer>();
    {
        this.window_histogram.setKeepZeroEntries(true);
    }

    /**
     * @param window
     * @param num_ticks
     * @param total
     */
    public void enableTemporalSkew(int window, int total) {
        this.window_size = window;
        this.window_total = total;

        for (int p = 0; p < this.window_total; p++) {
            this.window_histogram.put(p, 0);
        }

        this.tick();
    }

    /**
     * 
     */
    public void tick() {
        this.current_tick++;
        if (this.window_size != null) {
            Integer last_partition = -1;
            if (this.window_partitions.isEmpty() == false) {
                last_partition = this.window_partitions.lastElement();
            }

            this.window_partitions.clear();
            for (int ctr = 0; ctr < this.window_size; ctr++) {
                last_partition++;
                if (last_partition > this.window_total) {
                    last_partition = 0;
                }
                this.window_partitions.add(last_partition);
            } // FOR
            System.err.println("Tick #" + this.current_tick + " Window: " + this.window_partitions);
            System.err.println(this.window_histogram);
            this.window_histogram.clearValues();
        }
    }

    private int getPartition(Long seller_id) {
        return (TheHashinator.hashToPartition(seller_id, this.window_total));
    }

    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Get the scale factor value for this benchmark profile
     * 
     * @return
     */
    public long getScaleFactor() {
        return (this.scale_factor);
    }

    /**
     * Set the scale factor for this benchmark profile
     * 
     * @param scale_factor
     */
    public void setScaleFactor(long scale_factor) {
        assert (scale_factor > 0) : "Invalid scale factor " + scale_factor;
        this.scale_factor = scale_factor;
    }

    public long getTableSize(String table_name) {
        return (this.table_sizes.get(table_name));
    }

    public void setTableSize(String table_name, long size) {
        this.table_sizes.put(table_name, size);
    }

    /**
     * Add the give tuple to the running to total for the table
     * 
     * @param table_name
     * @param size
     */
    public void addToTableSize(String table_name, long size) {
        Long orig_size = this.table_sizes.get(table_name);
        if (orig_size == null) {
            orig_size = 0l;
        }
        this.setTableSize(table_name, orig_size + size);
    }

    public void addUserId(long userId) {
        synchronized (this.user_ids) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("@@@ adding userId = " + userId);
            }
            this.user_ids.add(userId);
        }
    }

    public long getUserId(int index) {
        return this.user_ids.get(index);
    }

    public List<Long> getUserIds() {
        return this.user_ids;
    }

    public int getUserIdCount() {
        return (this.user_ids.size());
    }

    /*
     * Available item manipulators
     */
    public void addAvailableItem(long sellerId, long itemId) {
        synchronized (this.user_available_items) {
            List<Long> itemList = this.user_available_items.get(sellerId);
            if (null == itemList) {
                itemList = new LinkedList<Long>();
                itemList.add(itemId);
                this.user_available_items.put(sellerId, itemList);
                this.user_available_items_histogram.put(sellerId);
            } else if (!itemList.contains(itemId)) {
                itemList.add(itemId);
                this.user_available_items_histogram.put(sellerId);
            }
        }
    }

    public void removeAvailableItem(long sellerId, long itemId) {
        synchronized (this.user_available_items) {
            List<Long> itemList = this.user_available_items.get(sellerId);
            if (null != itemList && itemList.remove(new Long(itemId))) {
                this.user_available_items_histogram.remove(sellerId, 1);
                if (0 == itemList.size()) {
                    this.user_available_items.remove(sellerId);
                }
            }
        }
    }

    /**
     * Gets a random user ID within the client.
     * 
     * @param rng
     * @return
     */
    private Long getRandomUserId(AbstractRandomGenerator rng) {
        Long user_id = null;
        assert (this.user_ids.isEmpty() == false) : "The list of user ids is empty!";
        synchronized (this.user_ids) {
            int num_user_ids = this.user_ids.size();
            if (num_user_ids > 0) {
                RandomDistribution.DiscreteRNG randomUserIndex = this.CACHE_getRandomUserId.get(rng);
                if (randomUserIndex == null || randomUserIndex.getMax() != num_user_ids) {
                    // Do we really want everything to be Guassian??
                    randomUserIndex = new Gaussian(rng, 0, num_user_ids - 1);
                    this.CACHE_getRandomUserId.put(rng, randomUserIndex);
                }
                user_id = this.user_ids.get(randomUserIndex.nextInt());
            }
        }
        return (user_id);
    }

    private final Map<AbstractRandomGenerator, RandomDistribution.DiscreteRNG> CACHE_getRandomUserId = new HashMap<AbstractRandomGenerator, RandomDistribution.DiscreteRNG>();

    /**
     * Gets a random buyer ID within the client.
     * 
     * @param rng
     * @return
     */
    public Long getRandomBuyerId(AbstractRandomGenerator rng) {
        // We don't care about skewing the buyerIds at this point, so just get
        // one from getRandomUserId
        return (this.getRandomUserId(rng));
    }

    /**
     * Gets a random seller ID within the client.
     * 
     * @param rng
     * @return
     */
    public Long getRandomSellerId(AbstractRandomGenerator rng) {
        Long seller_id = null;
        Integer partition = null;

        while (true) {
            partition = null;
            seller_id = this.getRandomUserId(rng);

            // Bad mojo!
            if (seller_id == null)
                break;

            // If there is no skew, then we can just jump out right here!
            if (this.window_size == null)
                break;

            // Otherwise we need to skew this mother trucker..
            partition = this.getPartition(seller_id);
            if (this.window_partitions.contains(partition)) {
                break;
            }
        } // WHILE
        if (partition != null) {
            this.window_histogram.put(partition);
        }
        return (seller_id);
    }

    /**
     * @param rng
     * @return
     */
    public Long[] getRandomAvailableItemIdSellerIdPair(AbstractRandomGenerator rng) {
        Long sellerId = null;
        Long itemId = null;
        synchronized (this.user_available_items) {
            FlatHistogram randomSeller = new FlatHistogram(rng, this.user_available_items_histogram);
            Integer partition = null;
            while (true) {
                partition = null;
                sellerId = randomSeller.nextLong();
                // Uniform
                if (this.window_size == null) {
                    break;
                }

                // Temporal Skew
                partition = this.getPartition(sellerId);
                if (this.window_partitions.contains(partition)) {
                    break;
                }
            } // WHILE
            if (this.window_size != null) {
                this.window_histogram.put(partition);
            }

            long numAvailableItems = this.user_available_items_histogram.get(sellerId);
            itemId = this.user_available_items.get(sellerId).get(rng.number(0, (int) numAvailableItems - 1));
        } // SYNCHRONIZED
        return new Long[] { itemId, sellerId };
    }

    /*
     * Complete item manipulators
     */
    public void addCompleteItem(long sellerId, long itemId) {
        synchronized (this.user_complete_items) {
            List<Long> itemList = this.user_complete_items.get(sellerId);
            if (null == itemList) {
                itemList = new LinkedList<Long>();
                itemList.add(itemId);
                this.user_complete_items.put(sellerId, itemList);
                this.user_complete_items_histogram.put(sellerId);
            } else if (!itemList.contains(itemId)) {
                itemList.add(itemId);
                this.user_complete_items_histogram.put(sellerId);
            }
        }
    }

    /**
     * Returns a pair containing <ItemId, SellerId>
     * 
     * @param rng
     * @return
     */
    public Long[] getRandomCompleteItemIdSellerIdPair(AbstractRandomGenerator rng) {
        synchronized (this.user_complete_items) {
            FlatHistogram randomSeller = new FlatHistogram(rng, this.user_complete_items_histogram);
            Long sellerId = randomSeller.nextLong();
            long numCompleteItems = this.user_complete_items_histogram.get(sellerId);
            Long itemId = this.user_complete_items.get(sellerId).get(rng.number(0, (int) numCompleteItems - 1));
            Long[] ret = { itemId, sellerId };
            return ret;
        }
    }

    public void addWaitForPurchaseItem(long sellerId, long itemId, long bidId, long buyerId) {
        synchronized (this.user_wait_for_purchase_items) {
            List<Long> itemList = this.user_wait_for_purchase_items.get(sellerId);
            this.item_bid_map.put(itemId, bidId);
            this.item_buyer_map.put(itemId, buyerId);
            if (null == itemList) {
                itemList = new LinkedList<Long>();
                itemList.add(itemId);
                this.user_wait_for_purchase_items.put(sellerId, itemList);
                this.user_wait_for_purchase_items_histogram.put(sellerId);
            } else if (!itemList.contains(itemId)) {
                itemList.add(itemId);
                this.user_wait_for_purchase_items_histogram.put(sellerId);
            }
        }
    }

    public void removeWaitForPurchaseItem(long sellerId, long itemId) {
        synchronized (this.user_wait_for_purchase_items) {
            List<Long> itemList = this.user_wait_for_purchase_items.get(sellerId);
            if (null != itemList && itemList.remove(new Long(itemId))) {
                this.user_wait_for_purchase_items_histogram.remove(sellerId, 1);
                if (0 == itemList.size()) {
                    this.user_wait_for_purchase_items.remove(sellerId);
                    this.item_bid_map.remove(itemId);
                    this.item_buyer_map.remove(itemId);
                }
            }
        }
    }

    public Long[] getRandomWaitForPurchaseItemIdSellerIdPair(AbstractRandomGenerator rng) {
        synchronized (this.user_wait_for_purchase_items) {
            FlatHistogram randomSeller = new FlatHistogram(rng, this.user_wait_for_purchase_items_histogram);
            Long sellerId = randomSeller.nextLong();
            long numWaitForPurchaseItems = this.user_wait_for_purchase_items.get(sellerId).size();
            Long itemId = this.user_wait_for_purchase_items.get(sellerId).get(rng.number(0, (int) numWaitForPurchaseItems - 1));
            Long[] ret = { itemId, sellerId };
            return ret;
        }
    }

    public long getBidId(long itemId) {
        return this.item_bid_map.get(itemId);
    }

    public long getBuyerId(long itemId) {
        return this.item_buyer_map.get(itemId);
    }

    /**
     * Gets random buyer ID who has a bid in "Wait For Purchase" status. Note
     * that this method will decrement the number of bids in "Wait For Purchase"
     * status of a buyer whose ID is return.
     * 
     * @param rng
     *            the random generator
     * @return random buyer ID who has a bid in the "Wait For Purchase" status.
     */
    /*
     * public Long
     * getRandomBuyerWhoHasWaitForPurchaseBid(AbstractRandomGenerator rng){ int
     * randomIndex = rng.number(0, buyer_num_wait_for_purchase_bids.size() - 1);
     * int i=0; Long buyerId = null; int numBids = 0; for(Map.Entry<Long,
     * Integer> entry: buyer_num_wait_for_purchase_bids.entrySet()){ if(i++ ==
     * randomIndex){ buyerId = entry.getKey(); numBids = entry.getValue();
     * break; } } if(numBids > 1){ buyer_num_wait_for_purchase_bids.put(buyerId,
     * numBids - 1); } else { buyer_num_wait_for_purchase_bids.remove(buyerId);
     * } return buyerId; }
     */

    /**
     * Increments the number of bids in the "Wait For Purchase" status of a
     * given buyerId.
     * 
     * @param buyerId
     */
    /*
     * public void addBuyerWhoHasWaitForPurchaseBid(Long buyerId){ int numBids;
     * if(buyer_num_wait_for_purchase_bids.containsKey(buyerId)){ numBids =
     * buyer_num_wait_for_purchase_bids.get(buyerId) + 1; } else { numBids = 1;
     * } buyer_num_wait_for_purchase_bids.put(buyerId, numBids); }
     */

    public void addGAGIdGAVIdPair(long GAGId, long GAVId) {
        List<Long> GAVIds = this.gag_gav_map.get(GAGId);
        if (null == GAVIds) {
            GAVIds = new ArrayList<Long>();
            this.gag_gav_map.put(GAGId, GAVIds);
        } else if (GAVIds.contains(GAGId)) {
            return;
        }
        GAVIds.add(GAVId);
        this.gag_gav_histogram.put(GAGId);
    }

    public Long[] getRandomGAGIdGAVIdPair(AbstractRandomGenerator rng) {

        FlatHistogram randomGAGId = new FlatHistogram(rng, this.gag_gav_histogram);
        Long GAGId = randomGAGId.nextLong();

        List<Long> GAVIds = this.gag_gav_map.get(GAGId);
        Long GAVId = GAVIds.get(rng.nextInt(GAVIds.size()));

        return new Long[] { GAGId, GAVId };
    }

    public long getRandomCategoryId(AbstractRandomGenerator rng) {
        FlatHistogram randomCategory = new FlatHistogram(rng, this.item_category_histogram);
        return randomCategory.nextLong();
    }

    /*
     * public void incrementTotalItems(long user_id) {
     * synchronized(user_total_items){ Integer totalItems =
     * user_total_items.get(user_id); if(null == totalItems){ totalItems = 1; }
     * else { totalItems++; } user_total_items.put(user_id, totalItems); } }
     * public int getTotalItems(long user_id) { synchronized(user_total_items){
     * if(user_total_items.containsKey(user_id)){ return
     * user_total_items.get(user_id); } else { return 0; } } }
     */
    /*
     * public void setItemsPerUser(long user_id, int total_items, int
     * completed_items) { // TODO (pavlo) } public void
     * setCompletedItemsPerUser(long user_id, int completed_items) { // TODO
     * (pavlo) } public int getCompletedItems(long user_id) { return 0; //
     * TODO(pavlo) } public int getAvailableItems(long user_id) { return 0; //
     * TODO(pavlo) }
     */

    // public

    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------

    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, AuctionMarkBenchmarkProfile.class, AuctionMarkBenchmarkProfile.Members.values());
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, AuctionMarkBenchmarkProfile.class, AuctionMarkBenchmarkProfile.Members.values());
    }
}
