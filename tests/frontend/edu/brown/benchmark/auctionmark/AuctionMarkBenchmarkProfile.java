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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.Database;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.auctionmark.util.ItemInfo;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class AuctionMarkBenchmarkProfile implements JSONSerializable {
    private static final Logger LOG = Logger.getLogger(AuctionMarkBaseClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------
    // SERIALIZABLE DATA MEMBERS
    // ----------------------------------------------------------------

    /**
     * Data Scale Factor
     */
    public double scale_factor;
    /**
     * Map from table names to the number of tuples we inserted during loading
     */
    public final Map<String, Long> table_sizes = Collections.synchronizedMap(new TreeMap<String, Long>());
    /**
     * Histogram for number of items per category (stored as category_id)
     */
    public Histogram<Long> item_category_histogram = new Histogram<Long>();

    /**
     * A histogram for the number of users that have the number of items listed
     * ItemCount -> # of Users
     */
    public Histogram<Integer> users_per_item_count = new Histogram<Integer>();
    
    /**
     * Three status types for an item:
     *  (1) Available (The auction of this item is still open)
     *  (2) Wait for purchase - The auction of this item is still open. 
     *      There is a bid winner and the bid winner has not purchased the item.
     *  (3) Complete (The auction is closed and (There is no bid winner or
     *      the bid winner has already purchased the item)
     */
    private final Set<ItemId> user_available_items = new HashSet<ItemId>();
    public Histogram<UserId> user_available_items_histogram = new Histogram<UserId>();
    
    public final Set<ItemId> user_wait_for_purchase_items = new HashSet<ItemId>();
    public Histogram<UserId> user_wait_for_purchase_items_histogram = new Histogram<UserId>();
    
    public final Set<ItemId> user_complete_items = new HashSet<ItemId>();
    public Histogram<UserId> user_complete_items_histogram = new Histogram<UserId>();

    public final Map<Long, Long> item_bid_map = new HashMap<Long, Long>();
    public final Map<Long, Long> item_buyer_map = new HashMap<Long, Long>();

    /** Map from global attribute group to list of global attribute value */
    public final Map<Long, List<Long>> gag_gav_map = new HashMap<Long, List<Long>>();
    public Histogram<Long> gag_gav_histogram = new Histogram<Long>();
    
    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------
    
    private transient FlatHistogram<Long> randomGAGId = null;

    private final Map<AbstractRandomGenerator, RandomDistribution.DiscreteRNG> CACHE_getRandomUserId = new HashMap<AbstractRandomGenerator, RandomDistribution.DiscreteRNG>();
    
    /**
     * Data used for calculating temporally skewed user ids
     */
    private Integer current_tick = -1;
    private Integer window_total = null;
    private Integer window_size = null;
    private final Histogram<Integer> window_histogram = new Histogram<Integer>();
    private final List<Integer> window_partitions = new ArrayList<Integer>();
    
    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public AuctionMarkBenchmarkProfile() {

        // Initialize table sizes
        for (String tableName : AuctionMarkConstants.TABLENAMES) {
            if (this.table_sizes.containsKey(tableName) == false) {
                this.table_sizes.put(tableName, 0l);
            }
        } // FOR

        this.window_histogram.setKeepZeroEntries(true);
        
        // _lastUserId = this.getTableSize(AuctionMarkConstants.TABLENAME_USER);

        LOG.debug("AuctionMarkBenchmarkProfile :: constructor");
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
                last_partition = CollectionUtil.last(this.window_partitions);
            }

            this.window_partitions.clear();
            for (int ctr = 0; ctr < this.window_size; ctr++) {
                last_partition++;
                if (last_partition > this.window_total) {
                    last_partition = 0;
                }
                this.window_partitions.add(last_partition);
            } // FOR
            LOG.info("Tick #" + this.current_tick + " Window: " + this.window_partitions);
            if (debug.get()) LOG.debug("Skew Window Histogram\n" + this.window_histogram);
            this.window_histogram.clearValues();
        }
    }

    private int getPartition(UserId seller_id) {
        return (TheHashinator.hashToPartition(seller_id, this.window_total));
    }

    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Get the scale factor value for this benchmark profile
     * @return
     */
    public double getScaleFactor() {
        return (this.scale_factor);
    }

    /**
     * Set the scale factor for this benchmark profile
     * @param scale_factor
     */
    public void setScaleFactor(double scale_factor) {
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
    public synchronized long addToTableSize(String table_name, long size) {
        Long orig_size = this.table_sizes.get(table_name);
        if (orig_size == null) {
            orig_size = 0l;
        }
        long new_size = orig_size + size;
        this.setTableSize(table_name, new_size);
        return (new_size);
    }
    
    // ----------------------------------------------------------------
    // USER METHODS
    // ----------------------------------------------------------------

    public long getUserIdCount() {
        return (this.table_sizes.get(AuctionMarkConstants.TABLENAME_USER));
    }

    /**
     * Gets a random user ID within the client.
     * @param rng
     * @return
     */
    private Long getRandomUserId(AbstractRandomGenerator rng) {
        Long user_id = null;
//        assert(this.user_ids.isEmpty() == false) : "The list of user ids is empty!";
//        synchronized (this.user_ids) {
//            int num_user_ids = this.user_ids.size();
//            if (num_user_ids > 0) {
//                RandomDistribution.DiscreteRNG randomUserIndex = this.CACHE_getRandomUserId.get(rng);
//                if (randomUserIndex == null || randomUserIndex.getMax() != num_user_ids) {
//                    // Do we really want everything to be Guassian??
//                    randomUserIndex = new Gaussian(rng, 0, num_user_ids - 1);
//                    this.CACHE_getRandomUserId.put(rng, randomUserIndex);
//                }
//                user_id = this.user_ids.get(randomUserIndex.nextInt());
//            }
//        }
        return (user_id);
    }

    /**
     * Gets a random buyer ID within the client.
     * @param rng
     * @return
     */
    public UserId getRandomBuyerId(AbstractRandomGenerator rng) {
        // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
        return (null); // TODO this.getRandomUserId(rng));
    }
    
    /**
     * Gets a random seller ID within the client.
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
            if (seller_id == null) break;
            
            // If there is no skew, then we can just jump out right here!
            if (this.window_size == null) break;
            
            // Otherwise we need to skew this mother trucker..
            partition = null; // TODO this.getPartition(seller_id);
            if (this.window_partitions.contains(partition)) {
                break;
            }
        } // WHILE
        if (partition != null) {
            this.window_histogram.put(partition);
        }
        return (seller_id);
    }
    
    // ----------------------------------------------------------------
    // ITEM METHODS
    // ----------------------------------------------------------------
    
    private void addItem(Set<ItemId> itemSet, Histogram<UserId> sellerHistogram, ItemId itemId) {
        synchronized (itemSet) {
            itemSet.add(itemId);
            sellerHistogram.put(itemId.getSellerId());
        } // SYNCH
    }
    private void removeItem(Set<ItemId> itemSet, Histogram<UserId> sellerHistogram, ItemId itemId) {
        synchronized (itemSet) {
            itemSet.remove(itemId);
            sellerHistogram.remove(itemId.getSellerId());
        } // SYNCH
    }
    private ItemId getRandomItemId(AbstractRandomGenerator rng, Set<ItemId> itemSet, Histogram<UserId> sellerHistogram) {
        UserId sellerId = null;
        ItemId itemId = null;
        synchronized (itemSet) {
            FlatHistogram<UserId> randomSeller = new FlatHistogram<UserId>(rng, sellerHistogram);
            Integer partition = null;
            while (true) {
                partition = null;
                sellerId = randomSeller.nextValue();
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
            itemId = null; // TODO this.user_available_items.get(sellerId).get(rng.number(0, (int) numAvailableItems - 1));
        } // SYNCHRONIZED
        return itemId;
    }
    
    
    public void addAvailableItem(ItemId itemId) {
        this.addItem(this.user_available_items, this.user_available_items_histogram, itemId);
    }
    public void removeAvailableItem(ItemId itemId) {
        this.removeItem(this.user_available_items, this.user_available_items_histogram, itemId);
    }
    public ItemId getRandomAvailableItemId(AbstractRandomGenerator rng) {
        return this.getRandomItemId(rng, this.user_available_items, this.user_available_items_histogram);
    }
    
    public void addWaitForPurchaseItem(ItemId itemId) {
        this.addItem(this.user_wait_for_purchase_items, this.user_wait_for_purchase_items_histogram, itemId);
    }
    public void removeWaitForPurchaseItem(ItemId itemId) {
        this.removeItem(this.user_wait_for_purchase_items, this.user_wait_for_purchase_items_histogram, itemId);
    }
    public ItemId getRandomWaitForPurchaseItemId(AbstractRandomGenerator rng) {
        return this.getRandomItemId(rng, this.user_wait_for_purchase_items, this.user_wait_for_purchase_items_histogram);
    }
    
    public void addCompleteItem(ItemId itemId) {
        this.addItem(this.user_complete_items, this.user_complete_items_histogram, itemId);
    }
    public void removeCompleteItem(ItemId itemId) {
        this.removeItem(this.user_complete_items, this.user_complete_items_histogram, itemId);
    }
    public ItemId getRandomCompleteItemId(AbstractRandomGenerator rng) {
        return this.getRandomItemId(rng, this.user_complete_items, this.user_complete_items_histogram);
    }
    


    public long getBidId(long itemId) {
        return this.item_bid_map.get(itemId);
    }

    public long getBuyerId(long itemId) {
        return this.item_buyer_map.get(itemId);
    }

    // ----------------------------------------------------------------
    // GLOBAL ATTRIBUTE METHODS
    // ----------------------------------------------------------------

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

    /**
     * Return a random attribute group/value pair
     * Pair<GLOBAL_ATTRIBUTE_GROUP, GLOBAL_ATTRIBUTE_VALUE>
     * @param rng
     * @return
     */
    public synchronized Pair<Long, Long> getRandomGAGIdGAVIdPair(AbstractRandomGenerator rng) {
        if (this.randomGAGId == null) {
            this.randomGAGId = new FlatHistogram<Long>(rng, this.gag_gav_histogram);
        }
        Long GAGId = this.randomGAGId.nextLong();
        List<Long> GAVIds = this.gag_gav_map.get(GAGId);
        Long GAVId = GAVIds.get(rng.nextInt(GAVIds.size()));

        return Pair.of(GAGId, GAVId);
    }
    
    public long getRandomCategoryId(AbstractRandomGenerator rng) {
        FlatHistogram<Long> randomCategory = new FlatHistogram<Long>(rng, this.item_category_histogram);
        return randomCategory.nextLong();
    }


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
        JSONUtil.fieldsToJSON(stringer, this, AuctionMarkBenchmarkProfile.class, JSONUtil.getSerializableFields(this.getClass()));
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, AuctionMarkBenchmarkProfile.class, false, JSONUtil.getSerializableFields(this.getClass()));
    }
}
