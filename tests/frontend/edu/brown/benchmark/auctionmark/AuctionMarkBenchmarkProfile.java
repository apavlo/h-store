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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.Database;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.benchmark.auctionmark.util.UserIdGenerator;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.rand.RandomDistribution.Zipf;
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
    public Histogram<String> table_sizes = new Histogram<String>();
    /**
     * Histogram for number of items per category (stored as category_id)
     */
    public Histogram<Long> item_category_histogram = new Histogram<Long>();

    /**
     * A histogram for the number of users that have the number of items listed
     * ItemCount -> # of Users
     */
    public Histogram<Long> users_per_item_count = new Histogram<Long>();
    
    /**
     * Three status types for an item:
     *  (1) Available (The auction of this item is still open)
     *  (2) Wait for purchase - The auction of this item is still open. 
     *      There is a bid winner and the bid winner has not purchased the item.
     *  (3) Complete (The auction is closed and (There is no bid winner or
     *      the bid winner has already purchased the item)
     */
    public final LinkedList<ItemId> user_available_items = new LinkedList<ItemId>();
    public final LinkedList<ItemId> user_wait_for_purchase_items = new LinkedList<ItemId>();
    public final LinkedList<ItemId> user_complete_items = new LinkedList<ItemId>();

    /** Map from global attribute group to list of global attribute value */
    public final Map<Long, List<Long>> gag_gav_map = new HashMap<Long, List<Long>>();
    public Histogram<Long> gag_gav_histogram = new Histogram<Long>();
    
    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------
    
    /**
     * Specialized random number generator
     */
    private final AbstractRandomGenerator rng;
    
    private final int num_clients;

    private transient final Map<Integer, UserIdGenerator> userIdGenerators = new HashMap<Integer, UserIdGenerator>();
    
    /**
     * Data used for calculating temporally skewed user ids
     */
    private Integer current_tick = -1;
    private Integer window_total = null;
    private Integer window_size = null;
    private final Histogram<Integer> window_histogram = new Histogram<Integer>();
    private final List<Integer> window_partitions = new ArrayList<Integer>();
    
    /** Random time different in seconds */
    public transient final Gaussian randomTimeDiff;
    
    /** Random duration in days */
    public transient final Gaussian randomDuration;

    public transient final Zipf randomNumImages;
    public transient final Zipf randomNumAttributes;
    public transient final Zipf randomPurchaseDuration;
    public transient final Zipf randomNumComments;
    public transient final Zipf randomInitialPrice;

    private transient FlatHistogram<Long> randomGAGId;
    private transient FlatHistogram<Long> randomCategory;
    private transient FlatHistogram<Long> randomItemCount;
    
    // -----------------------------------------------------------------
    // GENERAL METHODS
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public AuctionMarkBenchmarkProfile(AbstractRandomGenerator rng, int num_clients) {
        this.rng = rng;
        this.num_clients = num_clients;

        // Initialize table sizes
        this.table_sizes.setKeepZeroEntries(true);
        for (String tableName : AuctionMarkConstants.TABLENAMES) {
            if (this.table_sizes.contains(tableName) == false) {
                this.table_sizes.put(tableName, 0l);
            }
        } // FOR

        this.window_histogram.setKeepZeroEntries(true);
        
        this.randomInitialPrice = new Zipf(this.rng, AuctionMarkConstants.ITEM_MIN_INITIAL_PRICE,
                                                     AuctionMarkConstants.ITEM_MAX_INITIAL_PRICE, 1.001);
        
        // Random time difference in a second scale
        this.randomTimeDiff = new Gaussian(this.rng, -AuctionMarkConstants.ITEM_PRESERVE_DAYS * 24 * 60 * 60,
                                                     AuctionMarkConstants.ITEM_MAX_DURATION_DAYS * 24 * 60 * 60);
        this.randomDuration = new Gaussian(this.rng, 1, AuctionMarkConstants.ITEM_MAX_DURATION_DAYS);

        this.randomPurchaseDuration = new Zipf(this.rng, 0, AuctionMarkConstants.ITEM_MAX_PURCHASE_DURATION_DAYS, 1.001);

        this.randomNumImages = new Zipf(this.rng,   AuctionMarkConstants.ITEM_MIN_IMAGES,
                                                    AuctionMarkConstants.ITEM_MAX_IMAGES, 1.001);
        this.randomNumAttributes = new Zipf(this.rng,    AuctionMarkConstants.ITEM_MIN_GLOBAL_ATTRS,
                                                    AuctionMarkConstants.ITEM_MAX_GLOBAL_ATTRS, 1.001);
        this.randomNumComments = new Zipf(this.rng, AuctionMarkConstants.ITEM_MIN_COMMENTS,
                                                    AuctionMarkConstants.ITEM_MAX_COMMENTS, 1.001);

        
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
        this.table_sizes.set(table_name, size);
    }

    /**
     * Add the give tuple to the running to total for the table
     * @param table_name
     * @param size
     */
    public synchronized void addToTableSize(String table_name, long size) {
        this.table_sizes.put(table_name, size);
    }
    
    // ----------------------------------------------------------------
    // USER METHODS
    // ----------------------------------------------------------------

    public long getUserIdCount() {
        return (this.table_sizes.get(AuctionMarkConstants.TABLENAME_USER));
    }

    /**
     * 
     * @param min_item_count
     * @param client
     * @param skew
     * @param exclude
     * @return
     */
    private UserId getRandomUserId(int min_item_count, Integer client, boolean skew, UserId...exclude) {
        if (this.randomItemCount == null) {
            synchronized (this) {
                if (this.randomItemCount == null)
                    this.randomItemCount = new FlatHistogram<Long>(this.rng, this.users_per_item_count);
            } // SYNCH
        }
        skew = (skew && this.window_size != null);
        
        // First grab the UserIdGenerator for the client (or -1 if there is no client)
        // and seek to that itemCount. We use the UserIdGenerator to ensure that we always 
        // select the next UserId for a given client from the same set of UserIds
        Integer client_idx = (client == null ? -1 : client);
        UserIdGenerator gen = this.userIdGenerators.get(client_idx);
        if (gen == null) {
            synchronized (this.userIdGenerators) {
                gen = this.userIdGenerators.get(client);
                if (gen == null) {
                    gen = new UserIdGenerator(this.users_per_item_count, this.num_clients, client);
                    this.userIdGenerators.put(client_idx, gen);
                }
            } // SYNCH
        }
        
        UserId user_id = null;
        int tries = 1000;
        while (user_id == null && tries-- > 0) {
            // We first need to figure out how many items our seller needs to have
            long itemCount = -1;
            assert(min_item_count < this.users_per_item_count.getMaxValue());
            while (itemCount < min_item_count) {
                itemCount = this.randomItemCount.nextValue();
            } // WHILE
        
            // Set the current item count and then choose a random position
            // between where the generator is currently at and where it ends
            synchronized (gen) {
                gen.setCurrentItemCount(itemCount);
                long position = rng.number(gen.getCurrentPosition(), gen.getTotalUsers());
                user_id = gen.seekToPosition(position);
            } // SYNCH
            if (user_id == null) continue;
            
            // Make sure that we didn't select the same UserId as the one we were
            // told to exclude.
            if (exclude != null && exclude.length > 0) {
                for (UserId ex : exclude) {
                    if (ex != null && ex.equals(user_id)) {
                        if (trace.get()) LOG.trace("Excluding " + user_id);
                        user_id = null;
                        break;
                    }
                } // FOR
                if (user_id == null) continue;
            }
            
            // If we don't care about skew, then we're done right here
            if (skew == false || this.window_size == null) {
                if (trace.get()) LOG.trace("Selected " + user_id);
                break;
            }
            
            // Otherwise, check whether this seller maps to our current window
            Integer partition = this.getPartition(user_id);
            if (this.window_partitions.contains(partition)) {
                this.window_histogram.put(partition);
                break;
            }
            if (trace.get()) LOG.trace("Skipping " + user_id);
            user_id = null;
        } // WHILE
        assert(user_id != null) : String.format("Failed to select a random UserId " +
                                                "[min_item_count=%d, client=%d, skew=%s, exclude=%s, totalPossible=%d, currentPosition=%d]",
                                                min_item_count, client, skew, Arrays.toString(exclude),
                                                gen.getTotalUsers(), gen.getCurrentPosition());
        
        return (user_id);
    }

    /**
     * Gets a random buyer ID for all clients
     * @return
     */
    public UserId getRandomBuyerId(UserId...exclude) {
        // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
        return (this.getRandomUserId(0, null, false, exclude));
    }
    /**
     * Gets a random buyer ID for the given client
     * @return
     */
    public UserId getRandomBuyerId(int client, UserId...exclude) {
        // We don't care about skewing the buyerIds at this point, so just get one from getRandomUserId
        return (this.getRandomUserId(0, client, false, exclude));
    }
    /**
     * Get a random buyer UserId, where the probability that a particular user is selected
     * increases based on the number of bids that they have made in the past. We won't allow
     * the last bidder to be selected again
     * @param previousBidders
     * @return
     */
    public UserId getRandomBuyerId(Histogram<UserId> previousBidders, UserId...exclude) {
        // This is very inefficient, but it's probably good enough for now
        Histogram<UserId> new_h = new Histogram<UserId>(previousBidders);
        new_h.setKeepZeroEntries(false);
        for (UserId ex : exclude) new_h.removeAll(ex);
        new_h.put(this.getRandomBuyerId(exclude));
        try {
            LOG.trace("New Histogram:\n" + new_h);
        } catch (NullPointerException ex) {
            for (UserId user_id : new_h.values()) {
                System.err.println(String.format("%s => NEW:%s / ORIG:%s", user_id, new_h.get(user_id), previousBidders.get(user_id)));
            }
            throw ex;
        }
        
        FlatHistogram<UserId> rand_h = new FlatHistogram<UserId>(rng, new_h);
        return (rand_h.nextValue());
    }
    
    /**
     * Gets a random seller ID for all clients
     * @return
     */
    public UserId getRandomSellerId() {
        return (this.getRandomUserId(1, null, true));
    }
    /**
     * Gets a random buyer ID for the given client
     * @return
     */
    public UserId getRandomSellerId(int client) {
        return (this.getRandomUserId(1, client, true));
    }
    
    // ----------------------------------------------------------------
    // ITEM METHODS
    // ----------------------------------------------------------------
    
    private boolean addItem(LinkedList<ItemId> itemSet, ItemId itemId) {
        boolean added = false;
        synchronized (itemSet) {
            if (itemSet.contains(itemId)) return (true);
            
            // If we have room, shove it right in
            // We'll throw it in the back because we know it hasn't been used yet
            if (itemSet.size() < AuctionMarkConstants.ITEM_ID_CACHE_SIZE) {
                itemSet.addLast(itemId);
                added = true;
            
            // Otherwise, we can will randomly decide whether to pop one out
            } else if (this.rng.nextBoolean()) {
                itemSet.pop();
                itemSet.addLast(itemId);
                added = true;
            }
        } // SYNCH
        return (added);
    }
    private void removeItem(LinkedList<ItemId> itemSet, ItemId itemId) {
        synchronized (itemSet) {
            itemSet.remove(itemId);
        } // SYNCH
    }
    private ItemId getRandomItemId(LinkedList<ItemId> itemSet, boolean hasCurrentPrice) {
        ItemId itemId = null;
        synchronized (itemSet) {
            int num_items = itemSet.size();
            Integer partition = null;
            int idx = -1;
            while (num_items > 0) {
                partition = null;
                idx = this.rng.nextInt(num_items);
                itemId = itemSet.get(idx);
                assert(itemId != null);
                
                // Needs to have an embedded currentPrice
                if (hasCurrentPrice && itemId.hasCurrentPrice() == false) {
                    continue;
                }
                
                // Uniform
                if (this.window_size == null) {
                    break;
                }
                // Temporal Skew
                partition = this.getPartition(itemId.getSellerId());
                if (this.window_partitions.contains(partition)) {
                    this.window_histogram.put(partition);
                    break;
                }
            } // WHILE
            if (itemId == null) return (null);
            assert(idx >= 0);
            itemSet.remove(idx);
            itemSet.addFirst(itemId);
        } // SYNCHRONIZED
        return itemId;
    }
    
    public void addAvailableItem(ItemId itemId) {
        this.addItem(this.user_available_items, itemId);
    }
    public void removeAvailableItem(ItemId itemId) {
        this.removeItem(this.user_available_items, itemId);
    }
    public ItemId getRandomAvailableItemId() {
        return this.getRandomItemId(this.user_available_items, false);
    }
    public ItemId getRandomAvailableItemId(boolean hasCurrentPrice) {
        return this.getRandomItemId(this.user_available_items, hasCurrentPrice);
    }
    public int getAvailableItemIdsCount() {
        return this.user_available_items.size();
    }
    
    public void addWaitForPurchaseItem(ItemId itemId) {
        this.addItem(this.user_wait_for_purchase_items, itemId);
    }
    public void removeWaitForPurchaseItem(ItemId itemId) {
        this.removeItem(this.user_wait_for_purchase_items, itemId);
    }
    public ItemId getRandomWaitForPurchaseItemId() {
        return this.getRandomItemId(this.user_wait_for_purchase_items, false);
    }
    public int getWaitForPurchaseItemIdsCount() {
        return this.user_wait_for_purchase_items.size();
    }
    
    public void addCompleteItem(ItemId itemId) {
        this.addItem(this.user_complete_items, itemId);
    }
    public void removeCompleteItem(ItemId itemId) {
        this.removeItem(this.user_complete_items, itemId);
    }
    public ItemId getRandomCompleteItemId() {
        return this.getRandomItemId(this.user_complete_items, false);
    }
    public int getCompleteItemIdsCount() {
        return this.user_complete_items.size();
    }
    
    public int getAllItemIdsCount() {
        return (this.getAvailableItemIdsCount() +
                this.getWaitForPurchaseItemIdsCount() +
                this.getCompleteItemIdsCount());
    }
    public ItemId getRandomItemId() {
        assert(this.getAllItemIdsCount() > 0);
        LinkedList<ItemId> itemSet = null;
        while (itemSet == null || itemSet.isEmpty()) {
            int idx = rng.nextInt(3);
            if (idx == 0) itemSet = this.user_available_items;
            else if (idx == 1) itemSet = this.user_wait_for_purchase_items;
            else if (idx == 2) itemSet = this.user_complete_items;
        } // WHILE
        return (this.getRandomItemId(itemSet, false));
    }

    // ----------------------------------------------------------------
    // GLOBAL ATTRIBUTE METHODS
    // ----------------------------------------------------------------

    public synchronized void addGAGIdGAVIdPair(long GAGId, long GAVId) {
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
     * @return
     */
    public synchronized Pair<Long, Long> getRandomGAGIdGAVIdPair() {
        if (this.randomGAGId == null) {
            this.randomGAGId = new FlatHistogram<Long>(rng, this.gag_gav_histogram);
        }
        Long GAGId = this.randomGAGId.nextLong();
        List<Long> GAVIds = this.gag_gav_map.get(GAGId);
        Long GAVId = GAVIds.get(rng.nextInt(GAVIds.size()));

        return Pair.of(GAGId, GAVId);
    }
    
    public synchronized long getRandomCategoryId() {
        if (this.randomCategory == null) {
            this.randomCategory = new FlatHistogram<Long>(this.rng, this.item_category_histogram); 
        }
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
