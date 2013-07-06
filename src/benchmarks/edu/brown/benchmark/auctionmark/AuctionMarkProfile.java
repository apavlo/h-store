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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants.ItemStatus;
import edu.brown.benchmark.auctionmark.procedures.LoadConfig;
import edu.brown.benchmark.auctionmark.util.GlobalAttributeGroupId;
import edu.brown.benchmark.auctionmark.util.GlobalAttributeValueId;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.ItemInfo;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.benchmark.auctionmark.util.UserIdGenerator;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.DiscreteRNG;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

/**
 * AuctionMark Profile Information
 * @author pavlo
 */
public class AuctionMarkProfile {
    private static final Logger LOG = Logger.getLogger(AuctionMarkProfile.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * We maintain a cached version of the profile that we will copy from
     * This prevents the need to have every single client thread load up a separate copy
     */
    private static AuctionMarkProfile cachedProfile;
    
    // ----------------------------------------------------------------
    // REQUIRED REFERENCES
    // ----------------------------------------------------------------
    
    /**
     * Specialized random number generator
     */
    protected final AbstractRandomGenerator rng;
    private final int num_clients;
    protected transient File data_directory;

    // ----------------------------------------------------------------
    // SERIALIZABLE DATA MEMBERS
    // ----------------------------------------------------------------

    /**
     * Database Scale Factor
     */
    private double scale_factor;
    
    /**
     * The start time used when creating the data for this benchmark
     */
    private TimestampType benchmarkStartTime;
    
    /**
     * A histogram for the number of users that have the number of items listed
     * ItemCount -> # of Users
     */
    protected ObjectHistogram<Long> users_per_item_count = new ObjectHistogram<Long>();
    

    // ----------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------
    
    /**
     * Histogram for number of items per category (stored as category_id)
     */
    protected ObjectHistogram<Long> item_category_histogram = new ObjectHistogram<Long>();

    /**
     * Three status types for an item:
     *  (1) Available - The auction of this item is still open
     *  (2) Ending Soon
     *  (2) Wait for Purchase - The auction of this item is still open. 
     *      There is a bid winner and the bid winner has not purchased the item.
     *  (3) Complete (The auction is closed and (There is no bid winner or
     *      the bid winner has already purchased the item)
     */
    private transient final LinkedList<ItemInfo> items_available = new LinkedList<ItemInfo>();
    private transient final LinkedList<ItemInfo> items_endingSoon = new LinkedList<ItemInfo>();
    private transient final LinkedList<ItemInfo> items_waitingForPurchase = new LinkedList<ItemInfo>();
    private transient final LinkedList<ItemInfo> items_completed = new LinkedList<ItemInfo>();
    
    /**
     * Internal list of GlobalAttributeGroupIds
     */
    protected transient List<GlobalAttributeGroupId> gag_ids = new ArrayList<GlobalAttributeGroupId>();

    /**
     * Internal map of UserIdGenerators
     */
    private transient final Map<Integer, UserIdGenerator> userIdGenerators = new HashMap<Integer, UserIdGenerator>();
    
    /**
     * Data used for calculating temporally skewed user ids
     */
    private transient Integer current_tick = -1;
    private transient Integer window_total = null;
    private transient Integer window_size = null;
    private transient final ObjectHistogram<Integer> window_histogram = new ObjectHistogram<Integer>(true);
    private transient final List<Integer> window_partitions = new ArrayList<Integer>();
    
    /** Random time different in seconds */
    public transient final DiscreteRNG randomTimeDiff;
    
    /** Random duration in days */
    public transient final Gaussian randomDuration;

    protected transient final Zipf randomNumImages;
    protected transient final Zipf randomNumAttributes;
    protected transient final Zipf randomPurchaseDuration;
    protected transient final Zipf randomNumComments;
    protected transient final Zipf randomInitialPrice;

    private transient FlatHistogram<Long> randomCategory;
    private transient FlatHistogram<Long> randomItemCount;
    
    /**
     * The last time that we called CHECK_WINNING_BIDS on this client
     */
    private transient TimestampType lastCloseAuctionsTime;
    /**
     * When this client started executing
     */
    private TimestampType clientStartTime;
    /**
     * Current Timestamp
     */
    private TimestampType currentTime;
    
    /**
     * Keep track of previous waitForPurchase ItemIds so that we don't try to call NewPurchase
     * on them more than once
     */
    private transient Set<ItemInfo> previousWaitForPurchase = new HashSet<ItemInfo>();
    
    // -----------------------------------------------------------------
    // CONSTRUCTOR
    // -----------------------------------------------------------------

    /**
     * Constructor - Keep your pimp hand strong!
     */
    public AuctionMarkProfile(AbstractRandomGenerator rng, int num_clients) {
        this.rng = rng;
        this.num_clients = num_clients;

        this.randomInitialPrice = new Zipf(this.rng, AuctionMarkConstants.ITEM_MIN_INITIAL_PRICE,
                                                     AuctionMarkConstants.ITEM_MAX_INITIAL_PRICE, 1.001);
        
        // Random time difference in a second scale
        this.randomTimeDiff = new Gaussian(this.rng, -AuctionMarkConstants.ITEM_PRESERVE_DAYS * 24 * 60 * 60,
                                                     AuctionMarkConstants.ITEM_MAX_DURATION_DAYS * 24 * 60 * 60);
        this.randomDuration = new Gaussian(this.rng, 1, AuctionMarkConstants.ITEM_MAX_DURATION_DAYS);
        this.randomPurchaseDuration = new Zipf(this.rng, 0, AuctionMarkConstants.ITEM_MAX_PURCHASE_DURATION_DAYS, 1.001);
        this.randomNumImages = new Zipf(this.rng,   AuctionMarkConstants.ITEM_MIN_IMAGES,
                                                    AuctionMarkConstants.ITEM_MAX_IMAGES, 1.001);
        this.randomNumAttributes = new Zipf(this.rng, AuctionMarkConstants.ITEM_MIN_GLOBAL_ATTRS,
                                                    AuctionMarkConstants.ITEM_MAX_GLOBAL_ATTRS, 1.001);
        this.randomNumComments = new Zipf(this.rng, AuctionMarkConstants.ITEM_MIN_COMMENTS,
                                                    AuctionMarkConstants.ITEM_MAX_COMMENTS, 1.001);

        
        // _lastUserId = this.getTableSize(AuctionMarkConstants.TABLENAME_USER);

        LOG.debug("AuctionMarkBenchmarkProfile :: constructor");
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION METHODS
    // -----------------------------------------------------------------

    protected final void saveProfile(AuctionMarkLoader baseClient) {
        Database catalog_db = baseClient.getCatalogContext().database;
        
     // CONFIG_PROFILE
        Table catalog_tbl = catalog_db.getTables().get(AuctionMarkConstants.TABLENAME_CONFIG_PROFILE);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        assert(vt != null);
        vt.addRow(
            this.scale_factor,                  // CFP_SCALE_FACTOR
            this.benchmarkStartTime,            // CFP_BENCHMARK_START
            this.users_per_item_count.toJSONString() // CFP_USER_ITEM_HISTOGRAM
        );
        if (debug.val)
            LOG.debug("Saving profile information into " + catalog_tbl);
        baseClient.loadVoltTable(catalog_tbl.getName(), vt);
        
        return;
    }
    
    private AuctionMarkProfile copyProfile(AuctionMarkProfile other) {
        this.scale_factor = other.scale_factor;
        this.benchmarkStartTime = other.benchmarkStartTime;
        this.users_per_item_count = other.users_per_item_count;
        this.item_category_histogram = other.item_category_histogram;
        this.gag_ids = other.gag_ids;
        this.previousWaitForPurchase = other.previousWaitForPurchase;
        
        this.items_available.addAll(other.items_available);
        Collections.shuffle(this.items_available);
        
        this.items_endingSoon.addAll(other.items_endingSoon);
        Collections.shuffle(this.items_endingSoon);
        
        this.items_waitingForPurchase.addAll(other.items_waitingForPurchase);
        Collections.shuffle(this.items_waitingForPurchase);
        
        this.items_completed.addAll(other.items_completed);
        Collections.shuffle(this.items_completed);
        
        return (this);
    }
    
    /**
     * Load the profile information stored in the database
     * @param 
     */
    protected synchronized void loadProfile(AuctionMarkClient baseClient) {
        // Check whether we have a cached Profile we can copy from
        if (cachedProfile != null) {
            if (debug.val) LOG.debug("Using cached SEATSProfile");
            this.copyProfile(cachedProfile);
            return;
        }
        
        if (debug.val)
            LOG.debug("Loading AuctionMarkProfile for the first time");
        
        Client client = baseClient.getClientHandle();
        ClientResponse response = null;
        try {
            response = client.callProcedure(LoadConfig.class.getSimpleName());
        } catch (Exception ex) {
            throw new RuntimeException("Failed retrieve data from " + AuctionMarkConstants.TABLENAME_CONFIG_PROFILE, ex);
        }
        assert(response != null);
        assert(response.getStatus() == Status.OK) : "Unexpected " + response;

        VoltTable results[] = response.getResults();
        int result_idx = 0;
        
        // CONFIG_PROFILE
        this.loadConfigProfile(results[result_idx++]); 
        
        // IMPORTANT: We need to set these timestamps here. It must be done
        // after we have loaded benchmarkStartTime
        this.setAndGetClientStartTime();
        this.updateAndGetCurrentTime();
        
        // ITEM CATEGORY COUNTS
        this.loadItemCategoryCounts(results[result_idx++]);
        
        // ITEMS
        for (ItemStatus status : ItemStatus.values()) {
            if (status.isInternal()) continue;
            this.loadItems(results[result_idx++], status);
        } // FOR
        
        // GLOBAL_ATTRIBUTE_GROUPS
        this.loadGlobalAttributeGroups(results[result_idx++]);
        
        cachedProfile = this;
    }
    
    private final void loadConfigProfile(VoltTable vt) {
        boolean adv = vt.advanceRow();
        assert(adv);
        int col = 0;
        this.scale_factor = vt.getDouble(col++);
        this.benchmarkStartTime = vt.getTimestampAsTimestamp(col++);
        JSONUtil.fromJSONString(this.users_per_item_count, vt.getString(col++));
        
        if (debug.val)
            LOG.debug(String.format("Loaded %s data", AuctionMarkConstants.TABLENAME_CONFIG_PROFILE));
    }
    
    private final void loadItemCategoryCounts(VoltTable vt) {
        while (vt.advanceRow()) {
            int col = 0;
            long i_c_id = vt.getLong(col++);
            int count = (int)vt.getLong(col++);
            this.item_category_histogram.put(i_c_id, count);
        } // WHILE
        if (debug.val)
            LOG.debug(String.format("Loaded %d item category records from %s",
                                    this.item_category_histogram.getValueCount(), AuctionMarkConstants.TABLENAME_ITEM));
    }
    
    private final void loadItems(VoltTable vt, ItemStatus status) {
        int ctr = 0;
        while (vt.advanceRow()) {
            int col = 0;
            ItemId i_id = new ItemId(vt.getLong(col++));
            double i_current_price = vt.getDouble(col++);
            TimestampType i_end_date = vt.getTimestampAsTimestamp(col++);
            int i_num_bids = (int)vt.getLong(col++);
            ItemStatus i_status = ItemStatus.get(vt.getLong(col++));
            assert(i_status == status);
            
            ItemInfo itemInfo = new ItemInfo(i_id, i_current_price, i_end_date, i_num_bids);
            this.addItemToProperQueue(itemInfo, false);
            ctr++;
        } // WHILE
        
        if (debug.val)
            LOG.debug(String.format("Loaded %d records from %s",
                                    ctr, AuctionMarkConstants.TABLENAME_ITEM));
    }
    
    private final void loadGlobalAttributeGroups(VoltTable vt) {
        while (vt.advanceRow()) {
            long gag_id = vt.getLong(0);
            assert(gag_id != VoltType.NULL_BIGINT);
            this.gag_ids.add(new GlobalAttributeGroupId(gag_id));
        } // WHILE
        if (debug.val)
            LOG.debug(String.format("Loaded %d records from %s",
                                    this.gag_ids.size(), AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP));
    }
    
    
    // -----------------------------------------------------------------
    // UTILITY METHODS
    // -----------------------------------------------------------------
    
    public void setDataDirectory(File dataDir) {
        this.data_directory = dataDir;
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
        
        // Update Skew Window
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
            LOG.info("Skew Tick #" + this.current_tick + " Window: " + this.window_partitions);
            if (debug.val) LOG.debug("Skew Window Histogram\n" + this.window_histogram);
            this.window_histogram.clearValues();
        }
    }
    
    private int getPartition(UserId seller_id) {
        return (TheHashinator.hashToPartition(seller_id, this.window_total));
    }

    // -----------------------------------------------------------------
    // TIME METHODS
    // -----------------------------------------------------------------

    /**
     * 
     * @param benchmarkStart
     * @param clientStart
     * @param current
     * @return
     */
    public static TimestampType getScaledTimestamp(TimestampType benchmarkStart, TimestampType clientStart, TimestampType current) {
//        if (benchmarkStart == null || clientStart == null || current == null) return (null);
        
        // First get the offset between the benchmarkStart and the clientStart
        // We then subtract that value from the current time. This gives us the total elapsed 
        // time from the current time to the time that the benchmark start (with the gap 
        // from when the benchmark was loading data cut out) 
        long base = benchmarkStart.getTime();
        long offset = current.getTime() - (clientStart.getTime() - base);
        long elapsed = (offset - base) * AuctionMarkConstants.TIME_SCALE_FACTOR;
        return (new TimestampType(base + elapsed));
    }

    
    private TimestampType getScaledCurrentTimestamp() {
        assert(this.clientStartTime != null);
        TimestampType now = new TimestampType();
        TimestampType time = AuctionMarkProfile.getScaledTimestamp(this.benchmarkStartTime, this.clientStartTime, now);
        if (trace.val)
            LOG.trace(String.format("Scaled:%d / Now:%d / BenchmarkStart:%d / ClientStart:%d",
                                   time.getMSTime(), now.getMSTime(), this.benchmarkStartTime.getMSTime(), this.clientStartTime.getMSTime()));
        return (time);
    }
    
    public synchronized TimestampType updateAndGetCurrentTime() {
        this.currentTime = this.getScaledCurrentTimestamp();
        return this.currentTime;
    }
    public TimestampType getCurrentTime() {
        return this.currentTime;
    }
    
    public TimestampType setAndGetBenchmarkStartTime() {
        assert(this.benchmarkStartTime == null);
        this.benchmarkStartTime = new TimestampType();
        return (this.benchmarkStartTime);
    }
    public TimestampType getBenchmarkStartTime() {
        return (this.benchmarkStartTime);
    }

    public TimestampType setAndGetClientStartTime() {
        assert(this.clientStartTime == null);
        this.clientStartTime = new TimestampType();
        return (this.clientStartTime);
    }
    public TimestampType getClientStartTime() {
        return (this.clientStartTime);
    }
    public boolean hasClientStartTime() {
        return (this.clientStartTime != null);
    }
    

    public synchronized TimestampType updateAndGetLastCloseAuctionsTime() {
        this.lastCloseAuctionsTime = this.getScaledCurrentTimestamp();
        return this.lastCloseAuctionsTime;
    }
    public TimestampType getLastCloseAuctionsTime() {
        return this.lastCloseAuctionsTime;
    }
    public boolean hasLastCloseAuctionsTime() {
        return (this.lastCloseAuctionsTime != null);
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
    
    // ----------------------------------------------------------------
    // USER METHODS
    // ----------------------------------------------------------------

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
                        if (trace.val) LOG.trace("Excluding " + user_id);
                        user_id = null;
                        break;
                    }
                } // FOR
                if (user_id == null) continue;
            }
            
            // If we don't care about skew, then we're done right here
            if (skew == false || this.window_size == null) {
                if (trace.val) LOG.trace("Selected " + user_id);
                break;
            }
            
            // Otherwise, check whether this seller maps to our current window
            Integer partition = this.getPartition(user_id);
            if (this.window_partitions.contains(partition)) {
                this.window_histogram.put(partition);
                break;
            }
            if (trace.val) LOG.trace("Skipping " + user_id);
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
    public UserId getRandomBuyerId(ObjectHistogram<UserId> previousBidders, UserId...exclude) {
        // This is very inefficient, but it's probably good enough for now
        ObjectHistogram<UserId> new_h = new ObjectHistogram<UserId>(previousBidders);
        new_h.setKeepZeroEntries(false);
        for (UserId ex : exclude) new_h.remove(ex);
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
     * Gets a random SellerID for the given client
     * @return
     */
    public UserId getRandomSellerId(int client) {
        return (this.getRandomUserId(1, client, true));
    }
    
    // ----------------------------------------------------------------
    // ITEM METHODS
    // ----------------------------------------------------------------
    
    private boolean addItem(LinkedList<ItemInfo> itemSet, ItemInfo itemInfo) {
        boolean added = false;
        synchronized (itemSet) {
            if (itemSet.contains(itemInfo)) {
                // HACK: Always swap existing ItemInfos with our new one, since it will
                // more up-to-date information
                int idx = itemSet.indexOf(itemInfo);
                ItemInfo existing = itemSet.set(idx, itemInfo);
                assert(existing != null);
                return (true);
            }
            if (itemInfo.hasCurrentPrice()) 
                assert(itemInfo.getCurrentPrice() > 0) : "Negative current price for " + itemInfo;
            
            // If we have room, shove it right in
            // We'll throw it in the back because we know it hasn't been used yet
            if (itemSet.size() < AuctionMarkConstants.ITEM_ID_CACHE_SIZE) {
                itemSet.addLast(itemInfo);
                added = true;
            
            // Otherwise, we can will randomly decide whether to pop one out
            } else if (this.rng.nextBoolean()) {
                itemSet.pop();
                itemSet.addLast(itemInfo);
                added = true;
            }
        } // SYNCH
        return (added);
    }
    private void removeItem(LinkedList<ItemInfo> itemSet, ItemInfo itemInfo) {
        synchronized (itemSet) {
            itemSet.remove(itemInfo);
        } // SYNCH
    }
    
    public synchronized void updateItemQueues() {
        // HACK
        Set<ItemInfo> all = new HashSet<ItemInfo>();
        @SuppressWarnings("unchecked")
        LinkedList<ItemInfo> itemSets[] = new LinkedList[]{
            this.items_available,
            this.items_endingSoon,
            this.items_waitingForPurchase,
            this.items_completed,
        };
        for (LinkedList<ItemInfo> list : itemSets) {
            synchronized (list) {
                all.addAll(list);
            } // SYNCH
        } // FOR
        
        TimestampType currentTime = this.updateAndGetCurrentTime();
        assert(currentTime != null);
        if (debug.val) LOG.debug("CurrentTime: " + currentTime);
        for (ItemInfo itemInfo : all) {
            this.addItemToProperQueue(itemInfo, false);
        } // FOR
        
        if (debug.val) {
            Map<ItemStatus, Integer> m = new HashMap<ItemStatus, Integer>();
            m.put(ItemStatus.OPEN, this.items_available.size());
            m.put(ItemStatus.ENDING_SOON, this.items_endingSoon.size());
            m.put(ItemStatus.WAITING_FOR_PURCHASE, this.items_waitingForPurchase.size());
            m.put(ItemStatus.CLOSED, this.items_completed.size());
            LOG.debug(String.format("Updated Item Queues [%s]:\n%s",
                                    currentTime, StringUtil.formatMaps(m)));
        }
    }
    
    public ItemStatus addItemToProperQueue(ItemInfo itemInfo, boolean is_loader) {
        // Calculate how much time is left for this auction
        TimestampType baseTime = (is_loader ? this.getBenchmarkStartTime() :
                                              this.getCurrentTime());
        assert(itemInfo.endDate != null);
        assert(baseTime != null) : "is_loader=" + is_loader;
        long remaining = itemInfo.endDate.getMSTime() - baseTime.getMSTime();
        ItemStatus ret;
        
        // Already ended
        if (remaining <= 100000) {
            if (itemInfo.numBids > 0 && itemInfo.status != ItemStatus.CLOSED) {
                synchronized (this.previousWaitForPurchase) {
                    if (this.previousWaitForPurchase.contains(itemInfo) == false) {
                        this.previousWaitForPurchase.add(itemInfo);
                        this.addItem(this.items_waitingForPurchase, itemInfo);
                    }
                } // SYNCH
                ret = ItemStatus.WAITING_FOR_PURCHASE;
            } else {
                this.addItem(this.items_completed, itemInfo);
                ret = ItemStatus.CLOSED;
            }
            this.removeAvailableItem(itemInfo);
            this.removeEndingSoonItem(itemInfo);
        }
        // About to end soon
        else if (remaining < AuctionMarkConstants.ENDING_SOON) {
            this.addItem(this.items_endingSoon, itemInfo);
            this.removeAvailableItem(itemInfo);
            ret = ItemStatus.ENDING_SOON;
        }
        // Still available
        else {
            this.addItem(this.items_available, itemInfo);
            ret = ItemStatus.OPEN;
        }
        
        if (trace.val)
            LOG.trace(String.format("%s - #%d [%s]", ret, itemInfo.itemId.encode(), itemInfo.getEndDate()));
        
        return (ret);
    }
    
    /**
     * 
     * @param itemSet
     * @param needCurrentPrice
     * @param needFutureEndDate TODO
     * @return
     */
    private ItemInfo getRandomItem(LinkedList<ItemInfo> itemSet, boolean needCurrentPrice, boolean needFutureEndDate) {
        ItemInfo itemInfo = null;
        Set<ItemInfo> seen = new HashSet<ItemInfo>();
        TimestampType currentTime = this.updateAndGetCurrentTime();
        
        synchronized (itemSet) {
            int num_items = itemSet.size();
            Integer partition = null;
            int idx = -1;
            
            if (trace.val) 
                LOG.trace(String.format("Getting random ItemInfo [numItems=%d, currentTime=%s, needCurrentPrice=%s]",
                                       num_items, currentTime, needCurrentPrice));
            long tries = 1000;
            while (num_items > 0 && tries-- > 0 && seen.size() < num_items) {
                partition = null;
                idx = this.rng.nextInt(num_items);
                ItemInfo temp = itemSet.get(idx);
                assert(temp != null);
                if (seen.contains(temp)) continue;
                seen.add(temp);
                
                // Needs to have an embedded currentPrice
                if (needCurrentPrice && temp.hasCurrentPrice() == false) {
                    continue;
                }
                
                // If they want an item that is ending in the future, then we compare it with 
                // the current timestamp
                if (needFutureEndDate) {
                    boolean compareTo = (temp.getEndDate().compareTo(currentTime) < 0);
                    if (trace.val)
                        LOG.trace("CurrentTime:" + currentTime + " / EndTime:" + temp.getEndDate() + " [compareTo=" + compareTo + "]");
                    if (temp.hasEndDate() == false || compareTo) {
                        continue;
                    }
                }
                
                // Uniform
                if (this.window_size == null) {
                    itemInfo = temp;
                    break;
                }
                // Temporal Skew
                partition = this.getPartition(temp.getSellerId());
                if (this.window_partitions.contains(partition)) {
                    this.window_histogram.put(partition);
                    itemInfo = temp;
                    break;
                }
            } // WHILE
            if (itemInfo == null) {
                if (debug.val) LOG.debug("Failed to find ItemInfo [hasCurrentPrice=" + needCurrentPrice + ", needFutureEndDate=" + needFutureEndDate + "]");
                return (null);
            }
            assert(idx >= 0);
            
            // Take the item out of the set and insert back to the front
            // This is so that we can maintain MRU->LRU ordering
            itemSet.remove(idx);
            itemSet.addFirst(itemInfo);
        } // SYNCHRONIZED
        if (needCurrentPrice) {
            assert(itemInfo.hasCurrentPrice()) : "Missing currentPrice for " + itemInfo;
            assert(itemInfo.getCurrentPrice() > 0) : "Negative currentPrice '" + itemInfo.getCurrentPrice() + "' for " + itemInfo;
        }
        if (needFutureEndDate) {
            assert(itemInfo.hasEndDate()) : "Missing endDate for " + itemInfo;
        }
        return itemInfo;
    }
    
    /**********************************************************************************************
     * AVAILABLE ITEMS
     **********************************************************************************************/
    public void removeAvailableItem(ItemInfo itemInfo) {
        this.removeItem(this.items_available, itemInfo);
    }
    public ItemInfo getRandomAvailableItemId() {
        return this.getRandomItem(this.items_available, false, false);
    }
    public ItemInfo getRandomAvailableItem(boolean hasCurrentPrice) {
        return this.getRandomItem(this.items_available, hasCurrentPrice, false);
    }
    public int getAvailableItemsCount() {
        return this.items_available.size();
    }

    /**********************************************************************************************
     * ENDING SOON ITEMS
     **********************************************************************************************/
    public void removeEndingSoonItem(ItemInfo itemInfo) {
        this.removeItem(this.items_endingSoon, itemInfo);
    }
    public ItemInfo getRandomEndingSoonItem() {
        return this.getRandomItem(this.items_endingSoon, false, true);
    }
    public ItemInfo getRandomEndingSoonItem(boolean hasCurrentPrice) {
        return this.getRandomItem(this.items_endingSoon, hasCurrentPrice, true);
    }
    public int getEndingSoonItemsCount() {
        return this.items_endingSoon.size();
    }
    
    /**********************************************************************************************
     * WAITING FOR PURCHASE ITEMS
     **********************************************************************************************/
    public void removeWaitForPurchaseItem(ItemInfo itemInfo) {
        this.removeItem(this.items_waitingForPurchase, itemInfo);
    }
    public ItemInfo getRandomWaitForPurchaseItem() {
        return this.getRandomItem(this.items_waitingForPurchase, false, false);
    }
    public int getWaitForPurchaseItemsCount() {
        return this.items_waitingForPurchase.size();
    }

    /**********************************************************************************************
     * COMPLETED ITEMS
     **********************************************************************************************/
    public void removeCompleteItem(ItemInfo itemInfo) {
        this.removeItem(this.items_completed, itemInfo);
    }
    public ItemInfo getRandomCompleteItem() {
        return this.getRandomItem(this.items_completed, false, false);
    }
    public int getCompleteItemsCount() {
        return this.items_completed.size();
    }
    
    /**********************************************************************************************
     * ALL ITEMS
     **********************************************************************************************/
    public int getAllItemsCount() {
        return (this.getAvailableItemsCount() +
                this.getEndingSoonItemsCount() +
                this.getWaitForPurchaseItemsCount() +
                this.getCompleteItemsCount());
    }
    @SuppressWarnings("unchecked")
    public ItemInfo getRandomItem() {
        assert(this.getAllItemsCount() > 0);
        LinkedList<ItemInfo> itemSets[] = new LinkedList[]{
            this.items_available,
            this.items_endingSoon,
            this.items_waitingForPurchase,
            this.items_completed,
        };
        int idx = -1;
        while (idx == -1 || itemSets[idx].isEmpty()) {
            idx = rng.nextInt(itemSets.length);
        } // WHILE
        return (this.getRandomItem(itemSets[idx], false, false));
    }

    // ----------------------------------------------------------------
    // GLOBAL ATTRIBUTE METHODS
    // ----------------------------------------------------------------

    /**
     * Return a random GlobalAttributeValueId
     * @return
     */
    public synchronized GlobalAttributeValueId getRandomGlobalAttributeValue() {
        int offset = rng.nextInt(this.gag_ids.size());
        GlobalAttributeGroupId gag_id = this.gag_ids.get(offset);
        assert(gag_id != null);
        int count = rng.nextInt(gag_id.getCount());
        GlobalAttributeValueId gav_id = new GlobalAttributeValueId(gag_id, count);
        return gav_id;
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

//    @Override
//    public void load(String input_path, Database catalog_db) throws IOException {
//        JSONUtil.load(this, catalog_db, input_path);
//    }
//
//    @Override
//    public void save(String output_path) throws IOException {
//        JSONUtil.save(this, output_path);
//    }
//
//    @Override
//    public String toJSONString() {
//        return (JSONUtil.toJSONString(this));
//    }
//
//    @Override
//    public void toJSON(JSONStringer stringer) throws JSONException {
//        JSONUtil.fieldsToJSON(stringer, this, AuctionMarkBenchmarkProfile.class, JSONUtil.getSerializableFields(this.getClass()));
//    }
//
//    @Override
//    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
//        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, AuctionMarkBenchmarkProfile.class, false, JSONUtil.getSerializableFields(this.getClass()));
//    }
}
