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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants.ItemStatus;
import edu.brown.benchmark.auctionmark.util.GlobalAttributeValueId;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.ItemInfo;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CompositeId;
import edu.brown.utils.StringUtil;

public class AuctionMarkClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    protected final AuctionMarkProfile profile;
    
    /**
     * TODO
     */
    private final Map<UserId, Integer> seller_item_cnt = new HashMap<UserId, Integer>();

    /**
     * TODO
     */
    private final List<long[]> pending_commentResponse = Collections.synchronizedList(new ArrayList<long[]>());
    
    // --------------------------------------------------------------------
    // TXN PARAMETER GENERATOR
    // --------------------------------------------------------------------
    public interface AuctionMarkParamGenerator {
        /**
         * Returns true if the client will be able to successfully generate a new transaction call
         * The client passes in the current BenchmarkProfile handle and an optional VoltTable. This allows
         * you to invoke one txn using the output of a previously run txn.
         * Note that this is not thread safe, so you'll need to combine the call to this with generate()
         * in a single synchronization block.
         * @param client
         * @return
         */
        public boolean canGenerateParam(AuctionMarkClient client);
        /**
         * Generate the parameters array
         * Any elements that are CompositeIds will automatically be encoded before being
         * shipped off to the H-Store cluster
         * @param client
         * @return
         */
        public Object[] generateParams(AuctionMarkClient client);
    }
    
    // --------------------------------------------------------------------
    // BENCHMARK TRANSACTIONS
    // --------------------------------------------------------------------
    public enum Transaction {
        // ====================================================================
        // CLOSE_AUCTIONS
        // ====================================================================
        CLOSE_AUCTIONS(AuctionMarkConstants.FREQUENCY_CLOSE_AUCTIONS, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                return new Object[] { client.getTimestampParameterArray(),
                                      client.profile.getLastCloseAuctionsTime(),
                                      client.profile.updateAndGetLastCloseAuctionsTime() };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                if (AuctionMarkConstants.ENABLE_CLOSE_AUCTIONS && client.getClientId() == 0) {
                    // If we've never checked before, then we'll want to do that now
                    if (client.profile.hasLastCloseAuctionsTime() == false) return (true);

                    // Otherwise check whether enough time has passed since the last time we checked
                    TimestampType lastCheckWinningBidTime = client.profile.getLastCloseAuctionsTime();
                    TimestampType currentTime = client.profile.getCurrentTime();
                    long time_elapsed = Math.round((currentTime.getTime() - lastCheckWinningBidTime.getTime()) / 1000.0);
                    if (debug.val) LOG.debug(String.format("%s [start=%s, current=%s, elapsed=%d]", Transaction.CLOSE_AUCTIONS, client.profile.getBenchmarkStartTime(), currentTime, time_elapsed));
                    if (time_elapsed > AuctionMarkConstants.INTERVAL_CLOSE_AUCTIONS) return (true);
                }
                return (false);
            }
        }),
        // ====================================================================
        // GET_ITEM
        // ====================================================================
        GET_ITEM(AuctionMarkConstants.FREQUENCY_GET_ITEM, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemInfo itemInfo = client.profile.getRandomAvailableItemId();
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, itemInfo.getSellerId() };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.profile.getAvailableItemsCount() > 0);
            }
        }),
        // ====================================================================
        // GET_USER_INFO
        // ====================================================================
        GET_USER_INFO(AuctionMarkConstants.FREQUENCY_GET_USER_INFO, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                UserId userId = client.profile.getRandomBuyerId();
                int rand;
                
                // USER_FEEDBACK records
                rand = client.profile.rng.number(0, 100);
                long get_feedback = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_FEEDBACK ? 1 : VoltType.NULL_BIGINT); 

                // ITEM_COMMENT records
                rand = client.profile.rng.number(0, 100);
                long get_comments = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_COMMENTS ? 1 : VoltType.NULL_BIGINT);
                
                // Seller ITEM records
                rand = 100; // client.profile.rng.number(0, 100);
                long get_seller_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_SELLER_ITEMS ? 1 : VoltType.NULL_BIGINT); 

                // Buyer ITEM records
                rand = 100; // client.profile.rng.number(0, 100);
                long get_buyer_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_BUYER_ITEMS ? 1 : VoltType.NULL_BIGINT);
                
                // USER_WATCH records
                rand = 100; // client.profile.rng.number(0, 100);
                long get_watched_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_WATCHED_ITEMS ? 1 : VoltType.NULL_BIGINT); 
                
                return new Object[] { client.getTimestampParameterArray(),
                                      userId,
                                      get_feedback, get_comments,
                                      get_seller_items, get_buyer_items, get_watched_items };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (true);
            }
        }),
        // ====================================================================
        // NEW_BID
        // ====================================================================
        NEW_BID(AuctionMarkConstants.FREQUENCY_NEW_BID, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                
                ItemInfo itemInfo = null;
                UserId sellerId;
                UserId buyerId;
                double bid;
                double maxBid;
                
                boolean has_available = (client.profile.getAvailableItemsCount() > 0);
                boolean has_ending = (client.profile.getEndingSoonItemsCount() > 0);
                boolean has_waiting = (client.profile.getWaitForPurchaseItemsCount() > 0);
                boolean has_completed = (client.profile.getCompleteItemsCount() > 0); 
                
                // Some NEW_BIDs will be for items that have already ended.
                // This will simulate somebody trying to bid at the very end but failing
                if ((has_waiting || has_completed) &&
                    (client.profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM || has_available == false)) {
                    if (has_waiting) {
                        itemInfo = client.profile.getRandomWaitForPurchaseItem();
                        assert(itemInfo != null) : "Failed to get WaitForPurchase itemInfo [" + client.profile.getWaitForPurchaseItemsCount() + "]";
                    } else {
                        itemInfo = client.profile.getRandomCompleteItem();
                        assert(itemInfo != null) : "Failed to get Completed itemInfo [" + client.profile.getCompleteItemsCount() + "]";
                    }
                    sellerId = itemInfo.getSellerId();
                    buyerId = client.profile.getRandomBuyerId(sellerId);
                    
                    // The bid/maxBid do not matter because they won't be able to actually
                    // update the auction
                    bid = client.profile.rng.nextDouble();
                    maxBid = bid + 100;
                }
                
                // Otherwise we want to generate information for a real bid
                else {
                    assert(has_available || has_ending);
                    // 50% of NEW_BIDS will be for items that are ending soon
                    if ((has_ending && client.profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_NEWBID_CLOSED_ITEM) || has_available == false) {
                        itemInfo = client.profile.getRandomEndingSoonItem(true);
                    }
                    if (itemInfo == null) {
                        itemInfo = client.profile.getRandomAvailableItem(true);
                    }
                    if (itemInfo == null) {
                        itemInfo = client.profile.getRandomItem();
                    }
                    
                    sellerId = itemInfo.getSellerId();
                    buyerId = client.profile.getRandomBuyerId(sellerId);
                    
                    double currentPrice = itemInfo.getCurrentPrice();
                    bid = client.profile.rng.fixedPoint(2, currentPrice, currentPrice * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2)));
                    maxBid = client.profile.rng.fixedPoint(2, bid, (bid * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2))));
                }

                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyerId, maxBid, itemInfo.endDate };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.profile.getAllItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_COMMENT
        // ====================================================================
        NEW_COMMENT(AuctionMarkConstants.FREQUENCY_NEW_COMMENT, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemInfo itemInfo = client.profile.getRandomCompleteItem();
                UserId sellerId = itemInfo.getSellerId();
                UserId buyerId = client.profile.getRandomBuyerId(sellerId);
                String question = client.profile.rng.astring(10, 128);
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyerId, question };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.profile.getCompleteItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_COMMENT_RESPONSE
        // ====================================================================
        NEW_COMMENT_RESPONSE(AuctionMarkConstants.FREQUENCY_NEW_COMMENT_RESPONSE, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                Collections.shuffle(client.pending_commentResponse, client.profile.rng);
                long row[] = client.pending_commentResponse.remove(0);
                assert(row != null);
                
                long commentId = row[0];
                ItemId itemId = new ItemId(row[1]);
                UserId sellerId = itemId.getSellerId();
                String response = client.profile.rng.astring(10, 128);

                return new Object[] { client.getTimestampParameterArray(),
                                      itemId, sellerId, commentId, response };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.pending_commentResponse.isEmpty() == false);
            }
        }),
        // ====================================================================
        // NEW_FEEDBACK
        // ====================================================================
        NEW_FEEDBACK(AuctionMarkConstants.FREQUENCY_NEW_FEEDBACK, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemInfo itemInfo = client.profile.getRandomCompleteItem();
                UserId sellerId = itemInfo.getSellerId();
                UserId buyerId = client.profile.getRandomBuyerId(sellerId);
                long rating = (long) client.profile.rng.number(-1, 1);
                String feedback = client.profile.rng.astring(10, 80);
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyerId, rating, feedback };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.profile.getCompleteItemsCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_ITEM
        // ====================================================================
        NEW_ITEM(AuctionMarkConstants.FREQUENCY_NEW_ITEM, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                UserId sellerId = client.profile.getRandomSellerId(client.getClientId());
                ItemId itemId = client.getNextItemId(sellerId);

                String name = client.profile.rng.astring(6, 32);
                String description = client.profile.rng.astring(50, 255);
                long categoryId = client.profile.getRandomCategoryId();

                Double initial_price = (double) client.profile.randomInitialPrice.nextInt();
                String attributes = client.profile.rng.astring(50, 255);

                int numAttributes = client.profile.randomNumAttributes.nextInt();
                List<GlobalAttributeValueId> gavList = new ArrayList<GlobalAttributeValueId>(numAttributes);
                for (int i = 0; i < numAttributes; i++) {
                    GlobalAttributeValueId gav_id = client.profile.getRandomGlobalAttributeValue();
                    if (!gavList.contains(gav_id)) gavList.add(gav_id);
                } // FOR

                long[] gag_ids = new long[gavList.size()];
                long[] gav_ids = new long[gavList.size()];
                for (int i = 0, cnt = gag_ids.length; i < cnt; i++) {
                    GlobalAttributeValueId gav_id = gavList.get(i);
                    gag_ids[i] = gav_id.getGlobalAttributeGroup().encode();
                    gav_ids[i] = gav_id.encode();
                } // FOR

                int numImages = client.profile.randomNumImages.nextInt();
                String[] images = new String[numImages];
                for (int i = 0; i < numImages; i++) {
                    images[i] = client.profile.rng.astring(20, 100);
                } // FOR

                long duration = client.profile.randomDuration.nextInt();

                return new Object[] { client.getTimestampParameterArray(),
                                      itemId, sellerId, categoryId,
                                      name, description, duration, initial_price, attributes,
                                      gag_ids, gav_ids, images };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (true);
            }
        }),
        // ====================================================================
        // NEW_PURCHASE
        // ====================================================================
        NEW_PURCHASE(AuctionMarkConstants.FREQUENCY_NEW_PURCHASE, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemInfo itemInfo = client.profile.getRandomWaitForPurchaseItem();
                UserId sellerId = itemInfo.getSellerId();
                double buyer_credit = 0d;
                
                // Whether the buyer will not have enough money
                if (itemInfo.hasCurrentPrice()) {
                    if (client.profile.rng.number(1, 100) < AuctionMarkConstants.PROB_NEW_PURCHASE_NOT_ENOUGH_MONEY) {
                        buyer_credit = -1 * itemInfo.getCurrentPrice();
                    } else {
                        buyer_credit = itemInfo.getCurrentPrice();
                        client.profile.removeWaitForPurchaseItem(itemInfo);
                    }
                }
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, buyer_credit };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.profile.getWaitForPurchaseItemsCount() > 0);
            }
        }),
        // ====================================================================
        // UPDATE_ITEM
        // ====================================================================
        UPDATE_ITEM(AuctionMarkConstants.FREQUENCY_UPDATE_ITEM, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemInfo itemInfo = client.profile.getRandomAvailableItemId();
                UserId sellerId = itemInfo.getSellerId();
                String description = client.profile.rng.astring(50, 255);
                
                long delete_attribute = VoltType.NULL_BIGINT;
                long add_attribute[] = {
                    VoltType.NULL_BIGINT,
                    VoltType.NULL_BIGINT
                };
                
                // Delete ITEM_ATTRIBUTE
                if (client.profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_DELETE_ATTRIBUTE) {
                    delete_attribute = 1;
                }
                // Add ITEM_ATTRIBUTE
                else if (client.profile.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_ADD_ATTRIBUTE) {
                    GlobalAttributeValueId gav_id = client.profile.getRandomGlobalAttributeValue();
                    assert(gav_id != null);
                    add_attribute[0] = gav_id.getGlobalAttributeGroup().encode();
                    add_attribute[1] = gav_id.encode();
                }
                
                return new Object[] { client.getTimestampParameterArray(),
                                      itemInfo.itemId, sellerId, description,
                                      delete_attribute, add_attribute };
            }
            @Override
            public boolean canGenerateParam(AuctionMarkClient client) {
                return (client.profile.getAvailableItemsCount() > 0);
            }
        }), 
        ;
        
        /**
         * Constructor
         * @param weight The execution frequency weight for this txn 
         * @param generator
         */
        private Transaction(int weight, AuctionMarkParamGenerator generator) {
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
            this.callName = this.displayName.replace(" ", "");
            this.generator = generator;
        }

        public final int default_weight;
        public final String displayName;
        public final String callName;
        public final AuctionMarkParamGenerator generator;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
        }
        
        public int getDefaultWeight() {
            return (this.default_weight);
        }
        
        public String getDisplayName() {
            return (this.displayName);
        }
        
        public String getCallName() {
            return (this.callName);
        }
        
        /**
         * This will return true if we can call a new transaction for this procedure
         * A txn can be called if we can generate all of the parameters we need
         * @return
         */
        public boolean canExecute(AuctionMarkClient client) {
            if (debug.val) LOG.debug("Checking whether we can execute " + this + " now");
            return this.generator.canGenerateParam(client);
        }
        
        /**
         * Given a BenchmarkProfile object, call the AuctionMarkParamGenerator object for a given
         * transaction type to generate a set of parameters for a new txn invocation 
         * @param profile
         * @return
         */
        public Object[] generateParams(AuctionMarkClient client) {
            Object vals[] = this.generator.generateParams(client);
            // Automatically encode any CompositeIds
            for (int i = 0; i < vals.length; i++) {
                if (vals[i] instanceof CompositeId) vals[i] = ((CompositeId)vals[i]).encode();
            } // FOR
            return (vals);
        }
    }

    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final Map<Transaction, Integer> weights = new HashMap<Transaction, Integer>();
    private final Transaction xacts[] = new Transaction[100];
    
    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        edu.brown.api.BenchmarkComponent.main(AuctionMarkClient.class, args, false);
    }

    /**
     * Constructor
     * @param args
     */
    public AuctionMarkClient(String[] args) {
        super(args);
        
        int seed = 0;
        String randGenClassName = DefaultRandomGenerator.class.getName();
        String randGenProfilePath = null;
        Integer temporal_window = null;
        Integer temporal_total = null;
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Random Generator Seed
            if (key.equalsIgnoreCase("RANDOMSEED")) {
                seed = Integer.parseInt(value);
            }
            // Random Generator Class
            else if (key.equalsIgnoreCase("RANDOMGENERATOR")) {
                randGenClassName = value;
            }
            // Random Generator Profile File
            else if (key.equalsIgnoreCase("RANDOMPROFILE")) {
                randGenProfilePath = value;
            }
            // Temporal Skew
            else if (key.equalsIgnoreCase("TEMPORALWINDOW")) {
                assert(m_extraParams.containsKey("TEMPORALTOTAL")) : "Missing TEMPORALTOTAL parameter";
                temporal_window = Integer.valueOf(m_extraParams.get("TEMPORALWINDOW"));
                temporal_total = Integer.valueOf(m_extraParams.get("TEMPORALTOTAL"));
            }
        } // FOR
        
        // Random Generator
        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        
        // BenchmarkProfile
        profile = new AuctionMarkProfile(rng, getNumClients());
        profile.loadProfile(this);
        
        // Initialize Default Weights
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            this.weights.put(t, (weight != null ? weight : t.getDefaultWeight()));
        } // FOR

        // Create xact lookup array
        int total = 0;
        for (Transaction t : Transaction.values()) {
            for (int i = 0, cnt = this.weights.get(t); i < cnt; i++) {
                if (trace.val)
                    LOG.trace("xact " + total + " = " + t + ":" + t.getCallName());
                this.xacts[total++] = t;
            } // FOR
        } // FOR
        assert(total == xacts.length) : "The total weight for the transactions is " + total + ". It needs to be " + xacts.length;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        for (Transaction t : Transaction.values()) {
            names[t.ordinal()] = t.getDisplayName();
        }
        return names;
    }
    
    @Override
    public void runLoop() {
        final Client client = this.getClientHandle();
        
        // Execute Transactions
        try {
            while (true) {
                runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        } catch (NoConnectionsException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (IOException e) {
            /*
             * At shutdown an IOException is thrown for every connection to
             * the DB that is lost Ignore the exception here in order to not
             * get spammed, but will miss lost connections at runtime
             */
        }
    }
    
    @Override
    public void tickCallback(int counter) {
        super.tickCallback(counter);
        profile.tick();
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        // We need to subtract the different between this and the profile's start time,
        // since that accounts for the time gap between when the loader started and when the client start.
        // Otherwise, all of our cache date will be out dated if it took a really long time
        // to load everything up. Again, in order to keep things in synch, we only want to
        // set this on the first call to runOnce(). This will account for start a bunch of
        // clients on multiple nodes but then having to wait until they're all up and running
        // before starting the actual benchmark run.
        if (profile.hasClientStartTime() == false) profile.setAndGetClientStartTime();
        
        Transaction txn = null;
        Object[] params = null;

        // Always update the current timestamp
        profile.updateAndGetCurrentTime();

        // Find the next txn and its parameters that we will run. We want to wrap this
        // around a synchronization block so that nobody comes in and takes the parameters
        // from us before we actually run it
        int safety = 1000;
        while (safety-- > 0) {
            Transaction tempTxn = null;
            
            // Always check if we need to want to run CLOSE_AUCTIONS
            // We only do this from the first client
            if (Transaction.CLOSE_AUCTIONS.canExecute(this)) {
                tempTxn = Transaction.CLOSE_AUCTIONS;
            }
            // Otherwise randomly pick a transaction based on their distribution weights
            else {
                int idx = profile.rng.number(0, this.xacts.length - 1);
                if (trace.val) {
                    LOG.trace("idx = " + idx);
                    LOG.trace("random txn = " + this.xacts[idx].getDisplayName());
                }
                assert (idx >= 0);
                assert (idx < this.xacts.length);
                tempTxn = this.xacts[idx];
            }

            // Only execute this txn if it is ready
            // Example: NewBid can only be executed if there are item_ids retrieved by an earlier call by GetItem
            if (tempTxn.canExecute(this)) {
                txn = tempTxn;
                if (trace.val) LOG.trace("CAN EXECUTE: " + txn);
                try {
                    params = txn.generateParams(this);
                } catch (Throwable ex) {
                    throw new RuntimeException("Failed to generate parameters for " + txn, ex);
                }
                break;
            }
        } // WHILE
        assert (txn != null);
        
        if (params == null) {
            LOG.warn("Unable to execute " + txn + " because the parameters were null?");
            return (false);
        } else if (debug.val) {
            LOG.info("Executing new invocation of transaction " + txn);
        }
        
        BaseCallback callback = null;
        switch (txn) {
            case CLOSE_AUCTIONS:
                callback = new CloseAuctionsCallback(params);
                break;
            case GET_ITEM:
                callback = new GetItemCallback(params);
                break;
            case GET_USER_INFO:
                callback = new GetUserInfoCallback(params);
                break;
            case NEW_COMMENT:
                callback = new NewCommentCallback(params);
                break;
            case NEW_ITEM:
                callback = new NewItemCallback(params);
                break;
            case NEW_PURCHASE:
                callback = new NewPurchaseCallback(params);
                break;
            default:
                callback = new NullCallback(txn, params);
        } // SWITCH
        this.getClientHandle().callProcedure(callback, txn.getCallName(), params);
        return (true);
    }
    
    /**********************************************************************************************
     * Base Callback
     **********************************************************************************************/
    protected abstract class BaseCallback implements ProcedureCallback {
        final Transaction txn;
        final Object params[];
        final ObjectHistogram<ItemStatus> updated = new ObjectHistogram<ItemStatus>();
        
        public BaseCallback(Transaction txn, Object params[]) {
            this.txn = txn;
            this.params = params;
        }
        
        public abstract void process(VoltTable results[]);
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (trace.val) LOG.trace("clientCallback(cid = " + getClientId() + "):: txn = " + txn.getDisplayName());
            incrementTransactionCounter(clientResponse, this.txn.ordinal());
            VoltTable[] results = clientResponse.getResults();
            if (clientResponse.getStatus() == Status.OK) {
                try {
                    this.process(results);
                } catch (Throwable ex) {
                    LOG.error("PARAMS: " + Arrays.toString(this.params));
                    for (int i = 0; i < results.length; i++) {
                        LOG.info(String.format("[%02d] RESULT\n%s", i, results[i]));
                    } // FOR
                    throw new RuntimeException("Failed to process results for " + this.txn, ex);
                }
                    
            } else {
                if (debug.val) LOG.debug(String.format("%s: %s", this.txn, clientResponse.getStatusString()), clientResponse.getException());
            }
        }
        /**
         * For the given VoltTable that contains ITEM records, process the current
         * row of that table and update the benchmark profile based on item information
         * stored in that row. 
         * @param vt
         * @return
         */
        public ItemId processItemRecord(VoltTable vt) {
            ItemId itemId = new ItemId(vt.getLong("i_id"));
            TimestampType endDate = vt.getTimestampAsTimestamp("i_end_date");
            short numBids = (short)vt.getLong("i_num_bids");
            double currentPrice = vt.getDouble("i_current_price");
            ItemInfo itemInfo = new ItemInfo(itemId, currentPrice, endDate, numBids);
            if (vt.hasColumn("ip_id")) itemInfo.status = ItemStatus.CLOSED;
            if (vt.hasColumn("i_status")) itemInfo.status = ItemStatus.get(vt.getLong("i_status"));
            
            UserId sellerId = new UserId(vt.getLong("i_u_id"));
            assert (itemId.getSellerId().equals(sellerId));
            
            ItemStatus qtype = profile.addItemToProperQueue(itemInfo, false);
            this.updated.put(qtype);

            return (itemId);
        }
        @Override
        public String toString() {
            String cnts[] = new String[ItemStatus.values().length];
            for (ItemStatus qtype : ItemStatus.values()) {
                cnts[qtype.ordinal()] = String.format("%s=+%d", qtype, updated.get(qtype, 0));
            }
            return String.format("%s :: %s", this.txn, StringUtil.join(", ", cnts));
        }
    } // END CLASS
    
    /**********************************************************************************************
     * NULL Callback
     **********************************************************************************************/
    protected class NullCallback extends BaseCallback {
        public NullCallback(Transaction txn, Object params[]) {
            super(txn, params);
        }
        @Override
        public void process(VoltTable[] results) {
            // Nothing to do...
        }
    } // END CLASS
    
    /**********************************************************************************************
     * CLOSE_AUCTIONS Callback
     **********************************************************************************************/
    protected class CloseAuctionsCallback extends BaseCallback {
        public CloseAuctionsCallback(Object params[]) {
            super(Transaction.CLOSE_AUCTIONS, params);
        }
        @Override
        public void process(VoltTable[] results) {
            assert (null != results && results.length > 0);
            while (results[0].advanceRow()) {
                ItemId itemId = this.processItemRecord(results[0]);
                assert(itemId != null);
            } // WHILE
            if (debug.val) LOG.debug(super.toString());
            profile.updateItemQueues();
        }
    } // END CLASS
    
    /**********************************************************************************************
     * NEW_COMMENT Callback
     **********************************************************************************************/
    protected class NewCommentCallback extends BaseCallback {
        public NewCommentCallback(Object params[]) {
            super(Transaction.NEW_COMMENT, params);
        }
        @Override
        public void process(VoltTable[] results) {
            assert(results.length == 1);
            while (results[0].advanceRow()) {
                long vals[] = {
                    results[0].getLong("ic_id"),
                    results[0].getLong("ic_i_id"),
                    results[0].getLong("ic_u_id")
                };
                pending_commentResponse.add(vals);
            } // WHILE
        }
    } // END CLASS
        
    /**********************************************************************************************
     * GET_ITEM Callback
     **********************************************************************************************/
    protected class GetItemCallback extends BaseCallback {
        public GetItemCallback(Object params[]) {
            super(Transaction.GET_ITEM, params);
        }
        @Override
        public void process(VoltTable[] results) {
            assert (null != results && results.length > 0);
            while (results[0].advanceRow()) {
                ItemId itemId = this.processItemRecord(results[0]);
                assert(itemId != null);
            } // WHILE
            if (debug.val) LOG.debug(super.toString());
        }
    } // END CLASS
    
    /**********************************************************************************************
     * GET_USER_INFO Callback
     **********************************************************************************************/
    protected class GetUserInfoCallback extends BaseCallback {
        final boolean expect_user;
        final boolean expect_feedback;
        final boolean expect_comments;
        final boolean expect_seller;
        final boolean expect_buyer;
        final boolean expect_watched;
        
        public GetUserInfoCallback(Object params[]) {
            super(Transaction.GET_USER_INFO, params);
            
            int idx = 2;
            this.expect_user     = true;
            this.expect_feedback = ((Long)params[idx++] != VoltType.NULL_BIGINT);
            this.expect_comments = ((Long)params[idx++] != VoltType.NULL_BIGINT);
            this.expect_seller   = ((Long)params[idx++] != VoltType.NULL_BIGINT);
            this.expect_buyer    = ((Long)params[idx++] != VoltType.NULL_BIGINT);
            this.expect_watched  = ((Long)params[idx++] != VoltType.NULL_BIGINT);
        }
        @Override
        public void process(VoltTable[] results) {
            int idx = 0;
            
            // USER
            if (expect_user) {
                VoltTable vt = results[idx++];
                assert(vt != null);
                assert(vt.getRowCount() > 0);
            }
            // USER_FEEDBACK
            if (expect_feedback) {
                VoltTable vt = results[idx++];
                assert(vt != null);
            }
            // ITEM_COMMENT
            if (expect_comments) {
                VoltTable vt = results[idx++];
                assert(vt != null);
                while (vt.advanceRow()) {
                    long vals[] = {
                        vt.getLong("ic_id"),
                        vt.getLong("ic_i_id"),
                        vt.getLong("ic_u_id")
                    };
                    pending_commentResponse.add(vals);
                } // WHILE
            }
            
            // ITEM Result Tables
            for (int i = idx; i < results.length; i++) {
                VoltTable vt = results[i];
                assert(vt != null);
                while (vt.advanceRow()) {
                    ItemId itemId = this.processItemRecord(vt);
                    assert(itemId != null);
                } // WHILE
            } // FOR
        }
    } // END CLASS
    
    /**********************************************************************************************
     * NEW_BID Callback
     **********************************************************************************************/
    protected class NewBidCallback extends BaseCallback {
        public NewBidCallback(Object params[]) {
            super(Transaction.NEW_BID, params);
        }
        @Override
        public void process(VoltTable[] results) {
            assert(results.length == 1);
            while (results[0].advanceRow()) {
                ItemId itemId = this.processItemRecord(results[0]);
                assert(itemId != null);
            } // WHILE
            if (debug.val) LOG.debug(super.toString());
        }
    } // END CLASS

    /**********************************************************************************************
     * NEW_ITEM Callback
     **********************************************************************************************/
    protected class NewItemCallback extends BaseCallback {
        public NewItemCallback(Object params[]) {
            super(Transaction.NEW_ITEM, params);
        }
        @Override
        public void process(VoltTable[] results) {
            assert(results.length == 1);
            while (results[0].advanceRow()) {
                ItemId itemId = this.processItemRecord(results[0]);
                assert(itemId != null);
            } // WHILE
            if (debug.val) LOG.debug(super.toString());
        }
    } // END CLASS
    
    /**********************************************************************************************
     * NEW_PURCHASE Callback
     **********************************************************************************************/
    protected class NewPurchaseCallback extends BaseCallback {
        public NewPurchaseCallback(Object params[]) {
            super(Transaction.NEW_PURCHASE, params);
        }
        @Override
        public void process(VoltTable[] results) {
            assert(results.length == 1);
            while (results[0].advanceRow()) {
                ItemId itemId = this.processItemRecord(results[0]);
                assert(itemId != null);
            } // WHILE
            if (debug.val) LOG.debug(super.toString());
        }
    } // END CLASS
  
    
    public ItemId getNextItemId(UserId seller_id) {
        Integer cnt = this.seller_item_cnt.get(seller_id);
        if (cnt == null || cnt == 0) {
            cnt = (int)seller_id.getItemCount();
        }
        this.seller_item_cnt.put(seller_id, ++cnt);
        return (new ItemId(seller_id, cnt));
    }
    
    public TimestampType[] getTimestampParameterArray() {
        return new TimestampType[] { profile.getBenchmarkStartTime(),
                                     profile.getClientStartTime() };
    }
}