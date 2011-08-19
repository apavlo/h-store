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
import org.voltdb.utils.Pair;

import edu.brown.benchmark.auctionmark.util.CompositeId;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class AuctionMarkClient extends AuctionMarkBaseClient {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Current Timestamp
     */
    private TimestampType current_time = new TimestampType();

    /**
     * The last time that we called CHECK_WINNING_BIDS on this client
     */
    private TimestampType lastCheckWinningBidTime = null;
    
    private final Map<UserId, Integer> seller_item_cnt = new HashMap<UserId, Integer>();

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
                return new Object[] { client.getLastCheckWinningBidTime(),
                                      client.updateAndGetLastCheckWinningBidTime(),
                };
            }
			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
			    if (AuctionMarkConstants.ENABLE_CLOSE_AUCTIONS && client.getClientId() == 0) {
			        TimestampType lastCheckWinningBidTime = client.getLastCheckWinningBidTime();
			        
			        // If we've never checked before, then we'll want to do that now
			        if (lastCheckWinningBidTime == null) return (true);

			        // Otherwise check whether enough time has passed since the last time we checked
			        TimestampType currentTime = client.getCurrentTime();
			        long time_elapsed = Math.round((currentTime.getTime() - lastCheckWinningBidTime.getTime()) / 1000.0);
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
            	ItemId itemId = client.profile.getRandomAvailableItemId();
                return new Object[] { itemId, itemId.getSellerId() };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
				return (client.profile.getAvailableItemIdsCount() > 0);
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
                rand = client.rng.number(0, 100);
                long get_feedback = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_FEEDBACK ? 1 : VoltType.NULL_BIGINT); 

                // ITEM_COMMENT records
                rand = client.rng.number(0, 100);
                long get_comments = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_COMMENTS ? 1 : VoltType.NULL_BIGINT);
                
                // Seller ITEM records
                rand = client.rng.number(0, 100);
                long get_seller_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_SELLER_ITEMS ? 1 : VoltType.NULL_BIGINT); 

                // Buyer ITEM records
                rand = client.rng.number(0, 100);
                long get_buyer_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_BUYER_ITEMS ? 1 : VoltType.NULL_BIGINT);
                
                // USER_WATCH records
                rand = client.rng.number(0, 100);
                long get_watched_items = (rand <= AuctionMarkConstants.PROB_GETUSERINFO_INCLUDE_WATCHED_ITEMS ? 1 : VoltType.NULL_BIGINT); 
                
                return new Object[] { userId, get_feedback, get_comments, get_seller_items, get_buyer_items, get_watched_items };
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
                
                ItemId itemId;
                UserId sellerId;
                UserId buyerId;
                double bid;
                double maxBid;
                
                boolean has_available = (client.profile.getAvailableItemIdsCount() > 0);
                boolean has_waiting = (client.profile.getWaitForPurchaseItemIdsCount() > 0);
                boolean has_completed = (client.profile.getCompleteItemIdsCount() > 0); 
                
                // 5% of NEW_BIDs should be for items that have already ended.
                // This will simulate somebody trying to bid at the very end but failing
                if ((has_waiting || has_completed) && (client.rng.number(1, 100) <= 5 || has_available == false)) {
                    itemId = (has_waiting ? client.profile.getRandomWaitForPurchaseItemId() :
                                            client.profile.getRandomCompleteItemId());
                    sellerId = itemId.getSellerId();
                    buyerId = client.profile.getRandomBuyerId(sellerId);
                    
                    // The bid/maxBid do not matter because they won't be able to actually
                    // update the auction
                    bid = client.rng.nextDouble();
                    maxBid = bid + 100;
                }
                
                // Otherwise we want to generate information for a real bid
                else {
                    assert(has_available);
                    itemId = client.profile.getRandomAvailableItemId(true);
                    sellerId = itemId.getSellerId();
                    buyerId = client.profile.getRandomBuyerId(sellerId);
                    
                    double currentPrice = itemId.getCurrentPrice();
                    bid = client.rng.fixedPoint(2, currentPrice, currentPrice * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2)));
                    maxBid = client.rng.fixedPoint(2, bid, (bid * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2))));
                    itemId.setCurrentPrice(bid);
                }

                return new Object[] { itemId, sellerId, buyerId, bid, maxBid };
            }
			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
			    return (client.profile.getAllItemIdsCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_COMMENT
        // ====================================================================
        NEW_COMMENT(AuctionMarkConstants.FREQUENCY_NEW_COMMENT, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemId itemId = client.profile.getRandomCompleteItemId();
                UserId sellerId = itemId.getSellerId();
                UserId buyerId = client.profile.getRandomBuyerId(sellerId);
                String question = client.rng.astring(10, 128);
                return new Object[] { itemId, sellerId, buyerId, question };
            }
			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
				return (client.profile.getCompleteItemIdsCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_COMMENT_RESPONSE
        // ====================================================================
        NEW_COMMENT_RESPONSE(AuctionMarkConstants.FREQUENCY_NEW_COMMENT_RESPONSE, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                Collections.shuffle(client.pending_commentResponse, client.rng);
                long row[] = client.pending_commentResponse.remove(0);
                assert(row != null);
                
                long commentId = row[0];
                ItemId itemId = new ItemId(row[1]);
                UserId sellerId = itemId.getSellerId();
                String response = client.rng.astring(10, 128);

                return new Object[] { itemId, commentId, sellerId, response };
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
                ItemId itemId = client.profile.getRandomCompleteItemId();
                UserId sellerId = itemId.getSellerId();
                UserId buyerId = client.profile.getRandomBuyerId(sellerId);
                long rating = (long) client.rng.number(-1, 1);
                String feedback = client.rng.astring(10, 80);
                return new Object[] { itemId, sellerId, buyerId, rating, feedback };
            }
			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
				return (client.profile.getCompleteItemIdsCount() > 0);
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

                String name = client.rng.astring(6, 32);
                String description = client.rng.astring(50, 255);
                long categoryId = client.profile.getRandomCategoryId();

                Double initial_price = (double) client.profile.randomInitialPrice.nextInt();
                String attributes = client.rng.astring(50, 255);

                int numAttributes = client.profile.randomNumAttributes.nextInt();
                List<Long> gagList = new ArrayList<Long>(numAttributes);
                List<Long> gavList = new ArrayList<Long>(numAttributes);
                for (int i = 0; i < numAttributes; i++) {
                    Pair<Long, Long> GAGIdGAVIdPair = client.profile.getRandomGAGIdGAVIdPair();
                    if (!gavList.contains(GAGIdGAVIdPair.getSecond())) {
                        gagList.add(GAGIdGAVIdPair.getFirst());
                        gavList.add(GAGIdGAVIdPair.getSecond());
                    }
                } // FOR

                long[] gag_ids = new long[gagList.size()];
                for (int i = 0, cnt = gag_ids.length; i < cnt; i++) {
                    gag_ids[i] = gagList.get(i);
                } // FOR
                long[] gav_ids = new long[gavList.size()];
                for (int i = 0, cnt = gav_ids.length; i < cnt; i++) {
                    gav_ids[i] = gavList.get(i);
                } // FOR

                int numImages = client.profile.randomNumImages.nextInt();
                String[] images = new String[numImages];
                for (int i = 0; i < numImages; i++) {
                    images[i] = client.rng.astring(20, 100);
                } // FOR

                TimestampType start_date = new TimestampType();
                TimestampType end_date = new TimestampType(start_date.getTime() + (long) 10 * (long) 1000000);

                return new Object[] { itemId, sellerId, categoryId, name, description, initial_price, attributes, gag_ids, gav_ids, images, start_date, end_date };
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
                ItemId itemId = client.profile.getRandomWaitForPurchaseItemId();
                UserId sellerId = itemId.getSellerId();
                double buyer_credit = 0d;
                
                // Whether the buyer will not have enough money
                if (itemId.hasCurrentPrice()) {
                    if (client.rng.number(1, 100) < AuctionMarkConstants.PROB_NEW_PURCHASE_NOT_ENOUGH_MONEY) {
                        buyer_credit = -1 * itemId.getCurrentPrice();
                    } else {
                        buyer_credit = itemId.getCurrentPrice();
                        client.profile.removeWaitForPurchaseItemId(itemId);
                        client.profile.addCompleteItemId(itemId);
                    }
                }
                return new Object[] { itemId, sellerId, buyer_credit };
            }
			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
				return (client.profile.getWaitForPurchaseItemIdsCount() > 0);
			}
        }),
        // ====================================================================
        // UPDATE_ITEM
        // ====================================================================
        UPDATE_ITEM(AuctionMarkConstants.FREQUENCY_UPDATE_ITEM, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generateParams(AuctionMarkClient client) {
                ItemId itemId = client.profile.getRandomAvailableItemId();
                UserId sellerId = itemId.getSellerId();
                String description = client.rng.astring(50, 255);
                
                long delete_attribute = VoltType.NULL_BIGINT;
                long add_attribute[] = {
                    VoltType.NULL_BIGINT,
                    VoltType.NULL_BIGINT
                };
                
                // Delete ITEM_ATTRIBUTE
                if (client.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_DELETE_ATTRIBUTE) {
                    delete_attribute = 1;
                }
                // Add ITEM_ATTRIBUTE
                else if (client.rng.number(1, 100) < AuctionMarkConstants.PROB_UPDATEITEM_ADD_ATTRIBUTE) {
                    Pair<Long, Long> gag_gav = client.profile.getRandomGAGIdGAVIdPair();
                    assert(gag_gav != null);
                    add_attribute[0] = gag_gav.getFirst();
                    add_attribute[1] = gag_gav.getSecond();
                }
                
                return new Object[] { itemId, sellerId, description, delete_attribute, add_attribute };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client) {
				return (client.profile.getAvailableItemIdsCount() > 0);
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
            if (debug.get()) LOG.debug("Checking whether we can execute " + this + " now");
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
        edu.brown.benchmark.BenchmarkComponent.main(AuctionMarkClient.class, args, false);
    }

    /**
     * Constructor
     * @param args
     */
    public AuctionMarkClient(String[] args) {
        super(AuctionMarkClient.class, args);
        
        // Initialize Default Weights
        for (Transaction t : Transaction.values()) {
            this.weights.put(t, t.getDefaultWeight());
        } // FOR

        // Create xact lookup array
        int total = 0;
        for (Transaction t : Transaction.values()) {
            for (int i = 0, cnt = this.weights.get(t); i < cnt; i++) {
            	LOG.debug("xact " + total + " = " + t + ":" + t.getCallName());
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
    protected void tick() {
        super.tick();
        profile.tick();
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        Transaction txn = null;
        Object[] params = null;

        // Always update the current timestamp
        this.updateAndGetCurrentTime();

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
                int idx = this.rng.number(0, this.xacts.length - 1);
                if (trace.get()) {
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
                if (trace.get()) LOG.trace("CAN EXECUTE: " + txn);
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
        } else if (debug.get()) {
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
        protected final Transaction txn;
        protected final Object params[];
        
        public BaseCallback(Transaction txn, Object params[]) {
            this.txn = txn;
            this.params = params;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (trace.get()) LOG.trace("clientCallback(cid = " + getClientId() + "):: txn = " + txn.getDisplayName());
            incrementTransactionCounter(this.txn.ordinal());
            VoltTable[] results = clientResponse.getResults();
            this.process(results);
            
            if (debug.get() && clientResponse.getStatus() != ClientResponse.SUCCESS) {
                LOG.debug(String.format("%s: %s", this.txn, clientResponse.getStatusString()), clientResponse.getException());
            }
        }
        
        public abstract void process(VoltTable results[]);
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
            
            int with_bid = 0;
            int without_bid = 0;
            
            while (results[0].advanceRow()) {
                ItemId itemId = new ItemId(results[0].getLong("i_id"));
                UserId sellerId = new UserId(results[0].getLong("i_u_id"));
                UserId buyerId = new UserId(results[0].getLong("ib_buyer_id"));
                long itemStatus = results[0].getLong("i_status");
                assert(itemStatus == AuctionMarkConstants.STATUS_ITEM_OPEN);
                double currentPrice = results[0].getDouble("i_current_price");
                if (results[0].wasNull() == false) itemId.setCurrentPrice(currentPrice);

                profile.removeAvailableItemId(itemId);
                // No winning bid
                results[0].getLong("imb_ib_id");
                if (results[0].wasNull()) {
                    profile.addCompleteItemId(itemId);
                    with_bid++;
                }
                // Has winning bid
                else {
                    profile.addWaitForPurchaseItemId(itemId);
                    without_bid++;
                }
            } // WHILE
            results[0].resetRowPosition();
            
            if (debug.get()) LOG.debug(String.format("%s - WithBids=%d, NoBids=%d", txn, with_bid, without_bid));
        }
    }
    
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
            assert(results.length > 0);
            while (results[0].advanceRow()) {
                long i_id = results[0].getLong("i_id");
                if (results[0].wasNull() == false) {
                    ItemId itemId = new ItemId(i_id);
                    double current_price = results[0].getDouble("i_current_price");
                    if (results[0].wasNull() == false) {
                        itemId.setCurrentPrice(current_price);
                    }
                    profile.addAvailableItemId(itemId);
                }
            } // WHILE    
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
            
            int idx = 1;
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
                    try {
                        long vals[] = {
                            results[0].getLong("ic_id"),
                            results[0].getLong("ic_i_id"),
                            results[0].getLong("ic_u_id")
                        };
                        pending_commentResponse.add(vals);
                    } catch (Throwable ex) {
                        LOG.error("PARAMS: " + Arrays.toString(this.params));
                        throw new RuntimeException("?????");
                    }
                } // WHILE
            }
            
            // ITEM Result Tables
            for (int i = idx; i < results.length; i++) {
                VoltTable vt = results[i];
                assert(vt != null);
                
                while (vt.advanceRow()) {
                    ItemId itemId = new ItemId(vt.getLong("i_id"));
                    double current_price = vt.getDouble("i_current_price");
                    if (vt.wasNull() == false) itemId.setCurrentPrice(current_price);
                    int status = (int)vt.getLong("i_status");
                    switch (status) {
                        case AuctionMarkConstants.STATUS_ITEM_OPEN:
                            profile.addAvailableItemId(itemId);
                            break;
                        case AuctionMarkConstants.STATUS_ITEM_WAITING_FOR_PURCHASE:
                            profile.addWaitForPurchaseItemId(itemId);
                            break;
                        case AuctionMarkConstants.STATUS_ITEM_CLOSED:
                            profile.addCompleteItemId(itemId);
                            break;
                        default:
                            assert(false) : "Unexpected status '" + status + "' for " + itemId;
                    } // SWITCH
                } // WHILE
            } // FOR
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
            if (results.length > 0 && results[0].advanceRow()) {
                ItemId itemId = new ItemId(results[0].getLong("i_id"));
                itemId.setCurrentPrice(results[0].getDouble("i_current_price"));
                UserId sellerId = new UserId(results[0].getLong("i_u_id"));
                assert (itemId.getSellerId().equals(sellerId));
                profile.addAvailableItemId(itemId);
            }
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
            if (results.length > 0 && results[0].advanceRow()) {
                ItemId itemId = new ItemId(results[0].getLong("ip_ib_i_id"));
                UserId sellerId = new UserId(results[0].getLong("u_id"));
                if (trace.get())
                    LOG.trace("clientCallback:: NEW_PURCHASE itemId = " + itemId);
                if (trace.get())
                    LOG.trace("clientCallback:: NEW_PURCHASE sellerId = " + sellerId);
                profile.removeWaitForPurchaseItemId(itemId);
                profile.addCompleteItemId(itemId);
            }
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
    
    
    public synchronized TimestampType updateAndGetCurrentTime() {
        this.current_time = new TimestampType();
        return (this.current_time);
    }

    public TimestampType getCurrentTime() {
        return (this.current_time);
    }

    public TimestampType getLastCheckWinningBidTime() {
        return (this.lastCheckWinningBidTime);
    }

    public synchronized TimestampType updateAndGetLastCheckWinningBidTime() {
        this.lastCheckWinningBidTime = new TimestampType();
        return (this.lastCheckWinningBidTime);
    }
}