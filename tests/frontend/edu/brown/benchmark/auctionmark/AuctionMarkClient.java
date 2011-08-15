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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
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
    
    // inclusive offsets
    private long _partitionStartOffset;
    private long _partitionEndOffset;

    private transient final AtomicLong _nextUserId = new AtomicLong();
    private transient final AtomicLong _nextItemId = new AtomicLong();

    /**
     * Current Timestamp
     */
    private transient TimestampType current_time = new TimestampType();

    /**
     * The last time that we called CHECK_WINNING_BIDS on this client
     */
    private transient TimestampType lastCheckWinningBidTime = null;

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
         * @param voltTable
         * @return
         */
    	public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable);
    	/**
    	 * Generate the parameters 
    	 * @param client
    	 * @param rng
    	 * @param voltTable
    	 * @return
    	 */
    	public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable);
    }
    
    // --------------------------------------------------------------------
    // BENCHMARK TRANSACTIONS
    // --------------------------------------------------------------------
    public static enum Transaction {
        // ====================================================================
        // CHECK_WINNING_BIDS
        // ====================================================================
        CHECK_WINNING_BIDS(AuctionMarkConstants.FREQUENCY_CHECK_WINNING_BIDS, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	return null;
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return true;
			}
        }),
        // ====================================================================
        // GET_ITEM
        // ====================================================================
        GET_ITEM(AuctionMarkConstants.FREQUENCY_GET_ITEM, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	ItemId itemIdSellerIdPair = client.profile.getRandomAvailableItemId(rng);
                return new Object[]{itemIdSellerIdPair.encode(), itemIdSellerIdPair.getSellerId().encode()};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_available_items_histogram.getSampleCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_BID
        // ====================================================================
        NEW_BID(AuctionMarkConstants.FREQUENCY_NEW_BID, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long buyerId = null; // FIXME client.profile.getRandomBuyerId(rng);
            	assert(buyerId != null);

            	voltTable.resetRowPosition();
            	assert(1 == voltTable.getRowCount());
        		
            	boolean advanced = voltTable.advanceRow();
            	assert(advanced);
        		long itemId = -1;
        		try {
        		    itemId = voltTable.getLong(0); 
        		} catch (Exception ex) {
        		    ex.printStackTrace();
        		    return (null);
        		}
        		
        		long sellerId = voltTable.getLong(1);
        		double initialPrice = voltTable.getDouble(2);
        		double currentPrice = voltTable.getDouble(3);
        		
        		if(voltTable.wasNull()){
        			currentPrice = initialPrice;
        		}
        		
                double bid = rng.fixedPoint(2, currentPrice, currentPrice * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP/2)));
                double maxBid = rng.fixedPoint(2, bid, (bid * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP/2))));

                return new Object[]{itemId, sellerId, buyerId, bid, maxBid};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (null != voltTable && voltTable.getRowCount() > 0 && client.profile.getUserIdCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_COMMENT
        // ====================================================================
        NEW_COMMENT(AuctionMarkConstants.FREQUENCY_NEW_COMMENT, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long[] itemIdSellerIdPair = null; // FIXME client.profile.getRandomCompleteItemIdSellerIdPair(rng);
            	Long buyerId = null; // FIXME client.profile.getRandomBuyerId(rng);
            	assert(buyerId != null);
            	String question = rng.astring(10, 128);
                return new Object[]{itemIdSellerIdPair[0], itemIdSellerIdPair[1], buyerId, question};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_complete_items_histogram.getSampleCount() > 0 &&
				        client.profile.getUserIdCount() > 0);
			}
        }),
        // ====================================================================
        // GET_COMMENT
        // ====================================================================
        GET_COMMENT(AuctionMarkConstants.FREQUENCY_GET_COMMENT, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long[] itemIdSellerIdPair = null; // FIXME client.profile.getRandomCompleteItemIdSellerIdPair(rng);
                return new Object[]{itemIdSellerIdPair[1]};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_complete_items_histogram.getSampleCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_COMMENT_RESPONSE
        // ====================================================================
        NEW_COMMENT_RESPONSE(AuctionMarkConstants.FREQUENCY_NEW_COMMENT_RESPONSE, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	
            	int randomCommentIndex;
            	
            	if(voltTable.getRowCount() > 1){
            		randomCommentIndex = rng.number(0, voltTable.getRowCount() - 1);
            	} else {
            		randomCommentIndex = 0;
            	}
            	
            	for(int i=0; i<=randomCommentIndex; i++){
            		voltTable.advanceRow();
            	}
            	
            	long itemId = voltTable.getLong("ic_i_id");
            	long commentId = voltTable.getLong("ic_id");
            	long sellerId = voltTable.getLong("ic_u_id");
            	String response = rng.astring(10, 128);
                return new Object[]{itemId, commentId, sellerId, response};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (null != voltTable);
			}
        }),
        // ====================================================================
        // NEW_FEEDBACK
        // ====================================================================
        NEW_FEEDBACK(AuctionMarkConstants.FREQUENCY_NEW_FEEDBACK, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long[] itemIdSellerIdPair = null; // FIXME client.profile.getRandomCompleteItemIdSellerIdPair(rng);
            	Long buyerId = null; // FIXME client.profile.getRandomBuyerId(rng);
            	assert(buyerId != null);
            	Long rating = (long)rng.number(0, 10);
            	String feedback = rng.astring(10, 128);
                return new Object[]{itemIdSellerIdPair[0], itemIdSellerIdPair[1], buyerId, rating, feedback};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_complete_items_histogram.getSampleCount() > 0 &&
				        client.profile.getUserIdCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_ITEM
        // ====================================================================
        NEW_ITEM(AuctionMarkConstants.FREQUENCY_NEW_ITEM, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long sellerId = client.profile.getRandomSellerId(rng);
            	assert(sellerId != null);
            	
            	Long itemId = client.getNextItemId();
            	
            	Long categoryId = client.profile.getRandomCategoryId(rng);
            	String name = rng.astring(6, 32);
                String description = rng.astring(50, 255);
                Zipf randomInitialPrice = new Zipf(rng, AuctionMarkConstants.ITEM_MIN_INITIAL_PRICE,
                        AuctionMarkConstants.ITEM_MAX_INITIAL_PRICE, 1.001);
                Double initial_price = (double)randomInitialPrice.nextInt();
                String attributes = rng.astring(50, 255);
                
                Zipf randomNumAttributes = new Zipf(rng, 0, 10, 1.001);
                int numAttributes = randomNumAttributes.nextInt();
                
                List<Long> gagList = new ArrayList<Long>(numAttributes);
                List<Long> gavList = new ArrayList<Long>(numAttributes);
                
                for(int i=0; i<numAttributes; i++){
                	Pair<Long, Long> GAGIdGAVIdPair = client.profile.getRandomGAGIdGAVIdPair(rng);
                	if(!gavList.contains(GAGIdGAVIdPair.getSecond())){
                		gagList.add(GAGIdGAVIdPair.getFirst());
                		gavList.add(GAGIdGAVIdPair.getSecond());
                	}
                }
                
                long[] gag_ids = new long[gagList.size()];
                for (int i = 0, cnt = gag_ids.length; i < cnt; i++) {
                    gag_ids[i] = gagList.get(i);
                }
                long[] gav_ids = new long[gavList.size()];
                for (int i = 0, cnt = gav_ids.length; i < cnt; i++) {
                    gav_ids[i] = gavList.get(i);
                }
                
                Zipf randomNumImages = new Zipf(rng, AuctionMarkConstants.ITEM_MIN_IMAGES, AuctionMarkConstants.ITEM_MAX_IMAGES, 1.001);
                int numImages = randomNumImages.nextInt();
                String[] images = new String[numImages];
                for(int i=0; i<numImages; i++){
                	images[i] = rng.astring(20, 100);
                }
                
                TimestampType start_date = new TimestampType();

                TimestampType end_date = new TimestampType(start_date.getTime() + (long)10 * (long)1000000);
            	
            	return new Object[]{
            			itemId, sellerId, categoryId, name, description, initial_price, attributes, gag_ids, gav_ids, images, start_date, end_date
            	};
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
			    return (client.profile.getUserIdCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_PURCHASE
        // ====================================================================
        NEW_PURCHASE(AuctionMarkConstants.FREQUENCY_NEW_PURCHASE, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long[] itemIdSellerIdPair = null; // FIXME client.profile.getRandomWaitForPurchaseItemIdSellerIdPair(rng);
            	long bidId = client.profile.getBidId(itemIdSellerIdPair[0]);
            	long buyerId = client.profile.getBuyerId(itemIdSellerIdPair[0]);
            	long sellerId = itemIdSellerIdPair[1];
            	long itemId = itemIdSellerIdPair[0];
                return new Object[]{
                		bidId, itemId, sellerId, buyerId
                };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_wait_for_purchase_items_histogram.getSampleCount() > 0);
			}
        }),
        // ====================================================================
        // NEW_USER
        // ====================================================================
        NEW_USER(AuctionMarkConstants.FREQUENCY_NEW_USER, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	long u_id = client.getNextUserId();
            	
            	Flat randomRegion = new Flat(rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION); 	
            	long u_r_id = randomRegion.nextLong();
            	
            	String[] attributes = new String[8];
            	for(int i=0; i < attributes.length; i++){
            		attributes[i] = rng.astring(10, 64);
            	}
                return new Object[]{
                		u_id, u_r_id, attributes
                };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return true;
			}
        }),
        // ====================================================================
        // POST_AUCTION
        // ====================================================================
        POST_AUCTION(AuctionMarkConstants.FREQUENCY_POST_AUCTION, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	voltTable.resetRowPosition();
            	
            	final int num_rows = voltTable.getRowCount();
    			long[] i_ids = new long[num_rows];
    			long[] seller_ids = new long[num_rows];
    			long[] buyer_ids = new long[num_rows];
    			long[] ib_ids = new long[num_rows];
    			
    			for(int i=0; i < i_ids.length; i++){
    				voltTable.advanceRow();
    				i_ids[i] = voltTable.getLong("i_id");
    	    		seller_ids[i] = voltTable.getLong("i_u_id");
    	    		buyer_ids[i] = voltTable.getLong("ib_buyer_id");
    	    		if(voltTable.wasNull()){
    	    		    buyer_ids[i] = -1;
    	    		}
    	    		ib_ids[i] = voltTable.getLong("imb_ib_id");
    	    		if(voltTable.wasNull()){
    	    			ib_ids[i] = -1;
    	    		}
    			}

    			Object[] params = new Object[]{
    					i_ids, seller_ids, buyer_ids, ib_ids
    			};

                return params;
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (null != voltTable);
			}
        }),
        // ====================================================================
        // UPDATE_ITEM
        // ====================================================================
        UPDATE_ITEM(AuctionMarkConstants.FREQUENCY_UPDATE_ITEM, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long[] itemIdSellerIdPair = null; // FIXME client.profile.getRandomAvailableItemId(rng);
            	String description = rng.astring(50, 255);
                return new Object[]{
                		itemIdSellerIdPair[0], itemIdSellerIdPair[1], description
                };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_available_items_histogram.getSampleCount() > 0);
			}
        }), 
        // ====================================================================
        // GET_USER_INFO
        // ====================================================================
        GET_USER_INFO(AuctionMarkConstants.FREQUENCY_GET_USER_INFO, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long userId = null; // FIXME client.profile.getRandomAvailableItemId(rng)[1];
            	long get_seller_items = 0;
            	long get_buyer_items = 0;
            	long get_feedback = 0;
            	
                // 33% of the time they're going to ask for additional information
            	if (rng.number(0, 100) <= 33) {
            	    if (rng.number(0, 100) <= 75) {
            	        get_seller_items = 1;
            	    } else {
            	        get_buyer_items = 1;
            	    }
            	}
            	// 33% of the time we'll also get the feedback information
            	if (rng.number(0, 100) <= 33) {
            	    get_feedback = 1;
            	}

            	return new Object[]{
                	userId, get_seller_items, get_buyer_items, get_feedback
                };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.user_available_items_histogram.getSampleCount() > 0);
			}
        }),
        // ====================================================================
        // GET_WATCHED_ITEMS
        // ====================================================================
        GET_WATCHED_ITEMS(AuctionMarkConstants.FREQUENCY_GET_WATCHED_ITEMS, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
            	Long userId = null; // FIXME client.profile.getRandomBuyerId(rng);
            	assert(userId != null);
                return new Object[]{
                	userId
                };
            }

			@Override
			public boolean canGenerateParam(AuctionMarkClient client, VoltTable voltTable) {
				return (client.profile.getUserIdCount() > 0);
			}
        })
        ;
        
        /**
         * Constructor
         * @param weight The execution frequency weight for this txn 
         * @param needs_next_id If true, then this txn can only be called when next_id is populated
         * @param generator
         */
        private Transaction(int weight, boolean needs_next_id, AuctionMarkParamGenerator generator) {
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
            this.callName = this.displayName.replace(" ", "");
            this.generator = generator;
            this.needs_next_id = needs_next_id;
        }

        public final int default_weight;
        public final String displayName;
        public final String callName;
        public final AuctionMarkParamGenerator generator;
        
        /**
         * Some transactions need to be told by somebody else what id to use the next
         * time we want to call it
         */
        public final boolean needs_next_id;
        public final ConcurrentLinkedQueue<VoltTable> next_params = new ConcurrentLinkedQueue<VoltTable>();
        
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
            //return (!this.needs_next_id || !this.next_params.isEmpty());
        	return this.generator.canGenerateParam(client, this.next_params.peek());
        }
        
        /**
         * Given a BenchmarkProfile object, call the AuctionMarkParamGenerator object for a given
         * transaction type to generate a set of parameters for a new txn invocation 
         * @param rng
         * @param profile
         * @return
         */
        public Object[] params(AuctionMarkClient client, AbstractRandomGenerator rng, VoltTable voltTable) {
        	return (this.generator.generate(client, rng, voltTable));
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
        
        // Partitioned Client Parameters
        long partitionRange = AuctionMarkConstants.MAXIMUM_CLIENT_IDS;
        if (partitionRange < 1 || partitionRange > AuctionMarkConstants.MAXIMUM_CLIENT_IDS) {
            throw new IllegalArgumentException("partitionRange is not in range 1 to " + AuctionMarkConstants.MAXIMUM_CLIENT_IDS + " : " + partitionRange);
        }
        this._partitionStartOffset = this.getClientId() * partitionRange;
        this._partitionEndOffset = ((this.getClientId() + 1) * partitionRange) - 1;

        /** FIXME
        this.initItemMap(profile.user_available_items, profile.user_available_items, profile.user_available_items_histogram);
        this.initItemMap(profile.user_wait_for_purchase_items, profile.user_wait_for_purchase_items, profile.user_wait_for_purchase_items_histogram);
        this.initItemMap(profile.user_complete_items, profile.user_complete_items, profile.user_complete_items_histogram);
        this.initItemBidMap(profile.item_bid_map);
        this.initItemBuyerMap(profile.item_buyer_map);
        */
//        this.initUserIds(profile.user_ids);

        this._nextItemId.set(profile.getTableSize(AuctionMarkConstants.TABLENAME_ITEM));
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction t : Transaction.values()) {
            names[ii++] = t.getDisplayName();
        }
        return names;
    }
    
    @Override
    public void runLoop() {
        try {
            while (true) {
                this.runOnce();
            } // WHILE
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
    protected boolean runOnce() throws IOException {
        try {
            executeTransaction();
            this.getClientHandle().backpressureBarrier();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return (false);
        } catch (ProcCallException e) {
			e.printStackTrace();
		}
        return (true);
    }
    
    @Override
    protected void tick() {
        super.tick();
        profile.tick();
    }
    
    /**
     * 
     * @return
     * @throws IOException
     * @throws ProcCallException 
     * @throws InterruptedException 
     */
    private void executeTransaction() throws IOException, ProcCallException, InterruptedException {
        Transaction txn = null;
        Object[] params = null;
        int safety = 1000;
        
        if (debug.get()) LOG.debug("Execute Transaction [client_id=" + this.getClientId() + "]");
        
        if (Transaction.POST_AUCTION.canExecute(this) && this.getClientId() == 0) {
        	txn = Transaction.POST_AUCTION;
        	if (debug.get()) LOG.trace("Executing new invocation of transaction " + txn);
        	
        	if(txn.next_params.peek().getRowCount() > 0){
        	    params = txn.params(this, this.rng, txn.next_params.remove());
	        	if (debug.get()) LOG.debug("EXECUTING POST_AUCTION ------------------------");
	            this.getClientHandle().callProcedure(new AuctionMarkCallback(txn, this.rng), txn.getCallName(), params);
	            return;
        	} else {
        		if (debug.get()) LOG.trace("POST_AUCTION ::: no executiong (rows to process = " + txn.next_params.remove().getRowCount() + ") ");
        	}
        }
        
        // Update the current timestamp and check whether it's time to run the CheckWinningBids txn
        TimestampType currentTime = this.updateAndGetCurrentTime();
        TimestampType lastCheckWinningBidTime = this.getLastCheckWinningBidTime();
        if (AuctionMarkConstants.ENABLE_CHECK_WINNING_BIDS && (lastCheckWinningBidTime == null ||
        	(((currentTime.getTime() - lastCheckWinningBidTime.getTime()) / 1000.0) > AuctionMarkConstants.INTERVAL_CHECK_WINNING_BIDS))) {
        	txn = Transaction.CHECK_WINNING_BIDS;
        	if (debug.get()) LOG.trace("Executing new invocation of transaction " + txn);
        	
        	int clientId = 1;
        	
        	params = new Object[] { 
        	        this.getLastCheckWinningBidTime(), 
        			this.updateAndGetLastCheckWinningBidTime(), 
        			clientId,
        			AuctionMarkConstants.MAXIMUM_CLIENT_IDS
        	};
            if (debug.get()) LOG.trace("EXECUTING CHECK_WINNING BID------------------------");
            this.getClientHandle().callProcedure(new AuctionMarkCallback(txn, this.rng), txn.getCallName(), params);
            return;
        }
        
        // Find the next txn and its parameters that we will run. We want to wrap this
        // around a synchronization block so that nobody comes in and takes the parameters
        // from us before we actually run it
		while (true) {
			int idx = this.rng.number(0, this.xacts.length - 1);
			if (debug.get()) {
			    LOG.trace("idx = " + idx);
				LOG.trace("random txn = " + this.xacts[idx].getDisplayName());
			}
			assert(idx >= 0);
			assert(idx < this.xacts.length);

			// Only execute this txn if it is ready
			// Example: NewBid can only be executed if there are item_ids retrieved by an earlier call by GetItem
			if (this.xacts[idx].canExecute(this)) {
				if (debug.get()) LOG.trace("can execute");
				try {
    				txn = this.xacts[idx];
    				params = txn.params(this, rng, txn.next_params.peek());
				} catch (AssertionError ex) {
				    LOG.error("Failed to generate parameters for " + txn, ex);
				}
				if (params != null) break;
			}
			assert(safety-- > 0) : "Too many loops looking for a txn to execute!";
		} // WHILE
		assert (txn != null);
		
		if(!txn.next_params.isEmpty()){
			txn.next_params.remove();
		}
		if (params != null) {
            if (debug.get()) LOG.debug("Executing new invocation of transaction " + txn + " : callname = " + txn.getCallName());
            this.getClientHandle().callProcedure(new AuctionMarkCallback(txn, this.rng), txn.getCallName(), params);
		} else {
		    LOG.warn("Failed to execute " + txn + " because the parameters were null?");
		}
        return;
    }
    
    
    //Basic Callback Class
    protected class AuctionMarkCallback implements ProcedureCallback {
        private final Transaction txn;
        private final AbstractRandomGenerator rng; 
        
        public AuctionMarkCallback(Transaction txn, AbstractRandomGenerator rng) {
            super();
            this.txn = txn;
            this.rng = rng;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
        	if (trace.get()) LOG.trace("clientCallback(cid = " + getClientId() + "):: txn = " + txn.getDisplayName());
        	
        	incrementTransactionCounter(this.txn.ordinal());
        	VoltTable[] results = clientResponse.getResults();
        	
        	// Update the next_id lists for any txn that depends on the output of this txn
        	switch (this.txn) {
        		case CHECK_WINNING_BIDS: {
        			assert(null != results && results.length > 0);
        			
        			while(results[0].advanceRow()){
        				long itemId = results[0].getLong("i_id");
        				long sellerId = results[0].getLong("i_u_id");
        				long itemStatus = results[0].getLong("i_status");
        				long buyerId = results[0].getLong("ib_buyer_id");
        				
        				if(itemStatus == 0){
	        				// FIXME profile.removeAvailableItem(sellerId, itemId);
	        				
	        				long bidId = results[0].getLong("imb_ib_id");
	        				if(results[0].wasNull()){
	        					// No winning bid
	        				    // FIXME profile.addCompleteItem(sellerId, itemId);
	        				} else {
	        					// Has winning bid
	        				    // FIXME profile.addWaitForPurchaseItem(sellerId, itemId, bidId, buyerId);
	        				}
        				}
        			}
        			
        			results[0].resetRowPosition();
    	        	Transaction.POST_AUCTION.next_params.add(results[0]);
        			break;
        		}
        		case GET_ITEM: {
        	        if (this.rng.nextInt(100) < Math.round(((float)AuctionMarkConstants.FREQUENCY_NEW_BID * 100)/((float)AuctionMarkConstants.FREQUENCY_GET_ITEM))) {
        	        	if(results.length > 0){
        	        		Transaction.NEW_BID.next_params.add(results[0]);
        	        	}
        	        }
        	        break;
        	    }
        		case NEW_BID: {
        			break;
        		}
        		case NEW_COMMENT: break;
        		case GET_COMMENT: {
        			if (this.rng.nextInt(100) < Math.round(((float)AuctionMarkConstants.FREQUENCY_NEW_COMMENT_RESPONSE * 100)/((float)AuctionMarkConstants.FREQUENCY_NEW_COMMENT))) {
        				if(null != results && results.length > 0 && results[0].getRowCount() > 0){
        					Transaction.NEW_COMMENT_RESPONSE.next_params.add(results[0]);
        				}
        	        }
        			break;
        		}
        		case NEW_COMMENT_RESPONSE: break;
        		case NEW_FEEDBACK: {
        			break;
        		}
        		case NEW_ITEM: {
        			if(results.length > 0 && results[0].advanceRow()){
	        			long itemId = results[0].getLong("i_id");
	        			long sellerId = results[0].getLong("i_u_id");        			
	        			// FIXME profile.addAvailableItem(sellerId, itemId);
        			}
        			break;
        		}
        		case NEW_PURCHASE: {
        			if(results.length > 0 && results[0].advanceRow()){
	        			long itemId = results[0].getLong("ip_ib_i_id");
	        			long sellerId = results[0].getLong("u_id");
	        			if (trace.get()) LOG.trace("clientCallback:: NEW_PURCHASE itemId = " + itemId);
	        			if (trace.get()) LOG.trace("clientCallback:: NEW_PURCHASE sellerId = " + sellerId);
	        			// FIXME profile.removeWaitForPurchaseItem(sellerId, itemId);
	        			// FIXME profile.addCompleteItem(sellerId, itemId);
	        			if (trace.get()) LOG.trace("clientCallback:: NEW_PURCHASE END");
        			}
        			break;
        		}
        		case NEW_USER: {
        			break;
        		}
        		case POST_AUCTION: {
        			break;
        		}
        		case UPDATE_ITEM: {
        			break;
        		}
        		case GET_USER_INFO: {
        		    if (trace.get()) LOG.trace("GOT USER INFO:::::");
        			break;
        		}
        	} // SWITCH
        }
    } // END CLASS
    

    private void initItemMap(Map<Long, List<Long>> sourceMap, Map<Long, List<Long>> targetMap, Histogram targetHistogram) {
        long count = 0;
        for (Map.Entry<Long, List<Long>> entry : sourceMap.entrySet()) {
            if (this.isOffsetInRange(entry.getKey())) {
                targetHistogram.put(entry.getKey(), entry.getValue().size()); 
                count += entry.getValue().size();
                targetMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    /** FIXME
    private void initItemBidMap(Map<Long, Long> sourceItemBidMap) {
        for (Map.Entry<Long, List<Long>> entry : profile.user_wait_for_purchase_items.entrySet()) {
            for (Long itemId : entry.getValue()) {
                profile.item_bid_map.put(itemId, sourceItemBidMap.get(itemId));
            }
        }
    }

    private void initItemBuyerMap(Map<Long, Long> sourceItemBuyerMap) {
        for (Map.Entry<Long, List<Long>> entry : profile.user_wait_for_purchase_items.entrySet()) {
            for (Long itemId : entry.getValue()) {
                profile.item_buyer_map.put(itemId, sourceItemBuyerMap.get(itemId));
            }
        }
    }
    */

    private void initUserIds(List<Long> sourceUserIds) {
        long maxUserId = -1;
//        synchronized (profile.user_ids) {
//            for (Long userId : sourceUserIds) {
//                if (this.isOffsetInRange(userId)) {
//                    profile.user_ids.add(userId);
//                    if (userId > maxUserId) {
//                        maxUserId = userId;
//                    }
//                }
//            }
//            this._nextUserId.set(maxUserId + 1);
//        }
    }
    
    public long getNextItemId() {
        return this._nextItemId.getAndIncrement();
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
    
    /**
     * Gets next available user ID within the client.
     * 
     * @return long client ID
     */
    public long getNextUserId() {
        return (this._nextUserId.getAndIncrement());
    }

    private boolean isOffsetInRange(long offset) {
        return (offset >= this._partitionStartOffset && offset <= this._partitionEndOffset);
    }
}