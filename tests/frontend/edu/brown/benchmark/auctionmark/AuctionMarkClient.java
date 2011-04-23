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

import org.voltdb.VoltTable;
import org.voltdb.benchmark.ClientMain;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.utils.StringUtil;

public class AuctionMarkClient extends AuctionMarkBaseClient {
    // --------------------------------------------------------------------
    // TXN PARAMETER GENERATOR
    // --------------------------------------------------------------------
    public interface AuctionMarkParamGenerator {
        /**
         * Returns true if the client will be able to successfully generate a
         * new transaction call The client passes in the current
         * BenchmarkProfile handle and an optional VoltTable. This allows you to
         * invoke one txn using the output of a previously run txn. Note that
         * this is not thread safe, so you'll need to combine the call to this
         * with generate() in a single synchronization block.
         * 
         * @param profile
         * @param voltTable
         * @return
         */
        public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable);

        /**
         * Generate the parameters
         * 
         * @param rng
         * @param profile
         * @param voltTable
         * @return
         */
        public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db);
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
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                return null;
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return true;
            }
        }),
        // ====================================================================
        // GET_ITEM
        // ====================================================================
        GET_ITEM(AuctionMarkConstants.FREQUENCY_GET_ITEM, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile clientProfile, VoltTable voltTable, Catalog catalog_db) {
                Long[] itemIdSellerIdPair = clientProfile.getRandomAvailableItemIdSellerIdPair(rng);
                return new Object[] { itemIdSellerIdPair[0], itemIdSellerIdPair[1] };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_available_items_histogram.getSampleCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_BID
        // ====================================================================
        NEW_BID(AuctionMarkConstants.FREQUENCY_NEW_BID, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                voltTable.resetRowPosition();
                assert (1 == voltTable.getRowCount());

                boolean advanced = voltTable.advanceRow();
                assert (advanced);
                long itemId = -1;
                try {
                    itemId = voltTable.getLong(0);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return (null);
                }

                long sellerId = voltTable.getLong(1);

                Long buyerId;
                // System.out.println("new bid sellerid: " + sellerId);
                if (AuctionMarkClientBenchmarkProfile.zipf) {
                    buyerId = profile.getZipfBuyerId(sellerId, catalog_db);
                } else {
                    buyerId = profile.getRandomBuyerId(rng);
                }

                assert (buyerId != null);

                // System.out.println("new Bid sellerid: " + sellerId);
                double initialPrice = voltTable.getDouble(2);
                double currentPrice = voltTable.getDouble(3);

                if (voltTable.wasNull()) {
                    currentPrice = initialPrice;
                }

                double bid = rng.fixedPoint(2, currentPrice, currentPrice * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2)));
                double maxBid = rng.fixedPoint(2, bid, (bid * (1 + (AuctionMarkConstants.ITEM_BID_PERCENT_STEP / 2))));

                return new Object[] { itemId, sellerId, buyerId, bid, maxBid };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (null != voltTable && voltTable.getRowCount() > 0 && profile.getUserIdCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_COMMENT
        // ====================================================================
        NEW_COMMENT(AuctionMarkConstants.FREQUENCY_NEW_COMMENT, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                Long[] itemIdSellerIdPair = profile.getRandomCompleteItemIdSellerIdPair(rng);
                Long buyerId;
                // System.out.println("new comment sellerid: " +
                // itemIdSellerIdPair[1]);
                if (AuctionMarkClientBenchmarkProfile.zipf) {
                    buyerId = profile.getZipfBuyerId(itemIdSellerIdPair[1], catalog_db);
                } else {
                    buyerId = profile.getRandomBuyerId(rng);
                }
                assert (buyerId != null);
                String question = rng.astring(10, 128);
                return new Object[] { itemIdSellerIdPair[0], itemIdSellerIdPair[1], buyerId, question };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_complete_items_histogram.getSampleCount() > 0 && profile.getUserIdCount() > 0);
            }
        }),
        // ====================================================================
        // GET_COMMENT
        // ====================================================================
        GET_COMMENT(AuctionMarkConstants.FREQUENCY_GET_COMMENT, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                Long[] itemIdSellerIdPair = profile.getRandomCompleteItemIdSellerIdPair(rng);
                return new Object[] { itemIdSellerIdPair[1] };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_complete_items_histogram.getSampleCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_COMMENT_RESPONSE
        // ====================================================================
        NEW_COMMENT_RESPONSE(AuctionMarkConstants.FREQUENCY_NEW_COMMENT_RESPONSE, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {

                int randomCommentIndex;

                if (voltTable.getRowCount() > 1) {
                    randomCommentIndex = rng.number(0, voltTable.getRowCount() - 1);
                } else {
                    randomCommentIndex = 0;
                }

                for (int i = 0; i <= randomCommentIndex; i++) {
                    voltTable.advanceRow();
                }

                long itemId = voltTable.getLong("ic_i_id");
                long commentId = voltTable.getLong("ic_id");
                long sellerId = voltTable.getLong("ic_u_id");
                String response = rng.astring(10, 128);
                return new Object[] { itemId, commentId, sellerId, response };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (null != voltTable);
            }
        }),
        // ====================================================================
        // NEW_FEEDBACK
        // ====================================================================
        NEW_FEEDBACK(AuctionMarkConstants.FREQUENCY_NEW_FEEDBACK, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                Long[] itemIdSellerIdPair = profile.getRandomCompleteItemIdSellerIdPair(rng);
                Long buyerId = profile.getRandomBuyerId(rng);
                assert (buyerId != null);
                Long rating = (long) rng.number(0, 10);
                String feedback = rng.astring(10, 128);
                return new Object[] { itemIdSellerIdPair[0], itemIdSellerIdPair[1], buyerId, rating, feedback };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_complete_items_histogram.getSampleCount() > 0 && profile.getUserIdCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_ITEM
        // ====================================================================
        NEW_ITEM(AuctionMarkConstants.FREQUENCY_NEW_ITEM, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                Long sellerId = profile.getRandomSellerId(rng);
                assert (sellerId != null);

                Long itemId = profile.getNextItemId();

                Long categoryId = profile.getRandomCategoryId(rng);
                String name = rng.astring(6, 32);
                String description = rng.astring(50, 255);
                Zipf randomInitialPrice = new Zipf(rng, AuctionMarkConstants.ITEM_MIN_INITIAL_PRICE, AuctionMarkConstants.ITEM_MAX_INITIAL_PRICE, 1.001);
                Double initial_price = (double) randomInitialPrice.nextInt();
                String attributes = rng.astring(50, 255);

                Zipf randomNumAttributes = new Zipf(rng, 0, 10, 1.001);
                int numAttributes = randomNumAttributes.nextInt();

                List<Long> gagList = new ArrayList<Long>(numAttributes);
                List<Long> gavList = new ArrayList<Long>(numAttributes);

                for (int i = 0; i < numAttributes; i++) {
                    Long[] GAGIdGAVIdPair = profile.getRandomGAGIdGAVIdPair(rng);
                    if (!gavList.contains(GAGIdGAVIdPair[1])) {
                        gagList.add(GAGIdGAVIdPair[0]);
                        gavList.add(GAGIdGAVIdPair[1]);
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
                for (int i = 0; i < numImages; i++) {
                    images[i] = rng.astring(20, 100);
                }

                TimestampType start_date = new TimestampType();

                TimestampType end_date = new TimestampType(start_date.getTime() + (long) 10 * (long) 1000000);

                return new Object[] { itemId, sellerId, categoryId, name, description, initial_price, attributes, gag_ids, gav_ids, images, start_date, end_date };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.getUserIdCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_PURCHASE
        // ====================================================================
        NEW_PURCHASE(AuctionMarkConstants.FREQUENCY_NEW_PURCHASE, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile clientProfile, VoltTable voltTable, Catalog catalog_db) {
                Long[] itemIdSellerIdPair = clientProfile.getRandomWaitForPurchaseItemIdSellerIdPair(rng);
                long bidId = clientProfile.getBidId(itemIdSellerIdPair[0]);
                long buyerId = clientProfile.getBuyerId(itemIdSellerIdPair[0]);
                long sellerId = itemIdSellerIdPair[1];
                long itemId = itemIdSellerIdPair[0];
                return new Object[] { bidId, itemId, sellerId, buyerId };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_wait_for_purchase_items_histogram.getSampleCount() > 0);
            }
        }),
        // ====================================================================
        // NEW_USER
        // ====================================================================
        NEW_USER(AuctionMarkConstants.FREQUENCY_NEW_USER, false, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                long u_id = profile.getNextUserId();

                Flat randomRegion = new Flat(rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION);
                long u_r_id = randomRegion.nextLong();

                String[] attributes = new String[8];
                for (int i = 0; i < attributes.length; i++) {
                    attributes[i] = rng.astring(10, 64);
                }
                return new Object[] { u_id, u_r_id, attributes };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return true;
            }
        }),
        // ====================================================================
        // POST_AUCTION
        // ====================================================================
        POST_AUCTION(AuctionMarkConstants.FREQUENCY_POST_AUCTION, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                voltTable.resetRowPosition();

                final int num_rows = voltTable.getRowCount();
                long[] i_ids = new long[num_rows];
                long[] seller_ids = new long[num_rows];
                long[] buyer_ids = new long[num_rows];
                long[] ib_ids = new long[num_rows];

                for (int i = 0; i < i_ids.length; i++) {
                    voltTable.advanceRow();
                    i_ids[i] = voltTable.getLong("i_id");
                    seller_ids[i] = voltTable.getLong("i_u_id");
                    buyer_ids[i] = voltTable.getLong("ib_buyer_id");
                    if (voltTable.wasNull()) {
                        buyer_ids[i] = -1;
                    }
                    ib_ids[i] = voltTable.getLong("imb_ib_id");
                    if (voltTable.wasNull()) {
                        ib_ids[i] = -1;
                    }
                }

                Object[] params = new Object[] { i_ids, seller_ids, buyer_ids, ib_ids };

                return params;
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (null != voltTable);
            }
        }),
        // ====================================================================
        // UPDATE_ITEM
        // ====================================================================
        UPDATE_ITEM(AuctionMarkConstants.FREQUENCY_UPDATE_ITEM, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                Long[] itemIdSellerIdPair = profile.getRandomAvailableItemIdSellerIdPair(rng);
                String description = rng.astring(50, 255);
                return new Object[] { itemIdSellerIdPair[0], itemIdSellerIdPair[1], description };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_available_items_histogram.getSampleCount() > 0);
            }
        }),
        // ====================================================================
        // GET_USER_INFO
        // ====================================================================
        GET_USER_INFO(AuctionMarkConstants.FREQUENCY_GET_USER_INFO, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                long userId = profile.getRandomAvailableItemIdSellerIdPair(rng)[1];
                long get_seller_items = 0;
                long get_buyer_items = 0;
                long get_feedback = 0;

                // 33% of the time they're going to ask for additional
                // information
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

                return new Object[] { userId, get_seller_items, get_buyer_items, get_feedback };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.user_available_items_histogram.getSampleCount() > 0);
            }
        }),
        // ====================================================================
        // GET_WATCHED_ITEMS
        // ====================================================================
        GET_WATCHED_ITEMS(AuctionMarkConstants.FREQUENCY_GET_WATCHED_ITEMS, true, new AuctionMarkParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
                Long userId = profile.getRandomBuyerId(rng);
                assert (userId != null);
                return new Object[] { userId };
            }

            @Override
            public boolean canGenerateParam(AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable) {
                return (profile.getUserIdCount() > 0);
            }
        });

        /**
         * Constructor
         * 
         * @param weight
         *            The execution frequency weight for this txn
         * @param needs_next_id
         *            If true, then this txn can only be called when next_id is
         *            populated
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
         * Some transactions need to be told by somebody else what id to use the
         * next time we want to call it
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
            assert (idx >= 0);
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
         * This will return true if we can call a new transaction for this
         * procedure A txn can be called if we can generate all of the
         * parameters we need
         * 
         * @return
         */
        public boolean canExecute(AuctionMarkClientBenchmarkProfile clientProfile) {
            // return (!this.needs_next_id || !this.next_params.isEmpty());
            return this.generator.canGenerateParam(clientProfile, this.next_params.peek());
        }

        /**
         * Given a BenchmarkProfile object, call the AuctionMarkParamGenerator
         * object for a given transaction type to generate a set of parameters
         * for a new txn invocation
         * 
         * @param rng
         * @param profile
         * @return
         */
        public Object[] params(AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile, VoltTable voltTable, Catalog catalog_db) {
            /*
             * if(!next_params.isEmpty()){ voltTable = next_params.remove(); }
             */
            return (this.generator.generate(rng, profile, voltTable, catalog_db));
        }
    }

    // -----------------------------------------------------------------
    // REQUIRED DATA MEMBERS
    // -----------------------------------------------------------------

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = AuctionMarkProjectBuilder.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends ClientMain> m_loaderClass = AuctionMarkLoader.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final String m_jarFileName = "auctionmark.jar";

    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------

    private final Map<Transaction, Integer> weights = new HashMap<Transaction, Integer>();
    private final Transaction xacts[] = new Transaction[100];

    private AuctionMarkClientBenchmarkProfile clientProfile;
    private int _clientId;

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(AuctionMarkClient.class, args, false);
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkClient(String[] args) {
        super(AuctionMarkClient.class, args);

        _clientId = getClientId();

        clientProfile = new AuctionMarkClientBenchmarkProfile(profile, getClientId(), AuctionMarkConstants.MAXIMUM_CLIENT_IDS, m_catalog, rng);
        if (LOG.isTraceEnabled())
            LOG.trace("constructor : histogram size = " + this.clientProfile.user_available_items_histogram.getSampleCount());

        // Enable temporal skew
        if (m_extraParams.containsKey("TEMPORALWINDOW") && m_extraParams.containsKey("TEMPORALTOTAL")) {
            try {
                int temporal_window = Integer.valueOf(m_extraParams.get("TEMPORALWINDOW"));
                int temporal_total = Integer.valueOf(m_extraParams.get("TEMPORALTOTAL"));
                if (temporal_window > 0) {
                    this.clientProfile.enableTemporalSkew(temporal_window, temporal_total);
                    System.err.println(String.format("Enabling temporal skew [window=%d, total=%d]", temporal_window, temporal_total));
                }
            } catch (Exception ex) {
                System.err.println(ex.getLocalizedMessage());
                ex.printStackTrace();
                System.exit(1);
            }
        }

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
        assert (total == xacts.length) : "The total weight for the transactions is " + total + ". It needs to be " + xacts.length;
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
             * At shutdown an IOException is thrown for every connection to the
             * DB that is lost Ignore the exception here in order to not get
             * spammed, but will miss lost connections at runtime
             */
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        try {
            executeTransaction();
            m_voltClient.backpressureBarrier();
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
        this.clientProfile.tick();
        this.debug = LOG.isDebugEnabled();
    }

    /**
     * @return
     * @throws IOException
     * @throws ProcCallException
     * @throws InterruptedException
     */
    private void executeTransaction() throws IOException, ProcCallException, InterruptedException {
        Transaction txn = null;
        Object[] params = null;
        int safety = 1000;

        if (this.debug)
            LOG.debug("Execute Transaction [client_id=" + this.getClientId() + "]");

        if (Transaction.POST_AUCTION.canExecute(clientProfile) && this.getClientId() == 0) {
            txn = Transaction.POST_AUCTION;
            if (this.debug)
                LOG.trace("Executing new invocation of transaction " + txn);

            if (txn.next_params.peek().getRowCount() > 0) {
                params = txn.params(this.rng, this.clientProfile, txn.next_params.remove(), catalog);
                if (this.debug)
                    LOG.debug("EXECUTING POST_AUCTION ------------------------");
                m_voltClient.callProcedure(new AuctionMarkCallback(txn, this.rng, this.clientProfile), txn.getCallName(), params);
                return;
            } else {
                if (this.debug)
                    LOG.trace("POST_AUCTION ::: no executiong (rows to process = " + txn.next_params.remove().getRowCount() + ") ");
            }
        }

        // Update the current timestamp and check whether it's time to run the
        // CheckWinningBids txn
        TimestampType currentTime = this.clientProfile.updateAndGetCurrentTime();
        TimestampType lastCheckWinningBidTime = this.clientProfile.getLastCheckWinningBidTime();
        if (AuctionMarkConstants.ENABLE_CHECK_WINNING_BIDS
                && (lastCheckWinningBidTime == null || (((currentTime.getTime() - lastCheckWinningBidTime.getTime()) / 1000.0) > AuctionMarkConstants.INTERVAL_CHECK_WINNING_BIDS))) {
            txn = Transaction.CHECK_WINNING_BIDS;
            if (this.debug)
                LOG.trace("Executing new invocation of transaction " + txn);

            int clientId = 1;

            params = new Object[] { clientProfile.getLastCheckWinningBidTime(), clientProfile.updateAndGetLastCheckWinningBidTime(), clientId, AuctionMarkConstants.MAXIMUM_CLIENT_IDS };
            if (this.debug)
                LOG.trace("EXECUTING CHECK_WINNING BID------------------------");
            m_voltClient.callProcedure(new AuctionMarkCallback(txn, this.rng, this.clientProfile), txn.getCallName(), params);
            return;
        }

        // Find the next txn and its parameters that we will run. We want to
        // wrap this
        // around a synchronization block so that nobody comes in and takes the
        // parameters
        // from us before we actually run it
        synchronized (this.clientProfile) {
            while (true) {
                int idx = this.rng.number(0, this.xacts.length - 1);
                if (this.debug) {
                    LOG.trace("idx = " + idx);
                    LOG.trace("random txn = " + this.xacts[idx].getDisplayName());
                }
                assert (idx >= 0);
                assert (idx < this.xacts.length);

                // Only execute this txn if it is ready
                // Example: NewBid can only be executed if there are item_ids
                // retrieved by an earlier call by GetItem
                if (this.xacts[idx].canExecute(clientProfile)) {
                    if (debug)
                        LOG.trace("can execute");
                    try {
                        txn = this.xacts[idx];
                        params = txn.params(rng, clientProfile, txn.next_params.peek(), catalog);
                    } catch (AssertionError ex) {
                        LOG.error("Failed to generate parameters for " + txn, ex);
                    }
                    if (params != null)
                        break;
                }
                assert (safety-- > 0) : "Too many loops looking for a txn to execute!";
            } // WHILE
            assert (txn != null);

            if (!txn.next_params.isEmpty()) {
                txn.next_params.remove();
            }
        } // SYNC
        if (params != null) {
            if (debug)
                LOG.debug("Executing new invocation of transaction " + txn + " : callname = " + txn.getCallName());
            m_voltClient.callProcedure(new AuctionMarkCallback(txn, this.rng, this.clientProfile), txn.getCallName(), params);
        } else {
            LOG.warn("Failed to execute " + txn + " because the parameters were null?");
        }
        return;
    }

    // Basic Callback Class
    protected class AuctionMarkCallback implements ProcedureCallback {
        private final Transaction txn;
        private final AbstractRandomGenerator rng;
        private final AuctionMarkClientBenchmarkProfile profile;

        public AuctionMarkCallback(Transaction txn, AbstractRandomGenerator rng, AuctionMarkClientBenchmarkProfile profile) {
            super();
            this.txn = txn;
            this.rng = rng;
            this.profile = profile;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            final boolean trace = LOG.isTraceEnabled();
            if (trace)
                LOG.trace("clientCallback(cid = " + _clientId + "):: txn = " + txn.getDisplayName());

            AuctionMarkClient.this.m_counts[this.txn.ordinal()].incrementAndGet();
            VoltTable[] results = clientResponse.getResults();

            // Update the next_id lists for any txn that depends on the output
            // of this txn
            switch (this.txn) {
                case CHECK_WINNING_BIDS: {
                    assert (null != results && results.length > 0);

                    while (results[0].advanceRow()) {
                        long itemId = results[0].getLong("i_id");
                        long sellerId = results[0].getLong("i_u_id");
                        long itemStatus = results[0].getLong("i_status");
                        long buyerId = results[0].getLong("ib_buyer_id");

                        if (itemStatus == 0) {
                            profile.removeAvailableItem(sellerId, itemId);

                            long bidId = results[0].getLong("imb_ib_id");
                            if (results[0].wasNull()) {
                                // No winning bid
                                profile.addCompleteItem(sellerId, itemId);
                            } else {
                                // Has winning bid
                                profile.addWaitForPurchaseItem(sellerId, itemId, bidId, buyerId);
                            }
                        }
                    }

                    results[0].resetRowPosition();
                    Transaction.POST_AUCTION.next_params.add(results[0]);
                    break;
                }
                case GET_ITEM: {
                    if (this.rng.nextInt(100) < Math.round(((float) AuctionMarkConstants.FREQUENCY_NEW_BID * 100) / ((float) AuctionMarkConstants.FREQUENCY_GET_ITEM))) {
                        if (results.length > 0) {
                            Transaction.NEW_BID.next_params.add(results[0]);
                        }
                    }
                    break;
                }
                case NEW_BID: {
                    break;
                }
                case NEW_COMMENT:
                    break;
                case GET_COMMENT: {
                    if (this.rng.nextInt(100) < Math.round(((float) AuctionMarkConstants.FREQUENCY_NEW_COMMENT_RESPONSE * 100) / ((float) AuctionMarkConstants.FREQUENCY_NEW_COMMENT))) {
                        if (null != results && results.length > 0 && results[0].getRowCount() > 0) {
                            Transaction.NEW_COMMENT_RESPONSE.next_params.add(results[0]);
                        }
                    }
                    break;
                }
                case NEW_COMMENT_RESPONSE:
                    break;
                case NEW_FEEDBACK: {
                    break;
                }
                case NEW_ITEM: {
                    if (results.length > 0 && results[0].advanceRow()) {
                        long itemId = results[0].getLong("i_id");
                        long sellerId = results[0].getLong("i_u_id");
                        profile.addAvailableItem(sellerId, itemId);
                    }
                    break;
                }
                case NEW_PURCHASE: {
                    if (results.length > 0 && results[0].advanceRow()) {
                        long itemId = results[0].getLong("ip_ib_i_id");
                        long sellerId = results[0].getLong("u_id");
                        if (trace)
                            LOG.trace("clientCallback:: NEW_PURCHASE itemId = " + itemId);
                        if (trace)
                            LOG.trace("clientCallback:: NEW_PURCHASE sellerId = " + sellerId);
                        profile.removeWaitForPurchaseItem(sellerId, itemId);
                        profile.addCompleteItem(sellerId, itemId);
                        if (trace)
                            LOG.trace("clientCallback:: NEW_PURCHASE END");
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
                    if (trace)
                        LOG.trace("GOT USER INFO:::::");
                    break;
                }
            } // SWITCH
        }
    } // END CLASS

    @Override
    public String getApplicationName() {
        return "AuctionMark Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Client";
    }
}