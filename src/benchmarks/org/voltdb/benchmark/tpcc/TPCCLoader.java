/*
 * This file is part of VoltDB. Copyright (C) 2008-2010 VoltDB L.L.C. This file
 * contains original code and/or modifications of original code. Any
 * modifications made by VoltDB L.L.C. are licensed under the following terms
 * and conditions: Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including without
 * limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom
 * the Software is furnished to do so, subject to the following conditions: The
 * above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software. THE SOFTWARE IS PROVIDED
 * "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
 * OR OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * Copyright (C) 2008 Evan Jones Massachusetts Institute of Technology
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions: The above copyright
 * notice and this permission notice shall be included in all copies or
 * substantial portions of the Software. THE SOFTWARE IS PROVIDED "AS IS",
 * WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
 * OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.tpcc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;

/**
 * TPC-C database loader. Note: The methods order id parameters from "top level"
 * to "low level" parameters. However, the insert stored procedures use the
 * defined TPC-C table order, which goes from "low level" to "top level" (except
 * in the case of order line, which is inconsistent). As as example, this class
 * uses (o_w_id, o_d_id, o_id), whereas the order table is defined as (o_id,
 * o_d_id, o_w_id).
 */
public class TPCCLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    /**
     * Number of threads to create to do the loading.
     */
    private final LoadThread m_loadThreads[];
    private final TPCCConfig m_tpccConfig;
    private final int replicated_batch_size;
    private final AtomicInteger finishedWarehouses = new AtomicInteger(0);
    private final AtomicInteger finishedCounter = new AtomicInteger(0);
    private Throwable pendingError;

    private int MAX_BATCH_SIZE = 10000;
    
    public TPCCLoader(String args[]) {
        super(args);

        initTableNames();
        
        CatalogContext catalogContext = this.getCatalogContext();
        m_tpccConfig = TPCCConfig.createConfig(catalogContext, m_extraParams);
        m_loadThreads = new LoadThread[m_tpccConfig.num_loadthreads];
        
        // Scale the MAX_BATCH_SIZE based on the number of partitions
        this.replicated_batch_size = MAX_BATCH_SIZE / catalogContext.numberOfPartitions;
        
        if (debug.val)
            LOG.debug("Loader Configuration:\n" + m_tpccConfig);
        
        HStoreConf hstore_conf = this.getHStoreConf();
        for (int ii = 0; ii < m_tpccConfig.num_loadthreads; ii++) {
            ScaleParameters parameters = ScaleParameters.makeWithScaleFactor(m_tpccConfig, hstore_conf.client.scalefactor);
            assert parameters != null;

            RandomGenerator generator = new RandomGenerator.Implementation();
            TimestampType generationDateTime = new TimestampType();
            // TODO(evanj): The client needs these values to set its own values
            // correctly
            RandomGenerator.NURandC loadC = RandomGenerator.NURandC.makeForLoad(generator);
            generator.setC(loadC);

            m_loadThreads[ii] = new LoadThread(generator, generationDateTime, parameters, ii);
        }
    }

    private String[] table_names = new String[8]; // names of tables
    private final static int IDX_WAREHOUSES = 0;
    private final static int IDX_DISTRICTS = 1;
    private final static int IDX_CUSTOMERS = 2;
    private final static int IDX_STOCKS = 3;
    private final static int IDX_ORDERS = 4;
    private final static int IDX_NEWORDERS = 5;
    private final static int IDX_ORDERLINES = 6;
    private final static int IDX_HISTORIES = 7;

    private void initTableNames() {
        table_names[IDX_WAREHOUSES] = TPCCConstants.TABLENAME_WAREHOUSE;
        table_names[IDX_DISTRICTS] = TPCCConstants.TABLENAME_DISTRICT;
        table_names[IDX_CUSTOMERS] = TPCCConstants.TABLENAME_CUSTOMER;
        table_names[IDX_STOCKS] = TPCCConstants.TABLENAME_STOCK;
        table_names[IDX_ORDERS] = TPCCConstants.TABLENAME_ORDERS;
        table_names[IDX_NEWORDERS] = TPCCConstants.TABLENAME_NEW_ORDER;
        table_names[IDX_ORDERLINES] = TPCCConstants.TABLENAME_ORDER_LINE;
        table_names[IDX_HISTORIES] = TPCCConstants.TABLENAME_HISTORY;
    }

    public static void main(String[] args) {
        BenchmarkComponent.main(TPCCLoader.class, args, true);
    }

    /**
     * Hint used when constructing the Client to control the size of buffers
     * allocated for message serialization
     * 
     * @return
     */
    @Override
    protected int getExpectedOutgoingMessageSize() {
        return 10485760;
    }

    class LoadThread extends Thread {

        private final RandomGenerator m_generator;
        private final TimestampType m_generationDateTime;
        private final ScaleParameters m_parameters;
        private final int itemStart;
        private final int itemEnd;
        private boolean stop = false;

        /**
         * table data FOR CURRENT WAREHOUSE (LoadWarehouse is partitioned on
         * WID).
         */
        private final VoltTable[] data_tables = new VoltTable[8]; // non
                                                                  // replicated
                                                                  // tables
        public LoadThread(RandomGenerator generator, TimestampType generationDateTime, ScaleParameters parameters, int threadIndex) {
            super("Load Thread " + threadIndex);
            m_generator = generator;
            this.m_generationDateTime = generationDateTime;
            this.m_parameters = parameters;
            
            int itemsPerThread = parameters.num_items / m_tpccConfig.num_loadthreads; 
            this.itemStart = itemsPerThread * threadIndex;
            this.itemEnd = this.itemStart + itemsPerThread;
        }

        @Override
        public void run() {
            Integer warehouseId = null;
            if (debug.val)
                LOG.debug("Total # of Remaining Warehouses: " + availableWarehouseIds.size());
            while (this.stop == false && (warehouseId = availableWarehouseIds.poll()) != null) {
                if (debug.val)
                    LOG.debug(String.format("Loading warehouse %d / %d", (m_tpccConfig.num_warehouses - availableWarehouseIds.size()), m_tpccConfig.num_warehouses));
                makeStock(warehouseId); // STOCK is made separately to reduce
                                        // memory consumption
                createDataTables();
                makeWarehouse(warehouseId);
                for (int i = 0; i < data_tables.length; ++i)
                    data_tables[i] = null;
                if (this.stop == false) 
                    LOG.info(String.format("Finished WAREHOUSE %02d [%d/%d]",
                             warehouseId, finishedWarehouses.incrementAndGet(), m_tpccConfig.num_warehouses));
            } // WHILE
            if (this.stop) return;
            
            makeItems(this.itemStart, itemEnd);
            LOG.info(String.format("Finished ITEM %d - %d [%d/%d]",
                     this.itemStart, this.itemEnd, finishedCounter.incrementAndGet(), m_tpccConfig.num_loadthreads));

//            VoltTable results[] = null;
//            try {
//                results = this.getClientHandle().callProcedure(GetTableCounts.class.getSimpleName()).getResults();
//            } catch (Exception e) {
//                e.printStackTrace();
//                System.exit(1);
//            }
//            assert (results.length == 1);
//            String ret = "TPC-C Table Stats:\n";
//            while (results[0].advanceRow()) {
//                String name = results[0].getString(0);
//                long count = results[0].getLong(1);
//                ret += String.format("%-20s %d\n", name + ":", count);
//            } // WHILE
//            System.err.println(ret);
        }

        private double makeTax() {
            return m_generator.fixedPoint(TPCCConstants.TAX_DECIMALS, TPCCConstants.MIN_TAX, TPCCConstants.MAX_TAX);
        }

        private String makeZip() {
            int length = TPCCConstants.ZIP_LENGTH - TPCCConstants.ZIP_SUFFIX.length();
            return m_generator.nstring(length, length) + TPCCConstants.ZIP_SUFFIX;
        }

        private HashSet<Integer> selectUniqueIds(int numUnique, int minimum, int maximum) {
            HashSet<Integer> rows = new HashSet<Integer>(numUnique * 2);
            for (int i = 0; i < numUnique; ++i) {
                int index;
                do {
                    index = m_generator.number(minimum, maximum);
                } while (rows.contains(index));
                assert !rows.contains(index);
                rows.add(index);
            }
            assert rows.size() == numUnique;
            return rows;
        }

        /** @returns a string with ORIGINAL_STRING at a random position. */
        private String fillOriginal(String data) {
            int originalLength = TPCCConstants.ORIGINAL_STRING.length();
            int position = m_generator.number(0, data.length() - originalLength);
            String out = data.substring(0, position) + TPCCConstants.ORIGINAL_STRING
                    + data.substring(position + originalLength);
            assert out.length() == data.length();
            return out;
        }

        /**
         * Inserts a new item with id, generated according to the TPC-C
         * specification 4.3.3.1.
         * 
         * @param items
         * @param id
         * @param original
         */
        public void generateItem(VoltTable items, long id, boolean original) {
            long i_id = id;
            long i_im_id = m_generator.number(TPCCConstants.MIN_IM, TPCCConstants.MAX_IM);
            String i_name = m_generator.astring(TPCCConstants.MIN_I_NAME, TPCCConstants.MAX_I_NAME);
            double i_price = m_generator.fixedPoint(TPCCConstants.MONEY_DECIMALS, TPCCConstants.MIN_PRICE, TPCCConstants.MAX_PRICE);
            String i_data = m_generator.astring(TPCCConstants.MIN_I_DATA, TPCCConstants.MAX_I_DATA);

            if (original) {
                i_data = fillOriginal(i_data);
            }

            items.addRow(i_id, i_im_id, i_name, i_price, i_data);
        }

        public void generateWarehouse(long w_id) {
            double w_tax = makeTax();
            double w_ytd = TPCCConstants.INITIAL_W_YTD;

            ArrayList<Object> insertParameters = new ArrayList<Object>();
            insertParameters.add(w_id);
            addAddress(insertParameters);
            insertParameters.addAll(Arrays.asList(w_tax, w_ytd));

            data_tables[IDX_WAREHOUSES].addRow(insertParameters.toArray());
        }

        public void generateDistrict(long d_w_id, long d_id) {
            double d_tax = makeTax();
            double d_ytd = TPCCConstants.INITIAL_D_YTD;
            long d_next_o_id = m_parameters.customersPerDistrict + 1;

            ArrayList<Object> insertParameters = new ArrayList<Object>(Arrays.asList(d_id, d_w_id));
            addAddress(insertParameters);
            insertParameters.addAll(Arrays.asList(new Object[] { d_tax, d_ytd, d_next_o_id }));

            data_tables[IDX_DISTRICTS].addRow(insertParameters.toArray());
        }

        private final Object[] container_customer = new Object[6 + 5 + 10];

        public void generateCustomer(long c_w_id, long c_d_id, long c_id, boolean badCredit) {
            String c_first = m_generator.astring(TPCCConstants.MIN_FIRST, TPCCConstants.MAX_FIRST);
            String c_middle = TPCCConstants.MIDDLE;

            assert 1 <= c_id;
            String c_last;
            if (c_id <= 1000) {
                c_last = m_generator.makeLastName((int) c_id - 1);
            } else {
                c_last = m_generator.makeRandomLastName(m_parameters.customersPerDistrict);
            }

            String c_phone = m_generator.nstring(TPCCConstants.PHONE, TPCCConstants.PHONE);
            TimestampType c_since = m_generationDateTime;
            String c_credit = badCredit ? TPCCConstants.BAD_CREDIT : TPCCConstants.GOOD_CREDIT;
            double c_credit_lim = TPCCConstants.INITIAL_CREDIT_LIM;
            double c_discount = m_generator.fixedPoint(TPCCConstants.DISCOUNT_DECIMALS, TPCCConstants.MIN_DISCOUNT,
                    TPCCConstants.MAX_DISCOUNT);
            double c_balance = TPCCConstants.INITIAL_BALANCE;
            double c_ytd_payment = TPCCConstants.INITIAL_YTD_PAYMENT;
            long c_payment_cnt = TPCCConstants.INITIAL_PAYMENT_CNT;
            long c_delivery_cnt = TPCCConstants.INITIAL_DELIVERY_CNT;
            String c_data = m_generator.astring(TPCCConstants.MIN_C_DATA, TPCCConstants.MAX_C_DATA);

            int ind = 0;
            container_customer[ind++] = c_id;
            container_customer[ind++] = c_d_id;
            container_customer[ind++] = c_w_id;
            container_customer[ind++] = c_first;
            container_customer[ind++] = c_middle;
            container_customer[ind++] = c_last;

            String street1 = m_generator.astring(TPCCConstants.MIN_STREET, TPCCConstants.MAX_STREET);
            String street2 = m_generator.astring(TPCCConstants.MIN_STREET, TPCCConstants.MAX_STREET);
            String city = m_generator.astring(TPCCConstants.MIN_CITY, TPCCConstants.MAX_CITY);
            String state = m_generator.astring(TPCCConstants.STATE, TPCCConstants.STATE);
            String zip = makeZip();
            container_customer[ind++] = street1;
            container_customer[ind++] = street2;
            container_customer[ind++] = city;
            container_customer[ind++] = state;
            container_customer[ind++] = zip;

            container_customer[ind++] = c_phone;
            container_customer[ind++] = c_since;
            container_customer[ind++] = c_credit;
            container_customer[ind++] = c_credit_lim;
            container_customer[ind++] = c_discount;
            container_customer[ind++] = c_balance;
            container_customer[ind++] = c_ytd_payment;
            container_customer[ind++] = c_payment_cnt;
            container_customer[ind++] = c_delivery_cnt;
            container_customer[ind++] = c_data;
            data_tables[IDX_CUSTOMERS].addRow(container_customer);
        }

        /**
         * Appends the name and address fields to insertParameters. Used by both
         * generateWarehouse and generateDistrict.
         */
        private void addAddress(ArrayList<Object> insertParameters) {
            String name = m_generator.astring(TPCCConstants.MIN_NAME, TPCCConstants.MAX_NAME);
            insertParameters.add(name);
            addStreetAddress(insertParameters);
        }

        /**
         * Appends the street address fields to insertParameters. Used for
         * warehouses, districts and customers.
         */
        private void addStreetAddress(ArrayList<Object> insertParameters) {
            String street1 = m_generator.astring(TPCCConstants.MIN_STREET, TPCCConstants.MAX_STREET);
            String street2 = m_generator.astring(TPCCConstants.MIN_STREET, TPCCConstants.MAX_STREET);
            String city = m_generator.astring(TPCCConstants.MIN_CITY, TPCCConstants.MAX_CITY);
            String state = m_generator.astring(TPCCConstants.STATE, TPCCConstants.STATE);
            String zip = makeZip();

            insertParameters.addAll(Arrays.asList(street1, street2, city, state, zip));
        }

        private final Object[] container_stock = new Object[3 + TPCCConstants.DISTRICTS_PER_WAREHOUSE + 4];

        public void generateStock(long s_w_id, long s_i_id, boolean original) {
            long s_quantity = m_generator.number(TPCCConstants.MIN_QUANTITY, TPCCConstants.MAX_QUANTITY);
            long s_ytd = 0;
            long s_order_cnt = 0;
            long s_remote_cnt = 0;

            String s_data = m_generator.astring(TPCCConstants.MIN_I_DATA, TPCCConstants.MAX_I_DATA);
            if (original) {
                s_data = fillOriginal(s_data);
            }
            int ind = 0;
            container_stock[ind++] = s_i_id;
            container_stock[ind++] = s_w_id;
            container_stock[ind++] = s_quantity;
            for (int i = 0; i < TPCCConstants.DISTRICTS_PER_WAREHOUSE; ++i) {
                String s_dist_x = m_generator.astring(TPCCConstants.DIST, TPCCConstants.DIST);
                container_stock[ind++] = s_dist_x;
            }
            container_stock[ind++] = s_ytd;
            container_stock[ind++] = s_order_cnt;
            container_stock[ind++] = s_remote_cnt;
            container_stock[ind++] = s_data;

            data_tables[IDX_STOCKS].addRow(container_stock);
        }

        /* returns the generated o_ol_cnt value. */
        public long generateOrder(long o_w_id, long o_d_id, long o_id, long o_c_id, boolean newOrder) {
            TimestampType o_entry_d = m_generationDateTime;
            long o_carrier_id;
            if (!newOrder) {
                o_carrier_id = m_generator.number(TPCCConstants.MIN_CARRIER_ID, TPCCConstants.MAX_CARRIER_ID);
            } else {
                o_carrier_id = TPCCConstants.NULL_CARRIER_ID;
            }
            long o_ol_cnt = m_generator.number(TPCCConstants.MIN_OL_CNT, TPCCConstants.MAX_OL_CNT);
            long o_all_local = TPCCConstants.INITIAL_ALL_LOCAL;

            data_tables[IDX_ORDERS]
                    .addRow(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local);
            return o_ol_cnt;
        }

        public void generateOrderLine(long ol_w_id, long ol_d_id, long ol_o_id, long ol_number, boolean newOrder) {
            long ol_i_id = m_generator.number(1, m_parameters.num_items);
            long ol_supply_w_id = ol_w_id;
            TimestampType ol_delivery_d = m_generationDateTime;
            long ol_quantity = TPCCConstants.INITIAL_QUANTITY;

            double ol_amount;
            if (!newOrder) {
                ol_amount = 0.00;
            } else {
                ol_amount = m_generator.fixedPoint(TPCCConstants.MONEY_DECIMALS, TPCCConstants.MIN_AMOUNT, TPCCConstants.MAX_PRICE
                        * TPCCConstants.MAX_OL_QUANTITY);
                ol_delivery_d = null;
            }
            String ol_dist_info = m_generator.astring(TPCCConstants.DIST, TPCCConstants.DIST);

            data_tables[IDX_ORDERLINES].addRow(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id,
                    ol_delivery_d, ol_quantity, ol_amount, ol_dist_info);
        }

        // private long max_hid = 0;
        public void generateHistory(long h_c_w_id, long h_c_d_id, long h_c_id) {
            long h_w_id = h_c_w_id;
            long h_d_id = h_c_d_id;
            TimestampType h_date = m_generationDateTime;
            double h_amount = TPCCConstants.INITIAL_AMOUNT;
            String h_data = m_generator.astring(TPCCConstants.MIN_DATA, TPCCConstants.MAX_DATA);

            data_tables[IDX_HISTORIES].addRow(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data);
        }

        /** STOCK is made in different method to reduce memory consumption. */
        public void makeStock(int w_id) {
            // Select 10% of the stock to be marked "original"

            data_tables[IDX_STOCKS] = new VoltTable(
                    new VoltTable.ColumnInfo("S_I_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("S_QUANTITY", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_DIST_01", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_02", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_03", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_04", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_05", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_06", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_07", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_08", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_09", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_DIST_10", VoltType.STRING),
                    new VoltTable.ColumnInfo("S_YTD", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_ORDER_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_REMOTE_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("S_DATA", VoltType.STRING));
            // t.ensureRowCapacity(parameters.items / BATCH);
            // t.ensureStringCapacity(parameters.items * (32 * 10 + 64) /
            // BATCH);

            HashSet<Integer> selectedRows = selectUniqueIds(m_parameters.num_items / 10, 1, m_parameters.num_items);

            int firstItemId = TPCCConstants.STARTING_ITEM;
            int lastItemId = (TPCCConstants.STARTING_ITEM + m_parameters.num_items);
            for (int i_id = firstItemId; i_id < lastItemId; i_id++) {
                boolean original = selectedRows.contains(i_id);
                generateStock(w_id, i_id, original);
                if (i_id > 0 && i_id % MAX_BATCH_SIZE == 0) {
                    commitDataTables(w_id);
                    if (debug.val)
                        LOG.debug(String.format("%d/%d", i_id, m_parameters.num_items));
                }
            }
            if (data_tables[IDX_STOCKS].getRowCount() != 0) {
                commitDataTables(w_id);
            }
            data_tables[IDX_STOCKS] = null;

        }

        // TODO(evanj): The C++ version has tests for this code that could be
        // ported over, but it would need a fair bit of hacking in the unit test
        // to make them work, since it requires storing all the inserted tuples.
        public void makeWarehouse(long w_id) {
            generateWarehouse(w_id);

            // replicate name and id to every site
            
            for (int d_id = 1; d_id <= m_parameters.districtsPerWarehouse; ++d_id) {
                if (this.stop) return;
                
                // System.err.printf("Beginning District: %d\n", d_id);
                generateDistrict(w_id, d_id);

                // Select 10% of the customers to have bad credit
                HashSet<Integer> selectedRows = selectUniqueIds(m_parameters.customersPerDistrict / 10, 1,
                        m_parameters.customersPerDistrict);
                // long[] c_ids = new long[customersPerDistrict];
                for (int c_id = 1; c_id <= m_parameters.customersPerDistrict; ++c_id) {
                    boolean badCredit = selectedRows.contains(c_id);
                    generateCustomer(w_id, d_id, c_id, badCredit);
                    generateHistory(w_id, d_id, c_id);
                }

                // TPC-C 4.3.3.1. says that o_c_id should be a permutation of
                // [1, 3000]. But since it
                // is a c_id field, it seems to make sense to have it be a
                // permutation of the
                // customers. For the "real" thing this will be equivalent
                int[] cIdPermutation = new int[m_parameters.customersPerDistrict];
                for (int i = 0; i < m_parameters.customersPerDistrict; ++i) {
                    cIdPermutation[i] = i + 1;
                }
                assert cIdPermutation[0] == 1;
                assert cIdPermutation[m_parameters.customersPerDistrict - 1] == m_parameters.customersPerDistrict;
                Collections.shuffle(Arrays.asList(cIdPermutation));

                for (int o_id = 1; o_id <= m_parameters.customersPerDistrict; ++o_id) {
                    // The last newOrdersPerDistrict are new orders
                    boolean newOrder = m_parameters.customersPerDistrict - m_parameters.newOrdersPerDistrict < o_id;
                    long o_ol_cnt = generateOrder(w_id, d_id, o_id, cIdPermutation[o_id - 1], newOrder);

                    // Generate each OrderLine for the order
                    for (int ol_number = 1; ol_number <= o_ol_cnt; ++ol_number) {
                        generateOrderLine(w_id, d_id, o_id, ol_number, newOrder);
                    }

                    if (newOrder) {
                        // This is a new order: make one for it
                        data_tables[IDX_NEWORDERS].addRow((long) o_id, (long) d_id, w_id);
                    }
                }
                commitDataTables(w_id); // flushout current data to avoid
                                        // outofmemory
            }
        }

        /** generate replicated tables: ITEM */
        public void makeItems(int itemStart, int itemFinish) {
            // create ITEMS here to reduce memory consumption
            VoltTable items = new VoltTable(new VoltTable.ColumnInfo("I_ID", VoltType.INTEGER),
                                            new VoltTable.ColumnInfo("I_IM_ID", VoltType.INTEGER),
                                            new VoltTable.ColumnInfo("I_NAME", VoltType.STRING),
                                            new VoltTable.ColumnInfo("I_PRICE", VoltType.FLOAT),
                                            new VoltTable.ColumnInfo("I_DATA", VoltType.STRING));
            
            
            
            // items.ensureRowCapacity(parameters.items);
            // items.ensureStringCapacity(parameters.items * 96);
            // Select 10% of the rows to be marked "original"
            if (debug.val)
                LOG.debug(String.format("Loading replicated ITEM table [tuples=%d]", m_parameters.num_items));
            HashSet<Integer> originalRows = selectUniqueIds(m_parameters.num_items / 10, 1, m_parameters.num_items);
            for (int i = itemStart; i < itemFinish; ++i) {
                // if we're on a 10% boundary, print out some nice status info
                // if (i % (m_parameters.items / 10) == 0)
                // System.err.printf("   %d%%\n", (i * 100) /
                // m_parameters.items);

                boolean original = originalRows.contains(i);
                generateItem(items, i, original);
                
                // Items! Sail yo ho!
                if (items.getRowCount() == replicated_batch_size) {
                    if (debug.val)
                        LOG.debug(String.format("Loading replicated ITEM table [tuples=%d/%d]", i, m_parameters.num_items));
                    loadVoltTable("ITEM", items);
                    items.clearRowData();
                }
                if (this.stop) return;
            } // FOR
            if (items.getRowCount() > 0) {
                String extra = "";
                if (items.getRowCount() < m_parameters.num_items) extra = String.format(" [tuples=%d/%d]", m_parameters.num_items-items.getRowCount(), m_parameters.num_items);
                if (debug.val)
                    LOG.debug("Loading replicated ITEM table" + extra);
                loadVoltTable("ITEM", items);
                items.clearRowData();
            }
            items = null;
        }
        
        /** Send to data to VoltDB and/or to the jdbc connection */
        private void commitDataTables(long w_id) {
            assert(getClientHandle() != null);
            if (getClientHandle() != null) {
                commitDataTables_VoltDB(w_id);
            }
            for (int i = 0; i < data_tables.length; ++i) {
                if (data_tables[i] != null) {
                    data_tables[i].clearRowData();
                }
            }
        }

        private void commitDataTables_VoltDB(long w_id) {
//            Object[] params = new Object[data_tables.length + 1];
//            params[0] = w_id;
//            for (int i = 0; i < data_tables.length; ++i) {
//                if (data_tables[i] != null && data_tables[i].getRowCount() > 0) {
//                    params[i + 1] = data_tables[i];
//                }
//            }
            // PAVLO: We don't want to use LoadWarehouse because we want to let
            // the system figure out where to put all of the tuples
            for (int i = 0; i < data_tables.length; ++i) {
                if (data_tables[i] != null && data_tables[i].getRowCount() > 0) {
                    if (debug.val)
                        LOG.debug(String.format("WAREHOUSE[%02d]: %s %d tuples", w_id, table_names[i], data_tables[i].getRowCount()));
                    loadVoltTable(table_names[i], data_tables[i]);
                }
            }
            // rethrowExceptionLoad(Constants.LOAD_WAREHOUSE, params);
        }

        private void createDataTables() {
            // customerNames.ensureRowCapacity(parameters.warehouses *
            // parameters.districtsPerWarehouse *
            // parameters.customersPerDistrict);
            // customerNames.ensureStringCapacity(parameters.warehouses *
            // parameters.districtsPerWarehouse *
            // parameters.customersPerDistrict * (64));

            // non replicated tables
            for (int i = 0; i < data_tables.length; ++i)
                data_tables[i] = null;
            data_tables[IDX_WAREHOUSES] = new VoltTable(
                    new VoltTable.ColumnInfo("W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("W_NAME", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_STREET_1", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_STREET_2", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_CITY", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_STATE", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_ZIP", VoltType.STRING),
                    new VoltTable.ColumnInfo("W_TAX", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("W_YTD", VoltType.FLOAT));
            // t.ensureRowCapacity(1);
            // t.ensureStringCapacity(200);

            data_tables[IDX_DISTRICTS] = new VoltTable(
                    new VoltTable.ColumnInfo("D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("D_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("D_NAME", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_STREET_1", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_STREET_2", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_CITY", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_STATE", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_ZIP", VoltType.STRING),
                    new VoltTable.ColumnInfo("D_TAX", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("D_YTD", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("D_NEXT_O_ID", VoltType.INTEGER));
            // t.ensureRowCapacity(1);
            // t.ensureStringCapacity(1 * (16 + 96 + 2 + 9));

            data_tables[IDX_CUSTOMERS] = new VoltTable(
                    new VoltTable.ColumnInfo("C_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("C_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("C_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("C_FIRST", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_MIDDLE", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_LAST", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_STREET_1", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_STREET_2", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_CITY", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_STATE", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_ZIP", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_PHONE", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_SINCE", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("C_CREDIT", VoltType.STRING),
                    new VoltTable.ColumnInfo("C_CREDIT_LIM", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_DISCOUNT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_BALANCE", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_YTD_PAYMENT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("C_PAYMENT_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("C_DELIVERY_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("C_DATA", VoltType.STRING));
            // t.ensureRowCapacity(parameters.customersPerDistrict);
            // t.ensureStringCapacity(parameters.customersPerDistrict * (32 * 6
            // + 2 * 3 + 9 + 500));

            data_tables[IDX_ORDERS] = new VoltTable(
                    new VoltTable.ColumnInfo("O_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("O_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("O_C_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_ENTRY_D", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("O_CARRIER_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_OL_CNT", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("O_ALL_LOCAL", VoltType.INTEGER));
            // t.ensureRowCapacity(parameters.customersPerDistrict);

            data_tables[IDX_NEWORDERS] = new VoltTable(
                    new VoltTable.ColumnInfo("NO_O_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("NO_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("NO_W_ID", VoltType.SMALLINT));
            // t.ensureRowCapacity(parameters.customersPerDistrict);

            data_tables[IDX_ORDERLINES] = new VoltTable(
                    new VoltTable.ColumnInfo("OL_O_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("OL_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("OL_NUMBER", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_I_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_SUPPLY_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("OL_DELIVERY_D", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("OL_QUANTITY", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("OL_AMOUNT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("OL_DIST_INFO", VoltType.STRING));
            // t.ensureRowCapacity(parameters.customersPerDistrict *
            // Constants.MAX_OL_CNT);
            // t.ensureStringCapacity(parameters.customersPerDistrict *
            // Constants.MAX_OL_CNT * (32));

            data_tables[IDX_HISTORIES] = new VoltTable(
                    new VoltTable.ColumnInfo("H_C_ID", VoltType.INTEGER),
                    new VoltTable.ColumnInfo("H_C_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("H_C_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("H_D_ID", VoltType.TINYINT),
                    new VoltTable.ColumnInfo("H_W_ID", VoltType.SMALLINT),
                    new VoltTable.ColumnInfo("H_DATE", VoltType.TIMESTAMP),
                    new VoltTable.ColumnInfo("H_AMOUNT", VoltType.FLOAT),
                    new VoltTable.ColumnInfo("H_DATA", VoltType.STRING));
            // t.ensureRowCapacity(parameters.customersPerDistrict);
            // t.ensureStringCapacity(parameters.customersPerDistrict * (32));
        }
    }

    private ConcurrentLinkedQueue<Integer> availableWarehouseIds = new ConcurrentLinkedQueue<Integer>();

    @Override
    public void load() throws IOException {
        final EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        handler.addObserver(new EventObserver<Pair<Thread,Throwable>>() {
            public void update(edu.brown.utils.EventObservable<Pair<Thread,Throwable>> o, Pair<Thread,Throwable> t) {
                synchronized (TPCCLoader.this) {
                    if (TPCCLoader.this.pendingError != null) {
                        return;
                    }
                    TPCCLoader.this.pendingError = t.getSecond();
                } // SYNCH
                
                assert(TPCCLoader.this.pendingError != null);
                LOG.warn(String.format("Hit with %s from %s. Halting the loading process...",
                         t.getSecond().getClass().getSimpleName(), t.getFirst().getName()));
                
                for (LoadThread loadThread : m_loadThreads) {
                    loadThread.stop = true;
                } // FOR
                LOG.info("Waiting for all threads to clean themselves up");
                for (LoadThread loadThread : m_loadThreads) {
                    if (loadThread == Thread.currentThread()) continue;
                    try {
                        loadThread.join();
                    } catch (InterruptedException ex) {
                        // IGNORE
                    }
                } // FOR
            };
        });
        
        ArrayList<Integer> warehouseIds = new ArrayList<Integer>();
        int count = (m_tpccConfig.num_warehouses + m_tpccConfig.first_warehouse - 1);
        for (int ii = m_tpccConfig.first_warehouse; ii <= count; ii++) {
            warehouseIds.add(ii);
        }
        // Shuffling spreads the loading out across physical hosts better
        Collections.shuffle(warehouseIds);
        availableWarehouseIds.addAll(warehouseIds);

        LOG.info(String.format("Loading %d warehouses using %d load threads", warehouseIds.size(), m_loadThreads.length));
//        boolean doMakeReplicated = true;
        for (LoadThread loadThread : m_loadThreads) {
            if (debug.val) LOG.debug("Starting LoadThread...");
            loadThread.setUncaughtExceptionHandler(handler);
            // loadThread.start(true);
            loadThread.start();
//            doMakeReplicated = false;
        }

        for (int ii = 0; ii < m_loadThreads.length; ii++) {
            try {
                m_loadThreads[ii].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.info("Finished loading all warehouses");
        try {
            this.getClientHandle().drain();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        
        if (this.pendingError != null) {
            throw new RuntimeException(this.pendingError);
        }
    }
}
