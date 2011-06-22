/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.benchmark.tpcc;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.benchmark.Clock;
import org.voltdb.types.TimestampType;

import edu.brown.rand.RandomDistribution;
import edu.brown.utils.StringUtil;

public class TPCCSimulation {
    private static final Logger LOG = Logger.getLogger(TPCCSimulation.class);
    
    // type used by at least VoltDBClient and JDBCClient
    public static enum Transaction {
        STOCK_LEVEL("Stock Level", Constants.FREQUENCY_STOCK_LEVEL),
        DELIVERY("Delivery", Constants.FREQUENCY_DELIVERY),
        ORDER_STATUS("Order Status", Constants.FREQUENCY_ORDER_STATUS),
        PAYMENT("Payment", Constants.FREQUENCY_PAYMENT),
        NEW_ORDER("New Order", Constants.FREQUENCY_NEW_ORDER),
        RESET_WAREHOUSE("Reset Warehouse", 0);

        private Transaction(String displayName, int weight) {
            this.displayName = displayName;
            this.weight = weight;
        }
        public final String displayName;
        public final int weight;
    }


    public interface ProcCaller {
        public void callResetWarehouse(long w_id, long districtsPerWarehouse,
                long customersPerDistrict, long newOrdersPerDistrict)
        throws IOException;
        public void callStockLevel(short w_id, byte d_id, int threshold) throws IOException;
        public void callOrderStatus(String proc, Object... paramlist) throws IOException;
        public void callDelivery(short w_id, int carrier, TimestampType date) throws IOException;
        public void callPaymentByName(short w_id, byte d_id, double h_amount,
                short c_w_id, byte c_d_id, String c_last, TimestampType now) throws IOException;
        public void callPaymentById(short w_id, byte d_id, double h_amount,
                short c_w_id, byte c_d_id, int c_id, TimestampType now)
        throws IOException;
        public void callNewOrder(boolean rollback, boolean noop, Object... paramlist) throws IOException;
    }


    private final TPCCSimulation.ProcCaller client;
    private final RandomGenerator generator;
    private final Clock clock;
    public ScaleParameters parameters;
    private final boolean useWarehouseAffinity;
    private final long affineWarehouse;
    private final double m_skewFactor;
    private final boolean noop;
    private final int[] SAMPLE_TABLE = new int[100];
    
    private final boolean neworder_only;
    private final boolean neworder_abort;
    private final boolean neworder_multip;
    private final boolean neworder_all_multip;
    
    private final int max_w_id;
    static long lastAssignedWarehouseId = 1;
    
    private RandomDistribution.Zipf zipf;

    public TPCCSimulation(TPCCSimulation.ProcCaller client, RandomGenerator generator,
                          Clock clock, ScaleParameters parameters, boolean useWarehouseAffinity,
                          double skewFactor, boolean noop,
                          boolean neworder_only, boolean neworder_abort, boolean neworder_multip, boolean neworder_all_multip
    ) {
        assert parameters != null;
        this.client = client;
        this.generator = generator;
        this.clock = clock;
        this.parameters = parameters;
        this.useWarehouseAffinity = useWarehouseAffinity;
        this.affineWarehouse = lastAssignedWarehouseId;
        this.m_skewFactor = skewFactor;
        this.noop = noop;
        this.initSampleTable();
        
        this.neworder_only = neworder_only;
        this.neworder_abort = neworder_abort;
        this.neworder_multip = neworder_multip;
        this.neworder_all_multip = neworder_all_multip;
        
        this.max_w_id = (parameters.warehouses + parameters.starting_warehouse - 1);
        if (this.m_skewFactor > 0) {
            LOG.info("Enabling W_ID Zipfian Skew: " + m_skewFactor);
            this.zipf = new RandomDistribution.Zipf(new Random(), parameters.starting_warehouse, max_w_id+1, m_skewFactor);
            this.zipf.enableHistory();
        }

        lastAssignedWarehouseId += 1;
        if (lastAssignedWarehouseId > max_w_id)
            lastAssignedWarehouseId = 1;
        
        if (LOG.isDebugEnabled()) LOG.debug(this.toString());
    }
    
    /**
     * Initialize the sampling table
     */
    private void initSampleTable() {
        int i = 0;
        int sum = 0;
        for (Transaction t : Transaction.values()) {
            for (int ii = 0; ii < t.weight; ii++) {
                SAMPLE_TABLE[i++] = t.ordinal();
            }
            sum += t.weight;
        }
        assert (100 == sum);
    }
    
    @Override
    public String toString() {
        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.put("Use Warehouse Affinity", this.useWarehouseAffinity);
        m0.put("Affine Warehouse", lastAssignedWarehouseId);
        m0.put("Skew Factor", this.m_skewFactor);
        m0.put("Enable NOOP", this.noop);
        m0.put("NewOrder Only", this.neworder_only);
        m0.put("NewOrder Abort", this.neworder_abort);
        m0.put("NewOrder Remote Warehouses", this.neworder_multip);
        m0.put("NewOrder Only Warehouses", this.neworder_all_multip);
        
        Map<String, Object> m1 = new ListOrderedMap<String, Object>();
        m1.put("Warehouses", parameters.warehouses);
        m1.put("W_ID Range", String.format("[%d, %d]", parameters.starting_warehouse, this.max_w_id));
        m1.put("Districts per Warehouse", parameters.districtsPerWarehouse);
        m1.put("Custers per District", parameters.customersPerDistrict);
        m1.put("Initial Orders per District", parameters.newOrdersPerDistrict);
        m1.put("Items", parameters.items);
    
        return (String.format("TPCC Simulator Options\n%s", StringUtil.formatMaps(m0, m1)));
    }

    private short generateWarehouseId() {
        short w_id = -1;
        if (useWarehouseAffinity)
            w_id = (short)this.affineWarehouse;
        else if (this.zipf != null) {
            // long num_samples = this.zipf.getSampleCount();
            // if (num_samples > 0 && num_samples % 10000 == 0) System.err.println("W_ID Distribution:\n" + this.zipf.getHistory());
            w_id = (short)this.zipf.nextInt();
        } else if (m_skewFactor > 0.0d) {
            w_id = (short)generator.skewedNumber(parameters.starting_warehouse, max_w_id, m_skewFactor);
        } else {
            w_id = (short)generator.number(parameters.starting_warehouse, this.max_w_id);
        }
        
        assert(w_id >= parameters.starting_warehouse) : String.format("Invalid W_ID: %d [min=%d, max=%d]", w_id, parameters.starting_warehouse, max_w_id); 
        assert(w_id <= this.max_w_id) : String.format("Invalid W_ID: %d [min=%d, max=%d]", w_id, parameters.starting_warehouse, max_w_id);
        return w_id;
    }

    private byte generateDistrict() {
        return (byte)generator.number(1, parameters.districtsPerWarehouse);
    }

    private int generateCID() {
        return generator.NURand(1023, 1, parameters.customersPerDistrict);
    }

    private int generateItemID() {
        return generator.NURand(8191, 1, parameters.items);
    }

    /** Executes a reset warehouse transaction. */
    public void doResetWarehouse() throws IOException {
        long w_id = generateWarehouseId();
        client.callResetWarehouse(w_id, parameters.districtsPerWarehouse,
            parameters.customersPerDistrict, parameters.newOrdersPerDistrict);
    }

    /** Executes a stock level transaction. */
    public void doStockLevel() throws IOException {
        int threshold = generator.number(Constants.MIN_STOCK_LEVEL_THRESHOLD,
                                          Constants.MAX_STOCK_LEVEL_THRESHOLD);

        client.callStockLevel(generateWarehouseId(), generateDistrict(), threshold);
    }

    /** Executes an order status transaction. */
    public void doOrderStatus() throws IOException {
        int y = generator.number(1, 100);

        if (y <= 60) {
            // 60%: order status by last name
            String cLast = generator
                    .makeRandomLastName(parameters.customersPerDistrict);
            client.callOrderStatus(Constants.ORDER_STATUS_BY_NAME,
                                   generateWarehouseId(), generateDistrict(), cLast);

        } else {
            // 40%: order status by id
            assert y > 60;
            client.callOrderStatus(Constants.ORDER_STATUS_BY_ID,
                                   generateWarehouseId(), generateDistrict(), generateCID());
        }
    }

    /** Executes a delivery transaction. */
    public void doDelivery()  throws IOException {
        int carrier = generator.number(Constants.MIN_CARRIER_ID,
                                        Constants.MAX_CARRIER_ID);

        client.callDelivery(generateWarehouseId(), carrier, clock.getDateTime());
    }

    /** Executes a payment transaction. */
    public void doPayment()  throws IOException {
        int x = generator.number(1, 100);
        int y = generator.number(1, 100);

        short w_id = generateWarehouseId();
        byte d_id = generateDistrict();

        short c_w_id;
        byte c_d_id;
        if (parameters.warehouses == 1 || x <= 85) {
            // 85%: paying through own warehouse (or there is only 1 warehouse)
            c_w_id = w_id;
            c_d_id = d_id;
        } else {
            // 15%: paying through another warehouse:
            // select in range [1, num_warehouses] excluding w_id
            c_w_id = (short)generator.numberExcluding(parameters.starting_warehouse, max_w_id, w_id);
            assert c_w_id != w_id;
            c_d_id = generateDistrict();
        }
        double h_amount = generator.fixedPoint(2, Constants.MIN_PAYMENT,
                Constants.MAX_PAYMENT);

        TimestampType now = clock.getDateTime();

        if (y <= 60) {
            // 60%: payment by last name
            String c_last = generator
                    .makeRandomLastName(parameters.customersPerDistrict);
            client.callPaymentByName(w_id, d_id, h_amount, c_w_id, c_d_id, c_last, now);
        } else {
            // 40%: payment by id
            assert y > 60;
            client.callPaymentById(w_id, d_id, h_amount, c_w_id, c_d_id,
                                   generateCID(), now);
        }
    }

    /** Executes a new order transaction. */
    public void doNewOrder() throws IOException {
        boolean allow_rollback = this.neworder_abort;
        
        short warehouse_id = generateWarehouseId();
        int ol_cnt = generator.number(Constants.MIN_OL_CNT, Constants.MAX_OL_CNT);

        // 1% of transactions roll back
        boolean rollback = (allow_rollback && generator.number(1, 100) == 1);
        int local_warehouses = 0;
        int remote_warehouses = 0;

        int[] item_id = new int[ol_cnt];
        short[] supply_w_id = new short[ol_cnt];
        int[] quantity = new int[ol_cnt];
        for (int i = 0; i < ol_cnt; ++i) {
            if (rollback && i + 1 == ol_cnt) {
                // LOG.fine("[NOT_ERROR] Causing a rollback on purpose defined in TPCC spec. "
                //     + "You can ignore following 'ItemNotFound' exception.");
                item_id[i] = parameters.items + 1;
            } else {
                item_id[i] = generateItemID();
            }

            // 1% of items are from a remote warehouse
            boolean remote = (this.neworder_all_multip) || (this.neworder_multip && generator.number(1, 100) == 1);
            if (parameters.warehouses > 1 && remote) {
                supply_w_id[i] = (short)generator.numberExcluding(parameters.starting_warehouse, this.max_w_id, (int) warehouse_id);
                if (supply_w_id[i] != warehouse_id) remote_warehouses++;
                else local_warehouses++;
            } else {
                supply_w_id[i] = warehouse_id;
                local_warehouses++;
            }

            quantity[i] = generator.number(1, Constants.MAX_OL_QUANTITY);
        }
//        if (remote_warehouses > 0) {
//            System.err.println("newOrder(W_ID=" + warehouse_id + ") -> [" +
//                               "local_warehouses=" + local_warehouses + ", " +
//                               "remote_warehouses=" + remote_warehouses + "]");
//        }

        TimestampType now = clock.getDateTime();
        client.callNewOrder(rollback, this.noop, warehouse_id, generateDistrict(), generateCID(),
                            now, item_id, supply_w_id, quantity);
    }

    /**
     * Selects and executes a transaction at random. The number of new order
     * transactions executed per minute is the official "tpmC" metric. See TPC-C
     * 5.4.2 (page 71).
     *
     * @return the transaction that was executed..
     */
    public int doOne() throws IOException {
        // This is not strictly accurate: The requirement is for certain
        // *minimum* percentages to be maintained. This is close to the right
        // thing, but not precisely correct. See TPC-C 5.2.4 (page 68).
       if (this.noop || this.neworder_only) {
           doNewOrder();
           return Transaction.NEW_ORDER.ordinal();
        }
        
        int x = generator.number(0, 99);
        Transaction t = Transaction.values()[this.SAMPLE_TABLE[x]];
        switch (t) {
            case STOCK_LEVEL:
                doStockLevel();
                break;
            case DELIVERY:
                doDelivery();
                break;
            case ORDER_STATUS:
                doOrderStatus();
                break;
            case PAYMENT:
                doPayment();
                break;
            case NEW_ORDER:
                doNewOrder();
                break;
            default:
                assert(false) : "Unexpected transaction " + t;
        }
        return (t.ordinal());
    }
}
