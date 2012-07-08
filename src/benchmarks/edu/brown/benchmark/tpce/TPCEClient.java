/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package edu.brown.benchmark.tpce;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tm1.TM1Client.Transaction;
import edu.brown.benchmark.tpce.TPCEConstants.DriverType;
import edu.brown.benchmark.tpce.generators.*;
import edu.brown.benchmark.tpce.util.RandUtil;
import edu.brown.benchmark.tpce.util.TableStatistics;

/**
 * @author pavlo
 */
public class TPCEClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(TPCEClient.class);

    public static final int STATUS_SUCCESS = 0;

    private static Transaction XTRANS[] = Transaction.values();

    // EGen Drivers
    protected final EGenClientDriver egen_clientDriver;

    private class TPCECallback implements ProcedureCallback {
        private final Transaction t;
        public TPCECallback(Transaction t) {
            this.t = t;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, t.ordinal());
        }
    }
 
    /**
     * @author pavlo
     */
    private static class StatsCallback implements ProcedureCallback {
        public final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            LOG.info("Processing statistics callback information");
            VoltTable[] results = clientResponse.getResults();
            for (VoltTable vt : results) {
                //
                // [0] SITE_ID
                // [1] STAT_TIME
                // [2] TABLE_NAME
                // [3] TABLE_TYPE
                // [4] TABLE_ACTIVE_TUPLE_COUNT
                // [5] TABLE_ALLOCATED_TUPLE_COUNT
                // [6] TABLE_DELETED_TUPLE_COUNT
                //
                while (vt.advanceRow()) {
                    String table_name = vt.getString(2);
                    long count = vt.getLong(4);
                    TableStatistics.addTupleCount(table_name, count);
                } // WHILE
            } // FOR
            LOG.info("Finished collecting statistic. Number of tables = " + TableStatistics.getTables().size());
            latch.countDown();
        }
    }

    private static enum Transaction {
        BROKER_VOLUME("Broker Volume", "BrokerVolume", TPCEConstants.FREQUENCY_BROKER_VOLUME),

        CUSTOMER_POSITION("Customer Position", "CustomerPosition", TPCEConstants.FREQUENCY_CUSTOMER_POSITION),
//TODO not supported transaction
//        MARKET_FEED("Market Feed", "MarketFeed", TPCEConstants.FREQUENCY_MARKET_FEED),

        MARKET_WATCH("Market Watch", "MarketWatch", TPCEConstants.FREQUENCY_MARKET_WATCH),

        SECURITY_DETAIL("Security Detail", "SecurityDetail", TPCEConstants.FREQUENCY_SECURITY_DETAIL),

        TRADE_LOOKUP("Trade Lookup", "TradeLookup", TPCEConstants.FREQUENCY_TRADE_LOOKUP),

        TRADE_ORDER("Trade Order", "TradeOrder", TPCEConstants.FREQUENCY_TRADE_ORDER),

 //       TRADE_RESULT("Trade Result", "TradeResult", TPCEConstants.FREQUENCY_TRADE_RESULT),

        TRADE_STATUS("Trade Status", "TradeStatus", TPCEConstants.FREQUENCY_TRADE_STATUS),

        TRADE_UPDATE("Trade Update", "TradeUpdate", TPCEConstants.FREQUENCY_TRADE_UPDATE);

        private Transaction(String displayName, String callName, int weight) {
            this.displayName = displayName;
            this.callName = callName;
            this.weight = weight;
        }

        public final String displayName;
        public final String callName;
        public final int weight;
    }

    /**
     * Constructor
     *
     * @param args
     */
    public TPCEClient(String[] args) {

        super(args);
        if (!m_extraParams.containsKey("TPCE_LOADER_FILES")) {
            LOG.error("Unable to start benchmark. Missing '" + "TPCE_LOADER_FILES" + "' parameter");
            System.exit(1);
        }
        
        int total_customers = TPCEConstants.DEFAULT_NUM_CUSTOMERS;
        if (m_extraParams.containsKey("TPCE_TOTAL_CUSTOMERS")) {
            total_customers = Integer.valueOf(m_extraParams.get("TPCE_TOTAL_CUSTOMERS"));
        }
        
        int scale_factor = TPCEConstants.DEFAULT_SCALE_FACTOR;
        if (m_extraParams.containsKey("TPCE_SCALE_FACTOR")) {
            scale_factor = Integer.valueOf(m_extraParams.get("TPCE_SCALE_FACTOR"));
        }
        
        int initial_days = TPCEConstants.DEFAULT_INITIAL_DAYS;
        if (m_extraParams.containsKey("TPCE_INITIAL_DAYS")) {
            initial_days = Integer.valueOf(m_extraParams.get("TPCE_INITIAL_DAYS"));
        }

        this.egen_clientDriver = new EGenClientDriver(m_extraParams.get("TPCE_LOADER_FILES"), total_customers, scale_factor, initial_days);
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[XTRANS.length];
        int i = 0;
        for (Transaction transaction : XTRANS) {
            names[i++] = transaction.displayName;
        }
        return names;
    }

    protected Transaction selectTransaction() {
        int iTxnType = egen_clientDriver.driver_ptr.getCE().getCETxnMixGenerator().generateNextTxnType( );
        egen_clientDriver.driver_ptr.getCE().zeroInputBuffer(iTxnType);
        //      return Transaction.TRADE_UPDATE;
        return XTRANS[iTxnType];
    }

    /**
     * Main control loop
     */
    @Override
    public void runLoop() {
        int no_connection = 10000;
        try {
            final Transaction target = selectTransaction();

            LOG.debug("Executing txn " + target);

            while (!this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target))) {
                this.getClientHandle().backpressureBarrier();
            }
        } catch (NoConnectionsException e) {
            if (no_connection % 1000 == 0)
                System.err.println("No connections...");
            if (no_connection-- <= 0) {
                System.err.println("Screw this! I'm leaving! Thanks for nothing!");
                return;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        boolean ret = false;
        try {
            final Transaction target = selectTransaction();

            LOG.debug("Executing txn " + target);

            ret = this.getClientHandle().callProcedure(new TPCECallback(target), target.callName, this.generateClientArgs(target));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }

        return ret;
    }


    /**
     * For a given transaction type, use the EGenClientDriver to generate input
     * parameters for execution
     *
     * @param xact
     * @return
     */
    private Object[] generateClientArgs(Transaction xact) {
        switch (xact) {
            case BROKER_VOLUME:
                return (this.egen_clientDriver.getBrokerVolumeParams());
            case CUSTOMER_POSITION:
                return (this.egen_clientDriver.getCustomerPositionParams());
            case MARKET_WATCH:
                return (this.egen_clientDriver.getMarketWatchParams());
            case SECURITY_DETAIL:
                return (this.egen_clientDriver.getSecurityDetailParams());
            case TRADE_LOOKUP:
                return (this.egen_clientDriver.getTradeLookupParams());
            case TRADE_ORDER:
                return (this.egen_clientDriver.getTradeOrderParams());
            case TRADE_STATUS:
                return (this.egen_clientDriver.getTradeStatusParams());
            case TRADE_UPDATE:
                return (this.egen_clientDriver.getTradeUpdateParams());
            default:
                LOG.error("Unsupported client transaction: " + xact);
        } // SWITCH
        return (null);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        BenchmarkComponent.main(TPCEClient.class, args, false);
    }
}
