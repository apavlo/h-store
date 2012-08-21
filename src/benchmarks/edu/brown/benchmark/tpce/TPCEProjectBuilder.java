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

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tpce.procedures.BrokerVolume;
import edu.brown.benchmark.tpce.procedures.CustomerPosition;
import edu.brown.benchmark.tpce.procedures.DataMaintenance;
import edu.brown.benchmark.tpce.procedures.LoadTable;
import edu.brown.benchmark.tpce.procedures.MarketFeed;
import edu.brown.benchmark.tpce.procedures.MarketWatch;
import edu.brown.benchmark.tpce.procedures.SecurityDetail;
import edu.brown.benchmark.tpce.procedures.TradeCleanup;
import edu.brown.benchmark.tpce.procedures.TradeLookup;
import edu.brown.benchmark.tpce.procedures.TradeOrder;
import edu.brown.benchmark.tpce.procedures.TradeResult;
import edu.brown.benchmark.tpce.procedures.TradeStatus;
import edu.brown.benchmark.tpce.procedures.TradeUpdate;

public class TPCEProjectBuilder extends AbstractProjectBuilder {

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_clientClass = TPCEClient.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = TPCELoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        TradeCleanup.class,
        DataMaintenance.class,
        TradeOrder.class,
        TradeResult.class,
        TradeLookup.class,
        TradeUpdate.class,
        TradeStatus.class,
        CustomerPosition.class,
        BrokerVolume.class,
        SecurityDetail.class,
        MarketFeed.class,
        MarketWatch.class,
        LoadTable.class
    };

    // Transaction Frequencies
    {
        addTransactionFrequency(BrokerVolume.class, TPCEConstants.FREQUENCY_BROKER_VOLUME);
        addTransactionFrequency(CustomerPosition.class, TPCEConstants.FREQUENCY_CUSTOMER_POSITION);
        addTransactionFrequency(MarketFeed.class, TPCEConstants.FREQUENCY_MARKET_FEED);
        addTransactionFrequency(MarketWatch.class, TPCEConstants.FREQUENCY_MARKET_WATCH);
        addTransactionFrequency(SecurityDetail.class, TPCEConstants.FREQUENCY_SECURITY_DETAIL);
        addTransactionFrequency(TradeLookup.class, TPCEConstants.FREQUENCY_TRADE_LOOKUP);
        addTransactionFrequency(TradeOrder.class, TPCEConstants.FREQUENCY_TRADE_ORDER);
        addTransactionFrequency(TradeResult.class, TPCEConstants.FREQUENCY_TRADE_RESULT);
        addTransactionFrequency(TradeStatus.class, TPCEConstants.FREQUENCY_TRADE_STATUS);
        addTransactionFrequency(TradeUpdate.class, TPCEConstants.FREQUENCY_TRADE_UPDATE);
        addTransactionFrequency(DataMaintenance.class, 0); // TPCEConstants.FREQUENCY_DATA_MAINTENANCE);
        addTransactionFrequency(TradeCleanup.class, 0); // TPCEConstants.FREQUENCY_TRADE_CLEANUP);
    }

    public static String PARTITIONING[][] = new String[][] {
    // TODO(pavlo)
    /*{ TPCEConstants.TABLENAME_TRADE, "T_CA_ID" }, */};

    public TPCEProjectBuilder() {
        super("tpce", TPCEProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}