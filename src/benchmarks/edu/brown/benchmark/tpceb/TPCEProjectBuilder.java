package edu.brown.benchmark.tpceb;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.procedures.LoadTable;

import edu.brown.benchmark.tpceb.procedures.TradeResult;
import edu.brown.benchmark.tpceb.procedures.MarketFeed;
import edu.brown.benchmark.tpceb.procedures.MarketWatch;
import edu.brown.benchmark.tpceb.procedures.TradeOrder;



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
       TradeResult.class,
        MarketFeed.class,
      //  MarketWatch.class,
        TradeOrder.class
    };

    // Transaction Frequencies
    {
        addTransactionFrequency(MarketFeed.class, TPCEConstants.FREQUENCY_MARKET_FEED);
       // addTransactionFrequency(MarketWatch.class, TPCEConstants.FREQUENCY_MARKET_WATCH);
        addTransactionFrequency(TradeResult.class, TPCEConstants.FREQUENCY_TRADE_RESULT);
        addTransactionFrequency(TradeOrder.class, TPCEConstants.FREQUENCY_TRADE_ORDER);
    }

    public static String PARTITIONING[][] = new String[][] {
    // TODO(pavlo)
    /*{ TPCEConstants.TABLENAME_TRADE, "T_CA_ID" }, */};

    public TPCEProjectBuilder() {
        super("tpceb", TPCEProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}