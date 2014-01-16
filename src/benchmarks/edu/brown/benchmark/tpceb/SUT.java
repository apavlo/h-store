package edu.brown.benchmark.tpceb;

import edu.brown.benchmark.tpceb.generators.CESUTInterface;
import edu.brown.benchmark.tpceb.generators.TMarketFeedTxnInput;
import edu.brown.benchmark.tpceb.generators.TMarketWatchTxnInput;
import edu.brown.benchmark.tpceb.generators.TSecurityDetailTxnInput;
import edu.brown.benchmark.tpceb.generators.TTradeOrderTxnInput;
import edu.brown.benchmark.tpceb.generators.MEESUTInterface;
import edu.brown.benchmark.tpceb.generators.TTradeResultTxnInput;

public class SUT extends CESUTInterface{
    public  boolean MarketWatch( TMarketWatchTxnInput pTxnInput ){
        return true;
    }
    public  boolean TradeOrder( TTradeOrderTxnInput pTxnInput, int iTradeType){
        return true;
    }
} 
