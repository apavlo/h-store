package edu.brown.benchmark.tpceb;

import edu.brown.benchmark.tpceb.generators.CESUTInterface;
import edu.brown.benchmark.tpceb.generators.TMarketWatchTxnInput;
import edu.brown.benchmark.tpceb.generators.TSecurityDetailTxnInput;
import edu.brown.benchmark.tpceb.generators.TTradeOrderTxnInput;


public class SUT extends CESUTInterface{

    public  boolean TradeOrder( TTradeOrderTxnInput pTxnInput, int iTradeType){
        return true;
    }
} 
