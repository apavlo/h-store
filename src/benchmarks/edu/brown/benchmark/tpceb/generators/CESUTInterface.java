package edu.brown.benchmark.tpceb.generators;


public abstract class CESUTInterface {  
    public abstract boolean MarketWatch( TMarketWatchTxnInput pTxnInput ) ;     
    public abstract boolean TradeOrder( TTradeOrderTxnInput pTxnInput, int iTradeType) ;                                                  
}
