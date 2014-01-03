package edu.brown.benchmark.tpceb.generators;

public abstract class MEESUTInterface {
    public abstract boolean TradeResult( TTradeResultTxnInput pTxnInput );  
    public abstract boolean MarketFeed( TMarketFeedTxnInput pTxnInput );    
}
