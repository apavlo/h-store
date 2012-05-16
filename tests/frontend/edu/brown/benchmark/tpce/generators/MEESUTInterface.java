package edu.brown.benchmark.tpce.generators;

public abstract class MEESUTInterface {
	public abstract boolean TradeResult( TTradeResultTxnInput pTxnInput ); // return whether it was successful
    public abstract boolean MarketFeed( TMarketFeedTxnInput pTxnInput );   // return whether it was successful
}
