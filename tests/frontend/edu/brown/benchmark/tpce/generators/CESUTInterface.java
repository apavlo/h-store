package edu.brown.benchmark.tpce.generators;

public abstract class CESUTInterface {
	public abstract boolean BrokerVolume( TBrokerVolumeTxnInput pTxnInput ) ;                                               // return whether it was successful
    public abstract boolean CustomerPosition( TCustomerPositionTxnInput pTxnInput ) ;                                       // return whether it was successful
    public abstract boolean MarketWatch( TMarketWatchTxnInput pTxnInput ) ;                                                 // return whether it was successful
    public abstract boolean SecurityDetail( TSecurityDetailTxnInput pTxnInput ) ;                                           // return whether it was successful
    public abstract boolean TradeLookup( TTradeLookupTxnInput pTxnInput ) ;                                                 // return whether it was successful
    public abstract boolean TradeOrder( TTradeOrderTxnInput pTxnInput, int iTradeType, boolean bExecutorIsAccountOwner ) ;   // return whether it was successful
    public abstract boolean TradeStatus( TTradeStatusTxnInput pTxnInput ) ;                                                 // return whether it was successful
    public abstract boolean TradeUpdate( TTradeUpdateTxnInput pTxnInput ) ;                                                 // return whether it was successful
}
