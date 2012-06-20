package edu.brown.benchmark.tpce.generators;

public abstract class CESUTInterface {
    public abstract boolean BrokerVolume( TBrokerVolumeTxnInput pTxnInput ) ;                                               
    public abstract boolean CustomerPosition( TCustomerPositionTxnInput pTxnInput ) ;                                       
    public abstract boolean MarketWatch( TMarketWatchTxnInput pTxnInput ) ;                                                 
    public abstract boolean SecurityDetail( TSecurityDetailTxnInput pTxnInput ) ;                                           
    public abstract boolean TradeLookup( TTradeLookupTxnInput pTxnInput ) ;                                                 
    public abstract boolean TradeOrder( TTradeOrderTxnInput pTxnInput, int iTradeType, boolean bExecutorIsAccountOwner ) ;   
    public abstract boolean TradeStatus( TTradeStatusTxnInput pTxnInput ) ;                                                 
    public abstract boolean TradeUpdate( TTradeUpdateTxnInput pTxnInput ) ;                                                 
}
