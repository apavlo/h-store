package edu.brown.benchmark.tpce.generators;

public abstract class DMSUTInterface {
    public abstract boolean DataMaintenance( TDataMaintenanceTxnInput pTxnInput );
    public abstract boolean TradeCleanup( TTradeCleanupTxnInput pTxnInput ); 
}
