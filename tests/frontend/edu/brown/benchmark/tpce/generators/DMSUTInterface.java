package edu.brown.benchmark.tpce.generators;

public abstract class DMSUTInterface {
	public abstract boolean DataMaintenance( TDataMaintenanceTxnInput pTxnInput ); // return whether it was successful
    public abstract boolean TradeCleanup( TTradeCleanupTxnInput pTxnInput );   // return whether it was successful
}
