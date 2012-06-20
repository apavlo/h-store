package edu.brown.benchmark.tpce.generators;

public class DataMaintenanceCallback extends DMSUTInterface{
    
    public DataMaintenanceCallback(TDataMaintenanceTxnInput dmTxnInput, TTradeCleanupTxnInput tcTxnInput){
        dataMaintenanceTxnInput = dmTxnInput;
        tradeCleanupTxnInput = tcTxnInput;
    }
    
    public boolean DataMaintenance( TDataMaintenanceTxnInput txnInput ) {    
        dataMaintenanceTxnInput.setAcctId(txnInput.getAcctId());
        dataMaintenanceTxnInput.setCId(txnInput.getCId());
        dataMaintenanceTxnInput.setCoId(txnInput.getCoId());
        dataMaintenanceTxnInput.setDayOfMonth(txnInput.getDayOfMonth());
        dataMaintenanceTxnInput.setVolIncr(txnInput.getVolIncr());
        dataMaintenanceTxnInput.setSymbol(txnInput.getSymbol());
        dataMaintenanceTxnInput.setTableName(txnInput.getTableName());
        dataMaintenanceTxnInput.setTxId(txnInput.getTxId());
        return true;
    }
        
    public boolean TradeCleanup( TTradeCleanupTxnInput txnInput ) {
        tradeCleanupTxnInput.start_trade_id = txnInput.start_trade_id;
        System.arraycopy(txnInput.st_canceled_id, 0, tradeCleanupTxnInput.st_canceled_id, 0, TableConsts.cST_ID_len);
        System.arraycopy(txnInput.st_pending_id, 0, tradeCleanupTxnInput.st_pending_id, 0, TableConsts.cST_ID_len);
        System.arraycopy(txnInput.st_submitted_id, 0, tradeCleanupTxnInput.st_submitted_id, 0, TableConsts.cST_ID_len);
        return true;
        }
        
    private TDataMaintenanceTxnInput dataMaintenanceTxnInput;
    private TTradeCleanupTxnInput    tradeCleanupTxnInput;
}
