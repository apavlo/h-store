package edu.brown.benchmark.tpce.generators;

public class DataMaintenanceCallback extends DMSUTInterface{
	
       public DataMaintenanceCallback(TDataMaintenanceTxnInput dmTxnInput, TTradeCleanupTxnInput tcTxnInput){
    	   m_DataMaintenanceTxnInput = dmTxnInput;
           m_TradeCleanupTxnInput = tcTxnInput;
       }
    
        public boolean DataMaintenance( TDataMaintenanceTxnInput pTxnInput ) {    
        	
            System.out.println("DataMaintenance callback!\n");
            m_DataMaintenanceTxnInput.acct_id = pTxnInput.acct_id;
            m_DataMaintenanceTxnInput.c_id = pTxnInput.c_id;
            m_DataMaintenanceTxnInput.co_id = pTxnInput.co_id;
            m_DataMaintenanceTxnInput.day_of_month = pTxnInput.day_of_month;
            m_DataMaintenanceTxnInput.vol_incr = pTxnInput.vol_incr;
            System.arraycopy(pTxnInput.symbol, 0, m_DataMaintenanceTxnInput.symbol, 0, TableConsts.cSYMBOL_len);
            System.arraycopy(pTxnInput.table_name, 0, m_DataMaintenanceTxnInput.symbol, 0, TxnHarnessStructs.max_table_name);
            System.arraycopy(pTxnInput.tx_id, 0, m_DataMaintenanceTxnInput.tx_id, 0, TableConsts.cTX_ID_len);
            return true;
        }
        
        public boolean TradeCleanup( TTradeCleanupTxnInput pTxnInput ) {
        	System.out.println("DataMaintenance callback!\n");
            
            m_TradeCleanupTxnInput.start_trade_id = pTxnInput.start_trade_id;
            System.arraycopy(pTxnInput.st_canceled_id, 0, m_TradeCleanupTxnInput.st_canceled_id, 0, TableConsts.cST_ID_len);
            System.arraycopy(pTxnInput.st_pending_id, 0, m_TradeCleanupTxnInput.st_pending_id, 0, TableConsts.cST_ID_len);
            System.arraycopy(pTxnInput.st_submitted_id, 0, m_TradeCleanupTxnInput.st_submitted_id, 0, TableConsts.cST_ID_len);
            
            return true;
        }
        
    private TDataMaintenanceTxnInput m_DataMaintenanceTxnInput;
    private TTradeCleanupTxnInput    m_TradeCleanupTxnInput;
}
