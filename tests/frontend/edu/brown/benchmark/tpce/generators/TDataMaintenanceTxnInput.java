package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TDataMaintenanceTxnInput {
	long  acct_id;
    long  c_id;
    long  co_id;
    int   day_of_month;
    int   vol_incr;
    char[]    symbol;
    char[]    table_name;
    char[]    tx_id;
    
    public TDataMaintenanceTxnInput(){
    	symbol = new char[TableConsts.cSYMBOL_len];
    	table_name = new char[TxnHarnessStructs.max_table_name];
    	tx_id = new char[TableConsts.cTX_ID_len];
    }
    public void setZero(){
    	acct_id = 0;
        c_id = 0;
        co_id = 0;
        day_of_month = 0;
        vol_incr = 0;
        symbol[0] = '\0';
        table_name[0] = '\0';
        tx_id[0] = '\0';
    }
    
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(acct_id));
    	para.add(String.valueOf(c_id));
    	para.add(String.valueOf(co_id));
    	para.add(String.valueOf(day_of_month));
    	para.add(String.valueOf(vol_incr));
    	para.add(String.valueOf(symbol));
    	para.add(String.valueOf(table_name));
    	para.add(String.valueOf(tx_id));
    	return para;
    }
}
