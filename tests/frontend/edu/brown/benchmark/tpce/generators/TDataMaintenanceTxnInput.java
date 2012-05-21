package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TDataMaintenanceTxnInput {
	long  acct_id;
    long  c_id;
    long  co_id;
    int   day_of_month;
    int   vol_incr;
    String    symbol;
    String    table_name;
    String    tx_id;
    
    public TDataMaintenanceTxnInput(){
    	symbol = new String();
    	table_name = new String();
    	tx_id = new String();
    }
    public void setZero(){
    	acct_id = 0;
        c_id = 0;
        co_id = 0;
        day_of_month = 0;
        vol_incr = 0;
        
    }
    
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(acct_id);
    	para.add(c_id);
    	para.add(co_id);
    	para.add(day_of_month);
    	para.add(vol_incr);
    	para.add(symbol);
    	para.add(table_name);
    	para.add(tx_id);
    	return para;
    }
}
