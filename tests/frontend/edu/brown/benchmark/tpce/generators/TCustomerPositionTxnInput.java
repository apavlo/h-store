package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TCustomerPositionTxnInput {
	public long acct_id_idx;
	public long cust_id;
	public boolean get_history;
	public char[] tax_id;
	public TCustomerPositionTxnInput(){
		tax_id = new char [TableConsts.cTAX_ID_len];;
    }
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(acct_id_idx));
    	para.add(String.valueOf(cust_id));
    	para.add(String.valueOf(get_history));
    	para.add(String.valueOf(tax_id));
    	
    	return para;
    }
}
