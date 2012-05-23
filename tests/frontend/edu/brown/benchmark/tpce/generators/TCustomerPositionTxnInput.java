package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TCustomerPositionTxnInput {
	public long acct_id_idx;
	public long cust_id;
	public long get_history;
	public String tax_id;
	public TCustomerPositionTxnInput(){
		tax_id = new String();
    }
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(acct_id_idx);
    	para.add(cust_id);
    	para.add(get_history);
    	
/*    	Object[] obj = new Object[tax_id.length];
    	for (int i = 0; i < tax_id.length; i ++){
    		obj[i] = Array.get(tax_id, i);
    	}
    	para.add(obj);*/
    	para.add(tax_id);
    	
    	return para;
    }
}
