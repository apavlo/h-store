package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TCustomerPositionTxnInput {
	public long acct_id_idx;
	public long cust_id;
	public boolean get_history;
	public String tax_id;
	public TCustomerPositionTxnInput(){
		tax_id = new String();;
    }
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(new Long(acct_id_idx));
    	para.add(new Long(cust_id));
    	para.add(new Boolean(get_history));
    	
/*    	Object[] obj = new Object[tax_id.length];
    	for (int i = 0; i < tax_id.length; i ++){
    		obj[i] = Array.get(tax_id, i);
    	}
    	para.add(obj);*/
    	para.add(tax_id);
    	
    	return para;
    }
}
