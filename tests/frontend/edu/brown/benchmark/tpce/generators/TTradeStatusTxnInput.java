package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeStatusTxnInput {
	long              acct_id;
	public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(acct_id);
    	return para;
    }
}
