package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeStatusTxnInput {
	long              acct_id;
	public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(acct_id));
    	return para;
    }
}
