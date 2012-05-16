package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeResultTxnInput {
	 public double      trade_price;
	 public long      trade_id;
	 
	 public ArrayList<String>InputParameters(){
	    	ArrayList<String> para = new ArrayList<String>();
	    	para.add(String.valueOf(trade_price));
	    	para.add(String.valueOf(trade_id));
	    	return para;
	    }
}
