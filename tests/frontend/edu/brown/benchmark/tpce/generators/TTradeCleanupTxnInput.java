package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeCleanupTxnInput {
	 long                start_trade_id;
	 char[]              st_canceled_id;
	 char[]              st_pending_id;
	 char[]              st_submitted_id;
	 
	 public TTradeCleanupTxnInput(){
		 st_canceled_id = new char [TableConsts.cST_ID_len];
		 st_pending_id = new char [TableConsts.cST_ID_len];
		 st_submitted_id = new char [TableConsts.cST_ID_len];
	 }
	 
	 public void setZero(){
		 start_trade_id = 0;
		 st_canceled_id[0] = '\0';
		 st_pending_id[0] = '\0';
		 st_submitted_id[0] = '\0';
	 }
	 
	 public ArrayList<String>InputParameters(){
	    	ArrayList<String> para = new ArrayList<String>();
	    	para.add(String.valueOf(start_trade_id));
	    	para.add(String.valueOf(st_canceled_id));
	    	para.add(String.valueOf(st_pending_id));
	    	para.add(String.valueOf(st_submitted_id));
	    	return para;
	    }
}
