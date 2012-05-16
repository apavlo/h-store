package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;

import edu.brown.benchmark.tpce.TPCEConstants;

public class TTradeLookupTxnInput {
	public long[]            trade_id;
	public long              acct_id;
	public long              max_acct_id;
	public int               frame_to_execute;           // which of the frames to execute
	public int               max_trades;
	public Date    		  end_trade_dts;
	public Date    		  start_trade_dts;
	public char[]            symbol;
    
    public TTradeLookupTxnInput(){
    	trade_id = new long [TPCEConstants.TradeLookupFrame1MaxRows];
    	symbol = new char[TableConsts.cSYMBOL_len];
    }
    
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(trade_id));
    	para.add(String.valueOf(acct_id));
    	para.add(String.valueOf(max_acct_id));
    	para.add(String.valueOf(frame_to_execute));
    	para.add(String.valueOf(max_trades));
    	para.add(end_trade_dts.toString());
    	para.add(start_trade_dts.toString());
    	para.add(String.valueOf(symbol));
    	return para;
    }
}
