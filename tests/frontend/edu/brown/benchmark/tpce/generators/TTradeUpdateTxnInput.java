package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;

import edu.brown.benchmark.tpce.TPCEConstants;

public class TTradeUpdateTxnInput {
	long[]              trade_id;
    long              acct_id;
    long              max_acct_id;
    int               frame_to_execute;                   // which of the frames to execute
    int               max_trades;
    int               max_updates;
    Date    end_trade_dts;
    Date    start_trade_dts;
    char[]                symbol;
    
    public TTradeUpdateTxnInput(){
    	trade_id = new long [TPCEConstants.TradeUpdateFrame1MaxRows];
    	symbol = new char[TableConsts.cSYMBOL_len];
    }
    
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(trade_id));
    	para.add(String.valueOf(acct_id));
    	para.add(String.valueOf(max_acct_id));
    	para.add(String.valueOf(frame_to_execute));
    	para.add(String.valueOf(max_trades));
    	para.add(String.valueOf(max_updates));
    	para.add(end_trade_dts.toString());
    	para.add(start_trade_dts.toString());
    	para.add(String.valueOf(symbol));
    	return para;
    }
}
