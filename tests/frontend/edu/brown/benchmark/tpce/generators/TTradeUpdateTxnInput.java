package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;

import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;

public class TTradeUpdateTxnInput {
	long[]              trade_id;
    long              acct_id;
    long              max_acct_id;
    int               frame_to_execute;                   // which of the frames to execute
    int               max_trades;
    int               max_updates;
    TimestampType    end_trade_dts;
    TimestampType    start_trade_dts;
    String                symbol;
    
    public TTradeUpdateTxnInput(){
    	trade_id = new long [TPCEConstants.TradeUpdateFrame1MaxRows];
    	symbol = new String();
    	start_trade_dts = new TimestampType(new GregorianCalendar(0,0,0,0,0,0).getTime());
        end_trade_dts = new TimestampType(new GregorianCalendar(0,0,0,0,0,0).getTime());
    }
    
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(trade_id);
    	para.add(acct_id);
    	para.add(max_acct_id);
    	para.add(frame_to_execute);
    	para.add(max_trades);
    	para.add(max_updates);
    	para.add(end_trade_dts);
    	para.add(start_trade_dts);
    	para.add(symbol);
    	return para;
    }
}
