package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;

import edu.brown.benchmark.tpce.TPCEConstants;
import org.voltdb.types.*;

public class TTradeLookupTxnInput {
	public long[]            trade_id;
	public long              acct_id;
	public long              max_acct_id;
	public int               frame_to_execute;           // which of the frames to execute
	public int               max_trades;
	public TimestampType    		  end_trade_dts;
	public TimestampType    		  start_trade_dts;
	public String            symbol;
    
    public TTradeLookupTxnInput(){
    	trade_id = new long [TPCEConstants.TradeLookupFrame1MaxRows];
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
    	para.add(end_trade_dts);
    	para.add(start_trade_dts);
    	para.add(symbol);
    	return para;
    }
}
