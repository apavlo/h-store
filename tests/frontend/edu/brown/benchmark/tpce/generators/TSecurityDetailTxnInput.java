package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;
import org.voltdb.types.*;

public class TSecurityDetailTxnInput {
	public int                   max_rows_to_return;
    public boolean               access_lob_flag;
    public TimestampType                  start_day;
    public String                symbol;
    public TSecurityDetailTxnInput(){
    	symbol = new String();
    	start_day = new TimestampType();
    }
    
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(max_rows_to_return);
    	para.add(access_lob_flag);
    	para.add(start_day);
    	para.add(symbol);
    	return para;
    }
}
