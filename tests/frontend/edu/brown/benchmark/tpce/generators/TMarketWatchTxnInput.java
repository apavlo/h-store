package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;
import org.voltdb.types.*;

public class TMarketWatchTxnInput {
	long              acct_id;
    long              c_id;
    long              ending_co_id;
    long              starting_co_id;
    TimestampType    	      start_day;
    String            industry_name;
    
    public TMarketWatchTxnInput(){
    	industry_name = new String();
    	start_day = new TimestampType();
    }
    
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(acct_id);
    	para.add(c_id);
    	para.add(ending_co_id);
    	para.add(starting_co_id);
    	para.add(industry_name);
    	return para;
    }
}
