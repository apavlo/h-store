package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;

public class TMarketWatchTxnInput {
	long              acct_id;
    long              c_id;
    long              ending_co_id;
    long              starting_co_id;
    Date    	      start_day;
    char[]            industry_name;
    
    public TMarketWatchTxnInput(){
    	industry_name = new char[TableConsts.cIN_NAME_len];
    	start_day = new Date();
    }
    
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(acct_id));
    	para.add(String.valueOf(c_id));
    	para.add(String.valueOf(ending_co_id));
    	para.add(String.valueOf(starting_co_id));
    	para.add(start_day.toString());
    	para.add(String.valueOf(industry_name));
    	return para;
    }
}
