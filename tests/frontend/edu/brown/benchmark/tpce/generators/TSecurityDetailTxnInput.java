package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;

public class TSecurityDetailTxnInput {
	public int                   max_rows_to_return;
    public boolean               access_lob_flag;
    public Date                  start_day;
    public char[]                symbol;
    public TSecurityDetailTxnInput(){
    	symbol = new char[TableConsts.cSYMBOL_len];
    	start_day = new Date();
    }
    
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(max_rows_to_return));
    	para.add(String.valueOf(access_lob_flag));
    	para.add(start_day.toString());
    	para.add(String.valueOf(symbol));
    	return para;
    }
}
