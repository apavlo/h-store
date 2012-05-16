package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeOrderTxnInput {

	public double          requested_price;
	public long          	acct_id;
	public boolean           	is_lifo;
	public boolean           	roll_it_back;
	public int           	trade_qty;
	public boolean           	type_is_margin;
	public char[]          co_name;
	public char[]          exec_f_name;
	public char[]          exec_l_name;
	public char[]          exec_tax_id;
	public char[]          issue;
	public char[]          st_pending_id;
	public char[]          st_submitted_id;
	public char[]          symbol;
	public char[]          trade_type_id;
    
    public TTradeOrderTxnInput(){
    	co_name = new char[TableConsts.cCO_NAME_len];
    	exec_f_name = new char[TableConsts.cF_NAME_len];
    	exec_l_name = new char[TableConsts.cL_NAME_len];
    	exec_tax_id = new char[TableConsts.cTAX_ID_len];
    	issue = new char[TableConsts.cS_ISSUE_len];
    	st_pending_id = new char[TableConsts.cST_ID_len];
    	st_submitted_id = new char[TableConsts.cST_ID_len];
    	symbol = new char[TableConsts.cSYMBOL_len];
    	trade_type_id = new char[TableConsts.cTT_ID_len];
    }
    
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	para.add(String.valueOf(requested_price));
    	para.add(String.valueOf(acct_id));
    	para.add(String.valueOf(is_lifo));
    	para.add(String.valueOf(roll_it_back));
    	para.add(String.valueOf(trade_qty));
    	para.add(co_name.toString());
    	para.add(exec_f_name.toString());
    	para.add(exec_l_name.toString());
    	para.add(exec_tax_id.toString());
    	para.add(issue.toString());
    	para.add(st_pending_id.toString());
    	para.add(st_submitted_id.toString());
    	para.add(trade_type_id.toString());
    	para.add(String.valueOf(symbol));
    	return para;
    }
}
