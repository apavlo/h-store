package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeOrderTxnInput {

	public double          requested_price;
	public long          	acct_id;
	public long           	is_lifo;
	public long           	roll_it_back;
	public long           	trade_qty;
	public long           	type_is_margin;
	public String          co_name;
	public String          exec_f_name;
	public String          exec_l_name;
	public String          exec_tax_id;
	public String          issue;
	public String          st_pending_id;
	public String          st_submitted_id;
	public String          symbol;
	public String          trade_type_id;
    
    public TTradeOrderTxnInput(){
    	co_name = new String()/*char[TableConsts.cCO_NAME_len]*/;
    	exec_f_name = new String()/*char[TableConsts.cF_NAME_len]*/;
    	exec_l_name = new String()/*char[TableConsts.cL_NAME_len]*/;
    	exec_tax_id = new String()/*char[TableConsts.cTAX_ID_len]*/;
    	issue = new String()/*char[TableConsts.cS_ISSUE_len]*/;
    	st_pending_id = new String()/*char[TableConsts.cST_ID_len]*/;
    	st_submitted_id = new String()/*char[TableConsts.cST_ID_len]*/;
    	symbol = new String()/*char[TableConsts.cSYMBOL_len]*/;
    	trade_type_id = new String()/*char[TableConsts.cTT_ID_len]*/;
    }
    
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	para.add(requested_price);
    	para.add(acct_id);
    	para.add(is_lifo);
    	para.add(roll_it_back);
    	para.add(trade_qty);
    	para.add(type_is_margin);
    	para.add(co_name);
    	para.add(exec_f_name);
    	para.add(exec_l_name);
    	para.add(exec_tax_id);
    	para.add(issue);
    	para.add(st_pending_id);
    	para.add(st_submitted_id);
    	para.add(trade_type_id);
    	para.add(symbol);
    	return para;
    }
}
