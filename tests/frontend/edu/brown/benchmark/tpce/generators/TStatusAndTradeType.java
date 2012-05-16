package edu.brown.benchmark.tpce.generators;

public class TStatusAndTradeType {
	public char[]    status_submitted;
	public char[]    type_limit_buy;
	public char[]    type_limit_sell;
	public char[]    type_stop_loss;
    
    public TStatusAndTradeType(){
    	status_submitted = new char[TableConsts.cST_ID_len];
    	type_limit_buy = new char[TableConsts.cTT_ID_len];
    	type_limit_sell = new char[TableConsts.cTT_ID_len];
    	type_stop_loss = new char[TableConsts.cTT_ID_len];
    }
}
