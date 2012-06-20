package edu.brown.benchmark.tpce.generators;

public class TStatusAndTradeType {
	public String    status_submitted;
	public String    type_limit_buy;
	public String    type_limit_sell;
	public String    type_stop_loss;
    
    public TStatusAndTradeType(){
    	status_submitted = new String();
    	type_limit_buy = new String();
    	type_limit_sell = new String();
    	type_stop_loss = new String();
    }
}
 