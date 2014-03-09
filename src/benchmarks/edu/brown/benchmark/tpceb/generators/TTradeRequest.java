package edu.brown.benchmark.tpceb.generators;

import edu.brown.benchmark.tpceb.TPCEConstants.eMEETradeRequestAction;

public class TTradeRequest {
    public double              price_quote;
    public long              trade_id;
    public int               trade_qty;
   // public eMEETradeRequestAction      eAction;
    public int      eActionTemp;
    public String                symbol;
    public String                trade_type_id;
    public eMEETradeRequestAction      eAction;

    public TTradeRequest(){
        symbol = new String();
        trade_type_id = new String();
    }
}
