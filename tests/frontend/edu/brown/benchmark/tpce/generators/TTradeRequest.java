package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.TPCEConstants.eMEETradeRequestAction;

public class TTradeRequest {
    public double              price_quote;
    public long              trade_id;
    public int               trade_qty;
    public eMEETradeRequestAction      eAction;
    public String                symbol;
    public String                trade_type_id;

    public TTradeRequest(){
        symbol = new String();
        trade_type_id = new String();
    }
}
