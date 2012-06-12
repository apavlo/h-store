package edu.brown.benchmark.tpce.generators;

public class TTickerEntry {
    public double              price_quote;
    public int               trade_qty;
    public String                symbol;
    public TTickerEntry(){
        symbol = new String();
    }
}
