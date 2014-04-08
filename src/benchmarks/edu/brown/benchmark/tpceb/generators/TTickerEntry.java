package edu.brown.benchmark.tpceb.generators;

public class TTickerEntry {
    public double              price_quote;
    public int               trade_qty;
    public String                symbol;
    public TTickerEntry(){
       // System.out.println("creating a new instance");
        symbol = new String();
       // System.out.println("created instance");
    }
}
