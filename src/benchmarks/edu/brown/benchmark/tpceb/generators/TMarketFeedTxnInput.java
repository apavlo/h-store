package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;

public class TMarketFeedTxnInput {
    public TStatusAndTradeType   StatusAndTradeType;
    public char[]                zz_padding;
    public TTickerEntry[]        Entries;
    
    public TMarketFeedTxnInput(){
        StatusAndTradeType = new TStatusAndTradeType();
        zz_padding = new char[4];
        
        Entries = new TTickerEntry[TxnHarnessStructs.max_feed_len];
        price_quotes = new double[Entries.length];
        symbols = new String[Entries.length];
        trade_qtys = new long[Entries.length];
   
    }
    

    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
       for(int i =0; i < Entries.length; i++){
           price_quotes[i] = Entries[i].price_quote;
           symbols[i] = Entries[i].symbol;
          trade_qtys[i] = (long) Entries[i].trade_qty;
       }

        status_submitted =  StatusAndTradeType.status_submitted;
       type_limit_buy =  StatusAndTradeType.type_limit_buy;
       type_limit_sell =  StatusAndTradeType.type_limit_sell;
       type_stop_loss = StatusAndTradeType.type_stop_loss;
        para.add(price_quotes);
        para.add(status_submitted);
        para.add(symbols);
        para.add(trade_qtys);
        para.add( type_limit_buy );
        para.add( type_limit_sell);
        para.add( type_stop_loss);
 
       
        return para;
    }
  
    
    private double[]     price_quotes;
    private String       status_submitted;
    private String[]     symbols;
    private long[]       trade_qtys;
    private String       type_limit_buy;
    private String       type_limit_sell;
    private String       type_stop_loss;
}
