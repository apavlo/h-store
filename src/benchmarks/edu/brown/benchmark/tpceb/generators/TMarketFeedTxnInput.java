package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;

public class TMarketFeedTxnInput {
    public TStatusAndTradeType   StatusAndTradeType;
    public char[]                zz_padding;
    public TTickerEntry[]        Entries;
    
    public TMarketFeedTxnInput(){
        StatusAndTradeType = new TStatusAndTradeType();
        zz_padding = new char[4];
       // System.out.println("getting to entries");
        Entries = new TTickerEntry[TxnHarnessStructs.max_feed_len];
        price_quotes = new double[Entries.length];
        symbols = new String[Entries.length];
        trade_qtys = new long[Entries.length];
       // System.out.println("Entries length" +Entries.length);
      /*  for(int i =0; i < Entries.length; i++){
            System.out.println(Entries.length);
            System.out.println(Entries[i]);
            price_quotes[i] = Entries[i].price_quote;
            symbols[i] = Entries[i].symbol;
            trade_qtys[i] = Entries[i].trade_qty;
        }*/
        
    }
    
    //double[] price_quotes, String status_submitted, String[] symbols, long[] trade_qtys, String type_limit_buy, String type_limit_sell, String type_stop_loss
    /*added in order to complete*/
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
       System.out.println("in here");
       for(int i =0; i < Entries.length; i++){
           System.out.println(Entries.length);
           System.out.println(Entries[i]);
           price_quotes[i] = Entries[i].price_quote;
           symbols[i] = Entries[i].symbol;
           trade_qtys[i] = Entries[i].trade_qty;
       }
       System.out.println("compeleted loop");
        para.add(price_quotes);
        para.add(StatusAndTradeType.status_submitted);
        para.add(symbols);
        para.add(trade_qtys);
        para.add(StatusAndTradeType.type_limit_buy);
        para.add(StatusAndTradeType.type_limit_sell);
        para.add(StatusAndTradeType.type_stop_loss);
    
        return para;
    }
    /*
    public double[] getPriceQuotes(){
        price_quotes = new double[Entries.length];
        for(int i =0; i < Entries.length; i++){
            price_quotes[i] = Entries[i].price_quote;
        }
        return price_quotes;
    }
    
    public String getStatusSubmitted(){
        return StatusAndTradeType.status_submitted;
    }
    
    public String[] getSymbols(){
        symbols = new String[Entries.length];
        for(int i =0; i < Entries.length; i++){
            symbols[i] = Entries[i].symbol;
        }
        return symbols;
    }
    
    public long[] getTadeQuantities(){
        trade_qtys = new long[Entries.length];
        for(int i =0; i < Entries.length; i++){
            symbols[i] = Entries[i].symbol;
        }
        return trade_qtys;
    }
    
    public String getTypeLimitBuy(){
        return StatusAndTradeType.type_limit_buy;
    }
    
    public String getTypeLimitSell(){
        return StatusAndTradeType.type_limit_sell;
    }
    
    public String getTypeStopLoss(){
        return StatusAndTradeType.type_stop_loss;
    }
    
    public void setPriceQuotes(double[] price_quotes){
        price_quotes = new double[Entries.length];
        for(int i =0; i < Entries.length; i++){
            price_quotes[i] = Entries[i].price_quote;
        }
        this.price_quotes = price_quotes;
    }
    
    public void setStatusSubmitted(String status){
        this.StatusAndTradeType.status_submitted = status;
    }
    
    public void setSymbols(String[] symbols){
        symbols = new String[Entries.length];
        for(int i =0; i < Entries.length; i++){
            symbols[i] = Entries[i].symbol;
        }
        this.symbols = symbols;
    }
    
    public void setTadeQuantities(long[] quantities){
        trade_qtys = new long[Entries.length];
        for(int i =0; i < Entries.length; i++){
            symbols[i] = Entries[i].symbol;
        }
        this.trade_qtys = quantities;
    }
    
    public void setTypeLimitBuy(String limitBuy){
        this.StatusAndTradeType.type_limit_buy = limitBuy;
    }
    
    public void setTypeLimitSell(String limitSell){
        this.StatusAndTradeType.type_limit_sell = limitSell;
    }
    
    public void setTypeStopLoss(String stopLoss){
        this.StatusAndTradeType.type_stop_loss = stopLoss;
    }*/
    
    private double[]     price_quotes;
    private String       status_submitted;
    private String[]     symbols;
    private long[]       trade_qtys;
    private String       type_limit_buy;
    private String       type_limit_sell;
    private String       type_stop_loss;
}
