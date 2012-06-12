package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;

import edu.brown.benchmark.tpce.TPCEConstants;
import org.voltdb.types.*;

public class TTradeLookupTxnInput {
    public TTradeLookupTxnInput(){
        trade_id = new long [TPCEConstants.TradeLookupFrame1MaxRows];
        symbol = new String();
        start_trade_dts = new TimestampType(new GregorianCalendar(0,0,0,0,0,0).getTime());
        end_trade_dts = new TimestampType(new GregorianCalendar(0,0,0,0,0,0).getTime());
    }
    
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(trade_id);
        para.add(acct_id);
        para.add(max_acct_id);
        para.add(frame_to_execute);
        para.add(max_trades);
        para.add(end_trade_dts);
        para.add(start_trade_dts);
        para.add(symbol);
        return para;
    }
    
    public long[] getTradeId(){
        return trade_id;
    }
    public long getAcctId(){
        return acct_id;
    }
    public long getMaxAcctId(){
        return max_acct_id;
    }
    public int getFrameToExecute(){
        return frame_to_execute;
    }
    public int getMaxTrades(){
        return max_trades;
    }
    public TimestampType getEndTradeDts(){
        return end_trade_dts;
    }
    public TimestampType getStartTradeDts(){
        return start_trade_dts;
    }
    public String getSymbol(){
        return symbol;
    }
    
    public void setTradeId(int index, long tradeID){
        trade_id[index] = tradeID;
    }
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
    public void setMaxAcctId(long max_acct_id){
        this.max_acct_id = acct_id;
    }
    public void setFrameToExecute(int frame_to_execute){
        this.frame_to_execute = frame_to_execute;
    }
    public void setMaxTrades(int max_trades){
        this.max_trades = max_trades;
    }
    public void setEndTradeDts(TimestampType end_trade_dts){
        this.end_trade_dts = end_trade_dts;
    }
    public void setStartTradeDts(TimestampType start_trade_dts){
        this.start_trade_dts = start_trade_dts;
    }
    public void setSymbol(String symbol){
        this.symbol = symbol;
    }
    
    private long[]            trade_id;
    private long              acct_id;
    private long              max_acct_id;
    private int               frame_to_execute;           
    private int               max_trades;
    private TimestampType     end_trade_dts;
    private TimestampType     start_trade_dts;
    private String            symbol;
}
