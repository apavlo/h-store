package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TDataMaintenanceTxnInput {
    private long  acct_id;
    private long  c_id;
    private long  co_id;
    private int   day_of_month;
    private int   vol_incr;
    private String    symbol;
    private String    table_name;
    private String    tx_id;
    
    public TDataMaintenanceTxnInput(){
        symbol = new String();
        table_name = new String();
        tx_id = new String();
    }
    
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(acct_id);
        para.add(c_id);
        para.add(co_id);
        para.add(day_of_month);
        para.add(vol_incr);
        para.add(symbol);
        para.add(table_name);
        para.add(tx_id);
        return para;
    }
    
    public long getAcctId(){
        return acct_id;
    }
    public long getCId(){
        return c_id;
    }
    public long getCoId(){
        return co_id;
    }
    public int getDayOfMonth(){
        return day_of_month;
    }
    public int getVolIncr(){
        return vol_incr;
    }
    public String getSymbol(){
        return symbol;
    }
    public String getTableName(){
        return table_name;
    }
    public String getTxId(){
        return tx_id;
    }
    
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
    public void setCId(long c_id){
        this.c_id = c_id;
    }
    public void setCoId(long co_id){
        this.co_id = co_id;
    }
    public void setDayOfMonth(int day_of_month){
        this.day_of_month = day_of_month;
    }
    public void setVolIncr(int vol_incr){
        this.vol_incr = vol_incr;
    }
    public void setSymbol(String symbol){
        this.symbol = symbol;
    }
    public void setTableName(String table_name){
        this.table_name = table_name;
    }
    public void setTxId(String tx_id){
        this.tx_id = tx_id;
    }
    
}
