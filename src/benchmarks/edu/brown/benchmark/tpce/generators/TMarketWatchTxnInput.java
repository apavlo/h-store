package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;
import java.util.Date;
import org.voltdb.types.*;

public class TMarketWatchTxnInput {
    
    public TMarketWatchTxnInput(){
        industry_name = new String();
        start_day = new TimestampType();
    }
    
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(acct_id);
        para.add(c_id);
        para.add(ending_co_id);
        para.add(starting_co_id);
        para.add(industry_name);
        return para;
    }
    
    public long getAcctId(){
        return acct_id;
    }
    public long getCId(){
        return c_id;
    }
    public long getEndingCoId(){
        return ending_co_id;
    }
    public long getStartingCoId(){
        return starting_co_id;
    }
    public TimestampType getStartDay(){
        return start_day;
    }
    public String getIndustryName(){
        return industry_name;
    }
    
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
    public void setCId(long c_id){
        this.c_id = c_id;
    }
    public void setEndingCoId(long ending_co_id){
        this.ending_co_id = ending_co_id;
    }
    public void setStartingCoId(long starting_co_id){
        this.starting_co_id = starting_co_id;
    }
    public void setStartDay(TimestampType start_day){
        this.start_day = start_day;
    }
    public void setIndustryName(String industry_name){
        this.industry_name = industry_name;
    }
    
    private long              acct_id;
    private long              c_id;
    private long              ending_co_id;
    private long              starting_co_id;
    private TimestampType     start_day;
    private String            industry_name;
    
}
