package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeStatusTxnInput {
    private long acct_id;
    
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(acct_id);
        return para;
    }
    
    public long getAcctId(){
        return acct_id;
    }
    
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
}
