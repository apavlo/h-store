package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TCustomerPositionTxnInput {
        
    public TCustomerPositionTxnInput(){
        tax_id = new String();
    }
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(acct_id_idx);
        para.add(cust_id);
        para.add(get_history);
        para.add(tax_id);
        return para;
    }
    public long getAcctIdIndex(){
        return acct_id_idx;
    }
    public long getCustId(){
        return cust_id;
    }
    public long getHistory(){
        return get_history;
    }
    public String getTaxId(){
        return tax_id;
    }
    public void setAcctIdIndex(long acct_id_idx){
        this.acct_id_idx =  acct_id_idx;
    }
    public void setCustId(long cust_id){
        this.cust_id = cust_id;
    }
    public void setHistory(long get_history){
        this.get_history = get_history;
    }
    public void setTaxId(String tax_id){
        this.tax_id = tax_id;
    }
    
    private long acct_id_idx;
    private long cust_id;
    private long get_history;
    private String tax_id;
}
