package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeOrderTxnInput {

    public TTradeOrderTxnInput(){
        co_name = new String();
        exec_f_name = new String();
        exec_l_name = new String();
        exec_tax_id = new String();
        issue = new String();
        st_pending_id = new String();
        st_submitted_id = new String();
        symbol = new String();
        trade_type_id = new String();
    }
    
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(requested_price);
        para.add(acct_id);
        para.add(is_lifo);
        para.add(roll_it_back);
        para.add(trade_qty);
        para.add(type_is_margin);
        para.add(co_name);
        para.add(exec_f_name);
        para.add(exec_l_name);
        para.add(exec_tax_id);
        para.add(issue);
        para.add(st_pending_id);
        para.add(st_submitted_id);
        para.add(symbol);
        para.add(trade_type_id);
        para.add(trade_id);
        return para;
    }
    
    public double getRequestedPrice(){
        return requested_price;
    }
    public long getAcctId(){
        return acct_id;
    }
    public long getIsLifo(){
        return is_lifo;
    }
    public long getRollItBack(){
        return roll_it_back;
    }
    public long getTradeQty(){
        return trade_qty;
    }
    public long getTypeIsMargin(){
        return type_is_margin;
    }
    public long getTradeID(){
        return trade_id;
    }
    public String getCoNmae(){
        return co_name;
    }
    public String getExecFirstName(){
        return exec_f_name;
    }
    public String getExecLastName(){
        return exec_l_name;
    }
    public String getExecTaxId(){
        return exec_tax_id;
    }
    public String getIssue(){
        return issue;
    }
    public String getStPendingId(){
        return st_pending_id;
    }
    public String getStSubmittedId(){
        return st_submitted_id;
    }
    public String getSymbol(){
        return symbol;
    }
    public String getTradeTypeId(){
        return trade_type_id;
    }
    
    public void setRequestedPrice(double requested_price){
        this.requested_price = requested_price;
    }
    public void setAcctId(long acct_id){
        this.acct_id = acct_id;
    }
    public void setIsLifo(long is_lifo){
        this.is_lifo = is_lifo;
    }
    public void setRollItBack(long roll_it_back){
        this.roll_it_back = roll_it_back;
    }
    public void setTradeQty(long trade_qty){
        this.trade_qty = trade_qty;
    }
    public void setTypeIsMargin(long type_is_margin){
        this.type_is_margin = type_is_margin;
    }
    public void setTradeID(long trade_id){
        this.trade_id = trade_id;
    }
    public void setCoNmae(String co_name){
        this.co_name = co_name;
    }
    public void setExecFirstName(String exec_f_name){
        this.exec_f_name = exec_f_name;
    }
    public void setExecLastName(String exec_l_name){
        this.exec_l_name = exec_l_name;
    }
    public void setExecTaxId(String exec_tax_id){
        this.exec_tax_id = exec_tax_id;
    }
    public void setIssue(String issue){
        this.issue = issue;
    }
    public void setStPendingId(String st_pending_id){
        this.st_pending_id = st_pending_id;
    }
    public void setStSubmittedId(String st_submitted_id){
        this.st_submitted_id = st_submitted_id;
    }
    public void setSymbol(String symbol){
        this.symbol = symbol;
    }
    public void setTradeTypeId(String trade_type_id){
        this.trade_type_id = trade_type_id;
    }
    private double          requested_price;
    private long            acct_id;
    private long            is_lifo;
    private long            roll_it_back;
    private long            trade_qty;
    private long            type_is_margin;
    private long            trade_id;
    private String          co_name;
    private String          exec_f_name;
    private String          exec_l_name;
    private String          exec_tax_id;
    private String          issue;
    private String          st_pending_id;
    private String          st_submitted_id;
    private String          symbol;
    private String          trade_type_id;
}
