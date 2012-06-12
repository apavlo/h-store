package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeResultTxnInput {
     public double      trade_price;
     public long      trade_id;
     
     public ArrayList<Object>InputParameters(){
            ArrayList<Object> para = new ArrayList<Object>();
            para.add(trade_price);
            para.add(trade_id);
            return para;
        }
}
