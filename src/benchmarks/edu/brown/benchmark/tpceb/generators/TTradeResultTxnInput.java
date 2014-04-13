package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;

public class TTradeResultTxnInput {
     public double      trade_price;
     public long      trade_id;
     public float trade_price_float;
     public String st_completed_id;
     
     public ArrayList<Object>InputParameters(){
            ArrayList<Object> para = new ArrayList<Object>();
            para.add(trade_id);
         //   if(trade_price != 0){
          //  trade_price_float = (float)trade_price;
          //  }
           // else{
           //     trade_price_float = 0;
            //}
            //para.add(trade_price_float);
            
            para.add(trade_price);
            st_completed_id = "E_COMPLETE";
           // para.add(st_completed_id);
            return para;
        }
}
