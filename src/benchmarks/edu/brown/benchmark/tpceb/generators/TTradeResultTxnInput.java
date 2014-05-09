package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;
import java.util.Vector;

public class TTradeResultTxnInput {
     public double      trade_price;
     public long      trade_id;
     public float trade_price_float;
     public String st_completed_id;
     public Vector trade_ids = new Vector();
     
     public ArrayList<Object>InputParameters(){
            ArrayList<Object> para = new ArrayList<Object>();
       
            para.add(trade_id);
            
            para.add(trade_price);
            st_completed_id = "CMPT";
            para.add(st_completed_id);
            
            
            System.out.println(trade_id);
            System.out.println(trade_price);
           
            return para;
        }
}
