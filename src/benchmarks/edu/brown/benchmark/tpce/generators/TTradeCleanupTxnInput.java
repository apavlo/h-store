package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TTradeCleanupTxnInput {
     long                start_trade_id;
     String              st_canceled_id;
     String              st_pending_id;
     String              st_submitted_id;
     
     public TTradeCleanupTxnInput(){
         st_canceled_id = new String();
         st_pending_id = new String();
         st_submitted_id = new String();
     }
     
     public ArrayList<Object>InputParameters(){
            ArrayList<Object> para = new ArrayList<Object>();
            para.add(start_trade_id);
            para.add(st_canceled_id);
            para.add(st_pending_id);
            para.add(st_submitted_id);
            return para;
        }
}
