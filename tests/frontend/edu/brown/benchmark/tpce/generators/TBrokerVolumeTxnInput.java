package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TBrokerVolumeTxnInput {
	public String[]           broker_list;//[Globals.max_broker_list_len][cB_NAME_len+1];
	public String              sector_name;//[cSC_NAME_len+1];
	public TBrokerVolumeTxnInput(){
		broker_list = new String[TxnHarnessStructs.max_broker_list_len];
		sector_name = new String ();
		for (int i = 0; i < TxnHarnessStructs.max_broker_list_len; ++i)
        {
            broker_list[i] = null;
        }
    }
    public ArrayList<Object>InputParameters(){
    	ArrayList<Object> para = new ArrayList<Object>();
    	
    	para.add(broker_list);
    	
    	para.add(sector_name);
    	return para;
    }
}
