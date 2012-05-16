package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TBrokerVolumeTxnInput {
	public String[]            broker_list;//[Globals.max_broker_list_len][cB_NAME_len+1];
	public String              sector_name;//[cSC_NAME_len+1];
	public TBrokerVolumeTxnInput(){
		broker_list = new String [TxnHarnessStructs.max_broker_list_len];
		sector_name = new String ();
    }
    public ArrayList<String>InputParameters(){
    	ArrayList<String> para = new ArrayList<String>();
    	for (int i = 0; i < TxnHarnessStructs.max_broker_list_len; i ++){
    		para.add(broker_list[i]);
    	}
    	para.add(sector_name);
    	return para;
    }
}
