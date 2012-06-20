package edu.brown.benchmark.tpce.generators;

import java.util.ArrayList;

public class TBrokerVolumeTxnInput {
    private String[]           broker_list;
    private String             sector_name;
    public TBrokerVolumeTxnInput(){
        broker_list = new String[TxnHarnessStructs.max_broker_list_len];
        sector_name = new String ();
        for (int i = 0; i < TxnHarnessStructs.max_broker_list_len; ++i)
        {
            broker_list[i] = new String();
        }
    }
    public ArrayList<Object>InputParameters(){
        ArrayList<Object> para = new ArrayList<Object>();
        para.add(broker_list);
        para.add(sector_name);
        return para;
    }
    public String[] getBrokerList(){
        return broker_list;
    }
    public String getSectorName(){
        return sector_name;
    }
    public void setBrokerList(int numBrokers){
        broker_list = new String[numBrokers];
    }
    public void setSectorName(String sectorName){
        sector_name = sectorName;
    }
}
