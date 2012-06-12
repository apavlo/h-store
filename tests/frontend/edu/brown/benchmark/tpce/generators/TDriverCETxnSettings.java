package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.*;

public class TDriverCETxnSettings {

    public BrokerVolumeSettings       BV_settings;
    public CustomerPositionSettings   CP_settings;
    public MarketWatchSettings        MW_settings;
    public SecurityDetailSettings     SD_settings;
    public TradeLookupSettings        TL_settings;
    public TradeOrderSettings         TO_settings;
    public TradeUpdateSettings        TU_settings;

    TxnMixGeneratorSettings    TxnMixGenerator_settings;

    public TDriverCETxnSettings(){
        BV_settings = new BrokerVolumeSettings();
        CP_settings = new CustomerPositionSettings();
        MW_settings = new MarketWatchSettings();
        SD_settings = new SecurityDetailSettings();
        TL_settings = new TradeLookupSettings();
        TO_settings = new TradeOrderSettings();
        TU_settings = new TradeUpdateSettings();
        TxnMixGenerator_settings = new TxnMixGeneratorSettings();
    }
    public boolean isValid(){
        boolean isValid = true;
        isValid &= BV_settings.checkValid();
        isValid &= CP_settings.checkValid();
        isValid &= MW_settings.checkValid();
        isValid &= SD_settings.checkValid();
        isValid &= TL_settings.checkValid();
        isValid &= TO_settings.checkValid();
        isValid &= TU_settings.checkValid();
        isValid &= TxnMixGenerator_settings.checkValid();
        return isValid;
      
    }

    public void checkCompliant(){
        
        BV_settings.checkCompliant();
        CP_settings.checkCompliant();
        MW_settings.checkCompliant();
        SD_settings.checkCompliant();
        TL_settings.checkCompliant();
        TO_settings.checkCompliant();
        TU_settings.checkCompliant();
        TxnMixGenerator_settings.checkCompliant();
    }
}

/******************************************************************************
*   Parameter Derived Class / Template Instantiation
******************************************************************************/

class BrokerVolumeSettings extends ParametersWithDefaults{
    
    public BrokerVolumeSettings(){
        initialize();
    }

    public void initializeDefaults(){
    }

    public void checkDefaults(){
    }

    public boolean checkValid(){
        return true;
    }

    public boolean checkCompliant(){
        return true;
    }
    
    public boolean isValid(){
        return true;
    }
    public boolean isCompliant(){
        return true;
    }
}

class CustomerPositionSettings extends ParametersWithDefaults{

    public int dft_by_cust_id;
    public int dft_by_tax_id;
    public int dft_get_history;
    public int cur_by_cust_id;
    public int cur_by_tax_id;
    public int cur_get_history;
    public boolean state_by_cust_id;
    public boolean state_by_tax_id;
    public boolean state_get_history;
    
    public CustomerPositionSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_by_cust_id = 50;
        dft_by_tax_id = 50;
        dft_get_history = 50;
    }

    public void setToDefaults(){ 
        cur_by_cust_id = dft_by_cust_id;
        cur_by_tax_id = dft_by_tax_id;
        cur_get_history = dft_get_history;
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_by_cust_id = (cur_by_cust_id == dft_by_cust_id);
        state_by_tax_id = (cur_by_tax_id == dft_by_tax_id);
        state_get_history = (cur_get_history == dft_get_history);
    }

    public boolean checkValid() {
        try{
            driverParamCheckBetween( "by_cust_id",    cur_by_cust_id,                 0, 100);
            driverParamCheckBetween( "by_tax_id",     cur_by_tax_id,                  0, 100);
            driverParamCheckEqual(   "by_*_id total", cur_by_cust_id + cur_by_tax_id,    100);
            driverParamCheckBetween( "get_history",   cur_get_history,                0, 100);
            return true;
        }catch(CheckException e){
            return false;
        }
    }

    public boolean checkCompliant() {
        checkValid();
        try{
            driverParamCheckDefault(dft_by_cust_id, cur_by_cust_id, "by_cust_id");
            driverParamCheckDefault(dft_by_tax_id, cur_by_tax_id, "by_tax_id");
            driverParamCheckDefault(dft_get_history, cur_get_history, "get_history");
            return true;
        }catch(CheckException e){
            return false;
        }
    }
}

class MarketWatchSettings extends ParametersWithDefaults{
 
    public int   dft_by_acct_id;     
    public int   dft_by_industry;    
    public int   dft_by_watch_list;  
    public int    cur_by_acct_id;     
    public int    cur_by_industry;    
    public int    cur_by_watch_list;  
    public boolean state_by_acct_id;
    public boolean state_by_industry;
    public boolean state_by_watch_list;
    
    
    MarketWatchSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_by_acct_id = 35;
        dft_by_industry = 5;
        dft_by_watch_list = 60;
    }

    public void setToDefaults(){ 
        cur_by_acct_id = dft_by_acct_id;
        cur_by_industry = dft_by_industry;
        cur_by_watch_list = dft_by_watch_list;
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_by_acct_id = (cur_by_acct_id == dft_by_acct_id);
        state_by_industry = (cur_by_industry == dft_by_industry);
        state_by_watch_list = (cur_by_watch_list == dft_by_watch_list);
    }

    public boolean checkValid(){
        try{
            driverParamCheckBetween( "by_acct_id",    cur_by_acct_id,    0, 100);
            driverParamCheckBetween( "by_industry",   cur_by_industry,   0, 100);
            driverParamCheckBetween( "by_watch_list", cur_by_watch_list, 0, 100);
            driverParamCheckEqual(   "by_* total",    cur_by_acct_id + cur_by_industry + cur_by_watch_list, 100);
            return true;
        }catch(CheckException e){
            return false;
        }
    }

    public boolean checkCompliant(){
        checkValid();
        try{
            driverParamCheckDefault( dft_by_acct_id, cur_by_acct_id, "by_cust_id" );
            driverParamCheckDefault( dft_by_industry, cur_by_industry, "by_industry");
            driverParamCheckDefault( dft_by_watch_list, cur_by_watch_list, "by_watch_list" );
            return true;
        }catch(CheckException e){
            return false;
        }      
    }
}

class SecurityDetailSettings extends ParametersWithDefaults{
 
    public int dft_LOBAccessPercentage;
    public int cur_LOBAccessPercentage;
    public boolean state_LOBAccessPercentage;
    
    SecurityDetailSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_LOBAccessPercentage = 1;
    }

    public void setToDefaults(){ 
        cur_LOBAccessPercentage = dft_LOBAccessPercentage;
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_LOBAccessPercentage = (cur_LOBAccessPercentage == dft_LOBAccessPercentage);
    }

    public boolean checkValid(){
        try{
            driverParamCheckBetween( "LOBAccessPercentage", cur_LOBAccessPercentage, 0, 100);
            return true;
        }catch(CheckException e){
            return false;
        }
        
    }

    public boolean checkCompliant(){
        checkValid();
        try{
            driverParamCheckDefault( dft_LOBAccessPercentage, cur_LOBAccessPercentage, "LOBAccessPercentage" );
            return true;
        }catch(CheckException e){
                return false;
        }
    }
}

class TradeLookupSettings extends ParametersWithDefaults{
 
    public int dft_do_frame1;                       
    public int dft_do_frame2;                       
    public int dft_do_frame3;                       
    public int dft_do_frame4;                       
    public int dft_MaxRowsFrame1;                   
    public int dft_BackOffFromEndTimeFrame2;   
    public int dft_MaxRowsFrame2;                   
    public int dft_BackOffFromEndTimeFrame3;   
    public int dft_MaxRowsFrame3;                   
    public int dft_BackOffFromEndTimeFrame4;   
    public int dft_MaxRowsFrame4;                 
    
    public int cur_do_frame1;                       
    public int cur_do_frame2;                       
    public int cur_do_frame3;                       
    public int cur_do_frame4;                       
    public int cur_MaxRowsFrame1;                   
    public int cur_BackOffFromEndTimeFrame2;   
    public int cur_MaxRowsFrame2;                   
    public int cur_BackOffFromEndTimeFrame3;   
    public int cur_MaxRowsFrame3;                   
    public int cur_BackOffFromEndTimeFrame4;   
    public int cur_MaxRowsFrame4;                 
    
    public boolean    state_do_frame1;                       
    public boolean    state_do_frame2;                       
    public boolean    state_do_frame3;                       
    public boolean    state_do_frame4;                       
    public boolean    state_MaxRowsFrame1;                   
    public boolean    state_BackOffFromEndTimeFrame2;   
    public boolean    state_MaxRowsFrame2;                   
    public boolean    state_BackOffFromEndTimeFrame3;   
    public boolean    state_MaxRowsFrame3;                   
    public boolean    state_BackOffFromEndTimeFrame4;   
    public boolean    state_MaxRowsFrame4;                  
    
    TradeLookupSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_do_frame1 = 30;
        dft_do_frame2 = 30;
        dft_do_frame3 = 30;
        dft_do_frame4 = 10;
        dft_MaxRowsFrame1 = 20;
        dft_BackOffFromEndTimeFrame2 = 4 * 8 * 3600;    
        dft_MaxRowsFrame2 = 20;
        dft_BackOffFromEndTimeFrame3 = 200 * 60;        
        dft_MaxRowsFrame3 = 20;
        dft_BackOffFromEndTimeFrame4 = 500 * 60;        
        dft_MaxRowsFrame4 = 20;
    }

    public void checkDefaults(){
        state_do_frame1 = (cur_do_frame1 == dft_do_frame1);
        state_do_frame2 = (cur_do_frame2 == dft_do_frame2);
        state_do_frame3 = (cur_do_frame3 == dft_do_frame3);
        state_do_frame4 = (cur_do_frame4 == dft_do_frame4);
        state_MaxRowsFrame1 = (cur_MaxRowsFrame1 == dft_MaxRowsFrame1);
        state_BackOffFromEndTimeFrame2 = (cur_BackOffFromEndTimeFrame2 == dft_BackOffFromEndTimeFrame2);
        state_MaxRowsFrame2 = (cur_MaxRowsFrame2 == dft_MaxRowsFrame2);
        state_BackOffFromEndTimeFrame3 = (cur_BackOffFromEndTimeFrame3 == dft_BackOffFromEndTimeFrame3);
        state_MaxRowsFrame3 = (cur_MaxRowsFrame3 == dft_MaxRowsFrame3);
        state_BackOffFromEndTimeFrame4 = (cur_BackOffFromEndTimeFrame4 == dft_BackOffFromEndTimeFrame4);
        state_MaxRowsFrame4 = (cur_MaxRowsFrame4 == dft_MaxRowsFrame4);
    }

    public void setToDefaults(){ 
        cur_do_frame1 = dft_do_frame1; 
        cur_do_frame2 = dft_do_frame2;
        cur_do_frame3 = dft_do_frame3;
        cur_do_frame4 = dft_do_frame4;
        cur_MaxRowsFrame1 = dft_MaxRowsFrame1;
        cur_BackOffFromEndTimeFrame2 = dft_BackOffFromEndTimeFrame2;
        cur_MaxRowsFrame2 = dft_MaxRowsFrame2;
        cur_BackOffFromEndTimeFrame3 = dft_BackOffFromEndTimeFrame3;
        cur_MaxRowsFrame3 = dft_MaxRowsFrame3;
        cur_BackOffFromEndTimeFrame4 = dft_BackOffFromEndTimeFrame4;
        cur_MaxRowsFrame4 = dft_MaxRowsFrame4;
        checkDefaults();
    }
    
    public boolean checkValid() {
        try{
            driverParamCheckBetween( "do_frame1",       cur_do_frame1,    0, 100);
            driverParamCheckBetween( "do_frame2",       cur_do_frame2,    0, 100);
            driverParamCheckBetween( "do_frame3",       cur_do_frame3,    0, 100);
            driverParamCheckBetween( "do_frame4",       cur_do_frame4,    0, 100);
            driverParamCheckEqual(   "do_frame* total", cur_do_frame1 + cur_do_frame2 + cur_do_frame3 + cur_do_frame4, 100);
            driverParamCheckLE(      "MaxRowsFrame1", cur_MaxRowsFrame1, TPCEConstants.TradeLookupFrame1MaxRows);
            driverParamCheckLE(      "MaxRowsFrame2", cur_MaxRowsFrame2, TPCEConstants.TradeLookupFrame2MaxRows);
            driverParamCheckLE(      "MaxRowsFrame3", cur_MaxRowsFrame3, TPCEConstants.TradeLookupFrame3MaxRows);
            driverParamCheckLE(      "MaxRowsFrame4", cur_MaxRowsFrame4, TPCEConstants.TradeLookupFrame4MaxRows);
            return true;
        }catch(CheckException e){
            return false;
        }
    }

    public boolean checkCompliant(){
        checkValid();
        try{
            driverParamCheckDefault( dft_do_frame1, cur_do_frame1, "do_frame1" );
            driverParamCheckDefault( dft_do_frame2, cur_do_frame2, "do_frame2" );
            driverParamCheckDefault( dft_do_frame3, cur_do_frame3, "do_frame3" );
            driverParamCheckDefault( dft_do_frame4, cur_do_frame4, "do_frame4" );
            driverParamCheckDefault( dft_MaxRowsFrame1, cur_MaxRowsFrame1, "MaxRowsFrame1" );
            driverParamCheckDefault( dft_BackOffFromEndTimeFrame2, cur_BackOffFromEndTimeFrame2, "BackOffFromEndTimeFrame2" );
            driverParamCheckDefault( dft_MaxRowsFrame2, cur_MaxRowsFrame2, "MaxRowsFrame2" );
            driverParamCheckDefault( dft_BackOffFromEndTimeFrame3, cur_BackOffFromEndTimeFrame3, "BackOffFromEndTimeFrame3" );
            driverParamCheckDefault( dft_MaxRowsFrame3, cur_MaxRowsFrame3, "MaxRowsFrame3" );
            driverParamCheckDefault( dft_BackOffFromEndTimeFrame4, cur_BackOffFromEndTimeFrame4, "BackOffFromEndTimeFrame4" );
            driverParamCheckDefault( dft_MaxRowsFrame4, cur_MaxRowsFrame4, "MaxRowsFrame4" );
            return true;
        }catch(CheckException e){
                return false;
        }
    }
}

class TradeOrderSettings extends ParametersWithDefaults{
 
    public int dft_market;
    public int dft_limit;
    public int dft_stop_loss;
    public int dft_security_by_name;
    public int dft_security_by_symbol;
    public int dft_buy_orders;
    public int dft_sell_orders;
    public int dft_lifo;
    public int dft_exec_is_owner;
    public int dft_rollback;
    public int dft_type_is_margin;
    
    public int cur_market;
    public int cur_limit;
    public int cur_stop_loss;
    public int cur_security_by_name;
    public int cur_security_by_symbol;
    public int cur_buy_orders;
    public int cur_sell_orders;
    public int cur_lifo;
    public int cur_exec_is_owner;
    public int cur_rollback;
    public int cur_type_is_margin;
    
    public boolean    state_market;
    public boolean    state_limit;
    public boolean    state_stop_loss;
    public boolean    state_security_by_name;
    public boolean    state_security_by_symbol;
    public boolean    state_buy_orders;
    public boolean    state_sell_orders;
    public boolean    state_lifo;
    public boolean    state_exec_is_owner;
    public boolean    state_rollback;
    public boolean    state_type_is_margin;
    
    public TradeOrderSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_market = 60;
        dft_limit = 40;
        dft_stop_loss = 50;
        dft_security_by_name = 40;
        dft_security_by_symbol = 60;
        dft_buy_orders = 50;
        dft_sell_orders = 50;
        dft_lifo = 35;
        dft_exec_is_owner = 90;
        dft_rollback = 1;
        dft_type_is_margin = 8;
    }

    public void setToDefaults(){ 
        cur_market = dft_market; 
        cur_limit = dft_limit;
        cur_stop_loss = dft_stop_loss;
        cur_security_by_name = dft_security_by_name;
        cur_security_by_symbol = dft_security_by_symbol;
        cur_buy_orders = dft_buy_orders;
        cur_sell_orders = dft_sell_orders;
        cur_lifo = dft_lifo;
        cur_exec_is_owner = dft_exec_is_owner;
        cur_rollback = dft_rollback;
        cur_type_is_margin = dft_type_is_margin;
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_market = (cur_market == dft_market);
        state_limit = (cur_limit == dft_limit);
        state_stop_loss = (cur_stop_loss == dft_stop_loss);
        state_security_by_name = (cur_security_by_name == dft_security_by_name);
        state_security_by_symbol = (cur_security_by_symbol == dft_security_by_symbol);
        state_buy_orders = (cur_buy_orders == dft_buy_orders);
        state_sell_orders = (cur_sell_orders == dft_sell_orders);
        state_lifo = (cur_lifo == dft_lifo);
        state_exec_is_owner = (cur_exec_is_owner == dft_exec_is_owner);
        state_rollback = (cur_rollback == dft_rollback);
        state_type_is_margin = (cur_type_is_margin == dft_type_is_margin);
    }

    public boolean checkValid() {
        try{
            driverParamCheckBetween( "market",                cur_market,                                      0, 100);
            driverParamCheckBetween( "limit",                 cur_limit,                                       0, 100);
            driverParamCheckEqual(   "market or limit total", cur_market + cur_limit,                             100);
            driverParamCheckBetween( "stop_loss",             cur_stop_loss,                                   0, 100);
            driverParamCheckBetween( "security_by_name",      cur_security_by_name,                            0, 100);
            driverParamCheckBetween( "security_by_symbol",    cur_security_by_symbol,                          0, 100);
            driverParamCheckEqual(   "security_by_* total",   cur_security_by_name + cur_security_by_symbol,      100);
            driverParamCheckBetween( "buy_orders",            cur_buy_orders,                                  0, 100);
            driverParamCheckBetween( "sell_orders",           cur_sell_orders,                                 0, 100);
            driverParamCheckEqual(   "*_orders total",        cur_buy_orders + cur_sell_orders,                   100);
            driverParamCheckBetween( "lifo",                  cur_lifo,                                        0, 100);
            driverParamCheckBetween( "exec_is_owner",         cur_exec_is_owner,                               0, 100);
            driverParamCheckBetween( "rollback",              cur_rollback,                                    0, 100);
            driverParamCheckBetween( "type_is_margin",        cur_type_is_margin,                              0, 100);
            return true;
        }catch(CheckException e){
            return false;
        }
    }

    public boolean checkCompliant() {
        checkValid();
        try{
            driverParamCheckBetween( "exec_is_owner",         cur_exec_is_owner,                              60, 100);
            driverParamCheckDefault( dft_market, cur_market, "market" );
            driverParamCheckDefault( dft_limit, cur_limit, "limit" );
            driverParamCheckDefault( dft_stop_loss, cur_stop_loss, "stop_loss" );
            driverParamCheckDefault( dft_security_by_name, cur_security_by_name, "security_by_name" );
            driverParamCheckDefault( dft_security_by_symbol, cur_security_by_symbol, "security_by_symbol" );
            driverParamCheckDefault( dft_buy_orders, cur_buy_orders, "buy_orders" );
            driverParamCheckDefault( dft_sell_orders, cur_sell_orders, "sell_orders" );
            driverParamCheckDefault( dft_lifo, cur_lifo, "lifo" );
            driverParamCheckDefault( dft_exec_is_owner, cur_exec_is_owner, "exec_is_owner" );
            driverParamCheckDefault( dft_rollback, cur_rollback, "rollback" );
            driverParamCheckDefault( dft_type_is_margin, cur_type_is_margin, "type_is_margin" );
            return true;
        }catch(CheckException e){
            return false; 
        }      
    }
}

class TradeUpdateSettings extends ParametersWithDefaults{
 
    public int dft_do_frame1;  
    public int dft_do_frame2;  
    public int dft_do_frame3;  

    public int dft_MaxRowsFrame1;                   
    public int dft_MaxRowsToUpdateFrame1;          

    public int dft_BackOffFromEndTimeFrame2;   
    public int dft_MaxRowsFrame2;                   
    public int dft_MaxRowsToUpdateFrame2;          

    public int dft_BackOffFromEndTimeFrame3;   
    public int dft_MaxRowsFrame3;                   
    public int dft_MaxRowsToUpdateFrame3;          
    
    public int cur_do_frame1;  
    public int cur_do_frame2;  
    public int cur_do_frame3;  

    public int cur_MaxRowsFrame1;                   
    public int cur_MaxRowsToUpdateFrame1;          

    public int cur_BackOffFromEndTimeFrame2;   
    public int cur_MaxRowsFrame2;                   
    public int cur_MaxRowsToUpdateFrame2;          

    public int cur_BackOffFromEndTimeFrame3;   
    public int cur_MaxRowsFrame3;                   
    public int cur_MaxRowsToUpdateFrame3;          
    
    public boolean    state_do_frame1;  
    public boolean    state_do_frame2;  
    public boolean    state_do_frame3;  

    public boolean    state_MaxRowsFrame1;                   
    public boolean    state_MaxRowsToUpdateFrame1;          

    public boolean    state_BackOffFromEndTimeFrame2;   
    public boolean    state_MaxRowsFrame2;                   
    public boolean    state_MaxRowsToUpdateFrame2;          

    public boolean    state_BackOffFromEndTimeFrame3;   
    public boolean    state_MaxRowsFrame3;                   
    public boolean    state_MaxRowsToUpdateFrame3;          
    
    TradeUpdateSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_do_frame1 = 33;
        dft_do_frame2 = 33;
        dft_do_frame3 = 34;
        dft_MaxRowsFrame1 = 20;
        dft_MaxRowsToUpdateFrame1 = 20;
        dft_MaxRowsFrame2 = 20;
        dft_MaxRowsToUpdateFrame2 = 20;
        dft_BackOffFromEndTimeFrame2 = 4 * 8 * 3600;   
        dft_MaxRowsFrame3 = 20;
        dft_MaxRowsToUpdateFrame3 = 20;
        dft_BackOffFromEndTimeFrame3 = 200 * 60;        
    }

    public void setToDefaults(){ 
        cur_do_frame1 = dft_do_frame1; 
        cur_do_frame2 = dft_do_frame2;
        cur_do_frame3 = dft_do_frame3;
        cur_MaxRowsFrame1 = dft_MaxRowsFrame1;
        cur_MaxRowsToUpdateFrame1 = dft_MaxRowsToUpdateFrame1;
        cur_MaxRowsFrame2 = dft_MaxRowsFrame2;
        cur_MaxRowsToUpdateFrame2 = dft_MaxRowsToUpdateFrame2;
        cur_BackOffFromEndTimeFrame2 = dft_BackOffFromEndTimeFrame2;
        cur_MaxRowsFrame3 = dft_MaxRowsFrame3;
        cur_MaxRowsToUpdateFrame3 = dft_MaxRowsToUpdateFrame3;
        cur_BackOffFromEndTimeFrame3 = dft_BackOffFromEndTimeFrame3;
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_do_frame1 = (cur_do_frame1 == dft_do_frame1);
        state_do_frame2 = (cur_do_frame2 == dft_do_frame2);
        state_do_frame3 = (cur_do_frame3 == dft_do_frame3);
        state_MaxRowsFrame1 = (cur_MaxRowsFrame1 == dft_MaxRowsFrame1);
        state_MaxRowsToUpdateFrame1 = (cur_MaxRowsToUpdateFrame1 == dft_MaxRowsToUpdateFrame1);
        state_MaxRowsFrame2 = (cur_MaxRowsFrame2 == dft_MaxRowsFrame2);
        state_MaxRowsToUpdateFrame2 = (cur_MaxRowsToUpdateFrame2 == dft_MaxRowsToUpdateFrame2);
        state_BackOffFromEndTimeFrame2 = (cur_BackOffFromEndTimeFrame2 == dft_BackOffFromEndTimeFrame2);
        state_MaxRowsFrame3 = (cur_MaxRowsFrame3 == dft_MaxRowsFrame3);
        state_MaxRowsToUpdateFrame3 = (cur_MaxRowsToUpdateFrame3 == dft_MaxRowsToUpdateFrame3);
        state_BackOffFromEndTimeFrame3 = (cur_BackOffFromEndTimeFrame3 == dft_BackOffFromEndTimeFrame3);
    }

    public boolean checkValid() {
        try{
            driverParamCheckBetween( "do_frame1",             cur_do_frame1,                                 0, 100);
            driverParamCheckBetween( "do_frame2",             cur_do_frame2,                                 0, 100);
            driverParamCheckBetween( "do_frame3",             cur_do_frame3,                                 0, 100);
            driverParamCheckEqual(   "do_frame* total",       cur_do_frame1 + cur_do_frame2 + cur_do_frame3,    100);
            driverParamCheckLE(      "MaxRowsFrame1",         cur_MaxRowsFrame1,         TPCEConstants.TradeUpdateFrame1MaxRows);
            driverParamCheckLE(      "MaxRowsFrame2",         cur_MaxRowsFrame2,         TPCEConstants.TradeUpdateFrame2MaxRows);
            driverParamCheckLE(      "MaxRowsFrame3",         cur_MaxRowsFrame3,         TPCEConstants.TradeUpdateFrame3MaxRows);
            driverParamCheckLE(      "MaxRowsToUpdateFrame1", cur_MaxRowsToUpdateFrame1, TPCEConstants.TradeUpdateFrame1MaxRows);
            driverParamCheckLE(      "MaxRowsToUpdateFrame2", cur_MaxRowsToUpdateFrame2, TPCEConstants.TradeUpdateFrame2MaxRows);
            driverParamCheckLE(      "MaxRowsToUpdateFrame3", cur_MaxRowsToUpdateFrame3, TPCEConstants.TradeUpdateFrame3MaxRows);
            return true;
        }catch(CheckException e){
            return false;
        }   
    }

    public boolean checkCompliant() {
        
        checkValid();
        try{
            driverParamCheckDefault( dft_do_frame1, cur_do_frame1, "do_frame1" );
            driverParamCheckDefault( dft_do_frame2, cur_do_frame2, "do_frame2" );
            driverParamCheckDefault( dft_do_frame3, cur_do_frame3, "do_frame3" );
            driverParamCheckDefault( dft_MaxRowsFrame1, cur_MaxRowsFrame1, "MaxRowsFrame1" );
            driverParamCheckDefault( dft_MaxRowsToUpdateFrame1, cur_MaxRowsToUpdateFrame1, "MaxRowsToUpdateFrame1" );
            driverParamCheckDefault( dft_MaxRowsFrame2, cur_MaxRowsFrame2, "MaxRowsFrame2" );
            driverParamCheckDefault( dft_MaxRowsToUpdateFrame2, cur_MaxRowsToUpdateFrame2, "MaxRowsToUpdateFrame2" );
            driverParamCheckDefault( dft_BackOffFromEndTimeFrame2, cur_BackOffFromEndTimeFrame2, "BackOffFromEndTimeFrame2" );
            driverParamCheckDefault( dft_MaxRowsFrame3,cur_MaxRowsFrame3, "MaxRowsFrame3" );
            driverParamCheckDefault( dft_MaxRowsToUpdateFrame3, cur_MaxRowsToUpdateFrame3, "MaxRowsToUpdateFrame3" );
            driverParamCheckDefault( dft_BackOffFromEndTimeFrame3, cur_BackOffFromEndTimeFrame3, "BackOffFromEndTimeFrame3" );
            return true;
        }catch(CheckException e){
            return false;
        }
    }
}

class TxnMixGeneratorSettings extends ParametersWithDefaults{
 
    public int dft_BrokerVolumeMixLevel;
    public int dft_CustomerPositionMixLevel;
    public int dft_MarketFeedMixLevel;
    public int dft_MarketWatchMixLevel;
    public int dft_SecurityDetailMixLevel;
    public int dft_TradeLookupMixLevel;
    public int dft_TradeOrderMixLevel;
    public int dft_TradeResultMixLevel;
    public int dft_TradeStatusMixLevel;
    public int dft_TradeUpdateMixLevel;
    public int dft_TransactionMixTotal;
    
    public int cur_BrokerVolumeMixLevel;
    public int cur_CustomerPositionMixLevel;
    public int cur_MarketFeedMixLevel;
    public int cur_MarketWatchMixLevel;
    public int cur_SecurityDetailMixLevel;
    public int cur_TradeLookupMixLevel;
    public int cur_TradeOrderMixLevel;
    public int cur_TradeResultMixLevel;
    public int cur_TradeStatusMixLevel;
    public int cur_TradeUpdateMixLevel;
    public int cur_TransactionMixTotal;
    
    public boolean    state_BrokerVolumeMixLevel;
    public boolean    state_CustomerPositionMixLevel;
    public boolean    state_MarketWatchMixLevel;
    public boolean    state_SecurityDetailMixLevel;
    public boolean    state_TradeLookupMixLevel;
    public boolean    state_TradeOrderMixLevel;
    public boolean    state_TradeStatusMixLevel;
    public boolean    state_TradeUpdateMixLevel;

    
    public TxnMixGeneratorSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_BrokerVolumeMixLevel        =   49;
        dft_CustomerPositionMixLevel    =  130;
        dft_MarketWatchMixLevel         =  180;
        dft_SecurityDetailMixLevel      =  140;
        dft_TradeLookupMixLevel         =   80;
        dft_TradeOrderMixLevel          =  101;
        dft_TradeStatusMixLevel         =  190;
        dft_TradeUpdateMixLevel         =   20;
    }

    public void setToDefaults(){ 
        cur_BrokerVolumeMixLevel = dft_BrokerVolumeMixLevel; 
        cur_CustomerPositionMixLevel = dft_CustomerPositionMixLevel;
        cur_MarketFeedMixLevel = dft_MarketFeedMixLevel;
        cur_MarketWatchMixLevel = dft_MarketWatchMixLevel;
        cur_SecurityDetailMixLevel = dft_SecurityDetailMixLevel;
        cur_TradeLookupMixLevel = dft_TradeLookupMixLevel;
        cur_TradeOrderMixLevel = dft_TradeOrderMixLevel;
        cur_TradeStatusMixLevel = dft_TradeStatusMixLevel;
        cur_TradeResultMixLevel = dft_TradeResultMixLevel;
        cur_TradeStatusMixLevel = dft_TradeStatusMixLevel;
        cur_TradeUpdateMixLevel = dft_TradeUpdateMixLevel;
        cur_TransactionMixTotal = dft_TransactionMixTotal;
        
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_BrokerVolumeMixLevel     = (cur_BrokerVolumeMixLevel     == dft_BrokerVolumeMixLevel);
        state_CustomerPositionMixLevel = (cur_CustomerPositionMixLevel == dft_CustomerPositionMixLevel);
        state_MarketWatchMixLevel      = (cur_MarketWatchMixLevel      == dft_MarketWatchMixLevel);
        state_SecurityDetailMixLevel   = (cur_SecurityDetailMixLevel   == dft_SecurityDetailMixLevel);
        state_TradeLookupMixLevel      = (cur_TradeLookupMixLevel      == dft_TradeLookupMixLevel);
        state_TradeOrderMixLevel       = (cur_TradeOrderMixLevel       == dft_TradeOrderMixLevel);
        state_TradeStatusMixLevel      = (cur_TradeStatusMixLevel      == dft_TradeStatusMixLevel);
        state_TradeUpdateMixLevel      = (cur_TradeUpdateMixLevel      == dft_TradeUpdateMixLevel);
    }

    public boolean checkValid(){
        try{
            driverParamCheckGE( "BrokerVolumeMixLevel",     cur_BrokerVolumeMixLevel,     0 );
            driverParamCheckGE( "CustomerPositionMixLevel", cur_CustomerPositionMixLevel, 0 );
            driverParamCheckGE( "MarketWatchMixLevel",      cur_MarketWatchMixLevel,      0 );
            driverParamCheckGE( "SecurityDetailMixLevel",   cur_SecurityDetailMixLevel,   0 );
            driverParamCheckGE( "TradeLookupMixLevel",      cur_TradeLookupMixLevel,      0 );
            driverParamCheckGE( "TradeOrderMixLevel",       cur_TradeOrderMixLevel,       0 );
            driverParamCheckGE( "TradeStatusMixLevel",      cur_TradeStatusMixLevel,      0 );
            driverParamCheckGE( "TradeUpdateMixLevel",      cur_TradeUpdateMixLevel,      0 );
            return true;
        }catch(CheckException e){
            return false;
        }
        
    }

    public boolean checkCompliant(){
        checkValid();
        try{
            driverParamCheckDefault( dft_BrokerVolumeMixLevel, cur_BrokerVolumeMixLevel, "BrokerVolumeMixLevel" );
            driverParamCheckDefault( dft_CustomerPositionMixLevel, cur_CustomerPositionMixLevel, "CustomerPositionMixLevel" );
            driverParamCheckDefault( dft_MarketWatchMixLevel, cur_MarketWatchMixLevel, "MarketWatchMixLevel" );
            driverParamCheckDefault( dft_SecurityDetailMixLevel, cur_SecurityDetailMixLevel, "SecurityDetailMixLevel" );
            driverParamCheckDefault( dft_TradeLookupMixLevel,cur_TradeLookupMixLevel, "TradeLookupMixLevel" );
            driverParamCheckDefault( dft_TradeOrderMixLevel,cur_TradeOrderMixLevel, "TradeOrderMixLevel" );
            driverParamCheckDefault( dft_TradeStatusMixLevel, cur_TradeStatusMixLevel, "TradeStatusMixLevel" );
            driverParamCheckDefault( dft_TradeUpdateMixLevel, cur_TradeUpdateMixLevel, "TradeUpdateMixLevel" );
            return true;
        }catch(CheckException e){
            return false;
        }
    }
}

class LoaderSettings extends ParametersWithDefaults{
 
    public long   dft_iConfiguredCustomerCount;
    public long   dft_iActiveCustomerCount;
    public int       dft_iScaleFactor;
    public int    dft_iDaysOfInitialTrades;
    public long   dft_iStartingCustomer;
    public long   dft_iCustomerCount;
    
    public long   cur_iConfiguredCustomerCount;
    public long   cur_iActiveCustomerCount;
    public int    cur_iScaleFactor;
    public int    cur_iDaysOfInitialTrades;
    public long   cur_iStartingCustomer;
    public long   cur_iCustomerCount;
    
    public boolean    state_iConfiguredCustomerCount;
    public boolean    state_iActiveCustomerCount;
    public boolean    state_iScaleFactor;
    public boolean    state_iDaysOfInitialTrades;
    public boolean    state_iStartingCustomer;
    public boolean    state_iCustomerCount;
    
    public LoaderSettings( long  iConfiguredCustomerCount, long  iActiveCustomerCount,
                     long  iStartingCustomer, long  iCustomerCount,
                     int iScaleFactor, int iDaysOfInitialTrades ){
        initialize();

        cur_iConfiguredCustomerCount = iConfiguredCustomerCount;
        cur_iActiveCustomerCount = iActiveCustomerCount;
        cur_iStartingCustomer = iStartingCustomer;
        cur_iCustomerCount = iCustomerCount;
        cur_iScaleFactor = iScaleFactor;
        cur_iDaysOfInitialTrades = iDaysOfInitialTrades;

        checkDefaults();
    }
    
    public LoaderSettings(){
        initialize();
    }

    public void initializeDefaults(){
        dft_iConfiguredCustomerCount = 5000;     
        dft_iActiveCustomerCount = 5000;         
        dft_iStartingCustomer = 1;              
        dft_iCustomerCount = 5000;               
        dft_iScaleFactor = 500;                 
        dft_iDaysOfInitialTrades = 300;         
    }

    public void setToDefaults(){ 
        cur_iConfiguredCustomerCount = dft_iConfiguredCustomerCount; 
        cur_iActiveCustomerCount = dft_iActiveCustomerCount;
        cur_iScaleFactor = dft_iScaleFactor;
        cur_iDaysOfInitialTrades = dft_iDaysOfInitialTrades;
        cur_iStartingCustomer = dft_iStartingCustomer;
        cur_iCustomerCount = dft_iCustomerCount;
        
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_iConfiguredCustomerCount = true;
        state_iActiveCustomerCount = true;
        state_iStartingCustomer = true;
        state_iCustomerCount = true;
        state_iScaleFactor = (cur_iScaleFactor == dft_iScaleFactor);
        state_iDaysOfInitialTrades = (cur_iDaysOfInitialTrades == dft_iDaysOfInitialTrades);
    }

    public boolean checkValid(){
        try{
            driverParamCheckGE(    "iConfiguredCustomerCount", (int)cur_iConfiguredCustomerCount,                     1000);
            driverParamCheckGE(    "iActiveCustomerCount",     (int)cur_iActiveCustomerCount,                         1000);
            driverParamCheckLE(    "iActiveCustomerCount",     (int)cur_iActiveCustomerCount, (int)cur_iConfiguredCustomerCount);
            driverParamCheckEqual( "iConfiguredCustomerCount", (int)cur_iConfiguredCustomerCount % 1000,                 0);
            driverParamCheckGE(    "iStartingCustomer",        (int)cur_iStartingCustomer,                               1);
            driverParamCheckEqual( "iStartingCustomer",        (int)cur_iStartingCustomer % 1000,                        1);
            driverParamCheckEqual( "iCustomerCount",           (int)cur_iCustomerCount % 1000,                           0);
            driverParamCheckLE(    "iCustomerCount",           (int)(cur_iCustomerCount + cur_iStartingCustomer - 1), (int)cur_iConfiguredCustomerCount);
            return true;
        }catch(CheckException e){
            return false;
        }
        
    }

    public boolean checkCompliant(){
        checkValid();
        try{
            driverParamCheckGE(      "iConfiguredCustomerCount", (int)cur_iConfiguredCustomerCount, 5000);
            driverParamCheckGE(      "iActiveCustomerCount",     (int)cur_iActiveCustomerCount,     5000);
            driverParamCheckEqual(   "iActiveCustomerCount",     (int)cur_iActiveCustomerCount, (int)cur_iConfiguredCustomerCount);
            driverParamCheckDefault( dft_iScaleFactor, cur_iScaleFactor, "iScaleFactor" );
            driverParamCheckDefault( dft_iDaysOfInitialTrades, cur_iDaysOfInitialTrades, "iDaysOfInitialTrades" );
            return true;
        }catch(CheckException e){
            return false;
        }
        
    }
}

class DriverGlobalSettings extends ParametersWithDefaults{
 
    public long   dft_iConfiguredCustomerCount;
    public long   dft_iActiveCustomerCount;
    public int    dft_iScaleFactor;
    public int    dft_iDaysOfInitialTrades;
    
    public long   cur_iConfiguredCustomerCount;
    public long   cur_iActiveCustomerCount;
    public int    cur_iScaleFactor;
    public int    cur_iDaysOfInitialTrades;
    public boolean    state_iConfiguredCustomerCount;
    public boolean    state_iActiveCustomerCount;
    public boolean    state_iScaleFactor;
    public boolean    state_iDaysOfInitialTrades;
    
    public DriverGlobalSettings( long  iConfiguredCustomerCount, long  iActiveCustomerCount, int iScaleFactor, int iDaysOfInitialTrades ){
        initialize();

        cur_iConfiguredCustomerCount = iConfiguredCustomerCount;
        cur_iActiveCustomerCount = iActiveCustomerCount;
        cur_iScaleFactor = iScaleFactor;
        cur_iDaysOfInitialTrades = iDaysOfInitialTrades;

        checkDefaults();
    }

    public DriverGlobalSettings(){
        initialize();
    }

    public void setToDefaults(){ 
        cur_iConfiguredCustomerCount = dft_iConfiguredCustomerCount; 
        cur_iActiveCustomerCount = dft_iActiveCustomerCount;
        cur_iScaleFactor = dft_iScaleFactor;
        cur_iDaysOfInitialTrades = dft_iDaysOfInitialTrades;
        
        checkDefaults();
    }
    
    public void initializeDefaults(){
        
        dft_iConfiguredCustomerCount = 5000;    
        dft_iActiveCustomerCount = 5000;        
        dft_iScaleFactor = 500;                
        dft_iDaysOfInitialTrades = 300;        
    }

    public void checkDefaults(){
        state_iConfiguredCustomerCount = true;
        state_iActiveCustomerCount = true;
        state_iScaleFactor = (cur_iScaleFactor == dft_iScaleFactor);
        state_iDaysOfInitialTrades = (cur_iDaysOfInitialTrades == dft_iDaysOfInitialTrades);

    }

    public boolean checkValid(){
        try{
            driverParamCheckGE(    "iConfiguredCustomerCount",  (int)cur_iConfiguredCustomerCount,                     1000);
            driverParamCheckGE(    "iActiveCustomerCount",      (int)cur_iActiveCustomerCount,                         1000);
            driverParamCheckLE(    "iActiveCustomerCount",      (int)cur_iActiveCustomerCount, (int)cur_iConfiguredCustomerCount);
            driverParamCheckEqual( "iConfiguredCustomerCount",  (int)cur_iConfiguredCustomerCount % 1000,                 0);
            return true;
        }catch(CheckException e){
            return false;
        }
    }

    public boolean checkCompliant() {
        checkValid();
        try{
            driverParamCheckGE(      "iConfiguredCustomerCount", (int)cur_iConfiguredCustomerCount, 5000);
            driverParamCheckGE(      "iActiveCustomerCount",     (int)cur_iActiveCustomerCount,     5000);
            driverParamCheckEqual(   "iActiveCustomerCount",     (int)cur_iActiveCustomerCount, (int)cur_iConfiguredCustomerCount);
            driverParamCheckDefault( dft_iScaleFactor, cur_iScaleFactor, "iScaleFactor" );
            driverParamCheckDefault( dft_iDaysOfInitialTrades, cur_iDaysOfInitialTrades, "iDaysOfInitialTrades" );
            return true;
        }catch(CheckException e){
            return false;
        }
    }
}

class DriverCESettings extends ParametersWithoutDefaults{
    
    public long cur_UniqueId;
    public long cur_TxnMixRNGSeed;
    public long cur_TxnInputRNGSeed;
    
    public DriverCESettings(long  uniqueID, long TxnMixRNGSeed, long TxnInputRNGSeed ){
        cur_UniqueId = uniqueID;
        cur_TxnMixRNGSeed = TxnMixRNGSeed;
        cur_TxnInputRNGSeed = TxnInputRNGSeed;
    }

    public DriverCESettings() {}

    public void checkValid(){}

    public void checkCompliant(){}
    
    public boolean isValid(){
        return true;
    }
    
    public boolean isCompliant(){
        return true;
    }
}

class DriverCEPartitionSettings extends ParametersWithDefaults{
 
    public long   dft_iMyStartingCustomerId;
    public long   dft_iMyCustomerCount;
    public int    dft_iPartitionPercent;
    
    public long   cur_iMyStartingCustomerId;
    public long   cur_iMyCustomerCount;
    public int    cur_iPartitionPercent;
    
    public boolean    state_iPartitionPercent;
    
    public DriverCEPartitionSettings(long  iMyStartingCustomerId, long  iMyCustomerCount, int iPartitionPercent ){
        initialize();

        cur_iMyStartingCustomerId = iMyStartingCustomerId;
        cur_iMyCustomerCount = iMyCustomerCount;
        cur_iPartitionPercent = iPartitionPercent;

        checkDefaults();
    }

    public DriverCEPartitionSettings(){
        initialize();

        cur_iMyStartingCustomerId = 0;
        cur_iMyCustomerCount = 0;
        cur_iPartitionPercent = 0;

        checkDefaults();
    }

    public void initializeDefaults(){
        dft_iMyStartingCustomerId = 1;   
        dft_iMyCustomerCount = 5000;     
        dft_iPartitionPercent = 50;     
    }

    public void setToDefaults(){ 
        dft_iMyStartingCustomerId = cur_iMyStartingCustomerId; 
        dft_iMyCustomerCount = cur_iMyCustomerCount;
        dft_iPartitionPercent = cur_iPartitionPercent;
        
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_iPartitionPercent = (cur_iPartitionPercent == dft_iPartitionPercent);
    }
    

    public boolean checkValid() {
        try{
            driverParamCheckBetween( "iPartitionPercent", cur_iPartitionPercent, 0, 100);
        }catch(CheckException e){
            return false;
        }
        
        if ( cur_iMyStartingCustomerId == 0 && cur_iMyCustomerCount == 0 && cur_iPartitionPercent == 0 ) {
            /* Partitioning Disabled:
             * - in this case, the default constructor would have been used and all values
             * are set to 0.  This must be considered valid.
             */
            return true;
        } else {
            
            try{
                driverParamCheckEqual( "iMyStartingCustomerId", (int)cur_iMyStartingCustomerId  % 1000,    1 );
                driverParamCheckGE(    "iMyCustomerCount",      (int)cur_iMyCustomerCount,              1000 );
                driverParamCheckEqual( "iMyCustomerCount",      (int)cur_iMyCustomerCount % 1000,          0 );
                return true;
            }catch(CheckException e){
                return false;
            }
            
        }
    }

    public boolean checkCompliant(){
        checkValid();

        if ( cur_iMyStartingCustomerId == 0 && cur_iMyCustomerCount == 0 && cur_iPartitionPercent == 0 ) {
            return true;
        } else {
            try{
                driverParamCheckDefault( dft_iPartitionPercent, cur_iPartitionPercent, "iPartitionPercent" );
                return true;
            }catch(CheckException e){
                return false;
            }
            
        }
    }
}

class DriverMEESettings extends ParametersWithoutDefaults{
 
    public long   cur_UniqueId;
    public long   cur_RNGSeed;
    public long   cur_TickerTapeRNGSeed;
    public long   cur_TradingFloorRNGSeed;
    
    public DriverMEESettings( long  uniqueID, long RNGSeed, long TickerTapeRNGSeed, long TradingFloorRNGSeed ){
        cur_UniqueId = uniqueID;
        cur_RNGSeed =  RNGSeed;
        cur_TickerTapeRNGSeed = TickerTapeRNGSeed;
        cur_TradingFloorRNGSeed = TradingFloorRNGSeed;
    }

    public DriverMEESettings() {}

    public void checkValid(){}

    public void checkCompliant(){}
    
    public boolean isValid(){
        return true;
    }
    
    public boolean isCompliant(){
        return true;
    }
}

class DriverDMSettings extends ParametersWithoutDefaults{
 
    public long   cur_UniqueId;
    public long   cur_RNGSeed;
    
    public DriverDMSettings( long  uniqueID, long RNGSeed ){
        cur_UniqueId = uniqueID;
        cur_RNGSeed =  RNGSeed;
    }

    public DriverDMSettings() {}

    public void checkValid(){}

    public void checkCompliant(){}
    
    public boolean isValid(){
        return true;
    }
    
    public boolean isCompliant(){
        return true;
    }
}

