package edu.brown.benchmark.tpceb.generators;

import edu.brown.benchmark.tpceb.*;

public class TDriverCETxnSettings {

    public TradeOrderSettings         TO_settings;

    TxnMixGeneratorSettings    TxnMixGenerator_settings;

    public TDriverCETxnSettings(){
 
        TO_settings = new TradeOrderSettings();
   
        TxnMixGenerator_settings = new TxnMixGeneratorSettings();
    }
    public boolean isValid(){
        boolean isValid = true;
  
        isValid &= TO_settings.checkValid();
   
        isValid &= TxnMixGenerator_settings.checkValid();
        return isValid;
      
    }

    public void checkCompliant(){
   
        TO_settings.checkCompliant();
  
        TxnMixGenerator_settings.checkCompliant();
    }
}

/******************************************************************************
*   Parameter Derived Class / Template Instantiation
******************************************************************************/

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
    
        dft_security_by_symbol = 60;
        dft_buy_orders = 50;
        dft_sell_orders = 50;
        dft_lifo = 35;
    
        dft_rollback = 1;

    }

    public void setToDefaults(){ 
        cur_market = dft_market; 
        cur_limit = dft_limit;
        cur_stop_loss = dft_stop_loss;
     
        cur_security_by_symbol = dft_security_by_symbol;
        cur_buy_orders = dft_buy_orders;
        cur_sell_orders = dft_sell_orders;
        cur_lifo = dft_lifo;
     
        cur_rollback = dft_rollback;
 
        checkDefaults();
    }
    
    public void checkDefaults(){
        state_market = (cur_market == dft_market);
        state_limit = (cur_limit == dft_limit);
        state_stop_loss = (cur_stop_loss == dft_stop_loss);
     
        state_security_by_symbol = (cur_security_by_symbol == dft_security_by_symbol);
        state_buy_orders = (cur_buy_orders == dft_buy_orders);
        state_sell_orders = (cur_sell_orders == dft_sell_orders);
        state_lifo = (cur_lifo == dft_lifo);
     
        state_rollback = (cur_rollback == dft_rollback);
     
    }

    public boolean checkValid() {
        try{
            driverParamCheckBetween( "market",                cur_market,                                      0, 100);
            driverParamCheckBetween( "limit",                 cur_limit,                                       0, 100);
            driverParamCheckEqual(   "market or limit total", cur_market + cur_limit,                             100);
            driverParamCheckBetween( "stop_loss",             cur_stop_loss,                                   0, 100);
       
            driverParamCheckBetween( "security_by_symbol",    cur_security_by_symbol,                          0, 100);
      
            driverParamCheckBetween( "buy_orders",            cur_buy_orders,                                  0, 100);
            driverParamCheckBetween( "sell_orders",           cur_sell_orders,                                 0, 100);
            driverParamCheckEqual(   "*_orders total",        cur_buy_orders + cur_sell_orders,                   100);
            driverParamCheckBetween( "lifo",                  cur_lifo,                                        0, 100);
      
            driverParamCheckBetween( "rollback",              cur_rollback,                                    0, 100);
            return true;
        }catch(CheckException e){
            return false;
        }
    }

    public boolean checkCompliant() {
        checkValid();
        try{
         
            driverParamCheckDefault( dft_market, cur_market, "market" );
            System.out.println("past market valid");
            driverParamCheckDefault( dft_limit, cur_limit, "limit" );
            System.out.println("limit valid");
            driverParamCheckDefault( dft_stop_loss, cur_stop_loss, "stop_loss" );
            System.out.println("stop loss");
       
            driverParamCheckDefault( dft_security_by_symbol, cur_security_by_symbol, "security_by_symbol" );
            System.out.println("security symbol");
            driverParamCheckDefault( dft_buy_orders, cur_buy_orders, "buy_orders" );
            System.out.println("buy orders done");
            driverParamCheckDefault( dft_sell_orders, cur_sell_orders, "sell_orders" );
            System.out.println("sell orders done");
            driverParamCheckDefault( dft_lifo, cur_lifo, "lifo" );
        
            driverParamCheckDefault( dft_rollback, cur_rollback, "rollback" );
         
            return true;
        }catch(CheckException e){
            return false; 
        }      
    }
}


class TxnMixGeneratorSettings extends ParametersWithDefaults{
 

    public int dft_TradeOrderMixLevel;
  
    public int cur_TradeOrderMixLevel;
  
    public boolean    state_TradeOrderMixLevel;

    public TxnMixGeneratorSettings(){
        initialize();
    }

    public void initializeDefaults(){
   
        dft_TradeOrderMixLevel          =  101;
 
    }

    public void setToDefaults(){ 
   
        cur_TradeOrderMixLevel = dft_TradeOrderMixLevel;
  
        
        checkDefaults();
    }
    
    public void checkDefaults(){
  
        state_TradeOrderMixLevel       = (cur_TradeOrderMixLevel       == dft_TradeOrderMixLevel);
   
    }

    public boolean checkValid(){
        try{
            driverParamCheckGE( "TradeOrderMixLevel",       cur_TradeOrderMixLevel,       0 );
            return true;
        }catch(CheckException e){
            return false;
        }
        
    }

    public boolean checkCompliant(){
        checkValid();
        try{
             driverParamCheckDefault( dft_TradeOrderMixLevel,cur_TradeOrderMixLevel, "TradeOrderMixLevel" );
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

