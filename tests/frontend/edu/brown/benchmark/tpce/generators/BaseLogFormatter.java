package edu.brown.benchmark.tpce.generators;

public abstract class BaseLogFormatter {

	public abstract String GetLogOutput(BrokerVolumeSettings parms );
    public abstract String GetLogOutput(CustomerPositionSettings parms );
    public abstract String GetLogOutput(MarketWatchSettings parms );
    public abstract String GetLogOutput(SecurityDetailSettings parms );
    public abstract String GetLogOutput(TradeLookupSettings parms );
    public abstract String GetLogOutput(TradeOrderSettings parms );
    public abstract String GetLogOutput(TradeUpdateSettings parms );
    public abstract String GetLogOutput(TxnMixGeneratorSettings parms );
    public abstract String GetLogOutput(LoaderSettings parms );
    public abstract String GetLogOutput(DriverGlobalSettings parms );
    public abstract String GetLogOutput(DriverCESettings parms );
    public abstract String GetLogOutput(DriverCEPartitionSettings parms );
    public abstract String GetLogOutput(DriverMEESettings parms );
    public abstract String GetLogOutput(DriverDMSettings parms );
}
/*
class EGenLogFormatterTab extends BaseLogFormatter{
	////////////////////////////////////////////////////////////////
	// CE Transaction Settings
	////////////////////////////////////////////////////////////////
	
	public String GetLogOutput(BrokerVolumeSettings parms){
		String bufferedOutput = new String();
		
		bufferedOutput.concat(" \n");
		bufferedOutput.concat("Broker Volume Parameters:  NONE\n");
		return bufferedOutput;
		
	}
	
	public String GetLogOutput(CustomerPositionSettings parms ){
		String bufferedOutput = new String( );
		bufferedOutput.concat(" \n");
		bufferedOutput.concat("Customer Position Parameters:\n");
		bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?\n");
		bufferedOutput.concat("By Cust ID: " + parms.dft_by_cust_id + "\t" + parms.cur_by_cust_id + "\t" + (parms.state_by_cust_id ? "YES" : "NO") + "\n");
		bufferedOutput.concat("By Tax ID: " + parms.dft_by_tax_id + "\t" + parms.cur_by_tax_id + "\t" + (parms.state_by_tax_id ? "YES" : "NO") + "\n");
		bufferedOutput.concat("Get History: " + parms.dft_get_history + "\t" + parms.cur_get_history + "\t" + (parms.state_get_history ? "YES" : "NO") + "\n");
		return bufferedOutput;		
	}
	
	public String GetLogOutput(MarketWatchSettings parms ){
		String bufferedOutput = new String( );
		 
		bufferedOutput.concat(" \n");
		bufferedOutput.concat("Market Watch Parameters:\n");
		bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
		bufferedOutput.concat("By Account ID: " + parms.dft_by_acct_id + "\t" + parms.cur_by_acct_id + "\t" + (parms.state_by_acct_id ? "YES" : "NO") + "\n");
		bufferedOutput.concat("By Industry: " + parms.dft_by_industry + "\t" + parms.cur_by_industry + "\t" + (parms.state_by_industry ? "YES" : "NO") + "\n");
		bufferedOutput.concat("Get Watch List: " + parms.dft_by_watch_list + "\t" + parms.cur_by_watch_list + "\t" + (parms.state_by_watch_list ? "YES" : "NO") + "\n");
		return bufferedOutput;
	}
	
	public String GetLogOutput(SecurityDetailSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Security Detail Parameters:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("LOB Access Pct: " + parms.dft_LOBAccessPercentage + "\t" + parms.cur_LOBAccessPercentage + "\t" + (parms.state_LOBAccessPercentage ? "YES" : "NO") + "\n");
			return bufferedOutput;
	}
	
	public String GetLogOutput(TradeLookupSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Trade Lookup Parameters:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Frame 1 Pct:" + parms.dft_do_frame1 + "\t" + parms.cur_do_frame1 + "\t" + (parms.state_do_frame1 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 2 Pct:" + parms.dft_do_frame2 + "\t" + parms.cur_do_frame2 + "\t" + (parms.state_do_frame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 3 Pct:" + parms.dft_do_frame3 + "\t" + parms.cur_do_frame3 + "\t" + (parms.state_do_frame3 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 4 Pct:" + parms.dft_do_frame4 + "\t" + parms.cur_do_frame4 + "\t" + (parms.state_do_frame4 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 1 MaxRows:" + parms.dft_MaxRowsFrame1 + "\t" + parms.cur_MaxRowsFrame1 + "\t" + (parms.state_MaxRowsFrame1 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 2 MaxRows:" + parms.dft_MaxRowsFrame2 + "\t" + parms.cur_MaxRowsFrame2 + "\t" + (parms.state_MaxRowsFrame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 3 MaxRows:" + parms.dft_MaxRowsFrame3 + "\t" + parms.cur_MaxRowsFrame3 + "\t" + (parms.state_MaxRowsFrame3 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 4 MaxRows:" + parms.dft_MaxRowsFrame4 + "\t" + parms.cur_MaxRowsFrame4 + "\t" + (parms.state_MaxRowsFrame4 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("BackOffFromEndTimeFrame2:" + parms.dft_BackOffFromEndTimeFrame2 + "\t" + parms.cur_BackOffFromEndTimeFrame2 + "\t" + (parms.state_BackOffFromEndTimeFrame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("BackOffFromEndTimeFrame3:" + parms.dft_BackOffFromEndTimeFrame3 + "\t" + parms.cur_BackOffFromEndTimeFrame3 + "\t" + (parms.state_BackOffFromEndTimeFrame3 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("BackOffFromEndTimeFrame4:" + parms.dft_BackOffFromEndTimeFrame4 + "\t" + parms.cur_BackOffFromEndTimeFrame4 + "\t" + (parms.state_BackOffFromEndTimeFrame4 ? "YES" : "NO") + "\n");
			return bufferedOutput;
	}
	
	public String GetLogOutput(TradeOrderSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Trade Order Parameters:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Market Trade Pct:" + parms.dft_market + "\t" + parms.cur_market + "\t" + (parms.state_market ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Limit Trade Pct:" + parms.dft_limit + "\t" + parms.cur_limit + "\t" + (parms.state_limit ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Stop Loss Pct:" + parms.dft_stop_loss + "\t" + parms.cur_stop_loss + "\t" + (parms.state_stop_loss ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Security by Name Pct:" + parms.dft_security_by_name + "\t" + parms.cur_security_by_name + "\t" + (parms.state_security_by_name ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Security by Symbol Pct:" + parms.dft_security_by_symbol + "\t" + parms.cur_security_by_symbol + "\t" + (parms.state_security_by_symbol ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Buy Order Pct:" + parms.dft_buy_orders + "\t" + parms.cur_buy_orders + "\t" + (parms.state_buy_orders ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Sell Order Pct:" + parms.dft_sell_orders + "\t" + parms.cur_sell_orders + "\t" + (parms.state_sell_orders ? "YES" : "NO") + "\n");
			bufferedOutput.concat("LIFO Pct:" + parms.dft_lifo + "\t" + parms.cur_lifo + "\t" + (parms.state_lifo ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Margin Trade Pct:" + parms.dft_type_is_margin + "\t" + parms.cur_type_is_margin + "\t" + (parms.state_type_is_margin ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Executor as Owner Pct:" + parms.dft_exec_is_owner + "\t" + parms.cur_exec_is_owner + "\t" + (parms.state_exec_is_owner ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Rollback Pct:" + parms.dft_rollback + "\t" + parms.cur_rollback + "\t" + (parms.state_rollback ? "YES" : "NO") + "\n");
			return bufferedOutput;
	}
	
	public String GetLogOutput(TradeUpdateSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Trade Update Parameters:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Frame 1 Pct:" + parms.dft_do_frame1 + "\t" + parms.cur_do_frame1 + "\t" + (parms.state_do_frame1 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 2 Pct:" + parms.dft_do_frame2 + "\t" + parms.cur_do_frame2 + "\t" + (parms.state_do_frame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 3 Pct:" + parms.dft_do_frame3 + "\t" + parms.cur_do_frame3 + "\t" + (parms.state_do_frame3 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 1 MaxRows:" + parms.dft_MaxRowsFrame1 + "\t" + parms.cur_MaxRowsFrame1 + "\t" + (parms.state_MaxRowsFrame1 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 2 MaxRows:" + parms.dft_MaxRowsFrame2 + "\t" + parms.cur_MaxRowsFrame2 + "\t" + (parms.state_MaxRowsFrame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 3 MaxRows:" + parms.dft_MaxRowsFrame3 + "\t" + parms.cur_MaxRowsFrame3 + "\t" + (parms.state_MaxRowsFrame3 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 1 MaxRowsToUpdate:" + parms.dft_MaxRowsToUpdateFrame1 + "\t" + parms.cur_MaxRowsToUpdateFrame1 + "\t" + (parms.state_MaxRowsToUpdateFrame1 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 2 MaxRowsToUpdate:" + parms.dft_MaxRowsToUpdateFrame2 + "\t" + parms.cur_MaxRowsToUpdateFrame2 + "\t" + (parms.state_MaxRowsToUpdateFrame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Frame 3 MaxRowsToUpdate:" + parms.dft_MaxRowsToUpdateFrame3 + "\t" + parms.cur_MaxRowsToUpdateFrame3 + "\t" + (parms.state_MaxRowsToUpdateFrame3 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("BackOffFromEndTimeFrame2:" + parms.dft_BackOffFromEndTimeFrame2 + "\t" + parms.cur_BackOffFromEndTimeFrame2 + "\t" + (parms.state_BackOffFromEndTimeFrame2 ? "YES" : "NO") + "\n");
			bufferedOutput.concat("BackOffFromEndTimeFrame3:" + parms.dft_BackOffFromEndTimeFrame3 + "\t" + parms.cur_BackOffFromEndTimeFrame3 + "\t" + (parms.state_BackOffFromEndTimeFrame3 ? "YES" : "NO") + "\n");
			
			return bufferedOutput;
	}
	
	////////////////////////////////////////////////////////////////
	// CE Transaction Mix Settings
	////////////////////////////////////////////////////////////////
	
	public String GetLogOutput(TxnMixGeneratorSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Transaction Mixes:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Broker Volume: " + parms.dft_BrokerVolumeMixLevel + "\t" + parms.cur_BrokerVolumeMixLevel + "\t" + (parms.state_BrokerVolumeMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Customer Position: " + parms.dft_CustomerPositionMixLevel + "\t" + parms.cur_CustomerPositionMixLevel + "\t" + (parms.state_CustomerPositionMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Market Watch: " + parms.dft_MarketWatchMixLevel + "\t" + parms.cur_MarketWatchMixLevel + "\t" + (parms.state_MarketWatchMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Security Detail: " + parms.dft_SecurityDetailMixLevel + "\t" + parms.cur_SecurityDetailMixLevel + "\t" + (parms.state_SecurityDetailMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Trade Lookup: " + parms.dft_TradeLookupMixLevel + "\t" + parms.cur_TradeLookupMixLevel + "\t" + (parms.state_TradeLookupMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Trade Order: " + parms.dft_TradeOrderMixLevel + "\t" + parms.cur_TradeOrderMixLevel + "\t" + (parms.state_TradeOrderMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Trade Status: " + parms.dft_TradeStatusMixLevel + "\t" + parms.cur_TradeStatusMixLevel + "\t" + (parms.state_TradeStatusMixLevel ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Trade Update: " + parms.dft_TradeUpdateMixLevel + "\t" + parms.cur_TradeUpdateMixLevel + "\t" + (parms.state_TradeUpdateMixLevel ? "YES" : "NO") + "\n");
			
			return bufferedOutput;
	}
	
	////////////////////////////////////////////////////////////////
	// Loader Settings
	////////////////////////////////////////////////////////////////
	
	public String GetLogOutput(LoaderSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Loader Settings:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Configured Customers: " + parms.dft_iConfiguredCustomerCount + "\t" + parms.cur_iConfiguredCustomerCount + "\t" + (parms.state_iConfiguredCustomerCount ? "YES" : "NO") + "\n");
			if(parms.cur_iConfiguredCustomerCount != parms.cur_iActiveCustomerCount){
				bufferedOutput.concat("Active Customers:" + parms.dft_iActiveCustomerCount + "\t" + parms.cur_iActiveCustomerCount + "\t" + (parms.state_iActiveCustomerCount ? "YES" : "NO") + "\n");
			}
			bufferedOutput.concat("Starting Customer:" + parms.dft_iStartingCustomer + "\t" + parms.cur_iStartingCustomer + "\t" + (parms.state_iStartingCustomer ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Customer Count: " + parms.dft_iCustomerCount + "\t" + parms.cur_iCustomerCount + "\t" + (parms.state_iCustomerCount ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Scale Factor:" + parms.dft_iScaleFactor + "\t" + parms.cur_iScaleFactor + "\t" + (parms.state_iScaleFactor ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Days of Initial Trades:" + parms.dft_iDaysOfInitialTrades + "\t" + parms.cur_iDaysOfInitialTrades + "\t" + (parms.state_iDaysOfInitialTrades ? "YES" : "NO") + "\n");
			
			return bufferedOutput;
	}
	
	////////////////////////////////////////////////////////////////
	// Driver Settings
	////////////////////////////////////////////////////////////////
	
	public String GetLogOutput(DriverGlobalSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Driver Global Settings:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Configured Customers: " + parms.dft_iConfiguredCustomerCount + "\t" + parms.cur_iConfiguredCustomerCount + "\t" + (parms.state_iConfiguredCustomerCount ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Active Customers:" + parms.dft_iActiveCustomerCount + "\t" + parms.cur_iActiveCustomerCount + "\t" + (parms.state_iActiveCustomerCount ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Scale Factor:" + parms.dft_iScaleFactor + "\t" + parms.cur_iScaleFactor + "\t" + (parms.state_iScaleFactor ? "YES" : "NO") + "\n");
			bufferedOutput.concat("Days of Initial Trades:" + parms.dft_iDaysOfInitialTrades + "\t" + parms.cur_iDaysOfInitialTrades + "\t" + (parms.state_iDaysOfInitialTrades ? "YES" : "NO") + "\n");
			
			return bufferedOutput;
	}
	
	public String GetLogOutput(DriverCESettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Driver CE Settings:\n");
			bufferedOutput.concat("Parameter" + "Value"  + "\n");
			bufferedOutput.concat("Unique ID:" + parms.cur_UniqueId + "\n");
			bufferedOutput.concat("Txn Mix RNGSeed:" + parms.cur_TxnMixRNGSeed + "\n");
			bufferedOutput.concat("Txn Input RNGSeed:" + parms.cur_TxnInputRNGSeed + "\n");
			return bufferedOutput;
	}
	
	public String GetLogOutput(DriverCEPartitionSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Driver CE Partition Settings:\n");
			bufferedOutput.concat("Parameter Default" + "\t" + "Current" + "\t" + "Default?" + "\n");
			bufferedOutput.concat("Partition Starting Customer ID:" + parms.dft_iMyStartingCustomerId + "\t" + parms.cur_iMyStartingCustomerId + "\n");
			bufferedOutput.concat("Partition Customer Count:" + parms.dft_iMyCustomerCount + "\t" + parms.cur_iMyCustomerCount + "\n");
			bufferedOutput.concat("Partition Percent:" + parms.dft_iPartitionPercent + "\t" + parms.cur_iPartitionPercent + (parms.state_iPartitionPercent ? "YES" : "NO") + "\n");
			return bufferedOutput;
	}
	
	public String GetLogOutput(DriverMEESettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Driver MEE Settings:\n");
			bufferedOutput.concat("Parameter" + "Value"  + "\n");
			bufferedOutput.concat("Unique ID:" + parms.cur_UniqueId + "\n");
			bufferedOutput.concat("Ticker Tape RNGSeed:" + parms.cur_TickerTapeRNGSeed + "\n");
			bufferedOutput.concat("Trading Floor RNGSeed:" + parms.cur_TradingFloorRNGSeed + "\n");
			
			return bufferedOutput;
	}
	
	public String GetLogOutput(DriverDMSettings parms ){
		String bufferedOutput = new String( );
		 
			bufferedOutput.concat(" \n");
			bufferedOutput.concat("Driver DM Settings:\n");
			bufferedOutput.concat("Unique ID:" + parms.cur_UniqueId + "\n");
			bufferedOutput.concat("RNGSeed:" + parms.cur_RNGSeed + "\n");
			return bufferedOutput;
	}
}*/
