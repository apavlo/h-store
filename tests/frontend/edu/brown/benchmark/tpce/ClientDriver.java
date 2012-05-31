/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.tpce;

import edu.brown.benchmark.tpce.TPCEConstants.DriverType;
import edu.brown.benchmark.tpce.generators.*;
//import java.util.Random;
import java.io.File;
import java.util.Arrays;
//import org.apache.log4j.Logger;
//import org.voltdb.types.TimestampType;


public class ClientDriver {
    /*The constructor should load all the files*/
//	private FileLoader fileloader;
	private int m_configuredCustomerCount;
	private int m_totalCustomerCount;
	private int m_scaleFactor;
	private int m_initialDays;
	private TBrokerVolumeTxnInput       m_BrokerVolumeTxnInput;
//	private CETxnInputGenerator         m_TxnInputGenerator;
	private TCustomerPositionTxnInput   m_CustomerPositionTxnInput;
	private TDataMaintenanceTxnInput    m_DataMaintenanceTxnInput;
	private TMarketFeedTxnInput         m_MarketFeedTxnInput;
	private TMarketWatchTxnInput        m_MarketWatchTxnInput;
	private TSecurityDetailTxnInput     m_SecurityDetailTxnInput;
	private TTradeCleanupTxnInput       m_TradeCleanupTxnInput;
	private TTradeLookupTxnInput        m_TradeLookupTxnInput;
	private TTradeOrderTxnInput         m_TradeOrderTxnInput;
	private TTradeResultTxnInput        m_TradeResultTxnInput;
	private TTradeStatusTxnInput        m_TradeStatusTxnInput;
	private TTradeUpdateTxnInput        m_TradeUpdateTxnInput;
	private TDriverCETxnSettings        m_DriverCETxnSettings;
	private EGenLogFormatterTab         m_LogFormat;
	private BaseLogger            		m_Logger;
	public CE                   		m_CustomerGenerator;
	private CESUTInterface       		m_Sut;
	private DM                    		m_DataMaintenanceGenerator;
	private DMSUTInterface        		m_DataMaintenanceCallback;
	public MEE                   		m_MarketExchangeGenerator;
	private MEESUTInterface       		m_MarketExchangeCallback;
	private SecurityHandler				m_SecurityHandler;
	//TODO Datetime.h
	public int HoursPerWorkDay = 8;
	
	public ClientDriver(String dataPath, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays){
		//TODO unnecessary
		m_configuredCustomerCount = configuredCustomerCount;
		m_totalCustomerCount = totalCustomerCount;
		m_scaleFactor = scaleFactor;
		m_initialDays = initialDays;
		
		String filename = new String("/tmp/EGenClientDriver.log");
		m_LogFormat = new EGenLogFormatterTab();
		m_Logger = new EGenLogger(DriverType.eDriverEGenLoader, 0, filename, m_LogFormat);
		
	    m_BrokerVolumeTxnInput = new TBrokerVolumeTxnInput();
		m_CustomerPositionTxnInput = new TCustomerPositionTxnInput();
		m_DataMaintenanceTxnInput = new TDataMaintenanceTxnInput();
		m_MarketFeedTxnInput = new TMarketFeedTxnInput();
		m_MarketWatchTxnInput = new TMarketWatchTxnInput();
		m_SecurityDetailTxnInput = new TSecurityDetailTxnInput();
		m_TradeCleanupTxnInput = new TTradeCleanupTxnInput();
		m_TradeLookupTxnInput = new TTradeLookupTxnInput();
		m_TradeOrderTxnInput = new TTradeOrderTxnInput();
		m_TradeResultTxnInput = new TTradeResultTxnInput();
		m_TradeStatusTxnInput = new TTradeStatusTxnInput();
		m_TradeUpdateTxnInput = new TTradeUpdateTxnInput();
	    m_DriverCETxnSettings = new TDriverCETxnSettings();
	    
		File inputDir = new File(dataPath);
	    TPCEGenerator inputFiles = new TPCEGenerator(inputDir, totalCustomerCount, scaleFactor, initialDays);
	    m_SecurityHandler = new SecurityHandler(inputFiles);
	    
	    //CE input generator
	    m_Sut = new SUT();
	    m_CustomerGenerator = new CE(m_Sut, m_Logger, inputFiles, configuredCustomerCount, totalCustomerCount, scaleFactor, initialDays, 0, m_DriverCETxnSettings);
/*	    m_TxnInputGenerator = new CETxnInputGenerator(inputFiles, m_configuredCustomerCount, m_totalCustomerCount, m_scaleFactor, 
	    		m_initialDays * HoursPerWorkDay, m_Logger, m_DriverCETxnSettings);
	    m_TxnInputGenerator.UpdateTunables();*/
	    
	    // DataMaintenance input generator
	    m_DataMaintenanceCallback = new DataMaintenanceCallback(m_DataMaintenanceTxnInput, m_TradeCleanupTxnInput);
	    m_DataMaintenanceGenerator = new DM(m_DataMaintenanceCallback, m_Logger, inputFiles, m_configuredCustomerCount, m_totalCustomerCount, m_scaleFactor, m_initialDays, 1);
	   	    
	    //MarketExchange input generator
	    m_MarketExchangeCallback = new MarketExchangeCallback(m_TradeResultTxnInput, m_MarketFeedTxnInput);
	    m_MarketExchangeGenerator = new MEE(0, m_MarketExchangeCallback, m_Logger, m_SecurityHandler, 1, configuredCustomerCount);
	    m_MarketExchangeGenerator.enableTickerTape();
	    
	}
	public TBrokerVolumeTxnInput generateBrokerVolumeInput() {
		System.out.println("Executing generateBrokerVolumeInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );
	    return (m_BrokerVolumeTxnInput);
	}
	
	public TCustomerPositionTxnInput generateCustomerPositionInput() {
		System.out.println("Executing generateCustomerPositionInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateCustomerPositionInput( m_CustomerPositionTxnInput );
	    return (m_CustomerPositionTxnInput);
	}

	public TDataMaintenanceTxnInput generateDataMaintenanceInput() {
	    System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    m_DataMaintenanceGenerator.DoTxn();
	    return (m_DataMaintenanceTxnInput);
	}

	public TMarketFeedTxnInput generateMarketFeedInput() {
	    System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
//	    m_TxnInputGenerator.GenerateMarketFeedInput( m_MarketFeedTxnInput );
	    return (m_MarketFeedTxnInput);
	}

	public TMarketWatchTxnInput generateMarketWatchInput() {
		System.out.println("Executing generateMarketWatchInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateMarketWatchInput( m_MarketWatchTxnInput );
	    return (m_MarketWatchTxnInput);
	}

	public TSecurityDetailTxnInput generateSecurityDetailInput() {
		System.out.println("Executing generateSecurityDetailInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateSecurityDetailInput( m_SecurityDetailTxnInput );
	    return (m_SecurityDetailTxnInput);
	}

	public TTradeCleanupTxnInput generateTradeCleanupInput() {
	    System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    m_DataMaintenanceGenerator.DoCleanupTxn();
	    return (m_TradeCleanupTxnInput);
	}

	public TTradeLookupTxnInput generateTradeLookupInput() {
		System.out.println("Executing generateTradeLookupInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateTradeLookupInput( m_TradeLookupTxnInput );
	    return (m_TradeLookupTxnInput);
	}

	public TTradeOrderTxnInput generateTradeOrderInput(int iTradeType, boolean bExecutorIsAccountOwner) {
		System.out.println("Executing generateTradeOrderInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateTradeOrderInput( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
	    return (m_TradeOrderTxnInput);
	}

	public TTradeResultTxnInput generateTradeResultInput() {
	    System.out.println("Executing %s...\n" + "generateTradeResultInput");
	    m_MarketExchangeGenerator.generateTradeResult();
	    return (m_TradeResultTxnInput);
	}

	public TTradeStatusTxnInput generateTradeStatusInput() {
		System.out.println("Executing generateTradeStatusInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateTradeStatusInput( m_TradeStatusTxnInput );
	    return (m_TradeStatusTxnInput);
	}

	public TTradeUpdateTxnInput generateTradeUpdateInput() {
		System.out.println("Executing generateTradeUpdateInput ... \n");
		m_CustomerGenerator.m_TxnInputGenerator.GenerateTradeUpdateInput( m_TradeUpdateTxnInput );
	    return (m_TradeUpdateTxnInput);
	}

}