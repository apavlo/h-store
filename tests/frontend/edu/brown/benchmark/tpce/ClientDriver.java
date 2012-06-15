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
import java.io.File;


public class ClientDriver {
	
	public ClientDriver(String dataPath, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays){
		
//		String filename = new String("/tmp/EGenClientDriver.log");
		logFormat = new EGenLogFormatterTab();
		logger = new EGenLogger(DriverType.eDriverEGenLoader, 0, logFormat);
	    brokerVolumeTxnInput = new TBrokerVolumeTxnInput();
		customerPositionTxnInput = new TCustomerPositionTxnInput();
		dataMaintenanceTxnInput = new TDataMaintenanceTxnInput();
		marketFeedTxnInput = new TMarketFeedTxnInput();
		marketWatchTxnInput = new TMarketWatchTxnInput();
		securityDetailTxnInput = new TSecurityDetailTxnInput();
		tradeCleanupTxnInput = new TTradeCleanupTxnInput();
		tradeLookupTxnInput = new TTradeLookupTxnInput();
		tradeOrderTxnInput = new TTradeOrderTxnInput();
		tradeResultTxnInput = new TTradeResultTxnInput();
		tradeStatusTxnInput = new TTradeStatusTxnInput();
		tradeUpdateTxnInput = new TTradeUpdateTxnInput();
	    driverCETxnSettings = new TDriverCETxnSettings();
	    
		File inputDir = new File(dataPath);
	    TPCEGenerator inputFiles = new TPCEGenerator(inputDir, totalCustomerCount, scaleFactor, initialDays);
	    securityHandler = new SecurityHandler(inputFiles);
	    
	    //CE input generator
	    sut = new SUT();
	    cutomerEmulator = new CE(sut, logger, inputFiles, configuredCustomerCount, totalCustomerCount, scaleFactor, initialDays, 0, driverCETxnSettings);
	    
	    // DataMaintenance input generator
	    dataMaintenanceCallback = new DataMaintenanceCallback(dataMaintenanceTxnInput, tradeCleanupTxnInput);
	    dataMaintenanceGenerator = new DM(dataMaintenanceCallback, logger, inputFiles, configuredCustomerCount, totalCustomerCount, scaleFactor, initialDays, 1);
	   	    
	    //MarketExchange input generator
	    marketExchangeCallback = new MarketExchangeCallback(tradeResultTxnInput, marketFeedTxnInput);
	    marketExchangeGenerator = new MEE(0, marketExchangeCallback, logger, securityHandler, 1, configuredCustomerCount);
	    marketExchangeGenerator.enableTickerTape();
	    
	}
	public CE getCE(){
		return cutomerEmulator;
	}
	public MEE getMEE(){
		return marketExchangeGenerator;
	}
	public TBrokerVolumeTxnInput generateBrokerVolumeInput() {
//		System.out.println("Executing generateBrokerVolumeInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateBrokerVolumeInput( brokerVolumeTxnInput );
	    return (brokerVolumeTxnInput);
	}
	
	public TCustomerPositionTxnInput generateCustomerPositionInput() {
//		System.out.println("Executing generateCustomerPositionInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateCustomerPositionInput( customerPositionTxnInput );
	    return (customerPositionTxnInput);
	}

	public TDataMaintenanceTxnInput generateDataMaintenanceInput() {
//	    System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    dataMaintenanceGenerator.DoTxn();
	    return (dataMaintenanceTxnInput);
	}

	public TMarketFeedTxnInput generateMarketFeedInput() {
//	    System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    return (marketFeedTxnInput);
	}

	public TMarketWatchTxnInput generateMarketWatchInput() {
//		System.out.println("Executing generateMarketWatchInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateMarketWatchInput( marketWatchTxnInput );
	    return (marketWatchTxnInput);
	}

	public TSecurityDetailTxnInput generateSecurityDetailInput() {
//		System.out.println("Executing generateSecurityDetailInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateSecurityDetailInput( securityDetailTxnInput );
	    return (securityDetailTxnInput);
	}

	public TTradeCleanupTxnInput generateTradeCleanupInput() {
//	    System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    dataMaintenanceGenerator.DoCleanupTxn();
	    return (tradeCleanupTxnInput);
	}

	public TTradeLookupTxnInput generateTradeLookupInput() {
//		System.out.println("Executing generateTradeLookupInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateTradeLookupInput( tradeLookupTxnInput );
	    return (tradeLookupTxnInput);
	}

	public TTradeOrderTxnInput generateTradeOrderInput(int tradeType, boolean bExecutorIsAccountOwner) {
//		System.out.println("Executing generateTradeOrderInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateTradeOrderInput( tradeOrderTxnInput, tradeType, bExecutorIsAccountOwner );
	    return (tradeOrderTxnInput);
	}

	public TTradeResultTxnInput generateTradeResultInput() {
//	    System.out.println("Executing %s...\n" + "generateTradeResultInput");
	    marketExchangeGenerator.generateTradeResult();
	    return (tradeResultTxnInput);
	}

	public TTradeStatusTxnInput generateTradeStatusInput() {
//		System.out.println("Executing generateTradeStatusInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateTradeStatusInput( tradeStatusTxnInput );
	    return (tradeStatusTxnInput);
	}

	public TTradeUpdateTxnInput generateTradeUpdateInput() {
//		System.out.println("Executing generateTradeUpdateInput ... \n");
		cutomerEmulator.getCETxnInputGenerator().generateTradeUpdateInput( tradeUpdateTxnInput );
	    return (tradeUpdateTxnInput);
	}
	
	private TBrokerVolumeTxnInput       brokerVolumeTxnInput;
	private TCustomerPositionTxnInput   customerPositionTxnInput;
	private TDataMaintenanceTxnInput    dataMaintenanceTxnInput;
	private TMarketFeedTxnInput         marketFeedTxnInput;
	private TMarketWatchTxnInput        marketWatchTxnInput;
	private TSecurityDetailTxnInput     securityDetailTxnInput;
	private TTradeCleanupTxnInput       tradeCleanupTxnInput;
	private TTradeLookupTxnInput        tradeLookupTxnInput;
	private TTradeOrderTxnInput         tradeOrderTxnInput;
	private TTradeResultTxnInput        tradeResultTxnInput;
	private TTradeStatusTxnInput        tradeStatusTxnInput;
	private TTradeUpdateTxnInput        tradeUpdateTxnInput;
	private TDriverCETxnSettings        driverCETxnSettings;
	private EGenLogFormatterTab         logFormat;
	private BaseLogger            		logger;
	private CE                   		cutomerEmulator;
	private CESUTInterface       		sut;
	private DM                    		dataMaintenanceGenerator;
	private DMSUTInterface        		dataMaintenanceCallback;
	private MEE                   		marketExchangeGenerator;
	private MEESUTInterface       		marketExchangeCallback;
	private SecurityHandler				securityHandler;

}