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
package edu.brown.benchmark.tpceb;

import edu.brown.benchmark.tpceb.TPCEConstants.DriverType;
import edu.brown.benchmark.tpceb.TPCEConstants.eMEETradeRequestAction;
import edu.brown.benchmark.tpceb.generators.*;

import java.io.File;


public class ClientDriver {
    
    public ClientDriver(String dataPath, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays){
        
//      String filename = new String("/tmp/EGenClientDriver.log");
        logFormat = new EGenLogFormatterTab();
        logger = new EGenLogger(DriverType.eDriverEGenLoader, 0, logFormat);
  
        tradeOrderTxnInput = new TTradeOrderTxnInput();

        driverCETxnSettings = new TDriverCETxnSettings();
        
        marketWatchTxnInput = new TMarketWatchTxnInput(); //commented out
        tradeResultTxnInput = new TTradeResultTxnInput();
        marketFeedTxnInput = new TMarketFeedTxnInput();
        
        File inputDir = new File(dataPath);
        TPCEGenerator inputFiles = new TPCEGenerator(inputDir, totalCustomerCount, scaleFactor, initialDays);
        
        securityHandler = new SecurityHandler(inputFiles);
        
        //CE input generator
        sut = new SUT();
        customerEmulator = new CE(sut, logger, inputFiles, configuredCustomerCount, totalCustomerCount, scaleFactor, initialDays, 0, driverCETxnSettings);
        
        marketExchangeCallback = new MarketExchangeCallback(tradeResultTxnInput, marketFeedTxnInput);
        marketExchangeGenerator = new MEE(0, marketExchangeCallback, logger, securityHandler, 1, configuredCustomerCount);
        
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        pSendToMarket = new SendToMarket(marketExchangeGenerator); //ADDED!!!!!!!!!!!!!!!!!!!!!!!!
        //customerEmulator.setSendToMarket(pSendToMarket);
        /*for debugging*/
       /* tradeReq = new TTradeRequest();
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "HYBD";
        tradeReq.trade_id = 200000000173804l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 100.0;
        tradeReq.symbol = "LARS";
        tradeReq.trade_id = 200000000173806l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        /*This one*/
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
     /*   tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 1000.0;
        tradeReq.symbol = "BRY";
        tradeReq.trade_id = 200000000173803l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CGA";
        tradeReq.trade_id = 200000000173809l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CGA";
        tradeReq.trade_id = 200000000173818l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "ORCC";
        tradeReq.trade_id = 200000000173801l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "MCH";
        tradeReq.trade_id = 200000000173802l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CVAS";
        tradeReq.trade_id = 200000000173810l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        ///tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "BANC";
        tradeReq.trade_id = 200000000173810l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);

        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "BDK";
        tradeReq.trade_id = 200000000173811l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        

        /*This one*/
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
       /* tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "GUPB";
        tradeReq.trade_id = 200000000173840l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        

        tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        //tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CHTR";
        tradeReq.trade_id = 200000000173861l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        //tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10000.0;
        tradeReq.symbol = "AVZ";
        tradeReq.trade_id = 200000000173879l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        //tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "WFC";
        tradeReq.trade_id = 200000000173893l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TMS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "GD";
        tradeReq.trade_id = 200000000173890l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "EXBT";
        tradeReq.trade_id = 200000000173811l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CFFC";
        tradeReq.trade_id = 200000000173811l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CATY";
        tradeReq.trade_id = 200000000173850l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
        //tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CSPI";
        tradeReq.trade_id = 200000000173853l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);
        
        
       // tradeReq.eAction = eMEETradeRequestAction.eMEESetLimitOrderTrigger;
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "CHFN";
        tradeReq.trade_id = 200000000173243l;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "TLS";
        
        marketExchangeGenerator.submitTradeRequest(tradeReq);*/
      
        
        

        
        marketExchangeGenerator.enableTickerTape();   
        
    }
    
    public CE getCE(){
        return customerEmulator;
    }
    
    public MEE getMEE(){
        return marketExchangeGenerator;
    }

    /***ADDED!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!***/
    public SendToMarket getSendToMarket(){     /*****/
        return pSendToMarket;                  /*****/
    }                                          /*****/
    /*************************************** ********/
    
    /****MODIFIED!!!! now takes send to market obj too!!!!!!!!!!!!!!!!!*****************/
   public TTradeOrderTxnInput generateTradeOrderInput(int tradeType) {
        customerEmulator.getCETxnInputGenerator().generateTradeOrderInput( tradeOrderTxnInput, tradeType );
        return (tradeOrderTxnInput);
    }
   /***************************************************************************************/
   
   //commented out
   public TMarketWatchTxnInput generateMarketWatchInput() {
//      System.out.println("Executing generateMarketWatchInput ... \n");
        customerEmulator.getCETxnInputGenerator().generateMarketWatchInput( marketWatchTxnInput );
        return (marketWatchTxnInput);
    }
    
   public TMarketFeedTxnInput generateMarketFeedInput() {
//      System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
        return (marketFeedTxnInput);
    }
    
    public TTradeResultTxnInput generateTradeResultInput() {
//      System.out.println("Executing %s...\n" + "generateTradeResultInput");
        marketExchangeGenerator.generateTradeResult();
        return (tradeResultTxnInput);
    }

   public TTradeRequest tradeReq;
   private TTradeOrderTxnInput         tradeOrderTxnInput;
   private TTradeResultTxnInput        tradeResultTxnInput;
   private TMarketWatchTxnInput        marketWatchTxnInput;
   private TMarketFeedTxnInput         marketFeedTxnInput;
    
   private TDriverCETxnSettings        driverCETxnSettings;
   private EGenLogFormatterTab         logFormat;
   private BaseLogger                  logger;
   private CE                          customerEmulator;
   private CESUTInterface              sut;

   private MEE                         marketExchangeGenerator;
   private MEESUTInterface             marketExchangeCallback;
   private MEETradingFloor  marketExchangeTradingFloor;
    
   private SecurityHandler             securityHandler;
   
   //*!*!*!*!*!*!*!*!*!*!*
   private SendToMarket                 pSendToMarket; //ADDED***********************

}

/*package edu.brown.benchmark.tpceb;

import edu.brown.benchmark.tpceb.TPCEConstants.DriverType;
import edu.brown.benchmark.tpceb.generators.*;
import java.io.File;


public class ClientDriver {
    
    public ClientDriver(String dataPath, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays){
        
//      String filename = new String("/tmp/EGenClientDriver.log");
        logFormat = new EGenLogFormatterTab();
        logger = new EGenLogger(DriverType.eDriverEGenLoader, 0, logFormat);
  
        tradeOrderTxnInput = new TTradeOrderTxnInput();

        driverCETxnSettings = new TDriverCETxnSettings();
        
        File inputDir = new File(dataPath);
        TPCEGenerator inputFiles = new TPCEGenerator(inputDir, totalCustomerCount, scaleFactor, initialDays);
        securityHandler = new SecurityHandler(inputFiles);
        
        //CE input generator
        sut = new SUT();
        cutomerEmulator = new CE(sut, logger, inputFiles, configuredCustomerCount, totalCustomerCount, scaleFactor, initialDays, 0, driverCETxnSettings);

        
    }
    public CE getCE(){
        return cutomerEmulator;
    }


    public TTradeOrderTxnInput generateTradeOrderInput(int tradeType) {
        cutomerEmulator.getCETxnInputGenerator().generateTradeOrderInput( tradeOrderTxnInput, tradeType );
        return (tradeOrderTxnInput);
    }

    private TTradeOrderTxnInput         tradeOrderTxnInput;

    private TDriverCETxnSettings        driverCETxnSettings;
    private EGenLogFormatterTab         logFormat;
    private BaseLogger                  logger;
    private CE                          cutomerEmulator;
    private CESUTInterface              sut;

    private MEESUTInterface             MEEsut;
    private SecurityHandler             securityHandler;

}*/