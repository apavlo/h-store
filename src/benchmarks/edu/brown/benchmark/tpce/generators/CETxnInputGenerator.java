package edu.brown.benchmark.tpce.generators;

import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;

import edu.brown.benchmark.tpce.util.*;
import edu.brown.benchmark.tpce.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpce.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;


public class CETxnInputGenerator {
	
    
    public CETxnInputGenerator( TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
                    int scaleFactor, int hoursOfInitialTrades, BaseLogger logger, TDriverCETxnSettings driverCETxnSettings){
    	inputFiles.parseInputFiles();
        rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR);
        person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
        customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
        account = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
        accountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
        holdings = new HoldingsAndTrades(inputFiles);
        brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);
        companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
        securities = new SecurityHandler(inputFiles);
        industries = inputFiles.getInputFile(InputFile.INDUSTRY);
        sectors = inputFiles.getInputFile(InputFile.SECTOR);
        statusType = inputFiles.getInputFile(InputFile.STATUS);
        tradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
        this.driverCETxnSettings = driverCETxnSettings;
        this.logger = logger;
        this.configuredCustomerCount = configuredCustomerCount;
        this.activeCustomerCount = activeCustomerCount;
        startingCustomerID = TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        this.myCustomerCount = activeCustomerCount;
        partitionPercent = 100;
        this.scaleFactor = scaleFactor;
        this.hoursOfInitialTrades = hoursOfInitialTrades;
        
        initialize();
    }

    
      public CETxnInputGenerator( TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
                        int scaleFactor, int hoursOfInitialTrades, long RNGSeed, BaseLogger logger,
                        TDriverCETxnSettings driverCETxnSettings){
    	inputFiles.parseInputFiles();
    	rnd = new EGenRandom(RNGSeed);   //initialize with a default seed
        person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
        customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);    
        account = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
        accountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
        holdings = new HoldingsAndTrades(inputFiles); 
        brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	
        companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
        securities = new SecurityHandler(inputFiles);
        industries = inputFiles.getInputFile(InputFile.INDUSTRY);
        sectors = inputFiles.getInputFile(InputFile.SECTOR);
        statusType = inputFiles.getInputFile(InputFile.STATUS);
        tradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
        this.driverCETxnSettings = driverCETxnSettings;
        this.logger = logger;
        this.configuredCustomerCount = configuredCustomerCount;
        this.activeCustomerCount = activeCustomerCount;
        startingCustomerID = TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        this.myCustomerCount = activeCustomerCount;
        partitionPercent = 100;
        this.scaleFactor = scaleFactor;
        this.hoursOfInitialTrades = hoursOfInitialTrades;
        
        initialize();
    }
        
   
      public CETxnInputGenerator( TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
                        int scaleFactor, int hoursOfInitialTrades,  long startingCustomerID, long myCustomerCount, int partitionPercent,
                        BaseLogger logger, TDriverCETxnSettings driverCETxnSettings){
    	  inputFiles.parseInputFiles();
    	  rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR);   //initialize with a default seed
          person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
          customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
          
          account = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
          accountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
          holdings = new HoldingsAndTrades(inputFiles);
          
          brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	
          companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
          securities = new SecurityHandler(inputFiles);
          industries = inputFiles.getInputFile(InputFile.INDUSTRY);
          sectors = inputFiles.getInputFile(InputFile.SECTOR);
          statusType = inputFiles.getInputFile(InputFile.STATUS);
          tradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
          this.driverCETxnSettings = driverCETxnSettings;
          this.logger = logger;
          this.configuredCustomerCount = configuredCustomerCount;
          this.activeCustomerCount = activeCustomerCount;
          this.startingCustomerID = startingCustomerID;
          this.myCustomerCount = myCustomerCount;
          this.partitionPercent = partitionPercent;
          this.scaleFactor = scaleFactor;
          this.hoursOfInitialTrades = hoursOfInitialTrades;
          initialize();
      }

    
      public CETxnInputGenerator( TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
                        int scaleFactor, int hoursOfInitialTrades, long startingCustomerID, long myCustomerCount, int partitionPercent,
                        long RNGSeed, BaseLogger logger, TDriverCETxnSettings driverCETxnSettings){
    	  inputFiles.parseInputFiles();
    	  rnd = new EGenRandom(RNGSeed);   //initialize with a default seed
          person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
          customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
          
          account = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
          accountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
          holdings = new HoldingsAndTrades(inputFiles);
          
          brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);
          companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
          securities = new SecurityHandler(inputFiles);
          industries = inputFiles.getInputFile(InputFile.INDUSTRY);
          sectors = inputFiles.getInputFile(InputFile.SECTOR);
          statusType = inputFiles.getInputFile(InputFile.STATUS);
          tradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
          this.driverCETxnSettings = driverCETxnSettings;
          this.logger = logger;
          this.configuredCustomerCount = configuredCustomerCount;
          this.activeCustomerCount = activeCustomerCount;
          this.startingCustomerID = startingCustomerID;
          this.myCustomerCount = myCustomerCount;
          this.partitionPercent = partitionPercent;
          this.scaleFactor = scaleFactor;
          this.hoursOfInitialTrades = hoursOfInitialTrades;
          initialize();
      }

    public void initialize(){
        activeCompanyCount = companies.getCompanyCount();
        activeSecurityCount = SecurityHandler.getSecurityNum(myCustomerCount);
        industryCount = industries.getMaxKey();
        sectorCount = sectors.getMaxKey();
        startFromCompany = companies.generateCompId();

        maxActivePrePopulatedTradeID = (int)(( hoursOfInitialTrades * EGenDate.SecondsPerHour * ( activeCustomerCount / scaleFactor )) * TPCEConstants.AbortTrade / 100 );
        currentTradeID = new AtomicLong(maxActivePrePopulatedTradeID + 1);
        startTime = EGenDate.getDateFromTime(
        		TPCEConstants.initialTradePopulationBaseYear,
        		TPCEConstants.initialTradePopulationBaseMonth,
        		TPCEConstants.initialTradePopulationBaseDay,
        		TPCEConstants.initialTradePopulationBaseHour,
        		TPCEConstants.initialTradePopulationBaseMinute,
        		TPCEConstants.initialTradePopulationBaseSecond,
        		TPCEConstants.initialTradePopulationBaseFraction );
    }

   
    public long getRNGSeed(){
        return( rnd.getSeed() );
    }

    
    public void setRNGSeed( long RNGSeed ){
        rnd.setSeed( RNGSeed );
    }

    public void updateTunables(){
    	
        tradeLookupFrame2MaxTimeInMilliSeconds = (long)(( hoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( driverCETxnSettings.TL_settings.cur_BackOffFromEndTimeFrame2 )) * EGenDate.MsPerSecond;
        tradeLookupFrame3MaxTimeInMilliSeconds = (long)(( hoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( driverCETxnSettings.TL_settings.cur_BackOffFromEndTimeFrame3 )) * EGenDate.MsPerSecond;
        tradeLookupFrame4MaxTimeInMilliSeconds = (long)(( hoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( driverCETxnSettings.TL_settings.cur_BackOffFromEndTimeFrame4 )) * EGenDate.MsPerSecond;

        tradeUpdateFrame2MaxTimeInMilliSeconds = (long)(( hoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( driverCETxnSettings.TU_settings.cur_BackOffFromEndTimeFrame2 )) * EGenDate.MsPerSecond;
        tradeUpdateFrame3MaxTimeInMilliSeconds = (long)(( hoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( driverCETxnSettings.TU_settings.cur_BackOffFromEndTimeFrame3 )) * EGenDate.MsPerSecond;

        endTime = startTime;
        endTime = EGenDate.AddWorkMs( endTime, (long)(hoursOfInitialTrades * EGenDate.SecondsPerHour + 15 * EGenDate.SecondsPerMinute) * EGenDate.MsPerSecond );

        tradeOrderRollbackLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;
        tradeOrderRollbackLevel = driverCETxnSettings.TO_settings.cur_rollback;

        logger.sendToLogger(driverCETxnSettings.BV_settings);
        logger.sendToLogger(driverCETxnSettings.CP_settings);
        logger.sendToLogger(driverCETxnSettings.MW_settings);
        logger.sendToLogger(driverCETxnSettings.SD_settings);
        logger.sendToLogger(driverCETxnSettings.TL_settings);
        logger.sendToLogger(driverCETxnSettings.TO_settings);
        logger.sendToLogger(driverCETxnSettings.TU_settings);
    }

    public long generateRandomCustomerAccountId(){
        long          customerID;
        long          customerAccountId;
        TierId        tierID;
        Object[] customer = new Object[2];

        customer = customerSelection.genRandomCustomer();       
        customerID = Long.parseLong(customer[0].toString());
        tierID = (TierId)customer[1];
        
        customerAccountId = account.genRandomAccId( rnd, customerID, tierID)[0];

        return(customerAccountId);
    }

    public long generateNonUniformTradeID( int aValue, int sValue ){
    	long tradeID;

        tradeID = rnd.rndNU( 1, maxActivePrePopulatedTradeID, aValue, sValue );
        if ( HoldingsAndTrades.isAbortedTrade(tradeID) ){
            tradeID++;
        }
        tradeID += TPCEConstants.TRADE_SHIFT;
        return( tradeID );
    }

    public TimestampType generateNonUniformTradeDTS( long maxTimeInMS, int aValue, int sValue ){
    	GregorianCalendar tradeTime = new GregorianCalendar(TPCEConstants.initialTradePopulationBaseYear,
                    							TPCEConstants.initialTradePopulationBaseMonth,
                    							TPCEConstants.initialTradePopulationBaseDay,
                    							TPCEConstants.initialTradePopulationBaseHour,
                    							TPCEConstants.initialTradePopulationBaseMinute,
                    							TPCEConstants.initialTradePopulationBaseSecond );   
        long tradeTimeOffset;

        tradeTimeOffset = rnd.rndNU( 1, maxTimeInMS, aValue, sValue );
        return EGenDate.getTimeStamp(EGenDate.AddWorkMs( tradeTime.getTime(), tradeTimeOffset ));
        
    }

    /*
    *  Generate Broker-Volume transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateBrokerVolumeInput(TBrokerVolumeTxnInput inputStructure){
    	int           numBrokers;
        int           count, i;
        long[]        brokerID = new long [TxnHarnessStructs.max_broker_list_len];
        int           sectorIndex;
        
        numBrokers = rnd.intRange(TxnHarnessStructs.min_broker_list_len, TxnHarnessStructs.max_broker_list_len); 
        
        inputStructure.setBrokerList(numBrokers);
		for (i = 0; i < numBrokers; ++i){
			inputStructure.getBrokerList()[i] = new String();
        }
		
        if (numBrokers > brokers.GetBrokerCount()){
            numBrokers = (int)brokers.GetBrokerCount();
        }
        count = 0;
        do{
            brokerID[count] = brokers.GenerateRandomBrokerId(rnd);
            for (i = 0; (i < count) && (brokerID[i] != brokerID[count]); ++i) { };
            if (i == count){
            	inputStructure.getBrokerList()[i] = brokers.generateBrokerName(brokerID[count]);
                ++count;
            }
        } while (count < numBrokers);

        sectorIndex = rnd.intRange(0, sectorCount-1);
        inputStructure.setSectorName((String)sectors.getTupleByIndex(sectorIndex)[1].toString());
    }


    /*
    *  Generate Customer-Position transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateCustomerPositionInput(TCustomerPositionTxnInput inputStructure){
        Object[] customer = new Object[2];
        customer = customerSelection.genRandomCustomer();
        long customerID = Long.parseLong(customer[0].toString());
        TierId tierID = (TierId)customer[1];
        
        if (rnd.rndPercent(driverCETxnSettings.CP_settings.cur_by_tax_id)){
        	
        	inputStructure.setTaxId( person.getTaxID(customerID));
            inputStructure.setCustId(0);
        }
        else{
            inputStructure.setCustId(customerID);
            inputStructure.setTaxId(new String());
        }

        boolean get_history = rnd.rndPercent(driverCETxnSettings.CP_settings.cur_get_history);
        if( get_history ){
        	inputStructure.setHistory(1);
            inputStructure.setAcctIdIndex(rnd.intRange( 0, (int)account.genRandomAccId(rnd, customerID, tierID)[1] - 1));
        }
        else{
        	inputStructure.setHistory(0);
            inputStructure.setAcctIdIndex(-1);
        }
    }

    /*
    *  Generate Market-Watch transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateMarketWatchInput(TMarketWatchTxnInput inputStructure){
        long          customerID;
        TierId        tierID;
        int           threshold;
        int           week;
        int           dailyMarketDay;
        Object[] customer = new Object[2];
        Date date = EGenDate.getDateFromTime(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
                        TPCEConstants.dailyMarketBaseDay, TPCEConstants.dailyMarketBaseHour,
                        TPCEConstants.dailyMarketBaseMinute, TPCEConstants.dailyMarketBaseSecond, TPCEConstants.dailyMarketBaseMsec);
        threshold = rnd.rndPercentage();
        if (threshold <= driverCETxnSettings.MW_settings.cur_by_industry){
            
        	inputStructure.setIndustryName(industries.getTupleByIndex(rnd.intRange(0, industryCount-1))[1]);
            inputStructure.setCId(0);
            inputStructure.setAcctId(0);

            if( TxnHarnessStructs.iBaseCompanyCount < activeCompanyCount ){
                inputStructure.setStartingCoId(rnd.int64Range( startFromCompany, startFromCompany + activeCompanyCount - ( TxnHarnessStructs.iBaseCompanyCount - 1 )));
                inputStructure.setEndingCoId(inputStructure.getStartingCoId() + ( TxnHarnessStructs.iBaseCompanyCount - 1 ));
            }
            else{
            	inputStructure.setStartingCoId(startFromCompany);
                inputStructure.setEndingCoId(startFromCompany + activeCompanyCount - 1);
            }
        }
        else{
        	inputStructure.setStartingCoId(0);
        	inputStructure.setEndingCoId(0);
        	inputStructure.setIndustryName(new String());
			
            customer = customerSelection.genRandomCustomer();
            
            if (threshold <= (driverCETxnSettings.MW_settings.cur_by_industry + driverCETxnSettings.MW_settings.cur_by_watch_list)){
                tierID = (TierId)customer[1];
                inputStructure.setCId(Long.parseLong(customer[0].toString()));
                
                inputStructure.setAcctId(0);
            }
            else{
            	customerID = Long.parseLong(customer[0].toString());
            	tierID = (TierId)customer[1];
            	inputStructure.setAcctId(account.genRandomAccId(rnd, customerID, tierID, inputStructure.getAcctId(), -1)[0]);
                inputStructure.setCId(0);
            }
        }

        week = (int)rnd.rndNU(0, 255, 255, 0) + 5; 
       
        threshold = rnd.rndPercentage();
        if (threshold > 40){
            dailyMarketDay = week * EGenDate.DaysPerWeek + 4; 
        }
        else{
            if (threshold <= 20){
                dailyMarketDay = week * EGenDate.DaysPerWeek; 
            }
            else{
                if (threshold <= 27){
                    dailyMarketDay = week * EGenDate.DaysPerWeek + 1;
                }
                else{
                    if (threshold <= 33){
                        dailyMarketDay = week * EGenDate.DaysPerWeek + 2;
                    }
                    else{
                        dailyMarketDay = week * EGenDate.DaysPerWeek + 3;
                    }
                }
            }
        }
        date = EGenDate.addDaysMsecs(date, dailyMarketDay, 0, false);

        inputStructure.setStartDay(EGenDate.getTimeStamp(date));
    }

    /*
    *  Generate Security-Detail transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateSecurityDetailInput(TSecurityDetailTxnInput inputStructure){
    	Date date = EGenDate.getDateFromTime(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
    			TPCEConstants.dailyMarketBaseDay, TPCEConstants.dailyMarketBaseHour,
				TPCEConstants.dailyMarketBaseMinute, TPCEConstants.dailyMarketBaseSecond, TPCEConstants.dailyMarketBaseMsec);
        int startDay;

        char[] tmp = securities.createSymbol( rnd.int64Range(0, activeSecurityCount-1), TableConsts.cSYMBOL_len).toCharArray();
        
        inputStructure.setSymbol(String.copyValueOf(tmp, 0, tmp.length));

        if(rnd.rndPercent( driverCETxnSettings.SD_settings.cur_LOBAccessPercentage ))
        	inputStructure.setAccessLobFlag(1);
        else inputStructure.setAccessLobFlag(0);

        inputStructure.setMaxRowsToReturn(rnd.intRange(TPCEConstants.iSecurityDetailMinRows, TPCEConstants.iSecurityDetailMaxRows));

        startDay = rnd.intRange(0, DailyMarketGenerator.iDailyMarketTotalRows - inputStructure.getMaxRowsToReturn());

        date = EGenDate.addDaysMsecs(date, startDay, 0, false);
        inputStructure.setStartDay(EGenDate.getTimeStamp(date));
    }

    /*
    *  Generate Trade-Lookup transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateTradeLookupInput(TTradeLookupTxnInput inputStructure){
        int           threshold;
        threshold = rnd.rndPercentage();

        if( threshold <= driverCETxnSettings.TL_settings.cur_do_frame1 ){  
            inputStructure.setFrameToExecute(1);
            inputStructure.setMaxTrades(driverCETxnSettings.TL_settings.cur_MaxRowsFrame1);

            int     i, j;
            boolean    accepted;
            long  tradeID;

            for( i = 0; i < inputStructure.getMaxTrades(); i++ ){
                accepted = false;
                while( ! accepted ){
                    tradeID = generateNonUniformTradeID(TPCEConstants.TradeLookupAValueForTradeIDGenFrame1,
                    		TPCEConstants.TradeLookupSValueForTradeIDGenFrame1);
                    j = 0;
                    while( j < i && inputStructure.getTradeId()[j] != tradeID ){
                        j++;
                    }
                    if( j == i ){
                        inputStructure.getTradeId()[i] = tradeID;
                        accepted = true;
                    }
                }
            }
        
            inputStructure.setAcctId(0);                                          
            inputStructure.setMaxAcctId(0);                                      
            inputStructure.setSymbol(new String());							     
            
            GregorianCalendar date = new GregorianCalendar(0,0,0,0,0);        
            inputStructure.setStartTradeDts(new TimestampType(date.getTime()));  
            inputStructure.setEndTradeDts(new TimestampType(date.getTime()));  
        }
        else if( threshold <=  driverCETxnSettings.TL_settings.cur_do_frame1 + driverCETxnSettings.TL_settings.cur_do_frame2 ){
            inputStructure.setFrameToExecute(2);
            inputStructure.setAcctId(generateRandomCustomerAccountId());
            inputStructure.setMaxTrades(driverCETxnSettings.TL_settings.cur_MaxRowsFrame2);

            inputStructure.setStartTradeDts(generateNonUniformTradeDTS( tradeLookupFrame2MaxTimeInMilliSeconds, TPCEConstants.TradeLookupAValueForTimeGenFrame2, TPCEConstants.TradeLookupSValueForTimeGenFrame2 ));
            
            inputStructure.setEndTradeDts(EGenDate.getTimeStamp( endTime));

            inputStructure.setMaxAcctId(0);
            inputStructure.setSymbol( new String());
            Arrays.fill(inputStructure.getTradeId(), 0);
        }
        else if( threshold <=  driverCETxnSettings.TL_settings.cur_do_frame1 + driverCETxnSettings.TL_settings.cur_do_frame2 + driverCETxnSettings.TL_settings.cur_do_frame3 ){

        	inputStructure.setFrameToExecute(3);
        	inputStructure.setMaxTrades(driverCETxnSettings.TL_settings.cur_MaxRowsFrame3);

            inputStructure.setSymbol(securities.createSymbol( rnd.rndNU( 0, activeSecurityCount-1,
            		TPCEConstants.TradeLookupAValueForSymbolFrame3,
            		TPCEConstants.TradeLookupSValueForSymbolFrame3 ),TableConsts.cSYMBOL_len));

            inputStructure.setStartTradeDts(generateNonUniformTradeDTS( tradeLookupFrame3MaxTimeInMilliSeconds, TPCEConstants.TradeLookupAValueForTimeGenFrame3, TPCEConstants.TradeLookupSValueForTimeGenFrame3 ));

            inputStructure.setEndTradeDts(EGenDate.getTimeStamp(endTime));

            inputStructure.setMaxAcctId(CustomerAccountsGenerator.getEndtingAccId( activeCustomerCount ));
     
            inputStructure.setAcctId(0);
            Arrays.fill(inputStructure.getTradeId(), 0);
        }
        else{
        	inputStructure.setFrameToExecute(4);
            inputStructure.setAcctId(generateRandomCustomerAccountId());
            inputStructure.setStartTradeDts(generateNonUniformTradeDTS( tradeLookupFrame4MaxTimeInMilliSeconds, TPCEConstants.TradeLookupAValueForTimeGenFrame4, TPCEConstants.TradeLookupSValueForTimeGenFrame4 ));
            inputStructure.setMaxTrades(0);                                  
            inputStructure.setMaxAcctId(0);
            inputStructure.setSymbol(new String());
            Arrays.fill(inputStructure.getTradeId(), 0);
        }
    }

    /*
    *  Generate Trade-Order transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure         - input parameter structure filled in for the transaction.
    *           OUT TradeType              - integer representation of generated trade type (as eTradeTypeID enum).
    *           OUT executorIsAccountOwner - whether Trade-Order frame 2 should (FALSE) or shouldn't (TRUE) be called.
    *
    *  RETURNS:
    *           none.
    */
    public void generateTradeOrderInput(TTradeOrderTxnInput inputStructure, int iTradeType, boolean executorIsAccountOwner){
        long          customerID;
        TierId   	  tierID;
        boolean       bMarket;
        int           additionalPerms;
        int           secAcct;
        long          secFlatFileIndex;
        String[]      flTaxId;
        TradeType    eTradeType;
        inputStructure.setTradeID(currentTradeID.getAndIncrement());
        Object[] customer = new Object[2];

        customer = customerSelection.genRandomCustomer();
        
        customerID = Long.parseLong(customer[0].toString());
        tierID = (TierId)customer[1];
        
        long[] randomAccSecurity = holdings.generateRandomAccSecurity(customerID, tierID);

        inputStructure.setAcctId(randomAccSecurity[0]);
        secFlatFileIndex = randomAccSecurity[2];
        secAcct = (int)randomAccSecurity[1];
        additionalPerms = accountPerms.getNumPermsForAcc(inputStructure.getAcctId());
        if (additionalPerms == 1){
        	flTaxId = person.getFirstNameLastNameTaxID(customerID);
        	
            executorIsAccountOwner = true;
        }
        else{
            int exec_is_owner = (driverCETxnSettings.TO_settings.cur_exec_is_owner - AccountPermsGenerator.percentAccountAdditionalPermissions_0) * 100 / (100 - AccountPermsGenerator.percentAccountAdditionalPermissions_0);

            if ( rnd.rndPercent(exec_is_owner) ){
                flTaxId = person.getFirstNameLastNameTaxID(customerID);

                executorIsAccountOwner = true;
            }
            else{
            	accountPerms.generateCids(customerID, additionalPerms, inputStructure.getAcctId());
                if (additionalPerms == 2){

                	flTaxId = person.getFirstNameLastNameTaxID(accountPerms.permCids[1]);
                }
                else{
                	flTaxId = person.getFirstNameLastNameTaxID(accountPerms.permCids[2]);
                }

                executorIsAccountOwner = false;
            }
        }
        
        inputStructure.setExecFirstName(flTaxId[0]);
    	inputStructure.setExecLastName(flTaxId[1]);
    	inputStructure.setExecTaxId(flTaxId[2]);

        if (rnd.rndPercent(driverCETxnSettings.TO_settings.cur_security_by_symbol)){
        	char[] tmp = securities.createSymbol(secFlatFileIndex, TableConsts.cSYMBOL_len).toCharArray();
            
            inputStructure.setSymbol(String.copyValueOf(tmp, 0, tmp.length));
            inputStructure.setCoNmae(new String());
            inputStructure.setIssue(new String());
        }
        else{
        	inputStructure.setCoNmae(companies.generateCompanyName( securities.getCompanyIndex( secFlatFileIndex )));

        	char[] tmp = securities.getSecRecord(secFlatFileIndex)[3].toCharArray();
        	
        	inputStructure.setIssue(String.copyValueOf(tmp, 0, tmp.length));

        	inputStructure.setSymbol(new String());
        }

        inputStructure.setTradeQty(HoldingsAndTrades.TRADE_QTY_SIZES[rnd.intRange(0, HoldingsAndTrades.TRADE_QTY_SIZES.length - 1)]);
        inputStructure.setRequestedPrice(rnd.doubleIncrRange(HoldingsAndTrades.fMinSecPrice, HoldingsAndTrades.fMaxSecPrice, 0.01));

        bMarket = rnd.rndPercent(driverCETxnSettings.TO_settings.cur_market);

        if (rnd.rndPercent(driverCETxnSettings.TO_settings.cur_buy_orders)){
            if (bMarket){
                eTradeType = TradeType.eMarketBuy;
            }
            else{
                eTradeType = TradeType.eLimitBuy;
            }
            
            if (rnd.rndPercent(
                    driverCETxnSettings.TO_settings.cur_type_is_margin *
                    100 /
                    driverCETxnSettings.TO_settings.cur_buy_orders)){
            	inputStructure.setTypeIsMargin(1);
            }
            else inputStructure.setTypeIsMargin(0);
            
        }
        else{
            if (bMarket){
                eTradeType = TradeType.eMarketSell;
            }
            else{
                if (rnd.rndPercent(driverCETxnSettings.TO_settings.cur_stop_loss)){
                    eTradeType = TradeType.eStopLoss;
                }
                else{
                    eTradeType = TradeType.eLimitSell;
                }
            }

            inputStructure.setTypeIsMargin(0);
        }
        iTradeType = eTradeType.ordinal();
        
        if (rnd.rndPercent(driverCETxnSettings.TO_settings.cur_lifo)){
        	inputStructure.setIsLifo(1);
        }
        else inputStructure.setIsLifo(0);

        char[] tmp = (tradeType.getTupleByIndex(eTradeType.ordinal()))[0].toCharArray();
        inputStructure.setTradeTypeId(String.copyValueOf(tmp, 0, tmp.length));

        tmp = (statusType.getTupleByIndex(StatusTypeId.E_PENDING.ordinal()))[0].toCharArray();
        inputStructure.setStPendingId(String.copyValueOf(tmp, 0, tmp.length));
        tmp = (statusType.getTupleByIndex(StatusTypeId.E_SUBMITTED.ordinal()))[0].toCharArray();
        inputStructure.setStSubmittedId(String.copyValueOf(tmp, 0, tmp.length));
        
        if ( tradeOrderRollbackLevel >= rnd.intRange( 1, tradeOrderRollbackLimit )){
        	inputStructure.setRollItBack(1);
        }
        else inputStructure.setRollItBack(0);
    }

    /*
    *  Generate Trade-Status transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateTradeStatusInput(TTradeStatusTxnInput inputStructure){
        long          customerID;
        TierId   	  tierID;

        Object[] customer = new Object[2];
        customer = customerSelection.genRandomCustomer();
        customerID = Long.parseLong(customer[0].toString());
        tierID = (TierId)customer[1];
        inputStructure.setAcctId(account.genRandomAccId(rnd, customerID, tierID, inputStructure.getAcctId(), -1)[0]);
    }

    /*
    *  Generate Trade-Update transaction input.
    *
    *  PARAMETERS:
    *           OUT inputStructure                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void generateTradeUpdateInput(TTradeUpdateTxnInput inputStructure){
        int           threshold;

        threshold = rnd.rndPercentage();

        if( threshold <= driverCETxnSettings.TU_settings.cur_do_frame1 ){
        	inputStructure.setFrameToExecute(1);
            inputStructure.setMaxTrades(driverCETxnSettings.TU_settings.cur_MaxRowsFrame1);
            inputStructure.setMaxUpdates(driverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame1);

            int     i, j;
            boolean    accepted;
            long  tradeID;

            for( i = 0; i < inputStructure.getMaxTrades(); i++ ){
                accepted = false;
                while( ! accepted ){
                    tradeID = generateNonUniformTradeID(TPCEConstants.TradeUpdateAValueForTradeIDGenFrame1, TPCEConstants.TradeUpdateSValueForTradeIDGenFrame1);
                    j = 0;
                    while( j < i && inputStructure.getTradeId()[j] != tradeID ){
                        j++;
                    }
                    if( j == i ){
                        inputStructure.getTradeId()[i] = tradeID;
                        accepted = true;
                    }
                }
            }
            inputStructure.setAcctId(0);                                          
            inputStructure.setMaxAcctId(0);                                      
            inputStructure.setSymbol(new String());	
            
            GregorianCalendar date = new GregorianCalendar(0,0,0,0,0);     
            inputStructure.setStartTradeDts(new TimestampType(date.getTime()));  
            inputStructure.setEndTradeDts(new TimestampType(date.getTime()));
        }
        else if( threshold <=  driverCETxnSettings.TU_settings.cur_do_frame1 +
                                driverCETxnSettings.TU_settings.cur_do_frame2 ){
        	inputStructure.setFrameToExecute(2);
            inputStructure.setMaxTrades(driverCETxnSettings.TU_settings.cur_MaxRowsFrame2);
            inputStructure.setMaxUpdates(driverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame2);
            inputStructure.setAcctId(generateRandomCustomerAccountId());
			inputStructure.setStartTradeDts(generateNonUniformTradeDTS( tradeUpdateFrame2MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeUpdateAValueForTimeGenFrame2, TPCEConstants.TradeUpdateSValueForTimeGenFrame2 ));
			inputStructure.setEndTradeDts(EGenDate.getTimeStamp(endTime));

			inputStructure.setMaxAcctId(0);                                      
            inputStructure.setSymbol(new String());
            Arrays.fill( inputStructure.getTradeId(), 0); 
            
        }
        else{
        	inputStructure.setFrameToExecute(3);
        	inputStructure.setMaxTrades(driverCETxnSettings.TU_settings.cur_MaxRowsFrame3);
            inputStructure.setMaxUpdates(driverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame3);
            
            inputStructure.setSymbol( securities.createSymbol( rnd.rndNU( 0, activeSecurityCount-1,
            		TPCEConstants.TradeLookupAValueForSymbolFrame3, TPCEConstants.TradeLookupSValueForSymbolFrame3 ),TableConsts.cSYMBOL_len));

            inputStructure.setStartTradeDts(generateNonUniformTradeDTS(tradeUpdateFrame3MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeLookupAValueForTimeGenFrame3, TPCEConstants.TradeLookupSValueForTimeGenFrame3 ));

            inputStructure.setEndTradeDts(EGenDate.getTimeStamp(endTime));

            inputStructure.setMaxAcctId(CustomerAccountsGenerator.getEndtingAccId( activeCustomerCount ));
            inputStructure.setAcctId(0);                                     
            Arrays.fill( inputStructure.getTradeId(), 0); 

        }
    }
    
    private EGenRandom                                rnd;
	private PersonHandler                             person;
	private CustomerSelection                         customerSelection;
	private AccountPermsGenerator					  accountPerms;
	private CustomerAccountsGenerator                 account;
	private HoldingsAndTrades                         holdings;
	private BrokerGenerator                           brokers;
	private CompanyGenerator                          companies;
	private SecurityHandler                           securities;	
	private InputFileHandler                          industries;
	private InputFileHandler       					  sectors;
	private InputFileHandler                          statusType;
	private InputFileHandler                          tradeType;
	private TDriverCETxnSettings                      driverCETxnSettings;
	private BaseLogger                                logger;

	private long                                      configuredCustomerCount;
	private long                                      activeCustomerCount;
	private long                                      startingCustomerID;
	private long                                      myCustomerCount;
	private int                                       partitionPercent;
	private int                                       scaleFactor;
	private int                                       hoursOfInitialTrades;
	private int                                       maxActivePrePopulatedTradeID;
	static private AtomicLong						  currentTradeID;
	private long                                      tradeLookupFrame2MaxTimeInMilliSeconds;
	private long                                      tradeLookupFrame3MaxTimeInMilliSeconds;
	private long                                      tradeLookupFrame4MaxTimeInMilliSeconds;
	private long                                      tradeUpdateFrame2MaxTimeInMilliSeconds;
	private long                                      tradeUpdateFrame3MaxTimeInMilliSeconds;
	private long                                      activeSecurityCount;
	private long                                      activeCompanyCount;
	private int                                       industryCount;
	private int                                       sectorCount;
	private long                                      startFromCompany;
	private int                                       tradeOrderRollbackLimit;
	private int                                       tradeOrderRollbackLevel;
	private Date								  	  startTime;
	private Date								  	  endTime;

}
