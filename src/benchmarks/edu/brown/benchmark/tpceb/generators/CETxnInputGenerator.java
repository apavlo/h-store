package edu.brown.benchmark.tpceb.generators;

import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpceb.generators.TMarketWatchTxnInput;
import edu.brown.benchmark.tpceb.generators.TxnHarnessStructs;
import edu.brown.benchmark.tpceb.util.EGenDate;
import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.util.*;
import edu.brown.benchmark.tpceb.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpceb.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpceb.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpceb.generators.TradeGenerator.TradeType;


public class CETxnInputGenerator {
	
    
    public CETxnInputGenerator( TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
                    int scaleFactor, int hoursOfInitialTrades, BaseLogger logger, TDriverCETxnSettings driverCETxnSettings){
    	inputFiles.parseInputFiles();
        rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR);
        
        customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
        account = (CustomerCustAccCombined)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_INFO, null);
        
        holdings = new HoldingsAndTrades(inputFiles);
        brokers = (NewBrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);
        companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
        securities = new SecurityHandler(inputFiles);
        industries = inputFiles.getInputFile(InputFile.INDUSTRY);
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
      
        customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);    
        account = (CustomerCustAccCombined)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_INFO, null);
       
        holdings = new HoldingsAndTrades(inputFiles); 
        brokers = (NewBrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	
       companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
        securities = new SecurityHandler(inputFiles);
      industries = inputFiles.getInputFile(InputFile.INDUSTRY);
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
         
          customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
          
          account = (CustomerCustAccCombined)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_INFO, null);
         
          holdings = new HoldingsAndTrades(inputFiles);
          
          brokers = (NewBrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	
        companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
          securities = new SecurityHandler(inputFiles);
         industries = inputFiles.getInputFile(InputFile.INDUSTRY);
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
       
          customerSelection = new CustomerSelection(rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
          
          account = (CustomerCustAccCombined)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_INFO, null);

          holdings = new HoldingsAndTrades(inputFiles);
          
          brokers = (NewBrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);
          industries = inputFiles.getInputFile(InputFile.INDUSTRY);
          securities = new SecurityHandler(inputFiles);
          companies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
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

        activeSecurityCount = SecurityHandler.getSecurityNum(myCustomerCount);

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
        endTime = startTime;
        endTime = EGenDate.AddWorkMs( endTime, (long)(hoursOfInitialTrades * EGenDate.SecondsPerHour + 15 * EGenDate.SecondsPerMinute) * EGenDate.MsPerSecond );

       tradeOrderRollbackLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;
   
        tradeOrderRollbackLevel = driverCETxnSettings.TO_settings.cur_rollback;

        logger.sendToLogger(driverCETxnSettings.MW_settings);
        logger.sendToLogger(driverCETxnSettings.TO_settings);
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
    public void generateTradeOrderInput(TTradeOrderTxnInput inputStructure, int iTradeType){

        long          customerID;
        TierId   	  tierID;
        boolean       bMarket;
        int           additionalPerms;
        int           secAcct;
        long          secFlatFileIndex;
      //  String[]      flTaxId;
        TradeType    eTradeType;
        inputStructure.setTradeID(currentTradeID.getAndIncrement());

        Object[] customer = new Object[2];
        
        customer = customerSelection.genRandomCustomer();

        customerID = Long.parseLong(customer[0].toString());

        tierID = (TierId)customer[1];
        
        long[] randomAccSecurity = holdings.generateRandomAccSecurity(customerID, tierID);

        if(randomAccSecurity.length == 0){
               System.out.println("was null");  
        }
       
        inputStructure.setAcctId(randomAccSecurity[0]);
        secFlatFileIndex = randomAccSecurity[2];
        secAcct = (int)randomAccSecurity[1];

        char[] tmp1 = securities.createSymbol(secFlatFileIndex, TableConsts.cSYMBOL_len).toCharArray();
            
        inputStructure.setSymbol(String.copyValueOf(tmp1, 0, tmp1.length));
       

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
            
          inputStructure.setTypeIsMargin(0);
            
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
        
        System.out.println("Input Sympbol:" +inputStructure.getSymbol());
        System.out.println("Account:" + inputStructure.getAcctId());
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
       System.out.println("Got the date" + date);
        threshold = rnd.rndPercentage();
        
        if (threshold <= driverCETxnSettings.MW_settings.cur_by_industry){
            
            inputStructure.setIndustryName(industries.getTupleByIndex(rnd.intRange(0, industryCount-1))[1]);
            inputStructure.setCId(0);
            inputStructure.setAcctId(0);
            System.out.println("customer and account id set to 0");
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
            
          /*  if (threshold <= (driverCETxnSettings.MW_settings.cur_by_industry + driverCETxnSettings.MW_settings.cur_by_watch_list)){
                tierID = (TierId)customer[1];
                inputStructure.setCId(Long.parseLong(customer[0].toString()));
                System.out.println("customer id set, acct id is 0");
                inputStructure.setAcctId(0);
            }
            else{*/
            
            customer = customerSelection.genRandomCustomer();

            customerID = Long.parseLong(customer[0].toString());

            tierID = (TierId)customer[1];
            
            long[] randomAccSecurity = holdings.generateRandomAccSecurity(customerID, tierID);

            if(randomAccSecurity.length == 0){
                   System.out.println("was null");  
            }
           
            inputStructure.setAcctId(randomAccSecurity[0]);
              //  customerID = Long.parseLong(customer[0].toString());
               // tierID = (TierId)customer[1];
                System.out.println("first account id" + inputStructure.getAcctId());
              //  inputStructure.setAcctId(account.genRandomAccId(rnd, customerID, tierID, inputStructure.getAcctId(), -1)[0]);
                
                System.out.println("custID:" + customerID);
                System.out.println("TierID:" + tierID);
                System.out.println("AccountID: " + inputStructure.getAcctId());
                
                inputStructure.setCId(0);
           // }
        }

        week = (int)rnd.rndNU(0, 255, 255, 0) + 5; 
       
        threshold = rnd.rndPercentage();
        if (threshold > 40){
            System.out.println("threshold greater than 40");
            dailyMarketDay = week * EGenDate.DaysPerWeek + 4;
            System.out.println("DMD1:" + dailyMarketDay);
        }
        else{
            if (threshold <= 20){
                dailyMarketDay = week * EGenDate.DaysPerWeek; 
                System.out.println("DMD2:" + dailyMarketDay);
            }
            else{
                if (threshold <= 27){
                    dailyMarketDay = week * EGenDate.DaysPerWeek + 1;
                    System.out.println("DMD3:" + dailyMarketDay);
                }
                else{
                    if (threshold <= 33){
                        dailyMarketDay = week * EGenDate.DaysPerWeek + 2;
                        System.out.println("DMD4:" + dailyMarketDay);
                    }
                    else{
                        dailyMarketDay = week * EGenDate.DaysPerWeek + 3;
                        System.out.println("DMD5:" + dailyMarketDay);
                    }
                }
            }
        }
        System.out.println("Final date:" + date);
        System.out.println("Final DMD:" + dailyMarketDay);
        date = EGenDate.addDaysMsecs(date, dailyMarketDay, 0, false);
        System.out.println(date);
        
        inputStructure.setStartDay(EGenDate.getTimeStamp(date));
    }
   
    
    private EGenRandom                                rnd;
	
	private CustomerSelection                         customerSelection;

	private CustomerCustAccCombined                 account;
	private HoldingsAndTrades                         holdings;
	private NewBrokerGenerator                           brokers;
	private InputFileHandler                          industries;
	private CompanyGenerator                          companies;
	private SecurityHandler                           securities;	
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
	private long                                      activeSecurityCount;
	private long                                      activeCompanyCount;
    private int                                       industryCount;
	private long                                      startFromCompany;
	private int                                       tradeOrderRollbackLimit;
	private int                                       tradeOrderRollbackLevel;
	private Date								  	  startTime;
	private Date								  	  endTime;

}

