package edu.brown.benchmark.tpce.generators;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;

import edu.brown.benchmark.tpce.util.*;
import edu.brown.benchmark.tpce.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpce.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;


public class CETxnInputGenerator {
	private EGenRandom                                m_rnd;      //used inside for parameter generation
	private PersonHandler                             m_Person;
	private CustomerSelection                         m_CustomerSelection;
	private AccountPermsGenerator					  m_AccountPerms;
	private CustomerAccountsGenerator                 m_Accs;
	private HoldingsAndTrades                         m_Holdings;
	private BrokerGenerator                           m_Brokers;
	private CompanyGenerator                          m_pCompanies;
	private SecurityHandler                           m_pSecurities;
	
	private InputFileHandler                          m_pIndustries;
	private InputFileHandler       					  m_pSectors;
	private InputFileHandler                          m_pStatusType;
	private InputFileHandler                          m_pTradeType;
	private TDriverCETxnSettings                      m_pDriverCETxnSettings;
	private BaseLogger                                m_pLogger;

	private long                                      m_iConfiguredCustomerCount;
	private long                                      m_iActiveCustomerCount;

	private long                                      m_iMyStartingCustomerId;
	private long                                      m_iMyCustomerCount;
	private int                                       m_iPartitionPercent;

	private int                                       m_iScaleFactor;
	private int                                       m_iHoursOfInitialTrades;
	private int                                       m_iMaxActivePrePopulatedTradeID;

	private long                                      m_iTradeLookupFrame2MaxTimeInMilliSeconds;
	private long                                      m_iTradeLookupFrame3MaxTimeInMilliSeconds;
	private long                                      m_iTradeLookupFrame4MaxTimeInMilliSeconds;

	private long                                      m_iTradeUpdateFrame2MaxTimeInMilliSeconds;
	private long                                      m_iTradeUpdateFrame3MaxTimeInMilliSeconds;

    //number of securities (scaled based on active customers)
	private long                                      m_iActiveSecurityCount;
	private long                                      m_iActiveCompanyCount;
    //number of industries (from flat file)
	private int                                       m_iIndustryCount;
    //number of sector names (from flat file)
	private int                                       m_iSectorCount;
    //starting ids
	private long                                      m_iStartFromCompany;
	
	private Date								  m_StartTime;
	private Date								  m_EndTime;

    int                                       m_iTradeOrderRollbackLimit;
    int                                       m_iTradeOrderRollbackLevel;

    /*
    *  Constructor - no partitioning by C_ID.
    *
    *  PARAMETERS:
    *           IN  inputFiles                  - in-memory input flat files
    *           IN  iConfiguredCustomerCount    - number of configured customers in the database
    *           IN  iActiveCustomerCount        - number of active customers in the database
    *           IN  iScaleFactor                - scale factor (number of customers per 1 tpsE) of the database
    *           IN  iHoursOfInitialTrades       - number of hours of the initial trades portion of the database
    *           IN  pLogger                     - reference to parameter logging object
    *           IN  pDriverCETxnSettings        - initial transaction parameter settings
    *
    *  RETURNS:
    *           not applicable.
    */
    public CETxnInputGenerator( TPCEGenerator inputFiles,
                                                long iConfiguredCustomerCount, long iActiveCustomerCount,
                                                int iScaleFactor, int iHoursOfInitialTrades,
                                                BaseLogger pLogger,
                                                TDriverCETxnSettings pDriverCETxnSettings){
    	inputFiles.parseInputFiles();
        m_rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR);   //initialize with a default seed
        m_Person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
        m_CustomerSelection = new CustomerSelection(m_rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
        
        m_Accs = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
        m_AccountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
        //m_Accs = new CustomerAccountsGenerator(new Table(), inputFiles);
        //m_AccountPerms = new AccountPermsGenerator(new Table(), inputFiles);
        m_Holdings = new HoldingsAndTrades(inputFiles);
       
        m_Brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);
        m_pCompanies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
       // m_Brokers = new BrokerGenerator(new Table(), inputFiles);	//iDefaultStartFromCustomer = 1; iActiveCustomerCount <- ClienDriver.cpp.m_TxnInputGenerator;
       // m_pCompanies = new CompanyGenerator(new Table(), inputFiles);
        m_pSecurities = new SecurityHandler(inputFiles);
        m_pIndustries = inputFiles.getInputFile(InputFile.INDUSTRY);
        m_pSectors = inputFiles.getInputFile(InputFile.SECTOR);
        m_pStatusType = inputFiles.getInputFile(InputFile.STATUS);
        m_pTradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
        m_pDriverCETxnSettings = pDriverCETxnSettings;
        m_pLogger = pLogger;
        m_iConfiguredCustomerCount = iConfiguredCustomerCount;
        m_iActiveCustomerCount = iActiveCustomerCount;
        m_iMyStartingCustomerId = TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        m_iMyCustomerCount = iActiveCustomerCount;
        m_iPartitionPercent = 100;
        m_iScaleFactor = iScaleFactor;
        m_iHoursOfInitialTrades = iHoursOfInitialTrades;
        
        Initialize();
    }

    /*
    *  Constructor - no partitioning by C_ID, RNG seed provided.
    *
    *  RNG seed is for testing/engineering work allowing repeatable transaction
    *  parameter stream. This constructor is NOT legal for a benchmark publication.
    *
    *  PARAMETERS:
    *           IN  inputFiles                  - in-memory input flat files
    *           IN  iConfiguredCustomerCount    - number of configured customers in the database
    *           IN  iActiveCustomerCount        - number of active customers in the database
    *           IN  iScaleFactor                - scale factor (number of customers per 1 tpsE) of the database
    *           IN  iHoursOfInitialTrades       - number of hours of the initial trades portion of the database
    *           IN  RNGSeed                     - initial seed for random number generator
    *           IN  pLogger                     - reference to parameter logging object
    *           IN  pDriverCETxnSettings        - initial transaction parameter settings
    *
    *  RETURNS:
    *           not applicable.
    */
      public CETxnInputGenerator( TPCEGenerator inputFiles,
            long iConfiguredCustomerCount, long iActiveCustomerCount,
            int iScaleFactor, int iHoursOfInitialTrades,
            long RNGSeed,
            BaseLogger pLogger,
            TDriverCETxnSettings pDriverCETxnSettings){
    	  inputFiles.parseInputFiles();
    	m_rnd = new EGenRandom(RNGSeed);   //initialize with a default seed
        m_Person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
        m_CustomerSelection = new CustomerSelection(m_rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
        
        m_Accs = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
        m_AccountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
        m_Holdings = new HoldingsAndTrades(inputFiles);
        
        m_Brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	//iDefaultStartFromCustomer = 1; iActiveCustomerCount <- ClienDriver.cpp.m_TxnInputGenerator;
        m_pCompanies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
        m_pSecurities = new SecurityHandler(inputFiles);
        m_pIndustries = inputFiles.getInputFile(InputFile.INDUSTRY);
        m_pSectors = inputFiles.getInputFile(InputFile.SECTOR);
        m_pStatusType = inputFiles.getInputFile(InputFile.STATUS);
        m_pTradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
        m_pDriverCETxnSettings = pDriverCETxnSettings;
        m_pLogger = pLogger;
        m_iConfiguredCustomerCount = iConfiguredCustomerCount;
        m_iActiveCustomerCount = iActiveCustomerCount;
        m_iMyStartingCustomerId = TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        m_iMyCustomerCount = iActiveCustomerCount;
        m_iPartitionPercent = 100;
        m_iScaleFactor = iScaleFactor;
        m_iHoursOfInitialTrades = iHoursOfInitialTrades;
        
        Initialize();
    }
        
    /*
    *  Constructor - partitioning by C_ID.
    *
    *  PARAMETERS:
    *           IN  inputFiles                  - in-memory input flat files
    *           IN  iConfiguredCustomerCount    - number of configured customers in the database
    *           IN  iActiveCustomerCount        - number of active customers in the database
    *           IN  iScaleFactor                - scale factor (number of customers per 1 tpsE) of the database
    *           IN  iHoursOfInitialTrades       - number of hours of the initial trades portion of the database
    *           IN  iMyStartingCustomerId       - first customer id (1-based) of the partition for this instance
    *           IN  iMyCustomerCount            - number of customers in the partition for this instance
    *           IN  iPartitionPercent           - the percentage of C_IDs generated within this instance's partition
    *           IN  pLogger                     - reference to parameter logging object
    *           IN  pDriverCETxnSettings        - initial transaction parameter settings
    *
    *  RETURNS:
    *           not applicable.
    */
      public CETxnInputGenerator( TPCEGenerator inputFiles,
              long iConfiguredCustomerCount, long iActiveCustomerCount,
              int iScaleFactor, int iHoursOfInitialTrades,
              long iMyStartingCustomerId, long iMyCustomerCount, int iPartitionPercent,
              BaseLogger pLogger,
              TDriverCETxnSettings pDriverCETxnSettings){
    	  inputFiles.parseInputFiles();
    	  m_rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR);   //initialize with a default seed
          m_Person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
          m_CustomerSelection = new CustomerSelection(m_rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
          
          m_Accs = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
          m_AccountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
          m_Holdings = new HoldingsAndTrades(inputFiles);
          
          m_Brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	//iDefaultStartFromCustomer = 1; iActiveCustomerCount <- ClienDriver.cpp.m_TxnInputGenerator;
          m_pCompanies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
          m_pSecurities = new SecurityHandler(inputFiles);
          m_pIndustries = inputFiles.getInputFile(InputFile.INDUSTRY);
          m_pSectors = inputFiles.getInputFile(InputFile.SECTOR);
          m_pStatusType = inputFiles.getInputFile(InputFile.STATUS);
          m_pTradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
          m_pDriverCETxnSettings = pDriverCETxnSettings;
          m_pLogger = pLogger;
          m_iConfiguredCustomerCount = iConfiguredCustomerCount;
          m_iActiveCustomerCount = iActiveCustomerCount;
          m_iMyStartingCustomerId = iMyStartingCustomerId;
          m_iMyCustomerCount = iMyCustomerCount;
          m_iPartitionPercent = iPartitionPercent;
          m_iScaleFactor = iScaleFactor;
          m_iHoursOfInitialTrades = iHoursOfInitialTrades;
          Initialize();
      }

    /*
    *  Constructor - partitioning by C_ID, RNG seed provided.
    *
    *  RNG seed is for testing/engineering work allowing repeatable transaction
    *  parameter stream. This constructor is NOT legal for a benchmark publication.
    *
    *  PARAMETERS:
    *           IN  inputFiles                  - in-memory input flat files
    *           IN  iConfiguredCustomerCount    - number of configured customers in the database
    *           IN  iActiveCustomerCount        - number of active customers in the database
    *           IN  iScaleFactor                - scale factor (number of customers per 1 tpsE) of the database
    *           IN  iHoursOfInitialTrades       - number of hours of the initial trades portion of the database
    *           IN  iMyStartingCustomerId       - first customer id (1-based) of the partition for this instance
    *           IN  iMyCustomerCount            - number of customers in the partition for this instance
    *           IN  iPartitionPercent           - the percentage of C_IDs generated within this instance's partition
    *           IN  pLogger                     - reference to parameter logging object
    *           IN  pDriverCETxnSettings        - initial transaction parameter settings
    *
    *  RETURNS:
    *           not applicable.
    */
      public CETxnInputGenerator( TPCEGenerator inputFiles,
              long iConfiguredCustomerCount, long iActiveCustomerCount,
              int iScaleFactor, int iHoursOfInitialTrades,
              long iMyStartingCustomerId, long iMyCustomerCount, int iPartitionPercent,
              long RNGSeed,
              BaseLogger pLogger,
              TDriverCETxnSettings pDriverCETxnSettings){
    	  inputFiles.parseInputFiles();
    	  m_rnd = new EGenRandom(RNGSeed);   //initialize with a default seed
          m_Person = new PersonHandler(inputFiles.getInputFile(InputFile.LNAME), inputFiles.getInputFile(InputFile.FEMFNAME), inputFiles.getInputFile(InputFile.MALEFNAME));
          m_CustomerSelection = new CustomerSelection(m_rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
          
          m_Accs = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
          m_AccountPerms = (AccountPermsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
          m_Holdings = new HoldingsAndTrades(inputFiles);
          
          m_Brokers = (BrokerGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_BROKER, null);	//iDefaultStartFromCustomer = 1; iActiveCustomerCount <- ClienDriver.cpp.m_TxnInputGenerator;
          m_pCompanies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
          m_pSecurities = new SecurityHandler(inputFiles);
          m_pIndustries = inputFiles.getInputFile(InputFile.INDUSTRY);
          m_pSectors = inputFiles.getInputFile(InputFile.SECTOR);
          m_pStatusType = inputFiles.getInputFile(InputFile.STATUS);
          m_pTradeType = inputFiles.getInputFile(InputFile.TRADETYPE);
          m_pDriverCETxnSettings = pDriverCETxnSettings;
          m_pLogger = pLogger;
          m_iConfiguredCustomerCount = iConfiguredCustomerCount;
          m_iActiveCustomerCount = iActiveCustomerCount;
          m_iMyStartingCustomerId = iMyStartingCustomerId;
          m_iMyCustomerCount = iMyCustomerCount;
          m_iPartitionPercent = iPartitionPercent;
          m_iScaleFactor = iScaleFactor;
          m_iHoursOfInitialTrades = iHoursOfInitialTrades;
          Initialize();
      }
     
        //TODO iActiveCustomerCount?
//        , m_Brokers(inputFiles, iActiveCustomerCount, iDefaultStartFromCustomer)
       

    /*
    *  Perform initialization common to all constructors.
    *
    *  PARAMETERS:
    *           IN  pDriverCETxnSettings        - initial transaction parameter settings
    *
    *  RETURNS:
    *           none.
    */
    public void Initialize()
    {
        m_iActiveCompanyCount = m_pCompanies.getCompanyCount();
        m_iActiveSecurityCount = SecurityHandler.getSecurityNum(m_iMyCustomerCount);
        m_iIndustryCount = m_pIndustries.getMaxKey();
        m_iSectorCount = m_pSectors.getMaxKey();
        m_iStartFromCompany = m_pCompanies.generateCompId();    // from the first company

        m_iMaxActivePrePopulatedTradeID = (int)(( m_iHoursOfInitialTrades * EGenDate.SecondsPerHour * ( m_iActiveCustomerCount / m_iScaleFactor )) * TPCEConstants.AbortTrade / 100 );  // 1.01 to account for rollbacks

        //  Set the start time (time 0) to the base time
        m_StartTime = EGenDate.getDateFromTime(
        		TPCEConstants.initialTradePopulationBaseYear,
        		TPCEConstants.initialTradePopulationBaseMonth,
        		TPCEConstants.initialTradePopulationBaseDay,
        		TPCEConstants.initialTradePopulationBaseHour,
        		TPCEConstants.initialTradePopulationBaseMinute,
        		TPCEConstants.initialTradePopulationBaseSecond,
        		TPCEConstants.initialTradePopulationBaseFraction );
//TODO
//        UpdateTunables(); //is called from CCE constructor (Initialize)
    }

    /*
    *  Return internal random number generator seed.
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           current random number generator seed.
    */
    public long GetRNGSeed()
    {
        return( m_rnd.getSeed() );
    }

    /*
    *  Set internal random number generator seed.
    *
    *  PARAMETERS:
    *           IN  RNGSeed     - new random number generator seed
    *
    *  RETURNS:
    *           none.
    */
    public void SetRNGSeed( long RNGSeed )
    {
        m_rnd.setSeed( RNGSeed );
    }

    /*
    *  Refresh internal information from the external transaction parameters.
    *  This function should be called anytime the external transaction
    *  parameter structure changes.
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           none.
    */
    public void UpdateTunables()
    {
    	
//    		System.out.println(m_pDriverCETxnSettings == null);
    	
        m_iTradeLookupFrame2MaxTimeInMilliSeconds = (long)(( m_iHoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( m_pDriverCETxnSettings.TL_settings.cur_BackOffFromEndTimeFrame2 )) * EGenDate.MsPerSecond;
        m_iTradeLookupFrame3MaxTimeInMilliSeconds = (long)(( m_iHoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( m_pDriverCETxnSettings.TL_settings.cur_BackOffFromEndTimeFrame3 )) * EGenDate.MsPerSecond;
        m_iTradeLookupFrame4MaxTimeInMilliSeconds = (long)(( m_iHoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( m_pDriverCETxnSettings.TL_settings.cur_BackOffFromEndTimeFrame4 )) * EGenDate.MsPerSecond;

        m_iTradeUpdateFrame2MaxTimeInMilliSeconds = (long)(( m_iHoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( m_pDriverCETxnSettings.TU_settings.cur_BackOffFromEndTimeFrame2 )) * EGenDate.MsPerSecond;
        m_iTradeUpdateFrame3MaxTimeInMilliSeconds = (long)(( m_iHoursOfInitialTrades * EGenDate.SecondsPerHour ) - ( m_pDriverCETxnSettings.TU_settings.cur_BackOffFromEndTimeFrame3 )) * EGenDate.MsPerSecond;

        // Set the completion time of the last initial trade.
        // 15 minutes are added at the end of hours of initial trades for pending trades.
        m_EndTime = m_StartTime;
        m_EndTime = EGenDate.AddWorkMs( m_EndTime, (long)(m_iHoursOfInitialTrades * EGenDate.SecondsPerHour + 15 * EGenDate.SecondsPerMinute) * EGenDate.MsPerSecond );

        // Based on 10 * Trade-Order transaction mix percentage.
        // This is currently how the mix levels are set, so use that.
        m_iTradeOrderRollbackLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;
        m_iTradeOrderRollbackLevel = m_pDriverCETxnSettings.TO_settings.cur_rollback;

        // Log Tunables
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.BV_settings);
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.CP_settings);
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.MW_settings);
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.SD_settings);
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.TL_settings);
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.TO_settings);
        m_pLogger.SendToLogger(m_pDriverCETxnSettings.TU_settings);
    }

    /*
    *  Generate customer account ID (uniformly distributed).
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           iCustomerAccountId uniformly distributed across all load units.
    */
    public long GenerateRandomCustomerAccountId()
    {
        long          customerID;
        long          iCustomerAccountId;
        TierId        tierID;
        Object[] customer = new Object[2];

        customer = m_CustomerSelection.genRandomCustomer();       
        customerID = Long.parseLong(customer[0].toString());
        tierID = (TierId)customer[1];
        
        iCustomerAccountId = m_Accs.genRandomAccId( m_rnd, customerID, tierID)[0];

        return(iCustomerAccountId);
    }

    /*
    *  Generate a trade id to be used in Trade-Lookup / Trade-Update Frame 1.
    *
    *  PARAMETERS:
    *           IN  aValue      - parameter to NURAND function
    *           IN  sValue      - parameter to NURAND function
    *
    *  RETURNS:
    *           TradeId, distributed non-uniformly.
    */
    public long GenerateNonUniformTradeID( int aValue, int sValue )
    {
    	long TradeId;

        TradeId = m_rnd.rndNU( 1, m_iMaxActivePrePopulatedTradeID, aValue, sValue );
        if ( HoldingsAndTrades.isAbortedTrade(TradeId) ){
            TradeId++;
        }
        TradeId += TPCEConstants.TRADE_SHIFT;
        return( TradeId );
    }

    /*
    *  Generate a trade timestamp to be used in Trade-Lookup / Trade-Update.
    *
    *  PARAMETERS:
    *           OUT dts                     - returned timestamp
    *           IN  maxTimeInMS   			- time interval (from the first initial trade) in which to generate the timestamp
    *           IN  aValue                  - parameter to NURAND function
    *           IN  sValue                  - parameter to NURAND function
    *
    *  RETURNS:
    *           TimestampType.
    */
    public TimestampType GenerateNonUniformTradeDTS( TimestampType dts, long maxTimeInMS, int aValue, int sValue )
    {
    	GregorianCalendar TradeTime = new GregorianCalendar(TPCEConstants.initialTradePopulationBaseYear,
    														TPCEConstants.initialTradePopulationBaseMonth,
    														TPCEConstants.initialTradePopulationBaseDay,
    														TPCEConstants.initialTradePopulationBaseHour,
    														TPCEConstants.initialTradePopulationBaseMinute,
    														TPCEConstants.initialTradePopulationBaseSecond );   
    	
    	TradeTime.setTimeInMillis(TPCEConstants.initialTradePopulationBaseFraction);
        long       TradeTimeOffset;

        TradeTimeOffset = m_rnd.rndNU( 1, maxTimeInMS, aValue, sValue );
        dts = EGenDate.getTimeStamp(EGenDate.AddWorkMs( TradeTime.getTime(), TradeTimeOffset ));
        return dts;
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
    public void GenerateBrokerVolumeInput(TBrokerVolumeTxnInput inputStructure){
    	int           numBrokers;
        int           count, i;
        long[]        brokerID = new long [TxnHarnessStructs.max_broker_list_len];
        int           sectorIndex;
        
        //Generates the Broker number randomly from the range
        numBrokers = m_rnd.intRange(TxnHarnessStructs.min_broker_list_len, TxnHarnessStructs.max_broker_list_len); 
        
        inputStructure.broker_list = new String[numBrokers];
		for (i = 0; i < numBrokers; ++i){
			inputStructure.broker_list[i] = new String();
        }
		
        if (numBrokers > m_Brokers.GetBrokerCount()){
            numBrokers = (int)m_Brokers.GetBrokerCount();
        }
        count = 0;
        do{
            brokerID[count] = m_Brokers.GenerateRandomBrokerId(m_rnd);
            for (i = 0; (i < count) && (brokerID[i] != brokerID[count]); ++i) { };
            if (i == count){
            	inputStructure.broker_list[i] = m_Brokers.generateBrokerName(brokerID[count]);
                ++count;
            }
        } while (count < numBrokers);

        sectorIndex = m_rnd.intRange(0, m_iSectorCount-1);
        inputStructure.sector_name = (String)m_pSectors.getTupleByIndex(sectorIndex)[1].toString();
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
    public void GenerateCustomerPositionInput(TCustomerPositionTxnInput inputStructure){
        Object[] customer = new Object[2];
        customer = m_CustomerSelection.genRandomCustomer();
        long customerID = Long.parseLong(customer[0].toString());
        TierId tierID = (TierId)customer[1];
        
        if (m_rnd.rndPercent(m_pDriverCETxnSettings.CP_settings.cur_by_tax_id)){
        	
        	inputStructure.tax_id = m_Person.getTaxID(customerID);
            inputStructure.cust_id = 0;
        }
        else{
            inputStructure.cust_id = customerID;
            inputStructure.tax_id = new String();
        }

        boolean get_history = m_rnd.rndPercent(m_pDriverCETxnSettings.CP_settings.cur_get_history);
        if( get_history ){
        	inputStructure.get_history = 1;
            inputStructure.acct_id_idx = m_rnd.intRange( 0, (int)m_Accs.genRandomAccId(m_rnd, customerID, tierID)[1] - 1);
        }
        else{
        	inputStructure.get_history = 0;
            inputStructure.acct_id_idx = -1;
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
    public void GenerateMarketWatchInput(TMarketWatchTxnInput inputStructure){
        long          customerID;
        TierId        tierID;
        int           threshold;
        int           week;
        int           dailyMarketDay;
        Object[] customer = new Object[2];
        Date date = EGenDate.getDateFromTime(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
        							TPCEConstants.dailyMarketBaseDay, TPCEConstants.dailyMarketBaseHour,
        							TPCEConstants.dailyMarketBaseMinute, TPCEConstants.dailyMarketBaseSecond, TPCEConstants.dailyMarketBaseMsec);
        threshold = m_rnd.rndPercentage();
        if (threshold <= m_pDriverCETxnSettings.MW_settings.cur_by_industry){
            
        	inputStructure.industry_name = m_pIndustries.getTupleByIndex(m_rnd.intRange(0, m_iIndustryCount-1))[1];
            inputStructure.c_id = inputStructure.acct_id = 0;

            if( TxnHarnessStructs.iBaseCompanyCount < m_iActiveCompanyCount ){
                inputStructure.starting_co_id = m_rnd.int64Range( m_iStartFromCompany, m_iStartFromCompany + m_iActiveCompanyCount - ( TxnHarnessStructs.iBaseCompanyCount - 1 ));
                inputStructure.ending_co_id = inputStructure.starting_co_id + ( TxnHarnessStructs.iBaseCompanyCount - 1 );
            }
            else{
                inputStructure.starting_co_id = m_iStartFromCompany;
                inputStructure.ending_co_id = m_iStartFromCompany + m_iActiveCompanyCount - 1;
            }
        }
        else{
			inputStructure.starting_co_id = 0;
			inputStructure.ending_co_id = 0;
			inputStructure.industry_name = new String();
            customer = m_CustomerSelection.genRandomCustomer();
            
            if (threshold <= (m_pDriverCETxnSettings.MW_settings.cur_by_industry + m_pDriverCETxnSettings.MW_settings.cur_by_watch_list)){
                tierID = (TierId)customer[1];
                inputStructure.c_id = Long.parseLong(customer[0].toString());
                
                inputStructure.acct_id = 0;
            }
            else{
            	customerID = Long.parseLong(customer[0].toString());
            	tierID = (TierId)customer[1];
            	inputStructure.acct_id = m_Accs.genRandomAccId(m_rnd, customerID, tierID, inputStructure.acct_id, -1)[0];

                inputStructure.c_id = 0;
            }
        }

        week = (int)m_rnd.rndNU(0, 255, 255, 0) + 5; 
       
        threshold = m_rnd.rndPercentage();
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

        inputStructure.start_day = EGenDate.getTimeStamp(date);
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
    public void GenerateSecurityDetailInput(TSecurityDetailTxnInput inputStructure)
    {
    	Date date = EGenDate.getDateFromTime(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
				TPCEConstants.dailyMarketBaseDay, TPCEConstants.dailyMarketBaseHour,
				TPCEConstants.dailyMarketBaseMinute, TPCEConstants.dailyMarketBaseSecond, TPCEConstants.dailyMarketBaseMsec);
        int startDay;

        char[] tmp = m_pSecurities.createSymbol( m_rnd.int64Range(0, m_iActiveSecurityCount-1), TableConsts.cSYMBOL_len).toCharArray();
        
        inputStructure.symbol = String.copyValueOf(tmp, 0, tmp.length);

        if(m_rnd.rndPercent( m_pDriverCETxnSettings.SD_settings.cur_LOBAccessPercentage ))
        	inputStructure.access_lob_flag = 1;
        else inputStructure.access_lob_flag = 0;

        inputStructure.max_rows_to_return = m_rnd.intRange(TPCEConstants.iSecurityDetailMinRows, TPCEConstants.iSecurityDetailMaxRows);

        startDay = m_rnd.intRange(0, DailyMarketGenerator.iDailyMarketTotalRows - inputStructure.max_rows_to_return);

        date = EGenDate.addDaysMsecs(date, startDay, 0, false);
        inputStructure.start_day = EGenDate.getTimeStamp(date);
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
    public void GenerateTradeLookupInput(TTradeLookupTxnInput inputStructure){
        int           threshold;
        threshold = m_rnd.rndPercentage();

        if( threshold <= m_pDriverCETxnSettings.TL_settings.cur_do_frame1 ){  
            inputStructure.frame_to_execute = 1;
            inputStructure.max_trades = m_pDriverCETxnSettings.TL_settings.cur_MaxRowsFrame1;

            int     i, j;
            boolean    accepted;
            long  tradeID;

            for( i = 0; i < inputStructure.max_trades; i++ ){
                accepted = false;
                while( ! accepted ){
                    tradeID = GenerateNonUniformTradeID(TPCEConstants.TradeLookupAValueForTradeIDGenFrame1,
                    		TPCEConstants.TradeLookupSValueForTradeIDGenFrame1);
                    j = 0;
                    while( j < i && inputStructure.trade_id[j] != tradeID ){
                        j++;
                    }
                    if( j == i ){
                        inputStructure.trade_id[i] = tradeID;
                        accepted = true;
                    }
                }
            }
        
            inputStructure.acct_id = 0;                                          
            inputStructure.max_acct_id = 0;                                      
            inputStructure.symbol = new String();							     
            
            GregorianCalendar date = new GregorianCalendar(0,0,0,0,0);        
            inputStructure.start_trade_dts = new TimestampType(date.getTime());  
            inputStructure.end_trade_dts = new TimestampType(date.getTime());  
        }
        else if( threshold <=  m_pDriverCETxnSettings.TL_settings.cur_do_frame1 + m_pDriverCETxnSettings.TL_settings.cur_do_frame2 ){
            inputStructure.frame_to_execute = 2;
            inputStructure.acct_id = GenerateRandomCustomerAccountId();
            inputStructure.max_trades = m_pDriverCETxnSettings.TL_settings.cur_MaxRowsFrame2;

            inputStructure.start_trade_dts = GenerateNonUniformTradeDTS( inputStructure.start_trade_dts, m_iTradeLookupFrame2MaxTimeInMilliSeconds, TPCEConstants.TradeLookupAValueForTimeGenFrame2, TPCEConstants.TradeLookupSValueForTimeGenFrame2 );
            
            inputStructure.end_trade_dts = EGenDate.getTimeStamp( m_EndTime);

            inputStructure.max_acct_id = 0;
            inputStructure.symbol = new String();
            Arrays.fill(inputStructure.trade_id, 0);
        }
        else if( threshold <=  m_pDriverCETxnSettings.TL_settings.cur_do_frame1 + m_pDriverCETxnSettings.TL_settings.cur_do_frame2 + m_pDriverCETxnSettings.TL_settings.cur_do_frame3 ){

            inputStructure.frame_to_execute = 3;
            inputStructure.max_trades = m_pDriverCETxnSettings.TL_settings.cur_MaxRowsFrame3;

            inputStructure.symbol = m_pSecurities.createSymbol( m_rnd.rndNU( 0, m_iActiveSecurityCount-1,
            		TPCEConstants.TradeLookupAValueForSymbolFrame3,
            		TPCEConstants.TradeLookupSValueForSymbolFrame3 ),TableConsts.cSYMBOL_len);

            inputStructure.start_trade_dts = GenerateNonUniformTradeDTS( inputStructure.start_trade_dts,  m_iTradeLookupFrame3MaxTimeInMilliSeconds, TPCEConstants.TradeLookupAValueForTimeGenFrame3, TPCEConstants.TradeLookupSValueForTimeGenFrame3 );

            inputStructure.end_trade_dts = EGenDate.getTimeStamp(m_EndTime);

            inputStructure.max_acct_id = CustomerAccountsGenerator.getEndtingAccId( m_iActiveCustomerCount );
     
            inputStructure.acct_id = 0;
            Arrays.fill(inputStructure.trade_id, 0);
        }
        else{
            inputStructure.frame_to_execute = 4;
            inputStructure.acct_id = GenerateRandomCustomerAccountId();
            inputStructure.start_trade_dts = GenerateNonUniformTradeDTS( inputStructure.start_trade_dts, m_iTradeLookupFrame4MaxTimeInMilliSeconds, TPCEConstants.TradeLookupAValueForTimeGenFrame4, TPCEConstants.TradeLookupSValueForTimeGenFrame4 );
            inputStructure.max_trades = 0;                                  
            inputStructure.max_acct_id = 0;
            inputStructure.symbol = new String();
            Arrays.fill(inputStructure.trade_id, 0);
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
    public void GenerateTradeOrderInput(TTradeOrderTxnInput inputStructure, int iTradeType, boolean executorIsAccountOwner){
        long          customerID;
        TierId   		tierID;
        boolean            bMarket;
        int           additionalPerms;
        int             secAcct;
        long          secFlatFileIndex;
        String[] flTaxId;
        TradeType    eTradeType;

        Object[] customer = new Object[2];

        customer = m_CustomerSelection.genRandomCustomer();
        
        customerID = Long.parseLong(customer[0].toString());
        tierID = (TierId)customer[1];
        
        long[] randomAccSecurity = m_Holdings.generateRandomAccSecurity(customerID, tierID);

        inputStructure.acct_id = randomAccSecurity[0];
        secFlatFileIndex = randomAccSecurity[2];
        secAcct = (int)randomAccSecurity[1];
        additionalPerms = m_AccountPerms.getNumPermsForAcc(inputStructure.acct_id);
        if (additionalPerms == 1){
        	flTaxId = m_Person.getFirstNameLastNameTaxID(customerID);
        	
            executorIsAccountOwner = true;
        }
        else{
            int exec_is_owner = (m_pDriverCETxnSettings.TO_settings.cur_exec_is_owner - AccountPermsGenerator.percentAccountAdditionalPermissions_0) * 100 / (100 - AccountPermsGenerator.percentAccountAdditionalPermissions_0);

            if ( m_rnd.rndPercent(exec_is_owner) ){
                flTaxId = m_Person.getFirstNameLastNameTaxID(customerID);

                executorIsAccountOwner = true;
            }
            else{
            	m_AccountPerms.generateCids();
                if (additionalPerms == 2){

                	flTaxId = m_Person.getFirstNameLastNameTaxID(m_AccountPerms.permCids[1]);
                }
                else{
                	flTaxId = m_Person.getFirstNameLastNameTaxID(m_AccountPerms.permCids[2]);
                }

                executorIsAccountOwner = false;
            }
        }
        
        inputStructure.exec_f_name = flTaxId[0];
    	inputStructure.exec_l_name = flTaxId[1];
    	inputStructure.exec_tax_id = flTaxId[2];

        if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_security_by_symbol)){
        	char[] tmp = m_pSecurities.createSymbol(secFlatFileIndex, TableConsts.cSYMBOL_len).toCharArray();
            
            inputStructure.symbol = String.copyValueOf(tmp, 0, tmp.length);
            inputStructure.co_name = new String();
            inputStructure.issue = new String();
        }
        else{
        	inputStructure.co_name = m_pCompanies.generateCompanyName( m_pSecurities.getCompanyIndex( secFlatFileIndex ));

        	char[] tmp = m_pSecurities.getSecRecord(secFlatFileIndex)[1].toCharArray();
        	
        	inputStructure.issue = String.copyValueOf(tmp, 0, tmp.length);

        	inputStructure.symbol = new String();
        }

        inputStructure.trade_qty = HoldingsAndTrades.TRADE_QTY_SIZES[m_rnd.intRange(0, HoldingsAndTrades.TRADE_QTY_SIZES.length - 1)];
        inputStructure.requested_price = m_rnd.doubleIncrRange(HoldingsAndTrades.fMinSecPrice, HoldingsAndTrades.fMaxSecPrice, 0.01);

        bMarket = m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_market);

        if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_buy_orders)){
            if (bMarket){
                eTradeType = TradeType.eMarketBuy;
            }
            else{
                eTradeType = TradeType.eLimitBuy;
            }
            
            if (m_rnd.rndPercent(
                    m_pDriverCETxnSettings.TO_settings.cur_type_is_margin *
                    100 /
                    m_pDriverCETxnSettings.TO_settings.cur_buy_orders)){
            	inputStructure.type_is_margin = 1;
            }
            else inputStructure.type_is_margin = 0;
            
        }
        else{
            if (bMarket){
                eTradeType = TradeType.eMarketSell;
            }
            else{
                if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_stop_loss)){
                    eTradeType = TradeType.eStopLoss;
                }
                else{
                    eTradeType = TradeType.eLimitSell;
                }
            }

            inputStructure.type_is_margin = 0;
        }
        iTradeType = eTradeType.getValue();
        
        if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_lifo)){
        	inputStructure.is_lifo = 1;
        }
        else inputStructure.is_lifo = 0;

        char[] tmp = (m_pTradeType.getTupleByIndex(eTradeType.getValue()))[0].toCharArray();
        inputStructure.trade_type_id = String.copyValueOf(tmp, 0, tmp.length);

        tmp = (m_pStatusType.getTupleByIndex(StatusTypeId.E_PENDING.getValue()))[0].toCharArray();
        inputStructure.st_pending_id = String.copyValueOf(tmp, 0, tmp.length);
        tmp = (m_pStatusType.getTupleByIndex(StatusTypeId.E_SUBMITTED.getValue()))[0].toCharArray();
        inputStructure.st_submitted_id = String.copyValueOf(tmp, 0, tmp.length);
        
        if ( m_iTradeOrderRollbackLevel >= m_rnd.intRange( 1, m_iTradeOrderRollbackLimit )){
        	inputStructure.roll_it_back = 1;
        }
        else inputStructure.roll_it_back = 0;
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
    public void GenerateTradeStatusInput(TTradeStatusTxnInput inputStructure){
        long          customerID;
        TierId   	  tierID;

        Object[] customer = new Object[2];
        customer = m_CustomerSelection.genRandomCustomer();
        customerID = Long.parseLong(customer[0].toString());
        tierID = (TierId)customer[1];
        inputStructure.acct_id = m_Accs.genRandomAccId(m_rnd, customerID, tierID, inputStructure.acct_id, -1)[0];
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
    public void GenerateTradeUpdateInput(TTradeUpdateTxnInput inputStructure){
        int           threshold;

        threshold = m_rnd.rndPercentage();

        if( threshold <= m_pDriverCETxnSettings.TU_settings.cur_do_frame1 ){
            inputStructure.frame_to_execute = 1;
            inputStructure.max_trades = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsFrame1;
            inputStructure.max_updates = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame1;

            int     i, j;
            boolean    accepted;
            long  tradeID;

            for( i = 0; i < inputStructure.max_trades; i++ ){
                accepted = false;
                while( ! accepted ){
                    tradeID = GenerateNonUniformTradeID(TPCEConstants.TradeUpdateAValueForTradeIDGenFrame1, TPCEConstants.TradeUpdateSValueForTradeIDGenFrame1);
                    j = 0;
                    while( j < i && inputStructure.trade_id[j] != tradeID ){
                        j++;
                    }
                    if( j == i ){
                        inputStructure.trade_id[i] = tradeID;
                        accepted = true;
                    }
                }
            }

            inputStructure.acct_id = 0;                                                     
            inputStructure.max_acct_id = 0;                                                 
            inputStructure.symbol = new String();
            
            GregorianCalendar date = new GregorianCalendar(0,0,0,0,0);        
            inputStructure.start_trade_dts = new TimestampType(date.getTime());  
            inputStructure.end_trade_dts = new TimestampType(date.getTime());  
        }
        else if( threshold <=  m_pDriverCETxnSettings.TU_settings.cur_do_frame1 +
                                m_pDriverCETxnSettings.TU_settings.cur_do_frame2 ){
            inputStructure.frame_to_execute = 2;
            inputStructure.max_trades = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsFrame2;
            inputStructure.max_updates = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame2;
            inputStructure.acct_id = GenerateRandomCustomerAccountId();
System.out.println("CETXN: LINE 985: inputStructure.start_trade_dts: " + inputStructure.start_trade_dts);
			inputStructure.start_trade_dts = GenerateNonUniformTradeDTS( inputStructure.start_trade_dts, m_iTradeUpdateFrame2MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeUpdateAValueForTimeGenFrame2, TPCEConstants.TradeUpdateSValueForTimeGenFrame2 );
System.out.println("CETXN: LINE 988: inputStructure.start_trade_dts: " + inputStructure.start_trade_dts);
			inputStructure.end_trade_dts = EGenDate.getTimeStamp(m_EndTime);

            inputStructure.max_acct_id = 0;                                 
            inputStructure.symbol = new String();
            Arrays.fill( inputStructure.trade_id, 0); 
            
        }
        else{
            inputStructure.frame_to_execute = 3;
            inputStructure.max_trades = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsFrame3;
            inputStructure.max_updates = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame3;
            
            inputStructure.symbol = m_pSecurities.createSymbol( m_rnd.rndNU( 0, m_iActiveSecurityCount-1,
            		TPCEConstants.TradeLookupAValueForSymbolFrame3, TPCEConstants.TradeLookupSValueForSymbolFrame3 ),TableConsts.cSYMBOL_len);

            inputStructure.start_trade_dts = GenerateNonUniformTradeDTS( inputStructure.start_trade_dts, m_iTradeUpdateFrame3MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeLookupAValueForTimeGenFrame3, TPCEConstants.TradeLookupSValueForTimeGenFrame3 );

            inputStructure.end_trade_dts = EGenDate.getTimeStamp(m_EndTime);

            inputStructure.max_acct_id = CustomerAccountsGenerator.getEndtingAccId( m_iActiveCustomerCount );
            inputStructure.acct_id = 0;                                     
            Arrays.fill( inputStructure.trade_id, 0); 

        }
    }
}
