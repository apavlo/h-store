package edu.brown.benchmark.tpce.generators;

import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

import org.voltdb.catalog.Table;

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

        // UpdateTunables() is called from CCE constructor (Initialize)
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
        EGenDate.AddWorkMs( m_EndTime, (long)(m_iHoursOfInitialTrades * EGenDate.SecondsPerHour + 15 * EGenDate.SecondsPerMinute) * EGenDate.MsPerSecond );

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
    *  Generate Non-Uniform customer ID.
    *
    *  PARAMETERS:
    *           OUT  iCustomerId        - generated C_ID
    *           OUT  iCustomerTier      - generated C_TIER
    *
    *  RETURNS:
    *           none.
    */
 /*   public static void GenerateNonUniformRandomCustomerId()
    {
        m_CustomerSelection.genRandomCustomer();
    }
*/
    /*
    *  Generate customer account ID (uniformly distributed).
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           CA_ID uniformly distributed across all load units.
    */
    public long GenerateRandomCustomerAccountId()
    {
        long          iCustomerId;
        long          iCustomerAccountId;
        TierId        iCustomerTier;
        Object[] customerId = new Object[2];

        customerId = m_CustomerSelection.genRandomCustomer();       
        iCustomerId = Long.parseLong(customerId[0].toString());
        iCustomerTier = (TierId)customerId[1];
        
        iCustomerAccountId = m_Accs.genRandomAccId( m_rnd, iCustomerId, iCustomerTier)[0];

        return(iCustomerAccountId);
    }

    /*
    *  Generate a trade id to be used in Trade-Lookup / Trade-Update Frame 1.
    *
    *  PARAMETERS:
    *           IN  AValue      - parameter to NURAND function
    *           IN  SValue      - parameter to NURAND function
    *
    *  RETURNS:
    *           T_ID, distributed non-uniformly.
    */
    public long GenerateNonUniformTradeID( int AValue, int SValue )
    {
    	long TradeId;

        TradeId = m_rnd.rndNU( 1, m_iMaxActivePrePopulatedTradeID, AValue, SValue );

        // Skip over trade id's that were skipped over during load time.
        if ( HoldingsAndTrades.isAbortedTrade(TradeId) )
        {
            TradeId++;
        }

        TradeId += TPCEConstants.TRADE_SHIFT;    // shift trade id to 64-bit value

        return( TradeId );
    }

    /*
    *  Generate a trade timestamp to be used in Trade-Lookup / Trade-Update.
    *
    *  PARAMETERS:
    *           OUT dts                     - returned timestamp
    *           IN  MaxTimeInMilliSeconds   - time interval (from the first initial trade) in which to generate the timestamp
    *           IN  AValue                  - parameter to NURAND function
    *           IN  SValue                  - parameter to NURAND function
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateNonUniformTradeDTS( Date dts, long MaxTimeInMilliSeconds, int AValue, int SValue )
    {
    	GregorianCalendar TradeTime = new GregorianCalendar(TPCEConstants.initialTradePopulationBaseYear,
    			TPCEConstants.initialTradePopulationBaseMonth,
    			TPCEConstants.initialTradePopulationBaseDay,
    			TPCEConstants.initialTradePopulationBaseHour,
    			TPCEConstants.initialTradePopulationBaseMinute,
    			TPCEConstants.initialTradePopulationBaseSecond );   //NOTE: Interpret Fraction as milliseconds,
                                                                        // probably 0 anyway.
    	TradeTime.setTimeInMillis(TPCEConstants.initialTradePopulationBaseFraction);
        long       TradeTimeOffset;

        // Generate random number of seconds from the base time.
        //
        TradeTimeOffset = m_rnd.rndNU( 1, MaxTimeInMilliSeconds, AValue, SValue );

        // The time we have is an offset into the initial pre-populated trading time.
        // This needs to be converted into a "real" time taking into account 8 hour
        // business days, etc.

        EGenDate.AddWorkMs( TradeTime.getTime(), TradeTimeOffset );
        EGenDate.getTimeStamp( dts, TradeTime.getTime() );
    }

    /*
    *  Generate Broker-Volume transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           the number of brokers generated.
    */
    public void GenerateBrokerVolumeInput(TBrokerVolumeTxnInput TxnReq)
    {
    	int           iNumBrokers;
        int           iCount, i;
        long[]        B_ID = new long [TxnHarnessStructs.max_broker_list_len];
        int           iSectorIndex;

        //init all broker names to null
        for (i = 0; i < TxnHarnessStructs.max_broker_list_len; ++i)
        {
            TxnReq.broker_list[i] = null;
        }

        // Select the range of brokers, honoring partitioning by CID settings.
        //iBrokersStart = iStartingBrokerID;
        //iBrokersCount = m_iActiveCustomerCount / iBrokersDiv;
        iNumBrokers = m_rnd.intRange(TxnHarnessStructs.min_broker_list_len, TxnHarnessStructs.max_broker_list_len);       // 20..40 brokers
        // Small databases (<=4LUs) may contain less than the chosen number of brokers.
        // Broker names for Broker Volume are unique, so need to re-adjust or be caught
        // in an infinite loop below.
        if (iNumBrokers > m_Brokers.GetBrokerCount())	//m_Brokers.GetBrokerCount() = 50;
        {
            iNumBrokers = (int)m_Brokers.GetBrokerCount();     // adjust for small databases
        }

        iCount = 0;
        do
        {
            //select random broker ID (from active customer range)
            B_ID[iCount] = m_Brokers.GenerateRandomBrokerId(m_rnd);

            for (i = 0; (i < iCount) && (B_ID[i] != B_ID[iCount]); ++i) { };

            if (i == iCount)    //make sure brokers are distinct
            {
                //put the broker name into the input parameter
                m_Brokers.generateBrokerName(B_ID[iCount]);
                ++iCount;
            }

        } while (iCount < iNumBrokers);

        //select sector name
        iSectorIndex = m_rnd.intRange(0, m_iSectorCount-1);

        TxnReq.sector_name = (String)m_pSectors.getTupleByIndex(iSectorIndex)[1].toString();
//        System.arraycopy((String)m_pSectors.getTupleByIndex(iSectorIndex)[1], 0, TxnReq.sector_name, 0, TableConsts.cSC_NAME_len);
    }


    /*
    *  Generate Customer-Position transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateCustomerPositionInput(TCustomerPositionTxnInput TxnReq)
    {
        Object[] customerId = new Object[2];

        customerId = m_CustomerSelection.genRandomCustomer();
        
        long iCustomerId = Long.parseLong(customerId[0].toString());
        TierId tierID = (TierId)customerId[1];
        
        if (m_rnd.rndPercent(m_pDriverCETxnSettings.CP_settings.cur_by_tax_id))
        {
            //send tax id instead of customer id
        	if (TxnReq == null){
        		System.out.println("null");
        	}
        	TxnReq.tax_id = m_Person.getTaxID(iCustomerId).toCharArray();

            TxnReq.cust_id = 0; //don't need customer id since filled in the tax id
        }
        else
        {
            // send customer id and not the tax id
            TxnReq.cust_id = iCustomerId;

            TxnReq.tax_id[0] = '\0';
        }

        TxnReq.get_history = m_rnd.rndPercent(m_pDriverCETxnSettings.CP_settings.cur_get_history);
        if( TxnReq.get_history )
        {
            TxnReq.acct_id_idx = m_rnd.intRange( 0, (int)m_Accs.genRandomAccId(m_rnd, iCustomerId, tierID)[1] - 1);
        }
        else
        {
            TxnReq.acct_id_idx = -1;
        }
    }

    /*
    *  Generate Market-Watch transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateMarketWatchInput(TMarketWatchTxnInput TxnReq)
    {
        long          iCustomerId;
        TierId        iCustomerTier;
        int           iThreshold;
        int           iWeek;
        int           iDailyMarketDay;
        Object[] customerId = new Object[2];

        Date date = EGenDate.getDateFromTime(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
        							TPCEConstants.dailyMarketBaseDay, TPCEConstants.dailyMarketBaseHour,
        							TPCEConstants.dailyMarketBaseMinute, TPCEConstants.dailyMarketBaseSecond, TPCEConstants.dailyMarketBaseMsec);
        iThreshold = m_rnd.rndPercentage();

        //have some distribution on what inputs to send
        if (iThreshold <= m_pDriverCETxnSettings.MW_settings.cur_by_industry)
        {
            //send industry name
            System.arraycopy(m_pIndustries.getTupleByIndex(m_rnd.intRange(0, m_iIndustryCount-1)), 0, TxnReq.industry_name, 0, TableConsts.cIN_NAME_len);

            TxnReq.c_id = TxnReq.acct_id = 0;

            if( TxnHarnessStructs.iBaseCompanyCount < m_iActiveCompanyCount )
            {
                TxnReq.starting_co_id = m_rnd.int64Range( m_iStartFromCompany,
                                                            m_iStartFromCompany +
                                                            m_iActiveCompanyCount - ( TxnHarnessStructs.iBaseCompanyCount - 1 ));
                TxnReq.ending_co_id = TxnReq.starting_co_id + ( TxnHarnessStructs.iBaseCompanyCount - 1 );
            }
            else
            {
                TxnReq.starting_co_id = m_iStartFromCompany;
                TxnReq.ending_co_id = m_iStartFromCompany + m_iActiveCompanyCount - 1;
            }
        }
        else
        {
            TxnReq.industry_name[0] = '\0';
            TxnReq.starting_co_id = 0;
            TxnReq.ending_co_id = 0;
            customerId = m_CustomerSelection.genRandomCustomer();
            
            if (iThreshold <= (m_pDriverCETxnSettings.MW_settings.cur_by_industry + m_pDriverCETxnSettings.MW_settings.cur_by_watch_list))
            {
                // Send customer id
            	
                iCustomerTier = (TierId)customerId[1];
                TxnReq.c_id = Long.parseLong(customerId[0].toString());
                
                TxnReq.acct_id = 0;
            }
            else
            {
                // Send account id
            	
            	iCustomerId = Long.parseLong(customerId[0].toString());
            	iCustomerTier = (TierId)customerId[1];
            	m_Accs.genRandomAccId(m_rnd, iCustomerId, iCustomerTier, TxnReq.acct_id, -1);

                TxnReq.c_id = 0;
            }
        }

        // Set start_day for both cases of the 'if'.
        //
        iWeek = (int)m_rnd.rndNU(0, 255, 255, 0) + 5; // A = 255, S = 0
        // Week is now between 5 and 260.
        // Select a day within the week.
        //
        iThreshold = m_rnd.rndPercentage();
        if (iThreshold > 40)
        {
            iDailyMarketDay = iWeek * EGenDate.DaysPerWeek + 4;    // Friday
        }
        else    // 1..40 case
        {
            if (iThreshold <= 20)
            {
                iDailyMarketDay = iWeek * EGenDate.DaysPerWeek;    // Monday
            }
            else
            {
                if (iThreshold <= 27)
                {
                    iDailyMarketDay = iWeek * EGenDate.DaysPerWeek + 1;    // Tuesday
                }
                else
                {
                    if (iThreshold <= 33)
                    {
                        iDailyMarketDay = iWeek * EGenDate.DaysPerWeek + 2;    // Wednesday
                    }
                    else
                    {
                        iDailyMarketDay = iWeek * EGenDate.DaysPerWeek + 3;    // Thursday
                    }
                }
            }
        }

        // Go back 256 weeks and then add our calculated day.
        //
        date = EGenDate.addDaysMsecs(date, iDailyMarketDay, 0, false);

        EGenDate.getTimeStamp(TxnReq.start_day, date);
    }

    /*
    *  Generate Security-Detail transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateSecurityDetailInput(TSecurityDetailTxnInput TxnReq)
    {
    	Date date = EGenDate.getDateFromTime(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
				TPCEConstants.dailyMarketBaseDay, TPCEConstants.dailyMarketBaseHour,
				TPCEConstants.dailyMarketBaseMinute, TPCEConstants.dailyMarketBaseSecond, TPCEConstants.dailyMarketBaseMsec);
        int       iStartDay;  // day from the StartDate

        // random symbol
        TxnReq.symbol = m_pSecurities.createSymbol( m_rnd.int64Range(0, m_iActiveSecurityCount-1), TxnReq.symbol.toString().length()).toCharArray();

        // Whether or not to access the LOB.
        TxnReq.access_lob_flag = m_rnd.rndPercent( m_pDriverCETxnSettings.SD_settings.cur_LOBAccessPercentage );

        // random number of financial rows to return
        TxnReq.max_rows_to_return = m_rnd.intRange(TPCEConstants.iSecurityDetailMinRows, TPCEConstants.iSecurityDetailMaxRows);

        iStartDay = m_rnd.intRange(0, DailyMarketGenerator.iDailyMarketTotalRows - TxnReq.max_rows_to_return);

        // add the offset
        date = EGenDate.addDaysMsecs(date, iStartDay, 0, false);
//TODO unsure
        EGenDate.getTimeStamp(TxnReq.start_day, date);
    }

    /*
    *  Generate Trade-Lookup transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateTradeLookupInput(TTradeLookupTxnInput TxnReq)
    {
        int           iThreshold;

        iThreshold = m_rnd.rndPercentage();

        if( iThreshold <= m_pDriverCETxnSettings.TL_settings.cur_do_frame1 )
        {
            // Frame 1
            TxnReq.frame_to_execute = 1;
            TxnReq.max_trades = m_pDriverCETxnSettings.TL_settings.cur_MaxRowsFrame1;

            // Generate list of unique trade id's
            int     ii, jj;
            boolean    Accepted;
            long  TID;

            for( ii = 0; ii < TxnReq.max_trades; ii++ )
            {
                Accepted = false;
                while( ! Accepted )
                {
                    TID = GenerateNonUniformTradeID(TPCEConstants.TradeLookupAValueForTradeIDGenFrame1,
                    		TPCEConstants.TradeLookupSValueForTradeIDGenFrame1);
                    jj = 0;
                    while( jj < ii && TxnReq.trade_id[jj] != TID )
                    {
                        jj++;
                    }
                    if( jj == ii )
                    {
                        // We have a unique TID for this batch
                        TxnReq.trade_id[ii] = TID;
                        Accepted = true;
                    }
                }
            }

            // Params not used by this frame /////////////////////////////////////////
            TxnReq.acct_id = 0;                                                     //
            TxnReq.max_acct_id = 0;                                                 //
            Arrays.fill(TxnReq.symbol, '\0');                                       //
            TxnReq.start_trade_dts = new GregorianCalendar(0,0,0,0,0,0).getTime();  //
            TxnReq.end_trade_dts = new GregorianCalendar(0,0,0,0,0,0).getTime();    //
            //////////////////////////////////////////////////////////////////////////
        }
        else if( iThreshold <=  m_pDriverCETxnSettings.TL_settings.cur_do_frame1 +
                                m_pDriverCETxnSettings.TL_settings.cur_do_frame2 )
        {
            // Frame 2
            TxnReq.frame_to_execute = 2;
            TxnReq.acct_id = GenerateRandomCustomerAccountId();
            TxnReq.max_trades = m_pDriverCETxnSettings.TL_settings.cur_MaxRowsFrame2;

            GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
                                        m_iTradeLookupFrame2MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeLookupAValueForTimeGenFrame2,
                                        TPCEConstants.TradeLookupSValueForTimeGenFrame2 );

            // Set to the end of initial trades.
            
            EGenDate.getTimeStamp(TxnReq.end_trade_dts, m_EndTime);

            // Params not used by this frame /////////////////////////
            TxnReq.max_acct_id = 0;                                 //
            Arrays.fill(TxnReq.symbol, '\0');     					//
            Arrays.fill( TxnReq.trade_id, 0); 						//
            //////////////////////////////////////////////////////////
        }
        else if( iThreshold <=  m_pDriverCETxnSettings.TL_settings.cur_do_frame1 +
                                m_pDriverCETxnSettings.TL_settings.cur_do_frame2 +
                                m_pDriverCETxnSettings.TL_settings.cur_do_frame3 )
        {
            // Frame 3
            TxnReq.frame_to_execute = 3;
            TxnReq.max_trades = m_pDriverCETxnSettings.TL_settings.cur_MaxRowsFrame3;

            TxnReq.symbol = m_pSecurities.createSymbol( m_rnd.rndNU( 0, m_iActiveSecurityCount-1,
            		TPCEConstants.TradeLookupAValueForSymbolFrame3,
            		TPCEConstants.TradeLookupSValueForSymbolFrame3 ),TableConsts.cSYMBOL_len).toCharArray();

            GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
                                        m_iTradeLookupFrame3MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeLookupAValueForTimeGenFrame3,
                                        TPCEConstants.TradeLookupSValueForTimeGenFrame3 );

            // Set to the end of initial trades.
            EGenDate.getTimeStamp(TxnReq.end_trade_dts, m_EndTime);

            TxnReq.max_acct_id = CustomerAccountsGenerator.getEndtingAccId( m_iActiveCustomerCount );
            // Params not used by this frame /////////////////////////
            TxnReq.acct_id = 0;                                     //
            Arrays.fill( TxnReq.trade_id, 0); //
            //////////////////////////////////////////////////////////
        }
        else
        {
            // Frame 4
            TxnReq.frame_to_execute = 4;
            TxnReq.acct_id = GenerateRandomCustomerAccountId();
            GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
                                        m_iTradeLookupFrame4MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeLookupAValueForTimeGenFrame4,
                                        TPCEConstants.TradeLookupSValueForTimeGenFrame4 );

            // Params not used by this frame /////////////////////////
            TxnReq.max_trades = 0;                                  //
            TxnReq.max_acct_id = 0;									//
            Arrays.fill( TxnReq.symbol, '0');     					//
            Arrays.fill( TxnReq.trade_id, 0); 						//
            //////////////////////////////////////////////////////////
        }
    }

    /*
    *  Generate Trade-Order transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *           OUT TradeType               - integer representation of generated trade type (as eTradeTypeID enum).
    *           OUT bExecutorIsAccountOwner - whether Trade-Order frame 2 should (FALSE) or shouldn't (TRUE) be called.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateTradeOrderInput(TTradeOrderTxnInput TxnReq, int iTradeType, boolean bExecutorIsAccountOwner)
    {
        long          iCustomerId;    //owner
        TierId   		iCustomerTier;
        long          CID_1, CID_2;
        boolean            bMarket;
        int           iAdditionalPerms;
        int             iSymbIndex;
        long          iFlatFileSymbIndex;
        String[] flTaxId;
        TradeType    eTradeType;

        // Generate random customer
        //
        Object[] customerId = new Object[2];

        customerId = m_CustomerSelection.genRandomCustomer();
        
        iCustomerId = Long.parseLong(customerId[0].toString());
        iCustomerTier = (TierId)customerId[1];

        // Generate random account id and security index
        // 
        long[] randomAccSecurity = m_Holdings.generateRandomAccSecurity(iCustomerId, iCustomerTier);

        TxnReq.acct_id = randomAccSecurity[0];
        iFlatFileSymbIndex = randomAccSecurity[1];
        iSymbIndex = (int)randomAccSecurity[2];
        //find out how many permission rows there are for this account (in addition to the owner's)
        iAdditionalPerms = m_AccountPerms.getNumPermsForAcc(TxnReq.acct_id);
        //distribution same as in the loader for now
        if (iAdditionalPerms == 0)
        {   //select the owner
        	flTaxId = m_Person.getFirstNameLastNameTaxID(iCustomerId);
        	
            bExecutorIsAccountOwner = true;
        }
        else
        {
            // If there is more than one permission set on the account,
            // have some distribution on whether the executor is still
            // the account owner, or it is one of the additional permissions.
            // Here we must take into account the fact that we've excluded
            // a large portion of customers that don't have any additional
            // executors in the above code (iAdditionalPerms == 0); the
            // "exec_is_owner" percentage implicitly includes such customers
            // and must be factored out here.

            int exec_is_owner = (m_pDriverCETxnSettings.TO_settings.cur_exec_is_owner - AccountPermsGenerator.percentAccountAdditionalPermissions_0) * 100 / (100 - AccountPermsGenerator.percentAccountAdditionalPermissions_0);

            if ( m_rnd.rndPercent(exec_is_owner) )
            {
                flTaxId = m_Person.getFirstNameLastNameTaxID(iCustomerId);

                bExecutorIsAccountOwner = true;
            }
            else
            {
                if (iAdditionalPerms == 1)
                {
                    //select the first non-owner
                	m_AccountPerms.generateCids();
                	
                	CID_1 = m_AccountPerms.permCids[1];
                	CID_2 = m_AccountPerms.permCids[2];

                	flTaxId = m_Person.getFirstNameLastNameTaxID(CID_1);
                }
                else
                {
                    //select the second non-owner
                	m_AccountPerms.getNumPermsForAcc(TxnReq.acct_id);
                	
                	CID_1 = m_AccountPerms.permCids[1];
                	CID_2 = m_AccountPerms.permCids[2];
                	
                    //generate third account permission row
                	flTaxId = m_Person.getFirstNameLastNameTaxID(CID_2);
                }

                bExecutorIsAccountOwner = false;
            }
        }
        
        TxnReq.exec_f_name = flTaxId[0].toCharArray();
    	TxnReq.exec_l_name = flTaxId[1].toCharArray();
    	TxnReq.exec_tax_id = flTaxId[2].toCharArray();

        // Select either stock symbol or company from the securities flat file.
        //

        //have some distribution on the company/symbol input preference
        if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_security_by_symbol))
        {
            //Submit the symbol
        	TxnReq.symbol = m_pSecurities.createSymbol(iFlatFileSymbIndex, TableConsts.cSYMBOL_len).toCharArray();

            TxnReq.co_name[0] = '\0';
            TxnReq.issue[0] = '\0';
        }
        else
        {
            //Submit the company name
        	TxnReq.co_name = m_pCompanies.generateCompanyName( m_pSecurities.getCompanyIndex( iFlatFileSymbIndex )).toCharArray();

            System.arraycopy(m_pSecurities.getSecRecord(iFlatFileSymbIndex)[1], 0, TxnReq.issue, 0, TableConsts.cS_ISSUE_len);

            TxnReq.symbol[0] = '\0';
        }

        TxnReq.trade_qty = HoldingsAndTrades.TRADE_QTY_SIZES[m_rnd.intRange(0, HoldingsAndTrades.TRADE_QTY_SIZES.length - 1)];
        TxnReq.requested_price = m_rnd.doubleIncrRange(HoldingsAndTrades.fMinSecPrice, HoldingsAndTrades.fMaxSecPrice, 0.01);

        // Determine whether Market or Limit order
        bMarket = m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_market);

        //Determine whether Buy or Sell trade
        if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_buy_orders))
        {
            if (bMarket)
            {
                //Market Buy
                eTradeType = TradeType.eMarketBuy;
            }
            else
            {
                //Limit Buy
                eTradeType = TradeType.eLimitBuy;
            }
            
            // Set margin or cash for Buy
            TxnReq.type_is_margin = m_rnd.rndPercent(
                                                    // type_is_margin is specified for all orders, but used only for buys
                                                    m_pDriverCETxnSettings.TO_settings.cur_type_is_margin *
                                                    100 /
                                                    m_pDriverCETxnSettings.TO_settings.cur_buy_orders);
        }
        else
        {
            if (bMarket)
            {
                //Market Sell
                eTradeType = TradeType.eMarketSell;
            }
            else
            {
                // determine whether the Limit Sell is a Stop Loss
                if (m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_stop_loss))
                {
                    //Stop Loss
                    eTradeType = TradeType.eStopLoss;
                }
                else
                {
                    //Limit Sell
                    eTradeType = TradeType.eLimitSell;
                }
            }

            TxnReq.type_is_margin = false;  //all sell orders are cash
        }
        iTradeType = eTradeType.getValue();
        
        // Distribution of last-in-first-out flag
        TxnReq.is_lifo = m_rnd.rndPercent(m_pDriverCETxnSettings.TO_settings.cur_lifo);

        // Copy the trade type id from the flat file
        System.arraycopy((m_pTradeType.getTupleByIndex(eTradeType.getValue()))[0].toCharArray(), 0, TxnReq.trade_type_id, 0, TableConsts.cTT_ID_len);

        // Copy the status type id's from the flat file
        System.arraycopy((m_pStatusType.getTupleByIndex(StatusTypeId.E_PENDING.getValue()))[0].toCharArray(), 0, TxnReq.st_pending_id, 0, TableConsts.cST_ID_len);
        
        System.arraycopy((m_pStatusType.getTupleByIndex(StatusTypeId.E_SUBMITTED.getValue()))[0].toCharArray(), 0, TxnReq.st_submitted_id, 0, TableConsts.cST_ID_len);

        TxnReq.roll_it_back = ( m_iTradeOrderRollbackLevel >= m_rnd.intRange( 1, m_iTradeOrderRollbackLimit ));

        // Need to address logging more comprehensively.
        //return eTradeType;
    }

    /*
    *  Generate Trade-Status transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateTradeStatusInput(TTradeStatusTxnInput TxnReq)
    {
        long          iCustomerId;
        TierId   	  iCustomerTier;

        Object[] customerId = new Object[2];

        //select customer id first
        customerId = m_CustomerSelection.genRandomCustomer();
        iCustomerId = Long.parseLong(customerId[0].toString());
        iCustomerTier = (TierId)customerId[1];
        

        //select random account id
        m_Accs.genRandomAccId(m_rnd, iCustomerId, iCustomerTier, TxnReq.acct_id, -1);
    }

    /*
    *  Generate Trade-Update transaction input.
    *
    *  PARAMETERS:
    *           OUT TxnReq                  - input parameter structure filled in for the transaction.
    *
    *  RETURNS:
    *           none.
    */
    public void GenerateTradeUpdateInput(TTradeUpdateTxnInput TxnReq)
    {
        int           iThreshold;

        iThreshold = m_rnd.rndPercentage();

        if( iThreshold <= m_pDriverCETxnSettings.TU_settings.cur_do_frame1 )
        {
            // Frame 1
            TxnReq.frame_to_execute = 1;
            TxnReq.max_trades = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsFrame1;
            TxnReq.max_updates = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame1;

            // Generate list of unique trade id's
            int     ii, jj;
            boolean    Accepted;
            long  TID;

            for( ii = 0; ii < TxnReq.max_trades; ii++ )
            {
                Accepted = false;
                while( ! Accepted )
                {
                    TID = GenerateNonUniformTradeID(TPCEConstants.TradeUpdateAValueForTradeIDGenFrame1, TPCEConstants.TradeUpdateSValueForTradeIDGenFrame1);
                    jj = 0;
                    while( jj < ii && TxnReq.trade_id[jj] != TID )
                    {
                        jj++;
                    }
                    if( jj == ii )
                    {
                        // We have a unique TID for this batch
                        TxnReq.trade_id[ii] = TID;
                        Accepted = true;
                    }
                }
            }

            // Params not used by this frame /////////////////////////////////////////
            TxnReq.acct_id = 0;                                                     //
            TxnReq.max_acct_id = 0;                                                 //
            Arrays.fill(TxnReq.symbol, '\0');                                       //
            TxnReq.start_trade_dts = new GregorianCalendar(0,0,0,0,0,0).getTime();  //
            TxnReq.end_trade_dts = new GregorianCalendar(0,0,0,0,0,0).getTime();    //
            //////////////////////////////////////////////////////////////////////////
        }
        else if( iThreshold <=  m_pDriverCETxnSettings.TU_settings.cur_do_frame1 +
                                m_pDriverCETxnSettings.TU_settings.cur_do_frame2 )
        {
            // Frame 2
            TxnReq.frame_to_execute = 2;
            TxnReq.max_trades = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsFrame2;
            TxnReq.max_updates = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame2;
            TxnReq.acct_id = GenerateRandomCustomerAccountId();

            GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
                                        m_iTradeUpdateFrame2MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeUpdateAValueForTimeGenFrame2,
                                        TPCEConstants.TradeUpdateSValueForTimeGenFrame2 );

            // Set to the end of initial trades.
            EGenDate.getTimeStamp(TxnReq.end_trade_dts, m_EndTime);

            // Params not used by this frame /////////////////////////
            TxnReq.max_acct_id = 0;                                 //
            Arrays.fill(TxnReq.symbol, '\0');     					//
            Arrays.fill( TxnReq.trade_id, 0); 						//
            //////////////////////////////////////////////////////////
            
        }
        else
        {
            // Frame 3
            TxnReq.frame_to_execute = 3;
            TxnReq.max_trades = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsFrame3;
            TxnReq.max_updates = m_pDriverCETxnSettings.TU_settings.cur_MaxRowsToUpdateFrame3;
            
            TxnReq.symbol = m_pSecurities.createSymbol( m_rnd.rndNU( 0, m_iActiveSecurityCount-1,
            		TPCEConstants.TradeLookupAValueForSymbolFrame3,
            		TPCEConstants.TradeLookupSValueForSymbolFrame3 ),TableConsts.cSYMBOL_len).toCharArray();

            GenerateNonUniformTradeDTS( TxnReq.start_trade_dts,
            							m_iTradeUpdateFrame3MaxTimeInMilliSeconds,
                                        TPCEConstants.TradeLookupAValueForTimeGenFrame3,
                                        TPCEConstants.TradeLookupSValueForTimeGenFrame3 );

            // Set to the end of initial trades.
            EGenDate.getTimeStamp(TxnReq.end_trade_dts, m_EndTime);

            TxnReq.max_acct_id = CustomerAccountsGenerator.getEndtingAccId( m_iActiveCustomerCount );
            // Params not used by this frame /////////////////////////
            TxnReq.acct_id = 0;                                     //
            Arrays.fill( TxnReq.trade_id, 0); //
            //////////////////////////////////////////////////////////

        }
    }
}
