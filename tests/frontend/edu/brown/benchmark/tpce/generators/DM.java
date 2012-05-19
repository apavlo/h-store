package edu.brown.benchmark.tpce.generators;

import java.util.List;

import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.TPCEConstants.eStatusTypeID;
import edu.brown.benchmark.tpce.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpce.util.*;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;

public class DM {
	
	private DriverGlobalSettings                       m_DriverGlobalSettings;
	private DriverDMSettings                           m_DriverDMSettings;

	private EGenRandom                                     m_rnd;
	private CustomerSelection                          m_CustomerSelection;
	private CustomerAccountsGenerator                 m_Accs;
	private SecurityHandler                              m_pSecurities;
	private CompanyGenerator                               m_pCompanies;
	private InputFileHandler                      			m_pTaxRatesDivision;
	private InputFileHandler                            m_pStatusType;
	private long                                      m_iSecurityCount;
	private long                                      m_iCompanyCount;
	private long                                      m_iStartFromCompany;
	private int                                       m_iDivisionTaxCount;
	private long                                      m_iStartFromCustomer;
	private long									  m_iMyCustomerCount;
	
	private int                                       m_DataMaintenanceTableNum;

	private TDataMaintenanceTxnInput                    m_TxnInput;
	private TTradeCleanupTxnInput                       m_CleanupTxnInput;
	private DMSUTInterface                            m_pSUT;
	private BaseLogger                                m_pLogger;

	public static final int 							iDataMaintenanceTableCount = 12;
	
	public static String DataMaintenanceTableName[] = {
		"ACCOUNT_PERMISSION",
        "ADDRESS",
        "COMPANY",
        "CUSTOMER",
        "CUSTOMER_TAXRATE",
        "DAILY_MARKET",
        "EXCHANGE",
        "FINANCIAL",
        "NEWS_ITEM",
        "SECURITY",
        "TAXRATE",
        "WATCH_ITEM" 
	};
	    // Automatically generate unique RNG seeds
	private void AutoSetRNGSeeds( long UniqueId ){
	    int       baseYear, baseMonth, baseDay, millisec;

	    baseYear = EGenDate.getYear();
	    baseMonth = EGenDate.getMonth();
	    baseDay = EGenDate.getDay();
	    //TODO compare to c++
	    millisec = (EGenDate.getHour() * EGenDate.MinutesPerHour + EGenDate.getMinute()) * EGenDate.SecondsPerMinute + EGenDate.getSecond(); 
	    // Set the base year to be the most recent year that was a multiple of 5.
	    baseYear -= ( baseYear % 5 );

	    // Initialize the seed with the current time of day measured in 1/10's of a second.
	    // This will use up to 20 bits.
	    long Seed;
	    Seed = millisec / 100;

	    // Now add in the number of days since the base time.
	    // The number of days in the 5 year period requires 11 bits.
	    // So shift up by that much to make room in the "lower" bits.
	    Seed <<= 11;
	    Seed += EGenDate.getDayNo(baseYear, baseMonth, baseDay) - EGenDate.getDayNo(baseYear, 1, 1);

	    // So far, we've used up 31 bits.
	    // Save the "last" bit of the "upper" 32 for the RNG id. In
	    // this case, it is always 0 since we don't have a second
	    // RNG in this class.
	    // In addition, make room for the caller's 32-bit unique id.
	    // So shift a total of 33 bits.
	    Seed <<= 33;

	    // Now the "upper" 32-bits have been set with a value for RNG 0.
	    // Add in the sponsor's unique id for the "lower" 32-bits.
	    Seed += UniqueId;

	    // Set the RNG to the unique seed.
	    m_rnd.setSeed( Seed );
	    m_DriverDMSettings.cur_RNGSeed = Seed;
	}

	private long  GenerateRandomCustomerId(){
		return m_rnd.int64Range(m_iStartFromCustomer,
                m_iStartFromCustomer + m_DriverGlobalSettings.cur_iActiveCustomerCount - 1);
	}

	private long GenerateRandomCustomerAccountId(){
		long iCustomerId;
		TierId iCustomerTier;
		Object[] customerId;
		customerId = m_CustomerSelection.genRandomCustomer();
	    iCustomerId = Long.parseLong(customerId[0].toString());
        iCustomerTier = (TierId)customerId[0];

	    return( m_Accs.genRandomAccId(m_rnd, iCustomerId, iCustomerTier)[0]);
	}

	private long GenerateRandomCompanyId(){
		return m_rnd.int64Range(m_iStartFromCompany, m_iStartFromCompany + m_iCompanyCount - 1);
	}

	private long GenerateRandomSecurityId(){
		return m_rnd.int64Range(0, m_iSecurityCount-1);
	}

	    // Initialization that is common for all constructors.
	private void Initialize(){
		m_pLogger.SendToLogger(m_DriverGlobalSettings);

	    m_iSecurityCount = SecurityHandler.getSecurityNum(m_iMyCustomerCount);
	    m_iCompanyCount = m_pCompanies.getCompanyCount();
	    m_iStartFromCompany = m_pCompanies.generateCompId();
	    m_iDivisionTaxCount = m_pTaxRatesDivision.getRecordsNum();
	    m_iStartFromCustomer = TPCEConstants.DEFAULT_START_CUSTOMER_ID + TPCEConstants.IDENT_SHIFT;
	}

	    // Constructor - automatice RNG seed generation
	 public DM( DMSUTInterface pSUT, BaseLogger pLogger, TPCEGenerator inputFiles, long iConfiguredCustomerCount, long iActiveCustomerCount, int iScaleFactor, int iDaysOfInitialTrades, long UniqueId ){
		 	m_DriverGlobalSettings = new DriverGlobalSettings( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades );
		    m_DriverDMSettings = new DriverDMSettings( UniqueId, 0 );
		    //TODO seed parameter
		    m_rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR);
		    m_CustomerSelection = new CustomerSelection(m_rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
		  //TODO table
		    m_Accs = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
	        
		    m_pSecurities = new SecurityHandler(inputFiles);
		    m_pCompanies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
		    m_pTaxRatesDivision = inputFiles.getInputFile(InputFile.TAXDIV);
		    m_pStatusType = inputFiles.getInputFile(InputFile.STATUS);
		    m_iDivisionTaxCount = 0;
		    m_DataMaintenanceTableNum = 0;
		    m_pSUT = pSUT;
		    m_pLogger = pLogger ;
		    m_iMyCustomerCount = iActiveCustomerCount;
		    m_pLogger.SendToLogger("DM object constructed using constructor 1 (valid for publication: YES).");
		    Initialize();
		    AutoSetRNGSeeds( UniqueId );
		    m_pLogger.SendToLogger(m_DriverDMSettings);    // log the RNG seeds
		
	}

	    // Constructor - RNG seed provided
	 public DM( DMSUTInterface pSUT, BaseLogger pLogger, TPCEGenerator inputFiles, long iConfiguredCustomerCount, long iActiveCustomerCount, int iScaleFactor,
	    		int iDaysOfInitialTrades, long UniqueId, long RNGSeed ){
	    	m_DriverGlobalSettings = new DriverGlobalSettings( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades );
		    m_DriverDMSettings = new DriverDMSettings( UniqueId, RNGSeed );
		    m_CustomerSelection = new CustomerSelection(m_rnd, TPCEConstants.DEFAULT_START_CUSTOMER_ID, TPCEConstants.ACTIVECUSTOMERCOUNT);
		  //TODO table
		    m_Accs = (CustomerAccountsGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
	        m_rnd = new EGenRandom(RNGSeed);
		    m_pSecurities = new SecurityHandler(inputFiles);
		    m_pCompanies = (CompanyGenerator)inputFiles.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
		    m_pTaxRatesDivision = inputFiles.getInputFile(InputFile.TAXDIV);
		    m_pStatusType = inputFiles.getInputFile(InputFile.STATUS);
		    m_iDivisionTaxCount = 0;
		    m_DataMaintenanceTableNum = 0;
		    m_pSUT = pSUT;
		    m_pLogger = pLogger ;
		    m_pLogger.SendToLogger("DM object constructed using constructor 1 (valid for publication: YES).");
		    Initialize();
		    m_pLogger.SendToLogger(m_DriverDMSettings);    // log the RNG seeds
	    }


	 public long getRNGSeed(){
		 return( m_rnd.getSeed() );
	 }
	 public void DoTxn(){
		 m_TxnInput.setZero();
		 System.arraycopy(DataMaintenanceTableName[m_DataMaintenanceTableNum], 0, m_TxnInput.table_name, 0, TxnHarnessStructs.max_table_name);

		 switch( m_DataMaintenanceTableNum )
		 {
		 case 0: // ACCOUNT_PERMISSION
		     m_TxnInput.acct_id = GenerateRandomCustomerAccountId();
		     break;
		 case 1: // ADDRESS
		     if (m_rnd.rndPercent(67))
		     {
		         m_TxnInput.c_id = GenerateRandomCustomerId();
		     }
		     else
		     {
		         m_TxnInput.co_id = GenerateRandomCompanyId();
		     }
		     break;
		 case 2: // COMPANY
		     m_TxnInput.co_id = GenerateRandomCompanyId();
		     break;
		 case 3: // CUSTOMER
		     m_TxnInput.c_id = GenerateRandomCustomerId();
		     break;
		 case 4: // CUSTOMER_TAXRATE
		     m_TxnInput.c_id = GenerateRandomCustomerId();
		     break;
		 case 5: // DAILY_MARKET
			 m_TxnInput.symbol = m_pSecurities.createSymbol( GenerateRandomSecurityId(), m_TxnInput.symbol.toString().length()).toCharArray();
		     m_TxnInput.day_of_month = m_rnd.intRange(1, 31);
		     m_TxnInput.vol_incr = m_rnd.intRange(-2, 3);
		     if (m_TxnInput.vol_incr == 0)   // don't want 0 as increment
		     {
		         m_TxnInput.vol_incr = -3;
		     }
		     break;
		 case 6: // EXCHANGE
		     break;
		 case 7: // FINANCIAL
		     m_TxnInput.co_id = GenerateRandomCompanyId();
		     break;
		 case 8: // NEWS_ITEM
		     m_TxnInput.co_id = GenerateRandomCompanyId();
		     break;
		 case 9: // SECURITY
			 m_TxnInput.symbol = m_pSecurities.createSymbol( GenerateRandomSecurityId(), m_TxnInput.symbol.toString().length()).toCharArray();
		     break;
		     
		     //TODO unsure
		 case 10: // TAXRATE
			 List<String[]>  pRates;
		     int iThreshold;

		     pRates = m_pTaxRatesDivision.getTuplesByIndex(m_rnd.intRange(0, m_iDivisionTaxCount - 1));
		     iThreshold = m_rnd.intRange(0, pRates.size()-1);

		     System.arraycopy(pRates.get(iThreshold)[0], 0, m_TxnInput.tx_id, 0, TableConsts.cTX_ID_len);
		     break;
		 case 11: // WATCH_ITEM
		     m_TxnInput.c_id = GenerateRandomCustomerId();
		     break;

		 default:
		     assert(false);  // should never happen
		 }

		 m_pSUT.DataMaintenance( m_TxnInput );

		 m_DataMaintenanceTableNum = (m_DataMaintenanceTableNum + 1) % iDataMaintenanceTableCount;
	 }
	 public void    DoCleanupTxn(){
	    	m_CleanupTxnInput.setZero();

	        // Compute Starting Trade ID (Copied from CETxnInputGenerator.cpp)
	        m_CleanupTxnInput.start_trade_id = (long)((( m_DriverGlobalSettings.cur_iDaysOfInitialTrades * EGenDate.HoursPerWorkDay * EGenDate.SecondsPerHour * ( m_DriverGlobalSettings.cur_iActiveCustomerCount / m_DriverGlobalSettings.cur_iScaleFactor )) * TPCEConstants.AbortTrade / 100 ) + 1);  // 1.01 to account for rollbacks, +1 to get first runtime trade

	            // Copy the status type id's from the flat file
	        System.arraycopy(m_pStatusType.getTupleByKey(eStatusTypeID.ePending.getVal())[0], 0, m_CleanupTxnInput.st_pending_id, 0, TableConsts.cST_ID_len);
	        System.arraycopy(m_pStatusType.getTupleByKey(eStatusTypeID.eSubmitted.getVal())[0], 0, m_CleanupTxnInput.st_submitted_id, 0, TableConsts.cST_ID_len);
	        System.arraycopy(m_pStatusType.getTupleByKey(eStatusTypeID.eCanceled.getVal())[0], 0, m_CleanupTxnInput.st_canceled_id, 0, TableConsts.cST_ID_len);
	        

	        // Execute Transaction
	        m_pSUT.TradeCleanup( m_CleanupTxnInput );
	    }
}
