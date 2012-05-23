package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpce.util.EGenDate;

public class CE {
	
	private void Initialize( TDriverCETxnSettings pTxnParamSettings )
	{
	    m_pLogger.SendToLogger(m_DriverGlobalSettings);

	    // If the provided parameter settings are valid, use them.
	    // Otherwise use default settings.
	    if( pTxnParamSettings != null)
	    {
	        SetTxnTunables( pTxnParamSettings );
	    }
	    else
	    {
	        SetTxnTunables( m_DriverCETxnSettings );
	    }
	}

	// Automatically generate unique RNG seeds.
	// The CRandom class uses an unsigned 64-bit value for the seed.
	// This routine automatically generates two unique seeds. One is used for
	// the TxnInput generator RNG, and the other is for the TxnMixGenerator RNG.
	// The 64 bits are used as follows.
	//
	//  Bits    0 - 31  Caller provided unique unsigned 32-bit id.
	//  Bit     32      0 for TxnInputGenerator, 1 for TxnMixGenerator
	//  Bits    33 - 43 Number of days since the base time. The base time
//	                  is set to be January 1 of the most recent year that is
//	                  a multiple of 5. This allows enough space for the last
//	                  field, and it makes the algorithm "timeless" by resetting
//	                  the generated values every 5 years.
	//  Bits    44 - 63 Current time of day measured in 1/10's of a second.
	//
	private void AutoSetRNGSeeds( int UniqueId )
	{
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

	    // Set the TxnMixGenerator RNG to the unique seed.
	    m_TxnMixGenerator.setRNGSeed( Seed );
	    m_DriverCESettings.cur_TxnMixRNGSeed = Seed;

	    // Set the RNG Id to 1 for the TxnInputGenerator.
	    Seed |= 0x0000000100000000L;
	    m_TxnInputGenerator.SetRNGSeed( Seed );
	    m_DriverCESettings.cur_TxnInputRNGSeed = Seed;
	}

	/*
	* Constructor - no partitioning by C_ID, automatic RNG seed generation (requires unique input)
	*/
	public CE( CESUTInterface pSUT, BaseLogger pLogger, final TPCEGenerator inputFiles,
	                           long iConfiguredCustomerCount, long iActiveCustomerCount,
	                           int iScaleFactor, int iDaysOfInitialTrades,
	                           int UniqueId,
	                           final TDriverCETxnSettings pDriverCETxnSettings ){
		m_DriverGlobalSettings = new DriverGlobalSettings( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades );
	    m_DriverCESettings = new DriverCESettings( UniqueId, 0, 0 );
	    m_pSUT = pSUT;
	    m_pLogger = pLogger;
	    m_TxnMixGenerator = new CETxnMixGenerator(m_DriverCETxnSettings, m_pLogger );
	    m_TxnInputGenerator = new CETxnInputGenerator( inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades * EGenDate.HoursPerWorkDay, m_pLogger, m_DriverCETxnSettings );
	    m_bClearBufferBeforeGeneration = false;
	    m_pLogger.SendToLogger("CE object constructed using constructor 1 (valid for publication: YES).");

	    Initialize( pDriverCETxnSettings );
	    AutoSetRNGSeeds( UniqueId );

	    m_pLogger.SendToLogger(m_DriverCESettings);    // log the RNG seeds
	}
	   
	/*
	* Constructor - no partitioning by C_ID, RNG seeds provided
	*/
	
	public CE( CESUTInterface pSUT, BaseLogger pLogger, final TPCEGenerator inputFiles,
            long iConfiguredCustomerCount, long iActiveCustomerCount,
            int iScaleFactor, int iDaysOfInitialTrades,
            int UniqueId,long TxnMixRNGSeed, long TxnInputRNGSeed, final TDriverCETxnSettings pDriverCETxnSettings ){
		m_DriverGlobalSettings = new DriverGlobalSettings( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades );
	    m_DriverCESettings = new DriverCESettings( UniqueId, TxnMixRNGSeed, TxnInputRNGSeed );
	    m_pSUT = pSUT;
	    m_pLogger = pLogger;
	    m_TxnMixGenerator = new CETxnMixGenerator(m_DriverCETxnSettings, TxnMixRNGSeed, m_pLogger );
	    m_TxnInputGenerator = new CETxnInputGenerator( inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades * EGenDate.HoursPerWorkDay, TxnInputRNGSeed, m_pLogger, m_DriverCETxnSettings );
	    m_bClearBufferBeforeGeneration = false;
	    m_pLogger.SendToLogger("CE object constructed using constructor 2 (valid for publication: YES).");

	    Initialize( pDriverCETxnSettings );
	    AutoSetRNGSeeds( UniqueId );

	    m_pLogger.SendToLogger(m_DriverCESettings);    // log the RNG seeds
		
	}

	/*
	* Constructor - partitioning by C_ID, automatic RNG seed generation (requires unique input)
	*/
	public CE( CESUTInterface pSUT, BaseLogger pLogger, final TPCEGenerator inputFiles,
	                            long iConfiguredCustomerCount, long iActiveCustomerCount,
	                            long iMyStartingCustomerId, long iMyCustomerCount, int iPartitionPercent,
	                            int iScaleFactor, int iDaysOfInitialTrades,
	                            int UniqueId,
	                            final TDriverCETxnSettings pDriverCETxnSettings ){
		m_DriverGlobalSettings = new DriverGlobalSettings( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades );
	    m_DriverCESettings = new DriverCESettings( UniqueId, 0, 0 );
	    m_DriverCEPartitionSettings = new DriverCEPartitionSettings( iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent );
	    m_pSUT = pSUT;
	    m_pLogger = pLogger;
	    m_TxnMixGenerator = new CETxnMixGenerator(m_DriverCETxnSettings,  m_pLogger );
	    m_TxnInputGenerator = new CETxnInputGenerator( inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, 
	    		iDaysOfInitialTrades * EGenDate.HoursPerWorkDay, iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent, m_pLogger, m_DriverCETxnSettings );
	    m_bClearBufferBeforeGeneration = false;
	    m_pLogger.SendToLogger("CE object constructed using constructor 3 (valid for publication: YES).");

	    Initialize( pDriverCETxnSettings );
	    AutoSetRNGSeeds( UniqueId );
	    m_pLogger.SendToLogger(m_DriverCEPartitionSettings); // log the partition settings
	    m_pLogger.SendToLogger(m_DriverCESettings);    // log the RNG seeds
	}

	/*
	* Constructor - partitioning by C_ID, RNG seeds provided
	*/
	public CE(  CESUTInterface pSUT, BaseLogger pLogger, final TPCEGenerator inputFiles,
	                            long iConfiguredCustomerCount, long iActiveCustomerCount,
	                            long iMyStartingCustomerId, long iMyCustomerCount, int iPartitionPercent,
	                            int iScaleFactor, int iDaysOfInitialTrades,
	                            int UniqueId,
	                            long TxnMixRNGSeed,
	                            long TxnInputRNGSeed,
	                            final TDriverCETxnSettings pDriverCETxnSettings ){
		m_DriverGlobalSettings = new DriverGlobalSettings( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades );
	    m_DriverCESettings = new DriverCESettings( UniqueId, TxnMixRNGSeed, TxnInputRNGSeed );
	    m_DriverCEPartitionSettings = new DriverCEPartitionSettings( iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent );
	    m_pSUT = pSUT;
	    m_pLogger = pLogger;
	    m_TxnMixGenerator = new CETxnMixGenerator(m_DriverCETxnSettings, TxnMixRNGSeed, m_pLogger );
	    m_TxnInputGenerator = new CETxnInputGenerator( inputFiles, iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, 
	    		iDaysOfInitialTrades * EGenDate.HoursPerWorkDay, iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent, 
	    		TxnInputRNGSeed, m_pLogger, m_DriverCETxnSettings );
	    m_bClearBufferBeforeGeneration = false;
	    m_pLogger.SendToLogger("CE object constructed using constructor 4 (valid for publication: YES).");

	    Initialize( pDriverCETxnSettings );
	    AutoSetRNGSeeds( UniqueId );

	    m_pLogger.SendToLogger(m_DriverCEPartitionSettings); // log the partition settings
	    m_pLogger.SendToLogger(m_DriverCESettings);    // log the RNG seeds
	}

	public long getTxnInputGeneratorRNGSeed( )
	{
	    return( m_TxnInputGenerator.GetRNGSeed() );
	}

	public long getTxnMixGeneratorRNGSeed( )
	{
	    return( m_TxnMixGenerator.GetRNGSeed() );
	}

	public boolean SetTxnTunables( final TDriverCETxnSettings pTxnParamSettings )
	{
	    if( pTxnParamSettings.IsValid() == true )
	    {
	        // Update Tunables
	        if (pTxnParamSettings != m_DriverCETxnSettings)    // only copy from a different location
	        {
	            m_DriverCETxnSettings = pTxnParamSettings;
	        }

	        // Trigger Runtime Updates
	        m_TxnMixGenerator.UpdateTunables();
	        m_TxnInputGenerator.UpdateTunables();
	        return true;
	    }
	    else
	    {
	        return false;
	    }
	}

	public void DoTxn(){
	    int iTxnType = m_TxnMixGenerator.GenerateNextTxnType( );

	    if (m_bClearBufferBeforeGeneration){
	        ZeroInputBuffer(iTxnType);
	    }

	    switch( iTxnType ){
	    case CETxnMixGenerator.BROKER_VOLUME:
	        m_TxnInputGenerator.GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );
	        m_pSUT.BrokerVolume( m_BrokerVolumeTxnInput );
	        break;
	    case CETxnMixGenerator.CUSTOMER_POSITION:
	        m_TxnInputGenerator.GenerateCustomerPositionInput( m_CustomerPositionTxnInput );
	        m_pSUT.CustomerPosition( m_CustomerPositionTxnInput );
	        break;
	    case CETxnMixGenerator.MARKET_WATCH:
	        m_TxnInputGenerator.GenerateMarketWatchInput( m_MarketWatchTxnInput );
	        m_pSUT.MarketWatch( m_MarketWatchTxnInput );
	        break;
	    case CETxnMixGenerator.SECURITY_DETAIL:
	        m_TxnInputGenerator.GenerateSecurityDetailInput( m_SecurityDetailTxnInput );
	        m_pSUT.SecurityDetail( m_SecurityDetailTxnInput );
	        break;
	    case CETxnMixGenerator.TRADE_LOOKUP:
	        m_TxnInputGenerator.GenerateTradeLookupInput( m_TradeLookupTxnInput );
	        m_pSUT.TradeLookup( m_TradeLookupTxnInput );
	        break;
	    case CETxnMixGenerator.TRADE_ORDER:
	    	/*
	    	 * These two variables will be modified in the GenerateTradeOrderInput
	    	 */
	        boolean    bExecutorIsAccountOwner = true;
	        int   iTradeType = TradeType.eLimitBuy.getValue();
	        m_TxnInputGenerator.GenerateTradeOrderInput( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
	        m_pSUT.TradeOrder( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
	        break;
	    case CETxnMixGenerator.TRADE_STATUS:
	        m_TxnInputGenerator.GenerateTradeStatusInput( m_TradeStatusTxnInput );
	        m_pSUT.TradeStatus( m_TradeStatusTxnInput );
	        break;
	    case CETxnMixGenerator.TRADE_UPDATE:
	        m_TxnInputGenerator.GenerateTradeUpdateInput( m_TradeUpdateTxnInput );
	        m_pSUT.TradeUpdate( m_TradeUpdateTxnInput );
	        break;
	    default:
	    	System.err.println("CE: Generated illegal transaction");
	        System.exit(1);
	    }
	}

	/*
	*  Zero transaction input buffer.
	*
	*  PARAMETERS:
	*           IN iTxnType     - what transaction to zero the buffer for.
	*
	*  RETURNS:
	*           none.
	*/
	public void ZeroInputBuffer(int iTxnType)
	{
	    switch( iTxnType )
	    {
	    case CETxnMixGenerator.BROKER_VOLUME:
	        m_BrokerVolumeTxnInput = new TBrokerVolumeTxnInput();        
	        break;
	    case CETxnMixGenerator.CUSTOMER_POSITION:
	        m_CustomerPositionTxnInput = new TCustomerPositionTxnInput();
	        break;
	    case CETxnMixGenerator.MARKET_WATCH:
	        m_MarketWatchTxnInput = new TMarketWatchTxnInput();
	        break;
	    case CETxnMixGenerator.SECURITY_DETAIL:
	        m_SecurityDetailTxnInput = new TSecurityDetailTxnInput();
	        break;
	    case CETxnMixGenerator.TRADE_LOOKUP:
	        m_TradeLookupTxnInput = new TTradeLookupTxnInput();
	        break;
	    case CETxnMixGenerator.TRADE_ORDER:
	        m_TradeOrderTxnInput = new TTradeOrderTxnInput();
	        break;
	    case CETxnMixGenerator.TRADE_STATUS:
	        m_TradeStatusTxnInput = new TTradeStatusTxnInput();
	        break;
	    case CETxnMixGenerator.TRADE_UPDATE:
	        m_TradeUpdateTxnInput = new TTradeUpdateTxnInput();
	        break;
	    }
	}

	/*
	*  Whether to zero the buffer before generating transaction input data.
	*  Allows bitwise comparison of transaction input buffers.
	*
	*  Note: the option is set to 'false' by default (in the constructor).
	*
	*  PARAMETERS:
	*           IN bClearBufferBeforeGeneration     - zero the buffer before input generation, if true
	*
	*  RETURNS:
	*           none.
	*/
	public void SetClearBufferOption(boolean bClearBufferBeforeGeneration)
	{
	    m_bClearBufferBeforeGeneration = bClearBufferBeforeGeneration;
	}
	
	

	private  DriverGlobalSettings       m_DriverGlobalSettings;
	private  DriverCESettings           m_DriverCESettings;
	private  DriverCEPartitionSettings  m_DriverCEPartitionSettings;
	private  TDriverCETxnSettings        m_DriverCETxnSettings;

	private  CESUTInterface            m_pSUT;
	private  BaseLogger                m_pLogger;
	private  CETxnMixGenerator          m_TxnMixGenerator;
	private  CETxnInputGenerator        m_TxnInputGenerator;

	private  TBrokerVolumeTxnInput       m_BrokerVolumeTxnInput;
	private  TCustomerPositionTxnInput   m_CustomerPositionTxnInput;
	private  TMarketWatchTxnInput        m_MarketWatchTxnInput;
	private  TSecurityDetailTxnInput     m_SecurityDetailTxnInput;
	private  TTradeLookupTxnInput        m_TradeLookupTxnInput;
	private  TTradeOrderTxnInput         m_TradeOrderTxnInput;
	private  TTradeStatusTxnInput        m_TradeStatusTxnInput;
	private  TTradeUpdateTxnInput        m_TradeUpdateTxnInput;

	    // Whether to zero the buffer before generating transaction input data into it.
	private  boolean                        m_bClearBufferBeforeGeneration;
}
