package edu.brown.benchmark.tpce.generators;

import java.util.Date;

import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpce.util.EGenDate;

public class CE {
	
	private void Initialize( TDriverCETxnSettings pTxnParamSettings ){
	    m_pLogger.SendToLogger(m_DriverGlobalSettings);

	    // If the provided parameter settings are valid, use them.
	    // Otherwise use default settings.
	    if( pTxnParamSettings != null){
	        SetTxnTunables( pTxnParamSettings );
	    }
	    else{
	        SetTxnTunables( m_DriverCETxnSettings );
	    }
	}

	private void AutoSetRNGSeeds( int UniqueId ){
		 int       baseYear, baseMonth, baseDay, millisec;

		    baseYear = EGenDate.getYear();
		    baseMonth = EGenDate.getMonth();
		    baseDay = EGenDate.getDay();
		    //TODO compare to c++
		    millisec = (EGenDate.getHour() * EGenDate.MinutesPerHour + EGenDate.getMinute()) * EGenDate.SecondsPerMinute + EGenDate.getSecond(); 
		    baseYear -= ( baseYear % 5 );

		    long Seed;
		    Seed = millisec / 100;

		    Seed <<= 11;
		    Seed += EGenDate.getDayNo(baseYear, baseMonth, baseDay) - EGenDate.getDayNo(baseYear, 1, 1);

		    Seed <<= 33;

		    Seed += UniqueId;
		    
	    m_TxnMixGenerator.setRNGSeed( Seed );
	    m_DriverCESettings.cur_TxnMixRNGSeed = Seed;

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
		m_DriverCETxnSettings = new TDriverCETxnSettings();
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
		m_DriverCETxnSettings = new TDriverCETxnSettings();
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
		m_DriverCETxnSettings = new TDriverCETxnSettings();
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
		m_DriverCETxnSettings = new TDriverCETxnSettings();
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
	    int iTxnType = m_TxnMixGenerator.generateNextTxnType( );

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

	
	
	
	
/*	
	private Object[] cleanParams(Object[] orig) {
        // We need to switch java.util.Dates to the stupid volt TimestampType
        for (int i = 0; i < orig.length; i++) {
            if (orig[i] instanceof Date) {
                orig[i] = new TimestampType(((Date) orig[i]).getTime());
            }
        } // FOR
        return (orig);
    }
	
    public Object[] getBrokerVolumeParams() {
    	System.out.println("Executing generateBrokerVolumeInput ... \n");
    	m_BrokerVolumeTxnInput = new TBrokerVolumeTxnInput();
	    m_TxnInputGenerator.GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );
    	Object[] obj = m_BrokerVolumeTxnInput.InputParameters().toArray();
    	
System.out.println("CE: line: 342: " + obj[1]);
        return (this.cleanParams(obj));
    }

    public Object[] getCustomerPositionParams() {
    	System.out.println("Executing generateCustomerPositionInput ... \n");
    	m_CustomerPositionTxnInput = new TCustomerPositionTxnInput();
		m_TxnInputGenerator.GenerateCustomerPositionInput( m_CustomerPositionTxnInput );
    	Object[] obj = m_CustomerPositionTxnInput.InputParameters().toArray();
    	
System.out.println("EGenClientDriver: line: 123: acct_id_idx: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 124: cust_id: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 125: get_history: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 126: tax_id: " + obj[3]);
    	return (this.cleanParams(obj));
//        return (this.cleanParams(driver_ptr.generateCustomerPositionInput().InputParameters().toArray()));
    }

    public Object[] getDataMaintenanceParams() {
    	System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    m_DataMaintenanceGenerator.DoTxn();
        return (this.cleanParams(driver_ptr.generateDataMaintenanceInput().InputParameters().toArray()));
    }

    public Object[] getMarketFeedParams() {
        return (this.cleanParams(driver_ptr.generateMarketFeedInput().InputParameters().toArray()));
    }

    public Object[] getMarketWatchParams() {
    	System.out.println("Executing generateMarketWatchInput ... \n");
    	m_MarketWatchTxnInput = new TMarketWatchTxnInput();
	    m_TxnInputGenerator.GenerateMarketWatchInput( m_MarketWatchTxnInput );
    	Object[] obj = m_MarketWatchTxnInput.InputParameters().toArray();
    	
System.out.println("EGenClientDriver: line: 144: acct_id: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 145: c_id: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 146: ending_co_id: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 147: starting_co_id: " + obj[3].toString());
System.out.println("EGenClientDriver: line: 149: industry_name: " + obj[4].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getSecurityDetailParams() {
    	System.out.println("Executing generateSecurityDetailInput ... \n");
    	m_SecurityDetailTxnInput = new TSecurityDetailTxnInput();
	    m_TxnInputGenerator.GenerateSecurityDetailInput( m_SecurityDetailTxnInput );
    	Object[] obj = m_SecurityDetailTxnInput.InputParameters().toArray();
System.out.println("EGenClientDriver: line: 154: max_rows_to_return: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 155: access_lob_flag: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 156: start_day: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 157: symbol: " + obj[3].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getTradeCleanupParams() {
    	System.out.println("Executing %s...\n" + "generateBrokerVolumeInput");
	    m_DataMaintenanceGenerator.DoCleanupTxn();
        return (this.cleanParams(driver_ptr.generateTradeCleanupInput().InputParameters().toArray()));
    }

    public Object[] getTradeLookupParams() {
    	System.out.println("Executing generateTradeLookupInput ... \n");
    	m_TradeLookupTxnInput = new TTradeLookupTxnInput();
	    m_TxnInputGenerator.GenerateTradeLookupInput( m_TradeLookupTxnInput );
    	Object[] obj = m_TradeLookupTxnInput.InputParameters().toArray();
System.out.println("EGenClientDriver: line: 167: trade_id: " + obj[0]);
System.out.println("EGenClientDriver: line: 168: acct_id: " + obj[1]);
System.out.println("EGenClientDriver: line: 169: max_acct_id: " + obj[2]);
System.out.println("EGenClientDriver: line: 170: frame_to_execute: " + obj[3]);
System.out.println("EGenClientDriver: line: 171: max_trades: " + obj[4]);
System.out.println("EGenClientDriver: line: 172: end_trade_dts: " + obj[5].toString());
System.out.println("EGenClientDriver: line: 173: start_trade_dts: " + obj[6].toString());
System.out.println("EGenClientDriver: line: 174: symbol: " + obj[7].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getTradeOrderParams() {
    	int   iTradeType = 0;
        boolean    bExecutorIsAccountOwner = true;
        System.out.println("Executing generateTradeOrderInput ... \n");
        m_TradeOrderTxnInput = new TTradeOrderTxnInput();
	    m_TxnInputGenerator.GenerateTradeOrderInput( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
        Object[] obj = m_TradeOrderTxnInput.InputParameters().toArray();
        
        System.out.println("EGenClientDriver: line: 182: requested_price: " + obj[0]);
        System.out.println("EGenClientDriver: line: 183: acct_id: " + obj[1]);
        System.out.println("EGenClientDriver: line: 184: is_lifo: " + obj[2]);
        System.out.println("EGenClientDriver: line: 185: roll_it_back: " + obj[3]);
        System.out.println("EGenClientDriver: line: 186: trade_qty: " + obj[4]);
        System.out.println("EGenClientDriver: line: 187: type_is_margin: " + obj[5]);
        System.out.println("EGenClientDriver: line: 188: co_name: " + obj[6]);
        System.out.println("EGenClientDriver: line: 189: exec_f_name: " + obj[7]);
        System.out.println("EGenClientDriver: line: 190: exec_l_name: " + obj[8]);
        System.out.println("EGenClientDriver: line: 191: exec_tax_id: " + obj[9]);
        System.out.println("EGenClientDriver: line: 192: issue: " + obj[10]);
        System.out.println("EGenClientDriver: line: 193: st_pending_id: " + obj[11]);
        System.out.println("EGenClientDriver: line: 194: st_submitted_id: " + obj[12]);
        System.out.println("EGenClientDriver: line: 195: symbol: " + obj[13]);
        System.out.println("EGenClientDriver: line: 196: trade_type_id: " + obj[14]);
        return (this.cleanParams(obj));
    }

   public Object[] getTradeResultParams() {
	   System.out.println("Executing %s...\n" + "generateTradeResultInput");
	    m_MarketExchangeGenerator.generateTradeResult();
        return (this.cleanParams(driver_ptr.generateTradeResultInput().InputParameters().toArray()));
    }

    public Object[] getTradeStatusParams() {
    	System.out.println("Executing generateTradeStatusInput ... \n");
    	m_TradeStatusTxnInput = new TTradeStatusTxnInput();
	    m_TxnInputGenerator.GenerateTradeStatusInput( m_TradeStatusTxnInput );
    	Object[] obj = m_TradeStatusTxnInput.InputParameters().toArray();
    	
System.out.println("EGenClientDriver: line: 206: acct_id: " + obj[0]);
        return (this.cleanParams(obj));
    }

    public Object[] getTradeUpdateParams() {
    	System.out.println("Executing generateTradeUpdateInput ... \n");
    	m_TradeUpdateTxnInput = new TTradeUpdateTxnInput();
	    m_TxnInputGenerator.GenerateTradeUpdateInput( m_TradeUpdateTxnInput );
    	Object[] obj = m_TradeUpdateTxnInput.InputParameters().toArray();
    	
    	System.out.println("EGenClientDriver: line: 182: trade_id: " + obj[0]);
        System.out.println("EGenClientDriver: line: 183: acct_id: " + obj[1]);
        System.out.println("EGenClientDriver: line: 184: max_acct_id: " + obj[2]);
        System.out.println("EGenClientDriver: line: 185: frame_to_execute: " + obj[3]);
        System.out.println("EGenClientDriver: line: 186: max_trades: " + obj[4]);
        System.out.println("EGenClientDriver: line: 187: max_updates: " + obj[5]);
        System.out.println("EGenClientDriver: line: 188: end_trade_dts: " + obj[6]);
        System.out.println("EGenClientDriver: line: 189: start_trade_dts: " + obj[7]);
        System.out.println("EGenClientDriver: line: 190: symbol: " + obj[8]);
        return (this.cleanParams(obj));
    }*/

	private  DriverGlobalSettings       m_DriverGlobalSettings;
	private  DriverCESettings           m_DriverCESettings;
	private  DriverCEPartitionSettings  m_DriverCEPartitionSettings;
	private  TDriverCETxnSettings        m_DriverCETxnSettings;

	private  CESUTInterface            m_pSUT;
	private  BaseLogger                m_pLogger;
	public  CETxnMixGenerator          m_TxnMixGenerator;
	public  CETxnInputGenerator        m_TxnInputGenerator;

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
