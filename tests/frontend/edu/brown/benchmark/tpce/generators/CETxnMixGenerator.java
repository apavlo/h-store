package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.util.EGenRandom;

public class CETxnMixGenerator {
	public CETxnMixGenerator( TDriverCETxnSettings pDriverCETxnSettings, BaseLogger pLogger ){
		 m_pDriverCETxnSettings =  pDriverCETxnSettings;
		 m_rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR );   // initialize with default seed
		 m_pLogger = pLogger;
		 m_iTxnArrayCurrentIndex = 0;
		 m_pTxnArray = null;
	 }

	public CETxnMixGenerator( TDriverCETxnSettings pDriverCETxnSettings, long RNGSeed, BaseLogger pLogger ){
		 m_pDriverCETxnSettings =  pDriverCETxnSettings;
		 m_rnd = new EGenRandom(RNGSeed );   // initialize with default seed
		 m_pLogger = pLogger;
		 m_iTxnArrayCurrentIndex = 0;
		 m_pTxnArray = null;
	 }

	public long  GetRNGSeed(){
	    return m_rnd.getSeed();
	}
	
	public void  setRNGSeed(long RNGSeed ){
		 m_rnd.setSeed( RNGSeed );
	}
	
	public void  UpdateTunables( )
	{
	    int   i;
	    int   BrokerVolumeMixLimit;
	    int   CustomerPositionMixLimit;
	    int   MarketWatchMixLimit;
	    int   SecurityDetailMixLimit;
	    int   TradeLookupMixLimit;
	    int   TradeOrderMixLimit;
	    int   TradeStatusMixLimit;
	    int   TradeUpdateMixLimit;
	
	    // Add all the weights together
	    m_CETransactionMixTotal =
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_BrokerVolumeMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_CustomerPositionMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_MarketWatchMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_SecurityDetailMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeLookupMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeStatusMixLevel +
	        m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeUpdateMixLevel;
	
	    TradeStatusMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeStatusMixLevel;
	    MarketWatchMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_MarketWatchMixLevel + TradeStatusMixLimit;
	    SecurityDetailMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_SecurityDetailMixLevel + MarketWatchMixLimit;
	    CustomerPositionMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_CustomerPositionMixLevel + SecurityDetailMixLimit;
	    TradeOrderMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel + CustomerPositionMixLimit;
	    TradeLookupMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeLookupMixLevel + TradeOrderMixLimit;
	    TradeUpdateMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_TradeUpdateMixLevel + TradeLookupMixLimit;
	    BrokerVolumeMixLimit = m_pDriverCETxnSettings.TxnMixGenerator_settings.cur_BrokerVolumeMixLevel + TradeUpdateMixLimit;
	
	    // Reset the random transaction array.
	    //
	    if (m_pTxnArray != null)
	    {
	        m_pTxnArray = null;
	    }
	
	    m_pTxnArray = new char[m_CETransactionMixTotal];
	    // Initialize the array with transaction types.
	    //
	    for (i = 0; i < TradeStatusMixLimit; ++i)
	    {
	        m_pTxnArray[i] = TRADE_STATUS;
	    }
	    for (; i < MarketWatchMixLimit; ++i)
	    {
	        m_pTxnArray[i] = MARKET_WATCH;
	    }
	    for (; i < SecurityDetailMixLimit; ++i)
	    {
	        m_pTxnArray[i] = SECURITY_DETAIL;
	    }
	    for (; i < CustomerPositionMixLimit; ++i)
	    {
	        m_pTxnArray[i] = CUSTOMER_POSITION;
	    }
	    for (; i < TradeOrderMixLimit; ++i)
	    {
	        m_pTxnArray[i] = TRADE_ORDER;
	    }
	    for (; i < TradeLookupMixLimit; ++i)
	    {
	        m_pTxnArray[i] = TRADE_LOOKUP;
	    }
	    for (; i < TradeUpdateMixLimit; ++i)
	    {
	        m_pTxnArray[i] = TRADE_UPDATE;
	    }
	    for (; i < BrokerVolumeMixLimit; ++i)
	    {
	        m_pTxnArray[i] = BROKER_VOLUME;
	    }
	
	    m_iTxnArrayCurrentIndex = 0;    // reset the current element index
	
	    // Log Tunables
	    m_pLogger.SendToLogger(m_pDriverCETxnSettings.TxnMixGenerator_settings);
	}
	
	public int  generateNextTxnType()
	{
	   
	    int rnd = m_rnd.intRange( m_iTxnArrayCurrentIndex, m_CETransactionMixTotal - 1);
	
	    char iTxnType = m_pTxnArray[rnd];

	    m_pTxnArray[rnd] = m_pTxnArray[m_iTxnArrayCurrentIndex];
	    m_pTxnArray[m_iTxnArrayCurrentIndex] = iTxnType;
	
	    m_iTxnArrayCurrentIndex = (m_iTxnArrayCurrentIndex + 1) % m_CETransactionMixTotal;
	
	    return iTxnType;
	}

	 public static final int INVALID_TRANSACTION_TYPE = -1;
	 public static final int SECURITY_DETAIL          =  0;
	 public static final int BROKER_VOLUME            =  1;
	 public static final int CUSTOMER_POSITION        =  2;
	 public static final int MARKET_WATCH             =  3;
	 public static final int TRADE_STATUS             =  4;
	 public static final int TRADE_LOOKUP             =  5;
	 public static final int TRADE_ORDER              =  6;
	 public static final int TRADE_UPDATE             =  7;
	    //Trade-Result and Market-Feed are included for completness.
	 public static final int MARKET_FEED              =  8;
	 public static final int TRADE_RESULT             =  9;
	    
	private final TDriverCETxnSettings  m_pDriverCETxnSettings;
	private EGenRandom                 m_rnd;
	private BaseLogger            m_pLogger;

	private int                   m_CETransactionMixTotal;
	private int                   m_iTxnArrayCurrentIndex;
	private char[]                m_pTxnArray;
}
