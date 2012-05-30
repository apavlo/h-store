package edu.brown.benchmark.tpce.generators;

import java.util.Date;

import edu.brown.benchmark.tpce.util.EGenDate;

public class MEE {
	private DriverMEESettings  m_DriverMEESettings;
	private MEESUTInterface   m_pSUT;
	private BaseLogger        m_pLogger;
	private MEEPriceBoard      m_PriceBoard;
	private MEETickerTape      m_TickerTape;
	private MEETradingFloor    m_TradingFloor;
	private Date           m_BaseTime;
	private Date           m_CurrentTime;
	//TODO
//	private Mutex              m_MEELock;

	    // Automatically generate unique RNG seeds
	private void AutoSetRNGSeeds( long UniqueId ){
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

	    m_TickerTape.SetRNGSeed( Seed );
	    m_DriverMEESettings.cur_TickerTapeRNGSeed = Seed;

	    Seed |= 0x0000000100000000L;
	    m_TradingFloor.SetRNGSeed( Seed );
	    m_DriverMEESettings.cur_TradingFloorRNGSeed = Seed;
	}

	public static final int  NO_OUTSTANDING_TRADES = MEETradingFloor.NO_OUTSTANDING_TRADES;

	public MEE( int TradingTimeSoFar, MEESUTInterface  pSUT, BaseLogger  pLogger, SecurityHandler pSecurityFile, long UniqueId, int configuredCustomerCount ){
		m_DriverMEESettings = new DriverMEESettings( UniqueId, 0, 0, 0 );
	    m_pSUT = pSUT;
	    m_pLogger = pLogger;
	    m_PriceBoard = new MEEPriceBoard( TradingTimeSoFar,  m_BaseTime,  m_CurrentTime, pSecurityFile, configuredCustomerCount);
	    m_TickerTape = new MEETickerTape( pSUT,  m_PriceBoard,  m_BaseTime,  m_CurrentTime );
	    m_TradingFloor = new MEETradingFloor( pSUT,  m_PriceBoard,  m_TickerTape,  m_BaseTime,  m_CurrentTime );
//	    m_MEELock();
	    m_pLogger.SendToLogger("MEE object constructed using c'tor 1 (valid for publication: YES).");
	    
	    AutoSetRNGSeeds( UniqueId );

	    m_pLogger.SendToLogger(m_DriverMEESettings);
	}

	public MEE( int TradingTimeSoFar, MEESUTInterface  pSUT, BaseLogger  pLogger, SecurityHandler  pSecurityFile, long UniqueId, long TickerTapeRNGSeed, long TradingFloorRNGSeed, int configuredCustomerCount ){
		m_DriverMEESettings = new DriverMEESettings( UniqueId, 0, TickerTapeRNGSeed, TradingFloorRNGSeed );
	    m_pSUT = pSUT;
	    m_pLogger = pLogger;
	    m_PriceBoard = new MEEPriceBoard( TradingTimeSoFar,  m_BaseTime,  m_CurrentTime, pSecurityFile, configuredCustomerCount);
	    m_TickerTape = new MEETickerTape( pSUT,  m_PriceBoard,  m_BaseTime,  m_CurrentTime, TickerTapeRNGSeed );
	    m_TradingFloor = new MEETradingFloor( pSUT,  m_PriceBoard,  m_TickerTape,  m_BaseTime,  m_CurrentTime, TradingFloorRNGSeed );
//	    m_MEELock();
	    m_pLogger.SendToLogger("MEE object constructed using c'tor 2 (valid for publication: NO).");
	    m_pLogger.SendToLogger(m_DriverMEESettings);
	}

	public long getTickerTapeRNGSeed(){
	    return( m_TickerTape.GetRNGSeed() );
	}

	public long getTradingFloorRNGSeed(){
	    return( m_TradingFloor.GetRNGSeed() );
	}

	public void setBaseTime(){
//	    m_MEELock.ClaimLock();
	    m_BaseTime = new Date();
//	    m_MEELock.ReleaseLock();
	}

	public boolean disableTickerTape(){
	    boolean    Result;
//	    m_MEELock.ClaimLock();
	    Result = m_TickerTape.DisableTicker();
//	    m_MEELock.ReleaseLock();
	    return( Result );
	}

	public boolean enableTickerTape(){
	    boolean    Result;
//	    m_MEELock.ClaimLock();
	    Result = m_TickerTape.EnableTicker();
//	    m_MEELock.ReleaseLock();
	    return( Result );
	}

	public int generateTradeResult(){
	    int   NextTime;

//	    m_MEELock.ClaimLock();
	    m_CurrentTime = new Date();
	    NextTime = m_TradingFloor.generateTradeResult( );
//	    m_MEELock.ReleaseLock();
	    return( NextTime );
	}

	public int submitTradeRequest( TTradeRequest pTradeRequest ){
	    int NextTime;

//	    m_MEELock.ClaimLock();
	    m_CurrentTime = new Date();
	    NextTime = m_TradingFloor.SubmitTradeRequest( pTradeRequest );
//	    m_MEELock.ReleaseLock();
	    return( NextTime );
	}

}
