package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.util.EGenDate;

public class MEE {
/*	private DriverMEESettings  m_DriverMEESettings;
	private MEESUTInterface   m_pSUT;
	private BaseLogger        m_pLogger;
	private MEEPriceBoard      m_PriceBoard;
	private MEETickerTape      m_TickerTape;
	private MEETradingFloor    m_TradingFloor;
	private DateTime           m_BaseTime;
	private DateTime           m_CurrentTime;
	private long			m_iMyCustomerCount;
	private Mutex              m_MEELock;

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
	    m_TickerTape.SetRNGSeed( Seed );
	    m_DriverMEESettings.cur_TickerTapeRNGSeed = Seed;

	    // Set the RNG Id to 1 for the Trading Floor.
	    Seed |= 0x0000000100000000L;
	    m_TradingFloor.SetRNGSeed( Seed );
	    m_DriverMEESettings.cur_TradingFloorRNGSeed = Seed;
	}

	public static final int  NO_OUTSTANDING_TRADES = CMEETradingFloor::NO_OUTSTANDING_TRADES;

	    // Constructor - automatic RNG seed generation
	public MEE( int TradingTimeSoFar, MEESUTInterface pSUT, BaseLogger pLogger, InputFileHandler pSecurityFile, long UniqueId, long iActiveCustomerCount ){
		m_iMyCustomerCount = iActiveCustomerCount;
		m_DriverMEESettings = new DriverMEESettings( UniqueId, 0, 0, 0 );
	    m_pSUT= pSUT;
	    m_pLogger = pLogger;
	    m_PriceBoard = new MEEPriceBoard( TradingTimeSoFar, m_BaseTime, m_CurrentTime, pSecurityFile, iActiveCustomerCount );
	    m_TickerTape = new MEETickerTape (pSUT, m_PriceBoard, m_BaseTime, m_CurrentTime );
	    m_TradingFloor = new MEETradingFloor( pSUT, m_PriceBoard, m_TickerTape, m_BaseTime, m_CurrentTime );
	    m_MEELock();
	    m_pLogger.SendToLogger("MEE object constructed using c'tor 1 (valid for publication: YES).");
	    AutoSetRNGSeeds( UniqueId );
	    m_pLogger.SendToLogger(m_DriverMEESettings);
}
		

	    // Constructor - RNG seed provided
	public MEE( int TradingTimeSoFar, MEESUTInterface pSUT, BaseLogger pLogger, InputFileHandler pSecurityFile, long UniqueId, 
			long TickerTapeRNGSeed, long TradingFloorRNGSeed, long iActiveCustomerCount ){
		m_DriverMEESettings new DriverMEESettings( UniqueId, 0, TickerTapeRNGSeed, TradingFloorRNGSeed )
		m_pSUT= pSUT;
	    m_pLogger = pLogger;
	    m_PriceBoard = new MEEPriceBoard( TradingTimeSoFar, m_BaseTime, m_CurrentTime, pSecurityFile );
	    m_TickerTape = new MEETickerTape (pSUT, m_PriceBoard, m_BaseTime, m_CurrentTime );
	    m_TradingFloor = new MEETradingFloor( pSUT, m_PriceBoard, m_TickerTape, m_BaseTime, m_CurrentTime );
	    m_MEELock();
	    m_pLogger.SendToLogger("MEE object constructed using c'tor 2 (valid for publication: NO).");
	    m_pLogger.SendToLogger(m_DriverMEESettings);
	}


	public long GetTickerTapeRNGSeed();
	public long GetTradingFloorRNGSeed();

	public void    SetBaseTime();

	public int   SubmitTradeRequest( PTradeRequest pTradeRequest );
	public int   GenerateTradeResult();

	public boolean    EnableTickerTape();
	public boolean    DisableTickerTape();
	};*/

}
