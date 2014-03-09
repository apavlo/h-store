package edu.brown.benchmark.tpceb.generators;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.util.EGenDate;
import edu.brown.benchmark.tpceb.generators.TDriverCETxnSettings;
import edu.brown.benchmark.tpceb.TPCEConstants.eMEETradeRequestAction;;


public class MEE {
    private DriverMEESettings  driverMEESettings;
    private MEESUTInterface   sut;
    private BaseLogger        logger;
    private MEEPriceBoard      priceBoard;
    private MEETickerTape      tickerTape;
    private MEETradingFloor    tradingFloor;
    private Date           baseTime;
    private Date           currentTime;
    private static final Lock lock = new ReentrantLock();// =ock();
    public TTradeRequest tradeReq;
    
    public static final int  NO_OUTSTANDING_TRADES = MEETradingFloor.NO_OUTSTANDING_TRADES;
    
    /*private void AutoSetRNGSeeds( long uniqueID ){
        int baseYear, baseMonth, baseDay, millisec;
       
        GregorianCalendar Now = new GregorianCalendar();// added
        Now.getTimeInMillis();
        
        baseYear = EGenDate.getYear();
        baseMonth = EGenDate.getMonth();
        baseDay = EGenDate.getDay();
        //OLD: millisec = (EGenDate.getHour() * EGenDate.MinutesPerHour + EGenDate.getMinute()) * EGenDate.SecondsPerMinute + EGenDate.getSecond(); 
        baseYear -= ( baseYear % 5 );

        long Seed;
        Seed = millisec / 100; 
        Seed <<= 11;
        Seed += EGenDate.getDayNo(baseYear, baseMonth, baseDay) - EGenDate.getDayNo(baseYear, 1, 1);
        Seed <<= 33;
        Seed += uniqueID;

        System.out.println("setting RNGSeed for ticker tape");
        tickerTape.setRNGSeed( Seed );
        driverMEESettings.cur_TickerTapeRNGSeed = Seed;
        System.out.println("set rngseed");
        Seed |= 0x0000000100000000L;
        
        tradingFloor.setRNGSeed( Seed );
        driverMEESettings.cur_TradingFloorRNGSeed = Seed;
    }*/
    private void AutoSetRNGSeeds( long uniqueID ){
        int currentYear, currentMonth, currentDay, millisec;
        
        // added
       // Now.getTimeInMillis();
        
        currentYear = EGenDate.getYear();
        int baseYear = currentYear;
        currentMonth = EGenDate.getMonth();
        currentDay = EGenDate.getDay();
        GregorianCalendar Now = new GregorianCalendar(currentYear, currentMonth, currentDay);
        
        //OLD: millisec = (EGenDate.getHour() * EGenDate.MinutesPerHour + EGenDate.getMinute()) * EGenDate.SecondsPerMinute + EGenDate.getSecond(); 
        baseYear -= ( currentYear % 5 );
        //added
        GregorianCalendar Base = new GregorianCalendar(baseYear, 1, 1);
        //Now.
        long Seed;
        //millisSec
        Seed = Now.getTimeInMillis() / 100; //modified
        Seed <<= 11;
        //Seed += EGenDate.getDayNo(Now., month, day)
        double dSecs =  (double) ( EGenDate.getDayNo(currentYear, currentMonth, currentDay) - EGenDate.getDayNo(baseYear, 1, 1) );
        dSecs = dSecs * EGenDate.SecondsPerMinute * EGenDate.MinutesPerHour * EGenDate.HoursPerDay;
        GregorianCalendar Now2 = new GregorianCalendar();
        dSecs += (Now.getTimeInMillis() - Now2.getTimeInMillis()) / EGenDate.MsPerSecondDivisor;
        Seed +=  dSecs;
       // dSecs = (double)((m_dayno - dt.m_dayno) * SecondsPerMinute * MinutesPerHour * HoursPerDay);
        //dSecs += (double)(m_msec - dt.m_msec) / MsPerSecondDivisor;
        
        Seed <<= 33;
        Seed += uniqueID;

        System.out.println("setting RNGSeed for ticker tape");
        tickerTape.setRNGSeed( Seed );
        driverMEESettings.cur_TickerTapeRNGSeed = Seed;
        System.out.println("set rngseed");
        Seed |= 0x0000000100000000L;
        
        tradingFloor.setRNGSeed( Seed );
        driverMEESettings.cur_TradingFloorRNGSeed = Seed;
    }
    

    public MEE( int tradingTimeSoFar, MEESUTInterface  pSUT, BaseLogger  logger, SecurityHandler securityFile, long uniqueID, int configuredCustomerCount ){
        driverMEESettings = new DriverMEESettings( uniqueID, 0, 0, 0 );
        sut = pSUT;
        this.logger = logger;
        currentTime = new Date();
        baseTime = new Date();
        System.out.println("mee current time" + currentTime);
        System.out.println("trading time so far:" + tradingTimeSoFar);
        priceBoard = new MEEPriceBoard( tradingTimeSoFar,  baseTime,  currentTime, securityFile, configuredCustomerCount);
     
        System.out.println("Creating new ticker tape");
        tickerTape = new MEETickerTape( pSUT,  priceBoard,  baseTime,  currentTime );
        System.out.println("created new ticker tape");
        
        tradingFloor = new MEETradingFloor( pSUT,  priceBoard,  tickerTape,  baseTime,  currentTime );
        
     // eMEETradeRequestAction val =  ;
      /*  tradeReq = new TTradeRequest();
        tradeReq.eAction = eMEETradeRequestAction.eMEEProcessOrder;
        tradeReq.price_quote = 10.0;
        tradeReq.symbol = "AMPA";
        tradeReq.trade_id = 2000000415;
        tradeReq.trade_qty = 5;
        tradeReq.trade_type_id = "eLimitBuy";
        
        submitTradeRequest(tradeReq);*/
        
        logger.sendToLogger("MEE object constructed using c'tor 1 (valid for publication: YES).");
        AutoSetRNGSeeds( uniqueID );
        this.logger.sendToLogger(driverMEESettings);
    }

    public MEE( int tradingTimeSoFar, MEESUTInterface  pSUT, BaseLogger  logger, SecurityHandler  securityFile, long uniqueID, long tickerTapeRNGSeed, long tradingFloorRNGSeed, int configuredCustomerCount ){
        driverMEESettings = new DriverMEESettings( uniqueID, 0, tickerTapeRNGSeed, tradingFloorRNGSeed );
        sut = pSUT;
        this.logger = logger;
        currentTime = new Date();
        baseTime = new Date();
        priceBoard = new MEEPriceBoard( tradingTimeSoFar,  baseTime,  currentTime, securityFile, configuredCustomerCount);
        tickerTape = new MEETickerTape( pSUT,  priceBoard,  baseTime,  currentTime, tickerTapeRNGSeed );
        tradingFloor = new MEETradingFloor( pSUT,  priceBoard,  tickerTape,  baseTime,  currentTime, tradingFloorRNGSeed );
        
       
        
        this.logger.sendToLogger("MEE object constructed using c'tor 2 (valid for publication: NO).");
        this.logger.sendToLogger(driverMEESettings);
        
    }

    public long getTickerTapeRNGSeed(){
        return( tickerTape.getRNGSeed() );
    }

    public long getTradingFloorRNGSeed(){
        return( tradingFloor.getRNGSeed() );
    }

    public TTradeRequest getTradeRequest(){
        return tradeReq;
    }
    
    
    public void setBaseTime(){
       // baseTime = new Date();
        lock.lock();
        Calendar cal = Calendar.getInstance();
        baseTime.setTime(cal.getTimeInMillis());
        lock.unlock();
    }

    public boolean disableTickerTape(){
        boolean    result;
        lock.lock();
        result = tickerTape.DisableTicker();
        lock.unlock();
        return( result );
    }

    public boolean enableTickerTape(){
        boolean    result;
        lock.lock();
        result = tickerTape.EnableTicker();
        lock.unlock();
        return( result );
    }

    public int generateTradeResult(){
        int   nextTime;
      //  currentTime = new Date();
       // currentTime = ;
        lock.lock();
        Calendar cal = Calendar.getInstance();
        currentTime.setTime(cal.getTimeInMillis());
        nextTime = tradingFloor.generateTradeResult( );
        lock.unlock();
        return( nextTime );
    }

    public int submitTradeRequest( TTradeRequest pTradeRequest ){
        int nextTime;
        lock.lock();
        System.out.println();
        System.out.println();
        System.out.println("ACQUIRED LOCK");
        if(pTradeRequest.eActionTemp == 1){
            pTradeRequest.eAction = TPCEConstants.eMEETradeRequestAction.eMEEProcessOrder;
        }
        else{
            pTradeRequest.eAction = TPCEConstants.eMEETradeRequestAction.eMEESetLimitOrderTrigger;
            
        }
        Calendar cal = Calendar.getInstance();
        currentTime.setTime(cal.getTimeInMillis()); 
        //currentTime = new Date();
        nextTime = tradingFloor.submitTradeRequest( pTradeRequest );
        lock.unlock();
        System.out.println("RELEASED LOCK");
        System.out.println();
        System.out.println();
        return( nextTime );
    }

}
