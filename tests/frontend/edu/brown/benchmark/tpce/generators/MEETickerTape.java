package edu.brown.benchmark.tpce.generators;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.Queue;

import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpce.util.EGenMoney;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class MEETickerTape {
	
	public long  GetRNGSeed(){
	    return( m_rnd.getSeed() );
	}

	public void  SetRNGSeed( long RNGSeed ){
	    m_rnd.setSeed( RNGSeed );
	}

	public void  initialize(){
	    m_TxnInput.StatusAndTradeType.status_submitted = new String("SBMT");
	    m_TxnInput.StatusAndTradeType.type_limit_buy = new String("TLB");
	    m_TxnInput.StatusAndTradeType.type_limit_sell = new String("TLS");
	    m_TxnInput.StatusAndTradeType.type_stop_loss = new String("TSL");
	}

	public MEETickerTape(MEESUTInterface pSUT, MEEPriceBoard pPriceBoard, Date pBaseTime, Date pCurrentTime ){
	    m_pSUT = pSUT;
	    m_pPriceBoard = pPriceBoard;
	    m_BatchIndex = 0;
	    m_rnd = new EGenRandom( EGenRandom.RNG_SEED_BASE_MEE_TICKER_TAPE);
	    m_Enabled = true;
	    m_pBaseTime = pBaseTime;
	    m_pCurrentTime = pCurrentTime;
	    m_InTheMoneyLimitOrderQ = new LinkedList<TTickerEntry> ();
	    Method AddLimitTrigger = null;
        try{
        	AddLimitTrigger = MEETickerTape.class.getMethod("AddLimitTrigger", TTickerEntry.class);
        }catch(Exception e){
        	e.printStackTrace();
        }
	    m_LimitOrderTimers = new TimerWheel(TTickerEntry.class, this, AddLimitTrigger, 900, 1000);
	    initialize();
	}

	public MEETickerTape(MEESUTInterface pSUT, MEEPriceBoard pPriceBoard, Date pBaseTime, Date pCurrentTime, long RNGSeed ){
		 m_pSUT = pSUT;
		 m_pPriceBoard = pPriceBoard;
		 m_BatchIndex = 0;
		 m_rnd = new EGenRandom(RNGSeed);
		 m_Enabled = true;
		 m_pBaseTime = pBaseTime;
		 m_pCurrentTime = pCurrentTime;
		 m_InTheMoneyLimitOrderQ = new LinkedList<TTickerEntry> ();
		 initialize();
	 }
	   
	public boolean DisableTicker(){
	    m_Enabled = false;
	    return( ! m_Enabled );
	}

	public boolean  EnableTicker(){
	    m_Enabled = true;
	    return( m_Enabled );
	}

	public void  AddEntry( TTickerEntry pTickerEntry ){
	    if( m_Enabled ){
	        AddToBatch( pTickerEntry);
	        AddArtificialEntries( );
	    }
	}

	public void  PostLimitOrder( TTradeRequest pTradeRequest )
	{
		TradeType            eTradeType;
	    double      CurrentPrice = -1.0;
	    TTickerEntry    pNewEntry = new TTickerEntry();

	    eTradeType = ConvertTradeTypeIdToEnum(pTradeRequest.trade_type_id.toCharArray());

	    pNewEntry.price_quote = pTradeRequest.price_quote;
	    pNewEntry.symbol = new String(pTradeRequest.symbol);
	    pNewEntry.trade_qty = LIMIT_TRIGGER_TRADE_QTY;

	    CurrentPrice = m_pPriceBoard.getCurrentPrice( pTradeRequest.symbol ).getDollars();

	    if((( eTradeType == TradeType.eLimitBuy || eTradeType == TradeType.eStopLoss ) &&
	            CurrentPrice <= pTradeRequest.price_quote )
	        ||
	            (( eTradeType == TradeType.eLimitSell ) &&
	            CurrentPrice >= pTradeRequest.price_quote )){
	        pNewEntry.price_quote = CurrentPrice;
	        m_LimitOrderTimers.ProcessExpiredTimers();
	        m_InTheMoneyLimitOrderQ.add(pNewEntry);
	    }
	    else{
	        pNewEntry.price_quote = pTradeRequest.price_quote;
	        double TriggerTimeDelay;
	        GregorianCalendar currGreTime = new GregorianCalendar();
	    	GregorianCalendar baseGreTime = new GregorianCalendar();
	    	currGreTime.setTime(m_pCurrentTime);
	    	baseGreTime.setTime(m_pBaseTime);
	        double fCurrentTime = currGreTime.getTimeInMillis() - baseGreTime.getTimeInMillis();
	        //TODO the third para, compare to c++
	        TriggerTimeDelay = m_pPriceBoard.getSubmissionTime(pNewEntry.symbol, fCurrentTime, new EGenMoney(pNewEntry.price_quote), eTradeType) - fCurrentTime;
	        m_LimitOrderTimers.StartTimer( TriggerTimeDelay);
	    }
	}

	public void  AddLimitTrigger( TTickerEntry pTickerEntry ){
	    m_InTheMoneyLimitOrderQ.add( pTickerEntry );
	}

	public void  AddArtificialEntries(){
	    long              SecurityIndex;
	    TTickerEntry        TickerEntry = new TTickerEntry();
	    int                 TotalEntryCount = 0;
	    final int    PaddingLimit = (TxnHarnessStructs.max_feed_len / 10) - 1;
	    final int    PaddingLimitForAll = PaddingLimit;
	    final int    PaddingLimitForTriggers = PaddingLimit;

	    while ( TotalEntryCount < PaddingLimitForTriggers && !m_InTheMoneyLimitOrderQ.isEmpty() )
	    {
	        TTickerEntry pEntry = m_InTheMoneyLimitOrderQ.peek();
	        AddToBatch( pEntry );
	        m_InTheMoneyLimitOrderQ.poll();
	        TotalEntryCount++;
	    }

	    while ( TotalEntryCount < PaddingLimitForAll )
	    {
	    	TickerEntry.trade_qty = ( m_rnd.rndPercent( 50 )) ? RANDOM_TRADE_QTY_1 : RANDOM_TRADE_QTY_2;

	        SecurityIndex = m_rnd.int64Range( 0, m_pPriceBoard.m_iNumberOfSecurities - 1 );
	        TickerEntry.price_quote = (m_pPriceBoard.getCurrentPrice( SecurityIndex )).getDollars();
	        m_pPriceBoard.getSymbol( SecurityIndex, TickerEntry.symbol, TickerEntry.symbol.length() );

	        AddToBatch( TickerEntry );
	        TotalEntryCount++;
	    }
	}

	public void  AddToBatch( TTickerEntry pTickerEntry ){
	    m_TxnInput.Entries[m_BatchIndex++] = pTickerEntry;
	    if( TxnHarnessStructs.max_feed_len == m_BatchIndex ){
	        m_pSUT.MarketFeed( m_TxnInput );
	        m_BatchIndex = 0;
	    }
	}

	public TradeType  ConvertTradeTypeIdToEnum( char[] pTradeType ){
	    switch( pTradeType[0] ){
	    case 'T':
	        switch( pTradeType[1] ){
	        case 'L':
	            switch( pTradeType[2] ){
	            case 'B':
	                return( TradeType.eLimitBuy );
	            case 'S':
	                return( TradeType.eLimitSell );
	            default:
	                break;
	            }
	            break;
	        case 'M':
	            switch( pTradeType[2] ){
	            case 'B':
	                return( TradeType.eMarketBuy );
	            case 'S':
	                return( TradeType.eMarketSell );
	            default:
	                break;
	            }
	            break;
	        case 'S':
	            switch( pTradeType[2] ){
	            case 'L':
	                return( TradeType.eStopLoss );
	            default:
	                break;
	            }
	            break;
	        default:
	            break;
	        }
	        break;
	    default:
	        break;
	    }

	    assert(false);

	    return TradeType.eMarketBuy;
	}
	private MEESUTInterface   m_pSUT;
	private MEEPriceBoard     m_pPriceBoard;
	private TMarketFeedTxnInput m_TxnInput;
	private int              m_BatchIndex;
	private EGenRandom             m_rnd;
	private boolean                m_Enabled;
	private InputFileHandler    m_pStatusType;
	private InputFileHandler     m_pTradeType;

	public final int LIMIT_TRIGGER_TRADE_QTY = 375;
	public final int RANDOM_TRADE_QTY_1 = 325;
	public final int RANDOM_TRADE_QTY_2 = 425;
	
	private TimerWheel  m_LimitOrderTimers;
	private Queue<TTickerEntry> m_InTheMoneyLimitOrderQ;

	private Date         m_pBaseTime;
	private Date          m_pCurrentTime;
}

