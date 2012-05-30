package edu.brown.benchmark.tpce.generators;

import java.lang.reflect.Method;
import java.util.Date;

import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class MEETradingFloor {
	
	public long  GetRNGSeed(){
	    return( m_rnd.getSeed() );
	}
	
	public void  SetRNGSeed( long RNGSeed ){
	    m_rnd.setSeed( RNGSeed );
	}
	
	// Constructor - use default RNG seed
	 public MEETradingFloor( MEESUTInterface  pSUT, MEEPriceBoard  pPriceBoard, MEETickerTape  pTickerTape, Date  pBaseTime, Date  pCurrentTime ){
		 m_pSUT = pSUT ;
		 m_pPriceBoard = pPriceBoard;
		 m_pTickerTape = pTickerTape;
		 m_pBaseTime = pBaseTime;
		 m_pCurrentTime = pCurrentTime;
		 m_rnd =  new EGenRandom(EGenRandom.RNG_SEED_BASE_MEE_TRADING_FLOOR );
		 m_OrderProcessingDelayMean = 1.0;
		 Method SendTradeResult = null;
	        try{
	        	SendTradeResult = MEETradingFloor.class.getMethod("SendTradeResult", TTradeRequest.class);
	        }catch(Exception e){
	        	e.printStackTrace();
	        }
		 m_OrderTimers = new TimerWheel(TTradeRequest.class, this, SendTradeResult, 5, 1);
	 }
	    
	
	// Constructor - RNG seed provided
	 public MEETradingFloor(MEESUTInterface  pSUT, MEEPriceBoard  pPriceBoard, MEETickerTape  pTickerTape, Date  pBaseTime, Date  pCurrentTime, long RNGSeed){
		 m_pSUT = pSUT ;
		 m_pPriceBoard = pPriceBoard;
		 m_pTickerTape = pTickerTape;
		 m_pBaseTime = pBaseTime;
		 m_pCurrentTime = pCurrentTime;
		 m_rnd =  new EGenRandom(RNGSeed );
		 m_OrderProcessingDelayMean = 1.0;
	 }
	 
	 private double  genProcessingDelay(double Mean){
	    double result = ( -1.0 * Math.log( m_rnd.rndDouble() )) * Mean;
	
	    if( result > m_MaxOrderProcessingDelay ){
	        return( m_MaxOrderProcessingDelay );
	    }
	    else{
	        return result;
	    }
	}
	
	 public int  SubmitTradeRequest( TTradeRequest tradeReq ){
	    switch( tradeReq.eAction ){
	    case eMEEProcessOrder:
	        {
	        	return( m_OrderTimers.StartTimer( genProcessingDelay( m_OrderProcessingDelayMean )));
	        }
	    case eMEESetLimitOrderTrigger:
	        m_pTickerTape.PostLimitOrder( tradeReq );
	        return( m_OrderTimers.ProcessExpiredTimers() );
	    default:
	        return( m_OrderTimers.ProcessExpiredTimers() );
	    }
	}
	
	 public int  generateTradeResult(){
	    return( m_OrderTimers.ProcessExpiredTimers() );
	}
	
	public void  SendTradeResult( TTradeRequest tradeReq ){
	    TradeType            eTradeType;
	    TTradeResultTxnInput    TxnInput = new TTradeResultTxnInput();
	    TTickerEntry            TickerEntry = new TTickerEntry();
	    double                  CurrentPrice = -1.0;
	
	    eTradeType = m_pTickerTape.ConvertTradeTypeIdToEnum( tradeReq.trade_type_id.toCharArray() );
	    CurrentPrice = m_pPriceBoard.getCurrentPrice( tradeReq.symbol ).getDollars();
	
	    TxnInput.trade_id = tradeReq.trade_id;
	
	    if(( eTradeType == TradeType.eLimitBuy && tradeReq.price_quote < CurrentPrice )||( eTradeType == TradeType.eLimitSell && tradeReq.price_quote > CurrentPrice )){
	        TxnInput.trade_price = tradeReq.price_quote;
	    }
	    else{
	        TxnInput.trade_price = CurrentPrice;
	    }
	
	    m_pSUT.TradeResult(  TxnInput );
	
	    TickerEntry.symbol = new String( tradeReq.symbol);
	    TickerEntry.trade_qty = tradeReq.trade_qty;
	
	    TickerEntry.price_quote = CurrentPrice;
	
	    m_pTickerTape.AddEntry(TickerEntry);
	}
	
	private MEESUTInterface                                        m_pSUT;
	private MEEPriceBoard                                          m_pPriceBoard;
	private MEETickerTape                                          m_pTickerTape;

	private Date   m_pBaseTime;
	private Date   m_pCurrentTime;

	private TimerWheel    m_OrderTimers;
	private EGenRandom                                                 m_rnd;
	private  double                                                  m_OrderProcessingDelayMean;
	private static final int                                     m_MaxOrderProcessingDelay = 5;

	public static final int  NO_OUTSTANDING_TRADES = -1;
}
