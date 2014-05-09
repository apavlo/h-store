package edu.brown.benchmark.tpceb.generators;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.LinkedList;

import edu.brown.benchmark.tpceb.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpceb.util.EGenRandom;

public class MEETradingFloor {
    public LinkedList<Long> inputs = new LinkedList<Long>();
    public long  getRNGSeed(){
        return( rnd.getSeed() );
    }
    
    public void  setRNGSeed( long RNGSeed ){
        rnd.setSeed( RNGSeed );
    }
    
    public MEETradingFloor( MEESUTInterface  sut, MEEPriceBoard  priceBoard, MEETickerTape  tickerTape, Date  baseTime, Date  currentTime ){
        this.sut = sut ;
        this.priceBoard = priceBoard;
        this.tickerTape = tickerTape;
        this.baseTime = baseTime;
        this.currentTime = currentTime;
        rnd =  new EGenRandom(EGenRandom.RNG_SEED_BASE_MEE_TRADING_FLOOR );
        orderProcessingDelayMean = 1.0;
        Method SendTradeResult = null;
            try{
                SendTradeResult = MEETradingFloor.class.getMethod("sendTradeResult", TTradeRequest.class);
            }catch(Exception e){
                e.printStackTrace();
            }

        orderTimers = new TimerWheel(TTradeRequest.class, this, SendTradeResult, 5, 1);
     }
        
    
     public MEETradingFloor(MEESUTInterface  sut, MEEPriceBoard  priceBoard, MEETickerTape  tickerTape, Date  baseTime, Date  currentTime, long RNGSeed){
         this.sut = sut ;
         this.priceBoard = priceBoard;
         this.tickerTape = tickerTape;
         this.baseTime = baseTime;
         this.currentTime = currentTime;
         rnd =  new EGenRandom(RNGSeed );
         orderProcessingDelayMean = 1.0;
     }
     
     private double  genProcessingDelay(double mean){
        //changed result calculation to match egen
        //OLD: double result = ( -1.0 * Math.log( rnd.rndDouble() )) * mean;
        double result = ( -1.0 * Math.log(rnd.doubleIncrRange(0.0, 1.0, 0.000000000001)) * mean);
        if( result > maxOrderProcessingDelay ){
            return( maxOrderProcessingDelay );
        }
        else{
            return result;
        }
    }
    
     public int  submitTradeRequest( TTradeRequest tradeReq ){
        switch( tradeReq.eAction ){
        case eMEEProcessOrder:
            {
               /*added b/c original uses pointers*/
                TTradeRequest newTradeRequest = new TTradeRequest();
                newTradeRequest = tradeReq;
                return( orderTimers.startTimer( genProcessingDelay( orderProcessingDelayMean ), this, newTradeRequest));
            }
        case eMEESetLimitOrderTrigger:
            tickerTape.PostLimitOrder( tradeReq );
            return( orderTimers.processExpiredTimers() );
        default:
            return( orderTimers.processExpiredTimers() );
        }
    }
    
    public int  generateTradeResult(){
        return( orderTimers.processExpiredTimers() );
    }
    
    public void  sendTradeResult( TTradeRequest tradeReq ){
        TradeType            eTradeType;
        TTradeResultTxnInput    txnInput = new TTradeResultTxnInput();
        TTickerEntry            TickerEntry = new TTickerEntry();
        double                  CurrentPrice = -1.0;
    
        eTradeType = tickerTape.ConvertTradeTypeIdToEnum( tradeReq.trade_type_id.toCharArray() );
        CurrentPrice = priceBoard.getCurrentPrice( tradeReq.symbol ).getDollars();
    
        txnInput.trade_id = tradeReq.trade_id;
       
        if(( eTradeType == TradeType.eLimitBuy && tradeReq.price_quote < CurrentPrice )||( eTradeType == TradeType.eLimitSell && tradeReq.price_quote > CurrentPrice )){
            txnInput.trade_price = tradeReq.price_quote;
           // System.out.println("IN IF");
        }
        else{
            txnInput.trade_price = CurrentPrice;
           // System.out.println("IN ELSE");
        }
       // System.out.println("TRADEIDHERE"+  txnInput.trade_id);
        //System.out.println("TRADEPRICEHERE"+  txnInput.trade_price);
        if(!inputs.contains(txnInput.trade_id)){
            System.out.println("TRADEIDHERE"+  txnInput.trade_id);
            System.out.println("ADDINGIDOK");
            sut.TradeResult(  txnInput );
            inputs.add(txnInput.trade_id);
        }
       //System.out.println("INPUTS!:");
       //for(int i = 0; i < inputs.size(); i++){
       //    System.out.println("ID " + inputs.get(i));
       //}
       // System.out.println("Got to here");
    
        TickerEntry.symbol = new String( tradeReq.symbol);
        TickerEntry.trade_qty = tradeReq.trade_qty;
        //System.out.println("Got to here2");
    
        TickerEntry.price_quote = CurrentPrice;
       // System.out.println("Got to here3");
    
        tickerTape.AddEntry(TickerEntry);
       // System.out.println("Got to here4");
    }
    
    private MEESUTInterface                                        sut;
    private MEEPriceBoard                                          priceBoard;
    private MEETickerTape                                          tickerTape;

    private Date                                                      baseTime;
    private Date                                                      currentTime;

    private TimerWheel                                               orderTimers;
    private EGenRandom                                             rnd;
    private  double                                                orderProcessingDelayMean;
    private static final int                                       maxOrderProcessingDelay = 5;

    public static final int  NO_OUTSTANDING_TRADES = -1;
}
