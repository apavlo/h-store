package edu.brown.benchmark.tpceb.generators;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.Queue;

import edu.brown.benchmark.tpceb.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpceb.util.EGenDate;
import edu.brown.benchmark.tpceb.util.EGenMoney;
import edu.brown.benchmark.tpceb.util.EGenRandom;

public class MEETickerTape {
    
    public long  getRNGSeed(){
        return( rnd.getSeed() );
    }

    public void  setRNGSeed( long RNGSeed ){
        rnd.setSeed( RNGSeed );
    }

    public void  initialize(){
        txnInput = new TMarketFeedTxnInput();
        txnInput.StatusAndTradeType.status_submitted = new String("SBMT");
        txnInput.StatusAndTradeType.type_limit_buy = new String("TLB");
        txnInput.StatusAndTradeType.type_limit_sell = new String("TLS");
        txnInput.StatusAndTradeType.type_stop_loss = new String("TSL");
    }

    public MEETickerTape(MEESUTInterface sut, MEEPriceBoard priceBoard, Date baseTime, Date currentTime ){
        this.sut = sut;
        this.priceBoard = priceBoard;
        batchIndex = 0;
        batchDuplicates = 0; //added
        rnd = new EGenRandom( EGenRandom.RNG_SEED_BASE_MEE_TICKER_TAPE);
        enabled = true;
        this.baseTime = baseTime;
        this.currentTime = currentTime;
        inTheMoneyLimitOrderQ = new LinkedList<TTickerEntry> ();
        Method AddLimitTrigger = null;
        try{
            AddLimitTrigger = MEETickerTape.class.getMethod("AddLimitTrigger", TTickerEntry.class);
        }catch(Exception e){
            e.printStackTrace();
        }
        limitOrderTimers = new TimerWheel(TTickerEntry.class, this, AddLimitTrigger, 900, 1000);
        initialize();
    }

    public MEETickerTape(MEESUTInterface sut, MEEPriceBoard priceBoard, Date baseTime, Date currentTime, long RNGSeed ){
         this.sut = sut;
         this.priceBoard = priceBoard;
         batchIndex = 0;
         rnd = new EGenRandom(RNGSeed);
         enabled = true;
         this.baseTime = baseTime;
         this.currentTime = currentTime;
         inTheMoneyLimitOrderQ = new LinkedList<TTickerEntry> ();
         initialize();
     }
       
    public boolean DisableTicker(){
        enabled = false;
        return( ! enabled );
    }

    public boolean  EnableTicker(){
        enabled = true;
        return( enabled );
    }

    public void  AddEntry( TTickerEntry tickerEntry ){
        System.out.println("in add entry");
        if( enabled ){
            System.out.println("going to add to batch");
            AddToBatch( tickerEntry);
            System.out.println("finished add to batch");
            AddArtificialEntries( );
            System.out.println("going to add artificial entries");
        }
        System.out.println("FINISHED TICKER ENTRY");
    }

    public void  PostLimitOrder( TTradeRequest tradeRequest ){
        System.out.println("trying to post limit order");
        TradeType            eTradeType;
 
        double      CurrentPrice = -1.0;
        TTickerEntry    pNewEntry = new TTickerEntry();

        
        eTradeType = ConvertTradeTypeIdToEnum(tradeRequest.trade_type_id.toCharArray());

        pNewEntry.price_quote = tradeRequest.price_quote;
        System.out.println("tickerentry price quote" + pNewEntry.price_quote);
        
        pNewEntry.symbol = new String(tradeRequest.symbol);
        System.out.println("tickerentry symbol" + pNewEntry.symbol);
        
        pNewEntry.trade_qty = LIMIT_TRIGGER_TRADE_QTY;
        System.out.println("tickerentry trade_qty" + pNewEntry.trade_qty);

        CurrentPrice = priceBoard.getCurrentPrice( tradeRequest.symbol ).getDollars();
        
        System.out.println("Current price" + CurrentPrice);

        if((( eTradeType == TradeType.eLimitBuy || eTradeType == TradeType.eStopLoss ) &&
                CurrentPrice <= tradeRequest.price_quote )
            ||
                (( eTradeType == TradeType.eLimitSell ) &&
                CurrentPrice >= tradeRequest.price_quote )){
            pNewEntry.price_quote = CurrentPrice;
            
            System.out.println("update1 price" + pNewEntry.price_quote);
            
            limitOrderTimers.processExpiredTimers();
            System.out.println("processed timer");
            inTheMoneyLimitOrderQ.add(pNewEntry);
        
        }
        else{
            pNewEntry.price_quote = tradeRequest.price_quote;
            
            System.out.println("update2 price" + pNewEntry.price_quote);
            
            double TriggerTimeDelay;
            GregorianCalendar currGreTime = new GregorianCalendar();
            GregorianCalendar baseGreTime = new GregorianCalendar();
            currGreTime.setTime(currentTime);
            baseGreTime.setTime(baseTime);
           
            double dSecs =  (double) (currGreTime.get(Calendar.DAY_OF_WEEK) - baseGreTime.get(Calendar.DAY_OF_WEEK) );
            dSecs = dSecs * EGenDate.SecondsPerMinute * EGenDate.MinutesPerHour * EGenDate.HoursPerDay;
            GregorianCalendar Now2 = new GregorianCalendar();
            dSecs += (currGreTime.get(Calendar.MILLISECOND) - baseGreTime.get(Calendar.MILLISECOND)) / EGenDate.MsPerSecondDivisor;
             
            //double fCurrentTime = currGreTime.getTimeInMillis() - baseGreTime.getTimeInMillis();
            double fCurrentTime = dSecs;
            //TODO the third para, compare to c++
            TriggerTimeDelay = priceBoard.getSubmissionTime(pNewEntry.symbol, fCurrentTime, new EGenMoney(pNewEntry.price_quote), eTradeType) - fCurrentTime;
            //modified
            limitOrderTimers.startTimer( TriggerTimeDelay, this, pNewEntry);
        }
    }

    public void  AddLimitTrigger( TTickerEntry tickerEntry ){
        System.out.println("in add limit trigger");
        inTheMoneyLimitOrderQ.add( tickerEntry );
    }

    public void  AddArtificialEntries(){
        System.out.println("trying to add artificial entries");
        long              SecurityIndex;
        TTickerEntry        TickerEntry = new TTickerEntry();
        int                 TotalEntryCount = 0;
        final int    PaddingLimit = (TxnHarnessStructs.max_feed_len / 10) - 1;
        System.out.println("this was fine");
        final int    PaddingLimitForAll = PaddingLimit;
        final int    PaddingLimitForTriggers = PaddingLimit;

        while ( TotalEntryCount < PaddingLimitForTriggers && !inTheMoneyLimitOrderQ.isEmpty() )
        {
            System.out.println("in the money limit order" + inTheMoneyLimitOrderQ.peek());
            TTickerEntry pEntry = inTheMoneyLimitOrderQ.peek();
            AddToBatch( pEntry );
            inTheMoneyLimitOrderQ.poll();
            TotalEntryCount++;
        }

        while ( TotalEntryCount < PaddingLimitForAll )
        {
            System.out.println("in second loop");
            TickerEntry.trade_qty = ( rnd.rndPercent( 50 )) ? RANDOM_TRADE_QTY_1 : RANDOM_TRADE_QTY_2;
            System.out.println("here1");
            
            SecurityIndex = rnd.int64Range( 0, priceBoard.getNumOfSecurities() - 1 );
            System.out.println("here2");
            TickerEntry.price_quote = (priceBoard.getCurrentPrice( SecurityIndex )).getDollars();
            System.out.println("here3");
            System.out.println("Symbol:" + TickerEntry.symbol);
            priceBoard.getSymbol( SecurityIndex, TickerEntry.symbol, TickerEntry.symbol.length() );
            System.out.println("here4");
            AddToBatch( TickerEntry );
            TotalEntryCount++;
        }
    }

  /*  public void  AddToBatch( TTickerEntry tickerEntry ){
        System.out.println("in add to batch  --> batch index " + batchIndex);
        txnInput.Entries[batchIndex++] = tickerEntry;
        System.out.println("Ticker Entry:" + tickerEntry.price_quote + " " + tickerEntry.symbol + " " + tickerEntry.trade_qty);
        if( TxnHarnessStructs.max_feed_len == batchIndex ){
            System.out.println("max feed len equals batch index");
            sut.MarketFeed( txnInput );
            System.out.println("added to txnInput");
            batchIndex = 0;
        }
    }*/
    public void  AddToBatch( TTickerEntry tickerEntry ){
        System.out.println("in add to batch  --> batch index " + batchIndex);
        for(int i = 0; i < batchIndex; i++){
               if(tickerEntry.symbol.equals(txnInput.Entries[i])){
                   batchDuplicates++;
                      break;
               }
        }
        txnInput.Entries[batchIndex++] = tickerEntry;
        System.out.println("Ticker Entry:" + tickerEntry.price_quote + " " + tickerEntry.symbol + " " + tickerEntry.trade_qty);
        if( TxnHarnessStructs.max_feed_len == batchIndex ){
            System.out.println("max feed len equals batch index");
            sut.MarketFeed( txnInput );
            System.out.println("added to txnInput");
            batchIndex = 0;
            batchDuplicates = 0;
        }
    }

    public TradeType  ConvertTradeTypeIdToEnum( char[] tradeType ){
        switch( tradeType[0] ){
        case 'T':
            switch( tradeType[1] ){
            case 'L':
                switch( tradeType[2] ){
                case 'B':
                    return( TradeType.eLimitBuy );
                case 'S':
                    return( TradeType.eLimitSell );
                default:
                    break;
                }
                break;
            case 'M':
                switch( tradeType[2] ){
                case 'B':
                    return( TradeType.eMarketBuy );
                case 'S':
                    return( TradeType.eMarketSell );
                default:
                    break;
                }
                break;
            case 'S':
                switch( tradeType[2] ){
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
    private MEESUTInterface      sut;
    private MEEPriceBoard        priceBoard;
    private TMarketFeedTxnInput  txnInput;
    private int                  batchIndex;
    private EGenRandom           rnd;
    private boolean              enabled;
    private InputFileHandler     statusType;
    private InputFileHandler     tradeType;
    private int                     batchDuplicates; //added
    public final int              LIMIT_TRIGGER_TRADE_QTY = 375;
    public final int             RANDOM_TRADE_QTY_1 = 325;
    public final int              RANDOM_TRADE_QTY_2 = 425;
    
    private TimerWheel           limitOrderTimers;
    private Queue<TTickerEntry>  inTheMoneyLimitOrderQ;

    private Date                  baseTime;
    private Date                  currentTime;
}

