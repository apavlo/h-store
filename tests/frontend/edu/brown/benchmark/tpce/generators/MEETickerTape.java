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
        if( enabled ){
            AddToBatch( tickerEntry);
            AddArtificialEntries( );
        }
    }

    public void  PostLimitOrder( TTradeRequest tradeRequest ){
        TradeType            eTradeType;
        double      CurrentPrice = -1.0;
        TTickerEntry    pNewEntry = new TTickerEntry();

        eTradeType = ConvertTradeTypeIdToEnum(tradeRequest.trade_type_id.toCharArray());

        pNewEntry.price_quote = tradeRequest.price_quote;
        pNewEntry.symbol = new String(tradeRequest.symbol);
        pNewEntry.trade_qty = LIMIT_TRIGGER_TRADE_QTY;

        CurrentPrice = priceBoard.getCurrentPrice( tradeRequest.symbol ).getDollars();

        if((( eTradeType == TradeType.eLimitBuy || eTradeType == TradeType.eStopLoss ) &&
                CurrentPrice <= tradeRequest.price_quote )
            ||
                (( eTradeType == TradeType.eLimitSell ) &&
                CurrentPrice >= tradeRequest.price_quote )){
            pNewEntry.price_quote = CurrentPrice;
            limitOrderTimers.processExpiredTimers();
            inTheMoneyLimitOrderQ.add(pNewEntry);
        }
        else{
            pNewEntry.price_quote = tradeRequest.price_quote;
            double TriggerTimeDelay;
            GregorianCalendar currGreTime = new GregorianCalendar();
            GregorianCalendar baseGreTime = new GregorianCalendar();
            currGreTime.setTime(currentTime);
            baseGreTime.setTime(baseTime);
            double fCurrentTime = currGreTime.getTimeInMillis() - baseGreTime.getTimeInMillis();
            //TODO the third para, compare to c++
            TriggerTimeDelay = priceBoard.getSubmissionTime(pNewEntry.symbol, fCurrentTime, new EGenMoney(pNewEntry.price_quote), eTradeType) - fCurrentTime;
            limitOrderTimers.startTimer( TriggerTimeDelay);
        }
    }

    public void  AddLimitTrigger( TTickerEntry tickerEntry ){
        inTheMoneyLimitOrderQ.add( tickerEntry );
    }

    public void  AddArtificialEntries(){
        long              SecurityIndex;
        TTickerEntry        TickerEntry = new TTickerEntry();
        int                 TotalEntryCount = 0;
        final int    PaddingLimit = (TxnHarnessStructs.max_feed_len / 10) - 1;
        final int    PaddingLimitForAll = PaddingLimit;
        final int    PaddingLimitForTriggers = PaddingLimit;

        while ( TotalEntryCount < PaddingLimitForTriggers && !inTheMoneyLimitOrderQ.isEmpty() )
        {
            TTickerEntry pEntry = inTheMoneyLimitOrderQ.peek();
            AddToBatch( pEntry );
            inTheMoneyLimitOrderQ.poll();
            TotalEntryCount++;
        }

        while ( TotalEntryCount < PaddingLimitForAll )
        {
            TickerEntry.trade_qty = ( rnd.rndPercent( 50 )) ? RANDOM_TRADE_QTY_1 : RANDOM_TRADE_QTY_2;

            SecurityIndex = rnd.int64Range( 0, priceBoard.getNumOfSecurities() - 1 );
            TickerEntry.price_quote = (priceBoard.getCurrentPrice( SecurityIndex )).getDollars();
            priceBoard.getSymbol( SecurityIndex, TickerEntry.symbol, TickerEntry.symbol.length() );

            AddToBatch( TickerEntry );
            TotalEntryCount++;
        }
    }

    public void  AddToBatch( TTickerEntry tickerEntry ){
        txnInput.Entries[batchIndex++] = tickerEntry;
        if( TxnHarnessStructs.max_feed_len == batchIndex ){
            sut.MarketFeed( txnInput );
            batchIndex = 0;
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

    public final int              LIMIT_TRIGGER_TRADE_QTY = 375;
    public final int             RANDOM_TRADE_QTY_1 = 325;
    public final int              RANDOM_TRADE_QTY_2 = 425;
    
    private TimerWheel           limitOrderTimers;
    private Queue<TTickerEntry>  inTheMoneyLimitOrderQ;

    private Date                  baseTime;
    private Date                  currentTime;
}

