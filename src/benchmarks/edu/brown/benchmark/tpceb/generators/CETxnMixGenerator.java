package edu.brown.benchmark.tpceb.generators;

import edu.brown.benchmark.tpceb.util.EGenRandom;

public class CETxnMixGenerator {
    public CETxnMixGenerator( TDriverCETxnSettings driverCETxnSettings, BaseLogger logger ){
        this.driverCETxnSettings =  driverCETxnSettings;
        rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_TXN_MIX_GENERATOR );
        this.logger = logger;
        txnArrayCurrentIndex = 0;
        txnArray = null;
     }

    public CETxnMixGenerator( TDriverCETxnSettings driverCETxnSettings, long RNGSeed, BaseLogger logger ){
        this.driverCETxnSettings =  driverCETxnSettings;
        rnd = new EGenRandom(RNGSeed );
        this.logger = logger;
        txnArrayCurrentIndex = 0;
        txnArray = null;
     }

    public long  getRNGSeed(){
        return rnd.getSeed();
    }
    
    public void  setRNGSeed(long RNGSeed ){
        rnd.setSeed( RNGSeed );
    }
    
    public void  updateTunables( ){
        int   i;

        int   TradeOrderMixLimit;
        int   MarketWatchMixLimit;

        
        transactionMixTotalCE =   //driverCETxnSettings.TxnMixGenerator_settings.cur_MarketWatchMixLevel
                 driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;
                                
        TradeOrderMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;
        //MarketWatchMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_MarketWatchMixLevel + TradeOrderMixLimit;
//commented out
        if (txnArray != null){
            txnArray = null;
        }
        System.out.println("total" + transactionMixTotalCE);
        txnArray = new char[transactionMixTotalCE];
      
        for (i=0; i < TradeOrderMixLimit; ++i){
            txnArray[i] = TRADE_ORDER;
        }
        //commented out
      /*for (; i < MarketWatchMixLimit; ++i){
            txnArray[i] = MARKET_WATCH;
        }*/
        
        txnArrayCurrentIndex = 0;

        logger.sendToLogger(driverCETxnSettings.TxnMixGenerator_settings);
    }
    
    public int  generateNextTxnType(){
        int threshold = rnd.intRange( txnArrayCurrentIndex, transactionMixTotalCE - 1);
        
        char iTxnType = txnArray[threshold];

        txnArray[threshold] = txnArray[txnArrayCurrentIndex];
        txnArray[txnArrayCurrentIndex] = iTxnType;
    
        txnArrayCurrentIndex = (txnArrayCurrentIndex + 1) % transactionMixTotalCE;
    
        return iTxnType;
        //return 0;
  
    }

    public static final int INVALID_TRANSACTION_TYPE = -1;
   // public static final int TRADE_ORDER              =  6;
   // public static final int MARKET_WATCH             =  3;
    public static final int TRADE_ORDER              =  0;
   // public static final int MARKET_WATCH             =  1;
    public static final int MARKET_FEED              =  8;
    public static final int TRADE_RESULT             =  9;
        
    private final TDriverCETxnSettings  driverCETxnSettings;
    private EGenRandom                  rnd;
    private BaseLogger                  logger;

    private int                         transactionMixTotalCE;
    private int                         txnArrayCurrentIndex;
    private char[]                      txnArray;
}
