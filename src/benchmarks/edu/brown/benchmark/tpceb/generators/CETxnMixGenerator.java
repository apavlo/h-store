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

        
        transactionMixTotalCE = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;
                                
        TradeOrderMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel;

        if (txnArray != null){
            txnArray = null;
        }
    
        txnArray = new char[transactionMixTotalCE];
        for (i=0; i < TradeOrderMixLimit; ++i){
            txnArray[i] = TRADE_ORDER;
        }
        txnArrayCurrentIndex = 0;

        logger.sendToLogger(driverCETxnSettings.TxnMixGenerator_settings);
    }
    
    public int  generateNextTxnType(){
        return 0;
  
    }

    public static final int INVALID_TRANSACTION_TYPE = -1;
    public static final int TRADE_ORDER              =  6;

        
    private final TDriverCETxnSettings  driverCETxnSettings;
    private EGenRandom                  rnd;
    private BaseLogger                  logger;

    private int                         transactionMixTotalCE;
    private int                         txnArrayCurrentIndex;
    private char[]                      txnArray;
}
