package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.util.EGenRandom;

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
        int   BrokerVolumeMixLimit;
        int   CustomerPositionMixLimit;
        int   MarketWatchMixLimit;
        int   SecurityDetailMixLimit;
        int   TradeLookupMixLimit;
        int   TradeOrderMixLimit;
        int   TradeStatusMixLimit;
        int   TradeUpdateMixLimit;
        
        transactionMixTotalCE = driverCETxnSettings.TxnMixGenerator_settings.cur_BrokerVolumeMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_CustomerPositionMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_MarketWatchMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_SecurityDetailMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_TradeLookupMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_TradeStatusMixLevel +
                                 driverCETxnSettings.TxnMixGenerator_settings.cur_TradeUpdateMixLevel;
    
        TradeStatusMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeStatusMixLevel;
        MarketWatchMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_MarketWatchMixLevel + TradeStatusMixLimit;
        SecurityDetailMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_SecurityDetailMixLevel + MarketWatchMixLimit;
        CustomerPositionMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_CustomerPositionMixLevel + SecurityDetailMixLimit;
        TradeOrderMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeOrderMixLevel + CustomerPositionMixLimit;
        TradeLookupMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeLookupMixLevel + TradeOrderMixLimit;
        TradeUpdateMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_TradeUpdateMixLevel + TradeLookupMixLimit;
        BrokerVolumeMixLimit = driverCETxnSettings.TxnMixGenerator_settings.cur_BrokerVolumeMixLevel + TradeUpdateMixLimit;
    
        if (txnArray != null){
            txnArray = null;
        }
    
        txnArray = new char[transactionMixTotalCE];
       
        for (i = 0; i < TradeStatusMixLimit; ++i){
            txnArray[i] = TRADE_STATUS;
        }
        for (; i < MarketWatchMixLimit; ++i){
            txnArray[i] = MARKET_WATCH;
        }
        for (; i < SecurityDetailMixLimit; ++i){
            txnArray[i] = SECURITY_DETAIL;
        }
        for (; i < CustomerPositionMixLimit; ++i){
            txnArray[i] = CUSTOMER_POSITION;
        }
        for (; i < TradeOrderMixLimit; ++i){
            txnArray[i] = TRADE_ORDER;
        }
        for (; i < TradeLookupMixLimit; ++i){
            txnArray[i] = TRADE_LOOKUP;
        }
        for (; i < TradeUpdateMixLimit; ++i){
            txnArray[i] = TRADE_UPDATE;
        }
        for (; i < BrokerVolumeMixLimit; ++i){
            txnArray[i] = BROKER_VOLUME;
        }
    
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
    }

    public static final int INVALID_TRANSACTION_TYPE = -1;
    public static final int SECURITY_DETAIL          =  0;
    public static final int BROKER_VOLUME            =  1;
    public static final int CUSTOMER_POSITION        =  2;
    public static final int MARKET_WATCH             =  3;
    public static final int TRADE_STATUS             =  4;
    public static final int TRADE_LOOKUP             =  5;
    public static final int TRADE_ORDER              =  6;
    public static final int TRADE_UPDATE             =  7;
    public static final int MARKET_FEED              =  8;
    public static final int TRADE_RESULT             =  9;
        
    private final TDriverCETxnSettings  driverCETxnSettings;
    private EGenRandom                  rnd;
    private BaseLogger                  logger;

    private int                         transactionMixTotalCE;
    private int                         txnArrayCurrentIndex;
    private char[]                      txnArray;
}
