package edu.brown.benchmark.tpceb.generators;

import edu.brown.benchmark.tpceb.generators.CETxnMixGenerator;
import edu.brown.benchmark.tpceb.generators.TMarketWatchTxnInput;
import edu.brown.benchmark.tpceb.generators.TradeGenerator.TradeType;
import edu.brown.benchmark.tpceb.util.EGenDate;

public class CE {
    
    private void initialize( TDriverCETxnSettings txnParamSettings ){
        logger.sendToLogger(driverGlobalSettings);

        if( txnParamSettings != null){
            setTxnTunables( txnParamSettings );
        }
        else{
            setTxnTunables( driverCETxnSettings );
        }
    }

    private void autoSetRNGSeeds( int uniqueID ){
        int baseYear, baseMonth, baseDay, millisec;

        baseYear = EGenDate.getYear();
        baseMonth = EGenDate.getMonth();
        baseDay = EGenDate.getDay();
        millisec = (EGenDate.getHour() * EGenDate.MinutesPerHour + EGenDate.getMinute()) * EGenDate.SecondsPerMinute + EGenDate.getSecond(); 
        baseYear -= ( baseYear % 5 );

        long Seed;
        Seed = millisec / 100;
        Seed <<= 11;
        Seed += EGenDate.getDayNo(baseYear, baseMonth, baseDay) - EGenDate.getDayNo(baseYear, 1, 1);
        Seed <<= 33;
        Seed += uniqueID;
            
        txnMixGenerator.setRNGSeed( Seed );
        driverCESettings.cur_TxnMixRNGSeed = Seed;
        Seed |= 0x0000000100000000L;
        txnInputGenerator.setRNGSeed( Seed );
        driverCESettings.cur_TxnInputRNGSeed = Seed;
    }

    /*
    * Constructor - no partitioning by C_ID, automatic RNG seed generation (requires unique input)
    */
    public CE( CESUTInterface sut, BaseLogger logger, final TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
            int scaleFactor, int daysOfInitialTrades, int uniqueID, final TDriverCETxnSettings driverCETxnSettings ){
        this.driverCETxnSettings = new TDriverCETxnSettings();
        driverGlobalSettings = new DriverGlobalSettings( configuredCustomerCount, activeCustomerCount, scaleFactor, daysOfInitialTrades );
        driverCESettings = new DriverCESettings( uniqueID, 0, 0 );
        this.sut = sut;
        this.logger = logger;
        txnMixGenerator = new CETxnMixGenerator(driverCETxnSettings, this.logger );
        txnInputGenerator = new CETxnInputGenerator( inputFiles, configuredCustomerCount, activeCustomerCount, scaleFactor, 
                daysOfInitialTrades * EGenDate.HoursPerWorkDay, this.logger, driverCETxnSettings );
        bClearBufferBeforeGeneration = false;
        this.logger.sendToLogger("CE object constructed using constructor 1 (valid for publication: YES).");

        initialize( driverCETxnSettings );
        autoSetRNGSeeds( uniqueID );
       this.logger.sendToLogger(driverCESettings);
    }
       
    /*
    * Constructor - no partitioning by C_ID, RNG seeds provided
    */
    
    public CE( CESUTInterface sut, BaseLogger logger, final TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
            int scaleFactor, int daysOfInitialTrades, int uniqueID,long TxnMixRNGSeed, long TxnInputRNGSeed, final TDriverCETxnSettings driverCETxnSettings ){
        this.driverCETxnSettings = new TDriverCETxnSettings();
        driverGlobalSettings = new DriverGlobalSettings( configuredCustomerCount, activeCustomerCount, scaleFactor, daysOfInitialTrades );
        driverCESettings = new DriverCESettings( uniqueID, TxnMixRNGSeed, TxnInputRNGSeed );
        this.sut = sut;
        this.logger = logger;
        txnMixGenerator = new CETxnMixGenerator(driverCETxnSettings, TxnMixRNGSeed, this.logger );
        txnInputGenerator = new CETxnInputGenerator( inputFiles, configuredCustomerCount, activeCustomerCount, scaleFactor, daysOfInitialTrades * EGenDate.HoursPerWorkDay, TxnInputRNGSeed, this.logger, driverCETxnSettings );
        bClearBufferBeforeGeneration = false;
        this.logger.sendToLogger("CE object constructed using constructor 2 (valid for publication: YES).");

        initialize( driverCETxnSettings );
        autoSetRNGSeeds( uniqueID );

       this.logger.sendToLogger(driverCESettings);
        
    }

    /*
    * Constructor - partitioning by C_ID, automatic RNG seed generation (requires unique input)
    */
    public CE( CESUTInterface sut, BaseLogger logger, final TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
            long iMyStartingCustomerId, long iMyCustomerCount, int iPartitionPercent, int scaleFactor, int daysOfInitialTrades,
            int uniqueID, final TDriverCETxnSettings driverCETxnSettings ){
        this.driverCETxnSettings = new TDriverCETxnSettings();
        driverGlobalSettings = new DriverGlobalSettings( configuredCustomerCount, activeCustomerCount, scaleFactor, daysOfInitialTrades );
        driverCESettings = new DriverCESettings( uniqueID, 0, 0 );
        driverCEPartitionSettings = new DriverCEPartitionSettings( iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent );
        this.sut = sut;
        this.logger = logger;
        txnMixGenerator = new CETxnMixGenerator(driverCETxnSettings,  this.logger );
        txnInputGenerator = new CETxnInputGenerator( inputFiles, configuredCustomerCount, activeCustomerCount, scaleFactor, 
                daysOfInitialTrades * EGenDate.HoursPerWorkDay, iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent, this.logger, driverCETxnSettings );
        bClearBufferBeforeGeneration = false;
        this.logger.sendToLogger("CE object constructed using constructor 3 (valid for publication: YES).");

        initialize( driverCETxnSettings );
        autoSetRNGSeeds( uniqueID );
        this.logger.sendToLogger(driverCEPartitionSettings);
        this.logger.sendToLogger(driverCESettings);
    }

    /*
    * Constructor - partitioning by C_ID, RNG seeds provided
    */
    public CE( CESUTInterface sut, BaseLogger logger, final TPCEGenerator inputFiles, long configuredCustomerCount, long activeCustomerCount,
            long iMyStartingCustomerId, long iMyCustomerCount, int iPartitionPercent, int scaleFactor, int daysOfInitialTrades,
            int uniqueID, long TxnMixRNGSeed, long TxnInputRNGSeed, final TDriverCETxnSettings driverCETxnSettings ){
        this.driverCETxnSettings = new TDriverCETxnSettings();
        driverGlobalSettings = new DriverGlobalSettings( configuredCustomerCount, activeCustomerCount, scaleFactor, daysOfInitialTrades );
        driverCESettings = new DriverCESettings( uniqueID, TxnMixRNGSeed, TxnInputRNGSeed );
        driverCEPartitionSettings = new DriverCEPartitionSettings( iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent );
        this.sut = sut;
        this.logger = logger;
        txnMixGenerator = new CETxnMixGenerator(driverCETxnSettings, TxnMixRNGSeed, this.logger );
        txnInputGenerator = new CETxnInputGenerator( inputFiles, configuredCustomerCount, activeCustomerCount, scaleFactor, 
                daysOfInitialTrades * EGenDate.HoursPerWorkDay, iMyStartingCustomerId, iMyCustomerCount, iPartitionPercent, 
                TxnInputRNGSeed, this.logger, driverCETxnSettings );
        bClearBufferBeforeGeneration = false;
        this.logger.sendToLogger("CE object constructed using constructor 4 (valid for publication: YES).");

        initialize( driverCETxnSettings );
        autoSetRNGSeeds( uniqueID );

        this.logger.sendToLogger(driverCEPartitionSettings);
        this.logger.sendToLogger(driverCESettings);
    }

    public long getTxnInputGeneratorRNGSeed( ){
        return( txnInputGenerator.getRNGSeed() );
    }

    public long getTxnMixGeneratorRNGSeed( ){
        return( txnMixGenerator.getRNGSeed() );
    }

    public boolean setTxnTunables( final TDriverCETxnSettings txnParamSettings ){
        if( txnParamSettings.isValid() == true ){
            if (txnParamSettings != driverCETxnSettings){
                driverCETxnSettings = txnParamSettings;
            }

            txnMixGenerator.updateTunables();
            txnInputGenerator.updateTunables();
            return true;
        }
        else{
            return false;
        }
    }

    public void doTxn(){
        
        
        int iTxnType = txnMixGenerator.generateNextTxnType( );

        if (bClearBufferBeforeGeneration){
            zeroInputBuffer(iTxnType);
        }

        switch( iTxnType ){
            case CETxnMixGenerator.TRADE_ORDER:
                /*
                 * These two variables will be modified in the GenerateTradeOrderInput
                 */
                boolean    bExecutorIsAccountOwner = true;
                int   iTradeType = TradeType.eLimitBuy.ordinal();
                txnInputGenerator.generateTradeOrderInput( tradeOrderTxnInput, iTradeType );
                sut.TradeOrder( tradeOrderTxnInput, iTradeType);
                break;
        /*    case CETxnMixGenerator.MARKET_WATCH:
                txnInputGenerator.generateMarketWatchInput( marketWatchTxnInput );
                sut.MarketWatch( marketWatchTxnInput );
                break;*/
            default:
                System.err.println("CE: Generated illegal transaction");
                System.exit(1);
        }
        /*
         * case CETxnMixGenerator.MARKET_WATCH:
            txnInputGenerator.generateMarketWatchInput( marketWatchTxnInput );
            sut.MarketWatch( marketWatchTxnInput );
          //d  break;
            if (bClearBufferBeforeGeneration){
                zeroInputBuffer(CETxnMixGenerator.MARKET_WATCH);
            }*/
       /* if (bClearBufferBeforeGeneration){
            zeroInputBuffer(CETxnMixGenerator.TRADE_ORDER);
        }*/
     /*   int   iTradeType = TradeType.eLimitBuy.ordinal();
        txnInputGenerator.generateTradeOrderInput( tradeOrderTxnInput, iTradeType);
        sut.TradeOrder( tradeOrderTxnInput, iTradeType);*/

    }

    public void zeroInputBuffer(int iTxnType){
        switch( iTxnType ){
            case CETxnMixGenerator.TRADE_ORDER:
                tradeOrderTxnInput = new TTradeOrderTxnInput();
                break;
          /*  case CETxnMixGenerator.MARKET_WATCH:
                marketWatchTxnInput = new TMarketWatchTxnInput();
                break;*/
        }
            /* case CETxnMixGenerator.MARKET_WATCH:
            marketWatchTxnInput = new TMarketWatchTxnInput();
            break;*/
           // tradeOrderTxnInput = new TTradeOrderTxnInput();
       // marketWatchTxnInput = new TMarketWatchTxnInput();
           
    }

    public void setClearBufferOption(boolean bClearBufferBeforeGeneration){
        this.bClearBufferBeforeGeneration = bClearBufferBeforeGeneration;
    }

    public CETxnMixGenerator getCETxnMixGenerator(){
        return txnMixGenerator;
    }
    
    public CETxnInputGenerator getCETxnInputGenerator(){
        return txnInputGenerator;
    }

    private  DriverGlobalSettings        driverGlobalSettings;
    private  DriverCESettings            driverCESettings;
    private  DriverCEPartitionSettings   driverCEPartitionSettings;
    private  TDriverCETxnSettings        driverCETxnSettings;

    private  CESUTInterface              sut;
    private  BaseLogger                  logger;
    private  CETxnMixGenerator           txnMixGenerator;
    private  CETxnInputGenerator         txnInputGenerator;
    private  TMarketWatchTxnInput        marketWatchTxnInput;
    private  TTradeOrderTxnInput         tradeOrderTxnInput;

    private  boolean                     bClearBufferBeforeGeneration;
}
