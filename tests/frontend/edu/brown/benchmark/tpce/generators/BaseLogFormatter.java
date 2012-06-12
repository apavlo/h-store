package edu.brown.benchmark.tpce.generators;

public abstract class BaseLogFormatter {

    public abstract String getLogOutput(BrokerVolumeSettings parms );
    public abstract String getLogOutput(CustomerPositionSettings parms );
    public abstract String getLogOutput(MarketWatchSettings parms );
    public abstract String getLogOutput(SecurityDetailSettings parms );
    public abstract String getLogOutput(TradeLookupSettings parms );
    public abstract String getLogOutput(TradeOrderSettings parms );
    public abstract String getLogOutput(TradeUpdateSettings parms );
    public abstract String getLogOutput(TxnMixGeneratorSettings parms );
    public abstract String getLogOutput(LoaderSettings parms );
    public abstract String getLogOutput(DriverGlobalSettings parms );
    public abstract String getLogOutput(DriverCESettings parms );
    public abstract String getLogOutput(DriverCEPartitionSettings parms );
    public abstract String getLogOutput(DriverMEESettings parms );
    public abstract String getLogOutput(DriverDMSettings parms );
}
