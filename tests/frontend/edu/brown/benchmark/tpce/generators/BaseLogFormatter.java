package edu.brown.benchmark.tpce.generators;

public abstract class BaseLogFormatter {

	public abstract String GetLogOutput(BrokerVolumeSettings parms );
    public abstract String GetLogOutput(CustomerPositionSettings parms );
    public abstract String GetLogOutput(MarketWatchSettings parms );
    public abstract String GetLogOutput(SecurityDetailSettings parms );
    public abstract String GetLogOutput(TradeLookupSettings parms );
    public abstract String GetLogOutput(TradeOrderSettings parms );
    public abstract String GetLogOutput(TradeUpdateSettings parms );
    public abstract String GetLogOutput(TxnMixGeneratorSettings parms );
    public abstract String GetLogOutput(LoaderSettings parms );
    public abstract String GetLogOutput(DriverGlobalSettings parms );
    public abstract String GetLogOutput(DriverCESettings parms );
    public abstract String GetLogOutput(DriverCEPartitionSettings parms );
    public abstract String GetLogOutput(DriverMEESettings parms );
    public abstract String GetLogOutput(DriverDMSettings parms );
}
