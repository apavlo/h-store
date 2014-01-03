package edu.brown.benchmark.tpceb.generators;

public abstract class BaseLogFormatter {

    public abstract String getLogOutput(TradeOrderSettings parms );
    public abstract String getLogOutput(TxnMixGeneratorSettings parms );
    public abstract String getLogOutput(LoaderSettings parms );
    public abstract String getLogOutput(DriverGlobalSettings parms );
    public abstract String getLogOutput(DriverCESettings parms );
   public abstract String getLogOutput(DriverCEPartitionSettings parms );

}
