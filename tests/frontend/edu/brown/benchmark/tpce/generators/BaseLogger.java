package edu.brown.benchmark.tpce.generators;

import java.util.Date;
import java.text.SimpleDateFormat;

import edu.brown.benchmark.tpce.TPCEConstants.DriverType;
import edu.brown.benchmark.tpce.TPCEConstants;

public abstract class BaseLogger {

    private String prefix;
    private BaseLogFormatter logFormatter;
        
    protected BaseLogger(DriverType drvType, long uniqueID, BaseLogFormatter formatter){
        String m_Version;
        int length = 32;
        logFormatter = formatter;
        m_Version = EGenVersion.getEGenVersionString(length);
        prefix = new String (TPCEConstants.szDriverTypeNames[drvType.getVal()] + "(" + m_Version + ")" + "ID :" + uniqueID);
    }
    protected abstract boolean sendToLoggerImpl(final String szPrefix, String szTimestamp, final String szMsg);
        
    public boolean sendToLogger(final String szPrefix, final String szMsg){
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("dd MMM yyyy HH:mm:ss z");
        Date curTime = new Date();
        return sendToLoggerImpl(szPrefix, sdf.format(curTime), szMsg);
    }
        
        // Strings
    public boolean sendToLogger(String str){
        return sendToLogger(prefix, str);
    }
        // Parameter Structures
    public boolean sendToLogger(LoaderSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(DriverGlobalSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(DriverCESettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(DriverCEPartitionSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(DriverMEESettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(DriverDMSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(BrokerVolumeSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(CustomerPositionSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(MarketWatchSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(SecurityDetailSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(TradeLookupSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(TradeOrderSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(TradeUpdateSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(TxnMixGeneratorSettings parms){
        return sendToLogger(logFormatter.getLogOutput(parms ));
    }
    public boolean sendToLogger(TDriverCETxnSettings parms){
        boolean result = false;
        result |= sendToLogger(logFormatter.getLogOutput(parms.BV_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.CP_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.MW_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.SD_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.TL_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.TO_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.TU_settings ));
        result |= sendToLogger(logFormatter.getLogOutput(parms.TxnMixGenerator_settings ));
        return result;
    }
}
