package edu.brown.benchmark.tpce.generators;

import java.util.Date;
import java.text.SimpleDateFormat;

import edu.brown.benchmark.tpce.TPCEConstants.DriverType;
import edu.brown.benchmark.tpce.TPCEConstants;

public abstract class BaseLogger {

	    private String                m_Prefix;
	    private BaseLogFormatter  m_pLogFormatter;
	    
	    protected BaseLogger(DriverType drvType, long UniqueId, BaseLogFormatter formatter){
	    	char[] m_Version = new char[32];
	    		
	    	m_pLogFormatter = formatter;
	    	EGenVersion.GetEGenVersionString(m_Version, 32);
	    	m_Prefix = new String (TPCEConstants.szDriverTypeNames[drvType.getVal()] + "(" + m_Version.toString() + ")" + UniqueId);
	    	
	    }
	    
	    
	    protected abstract boolean SendToLoggerImpl(final char[] szPrefix, String szTimestamp, final String szMsg);
	    
	    public boolean SendToLogger(final char[] szPrefix, final String szMsg){
	    	SimpleDateFormat sdf = new SimpleDateFormat();
	    	sdf.applyPattern("dd MMM yyyy HH:mm:ss z");
	    	Date curTime = new Date();
	    	return SendToLoggerImpl(szPrefix, sdf.format(curTime), szMsg);
	    }
	    
	    // Strings
	    public boolean SendToLogger(String str){
	    	return SendToLogger(m_Prefix.toCharArray(), str);
	    }

	    // Parameter Structures
	    public boolean SendToLogger(LoaderSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(DriverGlobalSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(DriverCESettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(DriverCEPartitionSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(DriverMEESettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(DriverDMSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(BrokerVolumeSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(CustomerPositionSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(MarketWatchSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(SecurityDetailSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(TradeLookupSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(TradeOrderSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(TradeUpdateSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(TxnMixGeneratorSettings parms){
	    	return SendToLogger(m_pLogFormatter.GetLogOutput(parms ));
	    }
	    public boolean SendToLogger(TDriverCETxnSettings parms){
	    	boolean result = false;
	    	result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.BV_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.CP_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.MW_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.SD_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.TL_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.TO_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.TU_settings ));
	        result |= SendToLogger(m_pLogFormatter.GetLogOutput(parms.TxnMixGenerator_settings ));
	        return result;
	    }
}
