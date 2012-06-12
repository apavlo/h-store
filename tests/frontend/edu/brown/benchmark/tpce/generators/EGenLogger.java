package edu.brown.benchmark.tpce.generators;


import org.apache.log4j.Logger;

import edu.brown.benchmark.tpce.TPCEConstants.DriverType;

public class EGenLogger extends BaseLogger{

    public EGenLogger(DriverType drvType, long uniqueID, BaseLogFormatter logFormatter){
        super(drvType, uniqueID, logFormatter);
    }
    
    protected boolean sendToLoggerImpl(final String szPrefix, String szTimestamp, final String szMsg){
        
    	LOG.debug(szPrefix + " " + szTimestamp + " " + szMsg);
        return true;
    }
    
    private static final Logger LOG = Logger.getLogger(EGenLogger.class);
}
