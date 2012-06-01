package edu.brown.benchmark.tpce.generators;

import java.io.*;
import java.util.concurrent.locks.ReentrantLock;
import edu.brown.benchmark.tpce.TPCEConstants.DriverType;

public class EGenLogger extends BaseLogger{

	public EGenLogger(DriverType drvType, long uniqueID, final String szFilename, BaseLogFormatter logFormatter){
		super(drvType, uniqueID, logFormatter);
		try{
			fileWriter = new FileWriter(szFilename);
			buffered = new BufferedWriter(fileWriter);
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	
	protected boolean sendToLoggerImpl(final char[] szPrefix, String szTimestamp, final String szMsg){
		try{
			buffered.write(szPrefix.toString() + " " + szTimestamp + " " + szMsg);
		}catch(IOException e){
			e.printStackTrace();
		}
		return true;
	}
	
	private FileWriter fileWriter;
	private BufferedWriter buffered;
	
}
