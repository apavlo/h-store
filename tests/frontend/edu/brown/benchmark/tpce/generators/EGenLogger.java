package edu.brown.benchmark.tpce.generators;

import java.io.*;
import java.util.concurrent.locks.ReentrantLock;
import edu.brown.benchmark.tpce.TPCEConstants.DriverType;

public class EGenLogger extends BaseLogger{

	private ReentrantLock m_LogLock;
	private FileWriter fileWriter;
	private BufferedWriter buffered;
	
	public EGenLogger(DriverType drvType, long UniqueId, final String szFilename, BaseLogFormatter pLogFormatter){
		super(drvType, UniqueId, pLogFormatter);
		try{
			fileWriter = new FileWriter(szFilename);
			buffered = new BufferedWriter(fileWriter);
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}
	
	protected boolean SendToLoggerImpl(final char[] szPrefix, String szTimestamp, final String szMsg){
		m_LogLock = new ReentrantLock();
		try{
			buffered.write(szPrefix.toString() + " " + szTimestamp + " " + szMsg);
		}catch(IOException e){
			e.printStackTrace();
		}finally{
			m_LogLock.unlock();
		}
		return true;
	}
}
