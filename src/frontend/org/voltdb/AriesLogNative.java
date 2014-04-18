/**
 * 
 */
package org.voltdb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.utils.DBBPool.BBContainer;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class AriesLogNative extends AriesLog {
    private static final Logger LOG = Logger.getLogger(AriesLogNative.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

	boolean m_recoveryDone = false;
	boolean m_keepLogging = true;
	
	private boolean[] m_perSiteRecoveryDone;
	
	private long pointerToReplayLog;
	private long replayLogSize;
	
	RandomAccessFile ariesLogfile = null;
	
	private long totalLogSize;
	private long numTransactions;
	
	private long txnIdToBeginReplay;
	
	private static String m_logFileName ;

	private int m_numSites ;
    private int m_numPartitionsPerSite ;
	
	private static class LogDataWithAtom {
		public byte b[];
		public AtomicBoolean isDurable;
		
		public LogDataWithAtom(byte b[], AtomicBoolean isDurable) {
			this.b = b;
			this.isDurable = isDurable;
		}
	}
	
	private List<LogDataWithAtom> m_waitingToFlush;
	private List<LogDataWithAtom> m_beingFlushed;
	
	public AriesLogNative(int numSites, int numPartitionsPerSite, String logFileName) {
		this(numSites, numPartitionsPerSite, 0, logFileName); // hardcode to 0 MB for now.
	}
	
	public AriesLogNative(int numSites, int numPartitionsPerSite, int size, String logFileName) {
		// Hardcode for now -- default value of 16 is also specified in
		// org.voltdb.compiler.DeploymentFileSchema.xsd
		this(numSites, numPartitionsPerSite, size, 16, logFileName);		
	}
	
	public AriesLogNative(int numSites, int numPartitionsPerSite, int size, int syncFrequency, String logFileName) {	    
                //LOG.warn("AriesLogNative : numSites : "+numSites+ " logFileName : "+logFileName);
	    
	        m_perSiteRecoveryDone = new boolean[numSites*numPartitionsPerSite];
		
		for (int i = 0; i < numSites*numPartitionsPerSite; i++) {
			m_perSiteRecoveryDone[i] = false;
		}
		
		fsyncFrequency = syncFrequency;
		logsize = size;			
		
		m_waitingToFlush = new ArrayList<LogDataWithAtom>();
		m_beingFlushed = null;
		
		isInitialized = false;
		
		totalLogSize = 0;
		numTransactions = 0;
		
		txnIdToBeginReplay = Long.MIN_VALUE;
		
		// initially set pointer to invalid value
		pointerToReplayLog = Long.MIN_VALUE;
		replayLogSize = 0;
		
		m_logFileName = logFileName;
		m_numSites = numSites;
		m_numPartitionsPerSite= numPartitionsPerSite;
		
		new Thread(this).start();
	}
	
	public void setTxnIdToBeginReplay(long txnId) {
		if (txnId > 0) {
			txnIdToBeginReplay = txnId;	
		} else {
			txnIdToBeginReplay = 1;
		}
	}
	
	public long getTxnIdToBeginReplay() {
		return txnIdToBeginReplay;
	}
	
	public boolean isReadyForReplay() {
		return (txnIdToBeginReplay > 0);
	}

	public void setPointerToReplayLog(long ariesReplayPointer, long size) {
		pointerToReplayLog = ariesReplayPointer;
		replayLogSize = size;
	}
	
	public long getPointerToReplayLog() {
		return pointerToReplayLog;
	}
	
	public long getReplayLogSize() {
		return replayLogSize;
	}
	
	@Override
	public synchronized void init() {
        if (!isInitialized) {
			try {
				long logsizeInMB = logsize;
				
				//XXX Disable this
				ariesLogfile = new RandomAccessFile(m_logFileName, "rw");
				ariesLogfile.setLength(logsizeInMB * 1024 * 1024);
				
				ariesLogfile.seek(0);
				
				/*
				// XXX: DO NOT do this, its way too slow for a file several gigabytes in size. 
				// Zero the log file just to be safe.
				for (long l = 0; l < ariesLogfile.length(); l++) {
					ariesLogfile.writeByte(0);
				}
								
				ariesLogfile.seek(0);
				*/
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}				
			isInitialized = true;
        }
	}

	private double getAverageLogSize() {
		if (numTransactions == 0) {
			return 0;
		}
		
		double avgLogSize = ((double) totalLogSize)/numTransactions;
		return avgLogSize;
	}
	
	public String getStatistics() {
		return String.valueOf(getAverageLogSize());
	}
	
	private void flushData() {	    
		swapFlushListWithEmpty();
		
		if (m_beingFlushed == null || m_beingFlushed.size() == 0) {
		    if(m_beingFlushed == null){
		        //LOG.warn("AriesLogNative : m_beingFlushed :"+m_beingFlushed);
		    }
		    else{
		        //LOG.warn("AriesLogNative : m_beingFlushed size :"+m_beingFlushed.size());
		    }		        
			return;
		}
		
		// write all log data to the file 
		// in memory
		for(LogDataWithAtom l : m_beingFlushed) {
			try {
				ariesLogfile.write(l.b);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// flush the log out
		try {
			ariesLogfile.getFD().sync();
			
	        LOG.debug("AriesLogNative : finished flushData :: File Size : "+ariesLogfile.length());
		} catch (SyncFailedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(LogDataWithAtom l : m_beingFlushed) {
			l.isDurable.set(true);
		}

		m_beingFlushed = null;
	}

	/**
	 * 
	 */
	private void swapFlushListWithEmpty() {
		// Must synchronize to avoid insertions into the list 
		// during the swap.
		synchronized (this) {
			m_beingFlushed = m_waitingToFlush;
			m_waitingToFlush = new ArrayList<LogDataWithAtom>();
		}
	}

	@Override
	public synchronized boolean isRecoveryCompleted() {
		if (m_recoveryDone) {
			return true;
		}
		
		boolean isRecoveryDone = true;
		int cnt = 0;
		
		for (int i = 0; i < m_perSiteRecoveryDone.length; i++) {
			isRecoveryDone &= m_perSiteRecoveryDone[i];
			if(m_perSiteRecoveryDone[i] == true)
			    cnt++;
		}				
		
		if (isRecoveryDone || cnt == this.m_numPartitionsPerSite) {
			m_recoveryDone = true;
		}
		
		return isRecoveryDone;		
	}

	public boolean isRecoveryCompletedForSite(int siteId) {
		int index = siteId ;
		return m_perSiteRecoveryDone[index];
	}
	
	// Must synchronize because multiple sites might insert into the log
	// concurrently
	@Override
	public synchronized void log(byte[] logbytes, AtomicBoolean isDurable) {
		LogDataWithAtom atom = new LogDataWithAtom(logbytes, isDurable);
		
		LOG.warn("AriesLogNative : log add atom :: size :"+m_waitingToFlush.size());
		//totalLogSize += logbytes.length;
		//numTransactions++;
		
		// Must lock further to avoid insertions during a list swap.
		synchronized (this) {
			m_waitingToFlush.add(atom);
		}
	}

	@Override
	public synchronized void setRecoveryCompleted(int siteId) {
		int index = siteId ;
		m_perSiteRecoveryDone[index] = true;
	}
	
    @Override
    public void run() {
        LOG.debug("AriesLogNative : wait for recovery completion ");
        while (!isRecoveryCompleted()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        LOG.debug("AriesLogNative : recovery completed ");

        if (!isInitialized) {
            init();
        }

        LOG.debug("AriesLogNative : initialized log");

        while (m_keepLogging) {
            try {
                long flushTime = System.currentTimeMillis();

                flushData();

                flushTime = System.currentTimeMillis() - flushTime;

                long sleepDuration = fsyncFrequency - flushTime;

                if (sleepDuration > 0) {
                    Thread.sleep(sleepDuration);
                }
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
