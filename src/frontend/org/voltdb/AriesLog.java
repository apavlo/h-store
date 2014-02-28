package org.voltdb;

import java.util.concurrent.atomic.AtomicBoolean;

// ARIES
public abstract class AriesLog implements Runnable {
	protected int logsize;			// in MBs
	protected long fsyncFrequency; 	// in millis
	
	public boolean isInitialized;
	
    public abstract void init();
    public abstract void setTxnIdToBeginReplay(long txnId);
	public abstract long getTxnIdToBeginReplay();
	public abstract boolean isReadyForReplay();
    public abstract void log(byte[] logbytes, AtomicBoolean isDurable);
	public abstract void setRecoveryCompleted(int siteId);
	public abstract boolean isRecoveryCompleted();
	public abstract boolean isRecoveryCompletedForSite(int siteId);
	public abstract void setPointerToReplayLog(long ariesReplayPointer, long size);
	public abstract long getPointerToReplayLog();
	public abstract long getReplayLogSize();
}
