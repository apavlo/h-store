/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore.cmdlog;

import java.io.File;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.CommandLogWriterProfiler;
import edu.brown.utils.ExceptionHandlingRunnable;

/**
 * Transaction Command Log Writer
 * @author mkirsch
 * @author pavlo
 * @author debrabant
 */
public abstract class CommandLogWriter extends ExceptionHandlingRunnable implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(CommandLogWriter.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * The default file extension to use for the command log output
     */
    public static final String LOG_OUTPUT_EXT = ".cmdlog"; 
    
    /**
     * Special LogEntry that holds additional data that we
     * need in order to send back a ClientResponse
     */
    protected class WriterLogEntry extends LogEntry {
        protected ClientResponseImpl cresponse;
        protected RpcCallback<ClientResponseImpl> clientCallback;
        protected long initiateTime;
        protected int restartCounter;
        
        public LogEntry init(LocalTransaction ts, ClientResponseImpl cresponse) {
            this.cresponse = cresponse;
            this.clientCallback = ts.getClientCallback();
            this.initiateTime = ts.getInitiateTime();
            this.restartCounter = ts.getRestartCounter();
            return super.init(ts);
        }
        
        @Override
        public void finish() {
            super.finish();
            this.cresponse = null;
            this.clientCallback = null;
            this.initiateTime = -1;
            this.restartCounter = -1;
        }
    }
    
    /**
     * Constructor
     * @param catalog_db
     * @param path
     */
    public CommandLogWriter(HStoreSite hstore_site, File outputFile) {
    }
    
    abstract public CommandLogWriterProfiler getProfiler();
    
    /**
     * Get the total number of txns that are queued within this object.
     * <B>Note:</B> This is not thread-safe because the writer thread may swap
     * the entries in the middle of the count calculation. 
     * @return
     */
    abstract public int getTotalTxnCount();
 
    /**
     * Write a completed transaction handle out to the WAL file.
     * Returns true if the entry has been successfully written to disk and
     * the HStoreSite needs to send out the ClientResponse
     * @param ts
     * @return
     */
    abstract public boolean appendToLog(final LocalTransaction ts, final ClientResponseImpl cresponse);
}
