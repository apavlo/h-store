/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
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
package edu.mit.hstore.dtxn;

import org.apache.log4j.Logger;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.BatchPlanner.BatchPlan;

import com.google.protobuf.RpcCallback;

import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.dtxn.Dtxn;

/**
 * 
 * @author pavlo
 */
public class RemoteTransaction extends AbstractTransaction {
    protected static final Logger LOG = Logger.getLogger(RemoteTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * RemoteTransactionState Factory
     */
    public static class Factory extends CountingPoolableObjectFactory<RemoteTransaction> {
        private final ExecutionSite executor;
        
        public Factory(ExecutionSite executor, boolean enable_tracking) {
            super(enable_tracking);
            this.executor = executor;
        }
        @Override
        public RemoteTransaction makeObjectImpl() throws Exception {
            return new RemoteTransaction(this.executor);
        }
    };
    
    private RpcCallback<Dtxn.FragmentResponse> fragment_callback;
    
    public RemoteTransaction(ExecutionSite executor) {
        super(executor);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public RemoteTransaction init(long txnId, long clientHandle, int source_partition, boolean predict_singlePartitioned, boolean predict_readOnly, boolean predict_abortable) {
        return ((RemoteTransaction)super.init(txnId, clientHandle, source_partition, predict_singlePartitioned, predict_readOnly, predict_abortable, false));
    }
    
    @Override
    public void finish() {
        super.finish();
        this.fragment_callback = null;
    }
    
    @Override
    public void initRound(long undoToken) {
        super.initRound(undoToken);
    }
    
    @Override
    public void startRound() {
        // If the stored procedure is not executing locally then we need at least
        // one FragmentTaskMessage callback
        assert(this.fragment_callback != null) :
            "No FragmentTaskMessage callbacks available for txn #" + this.txn_id;
        super.startRound();
    }
    
    @Override
    public void finishRound() {
        super.finishRound();
    }
    
    @Override
    public boolean isHStoreSite_Finished() {
        return (true);
    }
    
    
    @Override
    public VoltTable[] getResults() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void addResponse(int partition, int dependencyId) {
        throw new RuntimeException("Trying to store a response for a transaction not executing locally [txn=" + this.txn_id + "]");
    }
    
    @Override
    public void addResult(int partition, int dependencyId, VoltTable result) {
        throw new RuntimeException("Trying to store a result for a transaction not executing locally [txn=" + this.txn_id + "]");
    }

    @Override
    public void addFinishedBatchPlan(BatchPlan plan) {
        // Nothing
    }
    

    /**
     * Return the previously stored callback for a FragmentTaskMessage
     * @return
     */
    public RpcCallback<Dtxn.FragmentResponse> getFragmentTaskCallback() {
        return (this.fragment_callback);
    }
    
    /**
     * Store a callback specifically for one FragmentTaskMessage 
     * @param callback
     */
    public void setFragmentTaskCallback(RpcCallback<Dtxn.FragmentResponse> callback) {
        assert(callback != null) : "Null callback for txn #" + this.txn_id;
        if (trace.get()) LOG.trace("Storing FragmentTask callback for txn #" + this.txn_id);
        this.fragment_callback = callback;
    }
    
    @Override
    public String toString() {
        if (this.isInitialized()) {
            return "REMOTE #" + this.txn_id;
        } else {
            return ("<Uninitialized>");
        }
    }
    
    @Override
    public String debug() {
        return (StringUtil.formatMaps(this.getDebugMap()));
    }
}
