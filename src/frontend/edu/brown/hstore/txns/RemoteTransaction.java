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
package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.callbacks.TransactionCleanupCallback;
import edu.brown.hstore.callbacks.TransactionWorkCallback;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * A RemoteTransaction is one whose Java control code is executing at a 
 * different partition then where we are using this handle.
 * @author pavlo
 */
public class RemoteTransaction extends AbstractTransaction {
    protected static final Logger LOG = Logger.getLogger(RemoteTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final TransactionWorkCallback work_callback;
    private final TransactionCleanupCallback cleanup_callback;
    private final ProtoRpcController rpc_transactionPrefetch[];
    
    public RemoteTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        this.work_callback = new TransactionWorkCallback(hstore_site);
        this.cleanup_callback = new TransactionCleanupCallback(hstore_site);
        
        int num_localPartitions = hstore_site.getLocalPartitionIds().size();
        this.rpc_transactionPrefetch = new ProtoRpcController[num_localPartitions];
    }
    
    public RemoteTransaction init(long txnId,
                                  int base_partition,
                                  ParameterSet parameters,
                                  Procedure catalog_proc,
                                  PartitionSet partitions,
                                  boolean predict_abortable) {
        super.init(txnId,              // TxnId
                   -1,                 // ClientHandle
                   base_partition,     // BasePartition
                   parameters,         // Procedure Parameters
                   catalog_proc,       // Procedure
                   partitions,         // Partitions
                   true,               // ReadOnly (???)
                   predict_abortable,  // Abortable
                   false               // ExecLocal
        );
        
        // Initialize TransactionCleanupCallback
        // NOTE: This must come *after* our call to AbstractTransaction.init()
        this.cleanup_callback.init(this, partitions);
        
        return (this);
    }
    
    @Override
    public void finish() {
        super.finish();
        this.work_callback.finish();
        this.cleanup_callback.finish();
        
        for (int i = 0; i < this.rpc_transactionPrefetch.length; i++) {
            // Tell the PretchQuery ProtoRpcControllers to cancel themselves
            // if we actually tried used them for this txn
            if (this.rpc_transactionPrefetch[i] != null && this.prefetch.partitions.get(i)) {
                this.rpc_transactionPrefetch[i].startCancel();
            }
        } // FOR
    }
    
    @Override
    public void startRound(int partition) {
        // If the stored procedure is not executing locally then we need at least
        // one FragmentTaskMessage callback
        assert(this.work_callback != null) :
            "No FragmentTaskMessage callbacks available for txn #" + this.txn_id;
        super.startRound(partition);
    }
    
    @Override
    public boolean isDeletable() {
        if (this.cleanup_callback.allCallbacksFinished() == false) {
            if (debug.get()) LOG.warn(String.format("%s - %s is not finished", this,
                                      this.cleanup_callback.getClass().getSimpleName()));
            return (false);
        }
        // XXX: Do we care about the TransactionWorkCallback?
        return (super.isDeletable());
    }
    
    public TransactionWorkCallback getWorkCallback() {
        return (this.work_callback);
    }
    
    /**
     * Get the TransactionCleanupCallback for this txn.
     * <B>Note:</B> You must call initCleanupCallback() first
     * @return
     */
    public TransactionCleanupCallback getCleanupCallback() {
        assert(this.cleanup_callback.isInitialized()) :
            String.format("Trying to grab the %s for %s before it has been initialized",
                          this.cleanup_callback.getClass().getSimpleName(), this);
        return (this.cleanup_callback);
    }

    /**
     * Get the ProtoRpcController to use to return a 
     * @param partition
     * @return
     */
    public ProtoRpcController getTransactionPrefetchController(int partition) {
        assert(hstore_site.isLocalPartition(partition));
        
        int offset = hstore_site.getLocalPartitionOffset(partition);
        if (this.rpc_transactionPrefetch[offset] == null) {
            this.rpc_transactionPrefetch[offset] = new ProtoRpcController();
        } else {
            this.rpc_transactionPrefetch[offset].reset();
        }
        return (this.rpc_transactionPrefetch[offset]);
    }

    @Override
    public String toStringImpl() {
        return String.format("%s-REMOTE #%d/%d", this.getProcedure().getName(),
                             this.txn_id,
                             this.base_partition);
    }
    
    @Override
    public String debug() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        
        // Base Class Info
        for (Map<String, Object> m : super.getDebugMaps()) {
            maps.add(m);
        } // FOR
        
        Map<String, Object> m;
        
        // Additional Info
        m = new LinkedHashMap<String, Object>();
        m.put("InitQueue Callback", this.initQueue_callback);
        m.put("PrepareWrapper Callback", this.prepareWrapper_callback);
        m.put("Work Callback", this.work_callback);
        m.put("CleanUp Callback", this.cleanup_callback);
        maps.add(m);
        
        return (StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()])));
    }
}

