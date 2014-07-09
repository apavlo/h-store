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
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Procedure;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.UnevictDataResponse;
import edu.brown.hstore.callbacks.RemoteInitQueueCallback;
import edu.brown.hstore.callbacks.RemotePrepareCallback;
import edu.brown.hstore.callbacks.RemoteFinishCallback;
import edu.brown.hstore.callbacks.RemoteWorkCallback;
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
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    private final RemoteInitQueueCallback init_callback;
    private final RemoteWorkCallback work_callback;
    private final List<RemotePrepareCallback> prepare_callbacks = new ArrayList<RemotePrepareCallback>();
    private final RemoteFinishCallback finish_callback;
    private RpcCallback<UnevictDataResponse> unevict_callback;
    
    // ----------------------------------------------------------------------------
    // PREFETCH
    // ----------------------------------------------------------------------------
    
    private final ProtoRpcController rpc_transactionPrefetch[];
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * This does not fully initialize this transaction.
     * You must call init() before this can be used
     * @param hstore_site
     */
    public RemoteTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        this.init_callback = new RemoteInitQueueCallback(hstore_site);
        this.work_callback = new RemoteWorkCallback(hstore_site);
        this.finish_callback = new RemoteFinishCallback(hstore_site);
        
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        this.rpc_transactionPrefetch = new ProtoRpcController[catalogContext.numberOfPartitions];
    }
    
    /**
     * Main initialization method for RemoteTransaction
     * @param txnId
     * @param base_partition
     * @param parameters
     * @param catalog_proc
     * @param partitions
     * @param predict_abortable
     * @return
     */
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
        
        // Initialize Prepare + Finish Callbacks
        // NOTE: This must come *after* our call to AbstractTransaction.init()
        this.finish_callback.init(this, partitions);
        
        return (this);
    }
    
    @Override
    public void finish() {
        super.finish();
        
        // Callbacks
        this.init_callback.finish();
        this.work_callback.finish();
        this.finish_callback.finish();
        for (RemotePrepareCallback callback : this.prepare_callbacks) {
            callback.finish();
        } // FOR
        
        // ProtoRpcControllers
        for (int i = 0; i < this.rpc_transactionPrefetch.length; i++) {
            // Tell the PretchQuery ProtoRpcControllers to cancel themselves
            // if we actually tried used them for this txn
            if (this.rpc_transactionPrefetch[i] != null && this.prefetch.partitions.contains(i)) {
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
        if (this.init_callback.allCallbacksFinished() == false) {
            if (debug.val)
                LOG.warn(String.format("%s - %s is not finished", this,
                         this.init_callback.getClass().getSimpleName()));
            return (false);
        }
        if (this.work_callback.allCallbacksFinished() == false) {
            if (debug.val)
                LOG.warn(String.format("%s - %s is not finished", this,
                         this.work_callback.getClass().getSimpleName()));
            return (false);
        }
        if (this.finish_callback.allCallbacksFinished() == false) {
            if (debug.val)
                LOG.warn(String.format("%s - %s is not finished", this,
                         this.finish_callback.getClass().getSimpleName()));
            return (false);
        }
        // XXX: Do we care about the TransactionWorkCallback?
        return (super.isDeletable());
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------------
    
    @SuppressWarnings("unchecked")
    @Override
    public RemoteInitQueueCallback getInitCallback() {
        return (this.init_callback);
    }
    /**
     * This will always create a new callback
     */
    @SuppressWarnings("unchecked")
    @Override
    public RemotePrepareCallback getPrepareCallback() {
        RemotePrepareCallback callback = new RemotePrepareCallback(hstore_site);
        this.prepare_callbacks.add(callback);
        return (callback);
    }
    @SuppressWarnings("unchecked")
    @Override
    public RemoteFinishCallback getFinishCallback() {
        return (this.finish_callback);
    }

    public RemoteWorkCallback getWorkCallback() {
        return (this.work_callback);
    }
    
    /**
     * Get the ProtoRpcController to use to return a 
     * @param partition
     * @return
     */
    public ProtoRpcController getTransactionPrefetchController(int partition) {
        assert(hstore_site.isLocalPartition(partition));
        
        if (this.rpc_transactionPrefetch[partition] == null) {
            this.rpc_transactionPrefetch[partition] = new ProtoRpcController();
        } else {
            this.rpc_transactionPrefetch[partition].reset();
        }
        return (this.rpc_transactionPrefetch[partition]);
    }
    
    public void setUnevictCallback(RpcCallback<UnevictDataResponse> callback){
    	this.unevict_callback = callback;
    }
    
	public RpcCallback<UnevictDataResponse> getUnevictCallback() {
		return this.unevict_callback;
	}

    // ----------------------------------------------------------------------------
    // DEBUG STUFF
    // ----------------------------------------------------------------------------

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
        m.put(RemoteInitQueueCallback.class.getSimpleName(), this.init_callback);
        m.put(RemoteWorkCallback.class.getSimpleName(), this.work_callback);
        m.put(RemotePrepareCallback.class.getSimpleName(), this.prepare_callbacks);
        m.put(RemoteFinishCallback.class.getSimpleName(), this.finish_callback);
        maps.add(m);
        
        return (StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()])));
    }
}

