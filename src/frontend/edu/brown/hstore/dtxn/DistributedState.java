package edu.brown.hstore.dtxn;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionInitCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.Poolable;

/**
 * Contrainer class for all of the objects needed by a distributed txn
 * @author pavlo
 */
public class DistributedState implements Poolable {

    // ----------------------------------------------------------------------------
    // CURRENT TXN
    // ----------------------------------------------------------------------------
    
    private LocalTransaction ts = null;
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    /**
     * This callback is used to release the transaction once we get
     * the acknowledgments back from all of the partitions that we're going to access.
     * This is only needed for distributed transactions. 
     */
    protected final TransactionInitCallback init_callback;
    
    /**
     * This callback is used to keep track of what partitions have replied that they are 
     * ready to commit/abort our transaction.
     * This is only needed for distributed transactions.
     */
    protected final TransactionPrepareCallback prepare_callback; 
    
    /**
     * This callback will keep track of whether we have gotten all the 2PC acknowledgments
     * from the remote partitions. Once this is finished, we can then invoke
     * HStoreSite.deleteTransaction()
     */
    protected final TransactionFinishCallback finish_callback;
    
    // ----------------------------------------------------------------------------
    // CACHED CONTROLLERS
    // ----------------------------------------------------------------------------
    
    /**
     * Cached ProtoRpcControllers
     */
    protected final ProtoRpcController rpc_transactionInit[];
    protected final ProtoRpcController rpc_transactionWork[];
    protected final ProtoRpcController rpc_transactionPrepare[];
    protected final ProtoRpcController rpc_transactionFinish[];
    
    
    /**
     * Constructor
     * @param hstore_site
     */
    public DistributedState(HStoreSite hstore_site) {
        int num_sites = CatalogUtil.getNumberOfSites(hstore_site.getSite());
        
        this.init_callback = new TransactionInitCallback(hstore_site);
        this.prepare_callback = new TransactionPrepareCallback(hstore_site);
        this.finish_callback = new TransactionFinishCallback(hstore_site);
        
        this.rpc_transactionInit = new ProtoRpcController[num_sites];
        this.rpc_transactionWork = new ProtoRpcController[num_sites];
        this.rpc_transactionPrepare = new ProtoRpcController[num_sites];
        this.rpc_transactionFinish = new ProtoRpcController[num_sites];
        
    }
    
    public void init(LocalTransaction ts) {
        this.ts = ts;
        this.init_callback.init(ts);
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null);
    }

    @Override
    public void finish() {
        this.init_callback.finish();
        this.prepare_callback.finish();
        this.finish_callback.finish();
        this.ts = null;
    }
    
    protected ProtoRpcController getProtoRpcController(ProtoRpcController cache[], int site_id) {
        if (cache[site_id] == null) {
            cache[site_id] = new ProtoRpcController();
        } else {
            cache[site_id].reset();
        }
        return (cache[site_id]);
    }

    
    
}
