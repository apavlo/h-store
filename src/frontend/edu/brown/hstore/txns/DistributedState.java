package edu.brown.hstore.txns;

import java.util.BitSet;

import org.voltdb.CatalogContext;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionInitCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.pools.Poolable;
import edu.brown.protorpc.ProtoRpcController;

/**
 * Container class for all of the objects needed by a distributed txn
 * @author pavlo
 */
public class DistributedState implements Poolable {

    /**
     * Current txn
     */
    private LocalTransaction ts = null;
    
    /**
     * 
     */
    protected final BitSet notified_prepare;
    
    /**
     * If true, then all of the partitions that this txn needs is on the same
     * site as the base partition
     */
    protected boolean is_all_local = true;
    
    
    /**
     * If this is a distributed transaction and we are doing aggressive spec exec,
     * then this bit map is used to keep track whether we have sent the Procedure
     * ParameterSet to a remote site.
     */
    protected final BitSet sent_parameters; 
    
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
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        this.notified_prepare = new BitSet(catalogContext.numberOfPartitions);
        this.sent_parameters = new BitSet(catalogContext.numberOfSites);
        
        this.init_callback = new TransactionInitCallback(hstore_site);
        this.prepare_callback = new TransactionPrepareCallback(hstore_site);
        this.finish_callback = new TransactionFinishCallback(hstore_site);
        
        this.rpc_transactionInit = new ProtoRpcController[catalogContext.numberOfSites];
        this.rpc_transactionWork = new ProtoRpcController[catalogContext.numberOfSites];
        this.rpc_transactionPrepare = new ProtoRpcController[catalogContext.numberOfSites];
        this.rpc_transactionFinish = new ProtoRpcController[catalogContext.numberOfSites];
    }
    
    public DistributedState init(LocalTransaction ts) {
        this.ts = ts;
        
        for (Integer p : ts.getPredictTouchedPartitions()) {
            if (ts.hstore_site.isLocalPartition(p.intValue()) == false) {
                this.is_all_local = false;
                break;
            }
        } // FOR
        
        return (this);
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
        this.is_all_local = true;
        this.notified_prepare.clear();
        this.sent_parameters.clear();
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
