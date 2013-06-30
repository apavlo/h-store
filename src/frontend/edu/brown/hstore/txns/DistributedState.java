package edu.brown.hstore.txns;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.CatalogContext;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.callbacks.LocalFinishCallback;
import edu.brown.hstore.callbacks.LocalPrepareCallback;
import edu.brown.pools.Poolable;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.PartitionSet;

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
     * The partitions that we notified that we are done with them
     */
    protected final PartitionSet exec_donePartitions = new PartitionSet();
    
    /**
     * 
     */
    protected final BitSet notified_prepare;
    
    /**
     * We need to make sure that we only blast out a finish
     * notification once per transaction.
     */
    protected final AtomicBoolean notified_finish = new AtomicBoolean(false);
    
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
    
    /**
     * Cached ProtoRpcControllers
     * SiteId -> Controller
     */
    private final ProtoRpcController rpc_transactionInit[];
    private final ProtoRpcController rpc_transactionWork[];
    // private final ProtoRpcController rpc_transactionPrepare[];
    private final ProtoRpcController rpc_transactionFinish[];
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    /**
     * This callback is used to keep track of what partitions have replied that they are 
     * ready to commit/abort our transaction.
     * This is only needed for distributed transactions.
     */
    protected final LocalPrepareCallback prepare_callback; 
    
    /**
     * This callback will keep track of whether we have gotten all the 2PC acknowledgments
     * from the remote partitions. Once this is finished, we can then invoke
     * HStoreSite.deleteTransaction()
     */
    protected final LocalFinishCallback finish_callback;

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param hstore_site
     */
    public DistributedState(HStoreSite hstore_site) {
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        this.notified_prepare = new BitSet(catalogContext.numberOfPartitions);
        this.sent_parameters = new BitSet(catalogContext.numberOfSites);
        
        this.prepare_callback = new LocalPrepareCallback(hstore_site);
        this.finish_callback = new LocalFinishCallback(hstore_site);
        
        this.rpc_transactionInit = new ProtoRpcController[catalogContext.numberOfSites];
        this.rpc_transactionWork = new ProtoRpcController[catalogContext.numberOfSites];
        // this.rpc_transactionPrepare = new ProtoRpcController[catalogContext.numberOfSites];
        this.rpc_transactionFinish = new ProtoRpcController[catalogContext.numberOfSites];
    }
    
    public DistributedState init(LocalTransaction ts) {
        this.ts = ts;
        
        // Initialize the prepare callback.
        // We have to do this in order to support early 2PC prepares
        PartitionSet partitions = ts.getPredictTouchedPartitions();
        this.prepare_callback.init(this.ts, partitions);
        
        // Compute whether all of the partitions for this txn are at the same local site
        for (int partition : partitions.values()) {
            if (ts.hstore_site.isLocalPartition(partition) == false) {
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
        this.prepare_callback.finish();
        this.finish_callback.finish();
        this.is_all_local = true;
        this.exec_donePartitions.clear();
        this.notified_prepare.clear();
        this.notified_finish.set(false);
        this.sent_parameters.clear();
        
        for (int i = 0; i < this.rpc_transactionInit.length; i++) {
            if (this.rpc_transactionInit[i] != null)
                this.rpc_transactionInit[i].reset();
            if (this.rpc_transactionWork[i] != null)
                this.rpc_transactionWork[i].reset();
//            if (this.rpc_transactionPrepare[i] != null)
//                this.rpc_transactionPrepare[i].reset();
            if (this.rpc_transactionFinish[i] != null)
                this.rpc_transactionFinish[i].reset();
        } // FOR
        
        this.ts = null;
    }
    
    protected ProtoRpcController getTransactionInitController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionInit, site_id);
    }
    protected ProtoRpcController getTransactionWorkController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionWork, site_id);
    }
    protected ProtoRpcController getTransactionPrepareController(int site_id) {
        // Always create a new ProtoRpcController
        return new ProtoRpcController();
        // return this.getProtoRpcController(this.rpc_transactionPrepare, site_id);
    }
    protected ProtoRpcController getTransactionFinishController(int site_id) {
        return this.getProtoRpcController(this.rpc_transactionFinish, site_id);
    }
    
    private final ProtoRpcController getProtoRpcController(ProtoRpcController cache[], int site_id) {
        if (cache[site_id] == null) {
            cache[site_id] = new ProtoRpcController();
        }
        return (cache[site_id]);
    }
    
}
