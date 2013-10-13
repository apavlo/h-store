package edu.brown.hstore.txns;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.callbacks.SendDataCallback;
import edu.brown.hstore.callbacks.RemoteFinishCallback;
import edu.brown.hstore.callbacks.TransactionMapCallback;
import edu.brown.hstore.callbacks.TransactionMapWrapperCallback;
import edu.brown.hstore.callbacks.TransactionReduceCallback;
import edu.brown.hstore.callbacks.TransactionReduceWrapperCallback;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionSet;

/**
 * Special transaction state object for MapReduce jobs
 * @author pavlo
 * @author xin
 */
public class MapReduceTransaction extends LocalTransaction {
    private static final Logger LOG = Logger.getLogger(MapReduceTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final LocalTransaction local_txns[];
    public int partitions_size;
    
    private VoltTable mapOutput[];
    private VoltTable reduceInput[];
    private VoltTable reduceOutput[];

    public enum State {
        MAP,
        SHUFFLE,
        REDUCE,
        FINISH;
    }

    /**
     * MapReduce Phases
     */
    private State mr_state = null;
    
    private Table mapEmit;
    private Table reduceEmit;
    
    /**
     * This is for non-blocking reduce executing in MapReduceHelperThread
     */
    public boolean basePartition_reduce_exec = false;
    public boolean basePartition_map_exec = false;
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------

    
    /**
     */
    private final TransactionMapCallback map_callback;

    private final TransactionMapWrapperCallback mapWrapper_callback;
    
    private final SendDataCallback sendData_callback;
    
    private final TransactionReduceCallback reduce_callback;
    
    private final TransactionReduceWrapperCallback reduceWrapper_callback;
    
    private final RemoteFinishCallback cleanup_callback;

    /**
     * Constructor 
     * @param hstore_site
     */
    public MapReduceTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        // new local_txns
        this.partitions_size = this.hstore_site.getLocalPartitionIds().size();
        this.local_txns = new LocalTransaction[this.partitions_size];
        for (int i = 0; i < this.partitions_size; i++) {
            this.local_txns[i] = new LocalTransaction(hstore_site) {
                @Override
                public String toStringImpl() {
                    return MapReduceTransaction.this.toString() + "/" + this.base_partition;
                }
            };
        } // FOR
        
        // new mapout and reduce output talbes for each partition it wants to touch
        this.mapOutput = new VoltTable[this.partitions_size];
        this.reduceInput = new VoltTable[this.partitions_size];
        this.reduceOutput = new VoltTable[this.partitions_size];
                
        this.map_callback = new TransactionMapCallback(hstore_site);
        this.mapWrapper_callback = new TransactionMapWrapperCallback(hstore_site);
        
        this.sendData_callback = new SendDataCallback(hstore_site);
        //this.sendDataWrapper_callback = new SendDataWrapperCallback(hstore_site);
        
        this.reduce_callback = new TransactionReduceCallback(hstore_site);
        this.reduceWrapper_callback = new TransactionReduceWrapperCallback(hstore_site);
        
        this.cleanup_callback = new RemoteFinishCallback(hstore_site);
    }
    
    
    @Override
    public LocalTransaction init(Long txn_id,
                                 long initiateTime,
                                 long clientHandle,
                                 int base_partition,
                                 PartitionSet predict_touchedPartitions,
                                 boolean predict_readOnly,
                                 boolean predict_canAbort,
                                 Procedure catalog_proc,
                                 ParameterSet params,
                                 RpcCallback<ClientResponseImpl> client_callback) {
        super.init(txn_id,
                   initiateTime,
                   clientHandle,
                   base_partition,
                   predict_touchedPartitions,
                   predict_readOnly,
                   predict_canAbort,
                   catalog_proc,
                   params,
                   client_callback);
        
        // Intialize MapReduce properties
        this.mapEmit = hstore_site.getCatalogContext().getTableByName(catalog_proc.getMapemittable());
        this.reduceEmit = hstore_site.getCatalogContext().getTableByName(catalog_proc.getReduceemittable());
        if (debug.val) {
            LOG.debug(" CatalogUtil.getVoltTable(thisMapEmit): -> " + catalog_proc.getMapemittable());
            LOG.debug("MapReduce LocalPartitionIds: " + this.hstore_site.getLocalPartitionIds());
        }
        
        // Get the Table catalog object for the map/reduce outputs
        // For each partition there should be a map/reduce output voltTable
        for (int partition : this.hstore_site.getLocalPartitionIds()) {
            if (debug.val) LOG.debug(String.format("Partition[%d] -> Offset[%d]", partition, partition));
            this.local_txns[partition].init(this.txn_id,
                                         initiateTime,
                                         this.client_handle,
                                         partition,
                                         hstore_site.getCatalogContext().getPartitionSetSingleton(partition),
                                         this.predict_readOnly,
                                         this.predict_abortable,
                                         catalog_proc,
                                         params,
                                         null);
            this.local_txns[partition].markMapReduce();
            
            // init map/reduce Output for each partition
            assert(this.mapEmit != null): "mapEmit has not been initialized\n ";
            assert(this.reduceEmit != null): "reduceEmit has not been initialized\n ";
            this.mapOutput[partition] = CatalogUtil.getVoltTable(this.mapEmit);
            this.reduceInput[partition] = CatalogUtil.getVoltTable(this.mapEmit);
            this.reduceOutput[partition] = CatalogUtil.getVoltTable(this.reduceEmit);
            
        } // FOR
        
        this.setMapPhase();
        this.map_callback.init(this);
        assert(this.map_callback.isInitialized()) : "Unexpected error for " + this;
        this.reduce_callback.init(this);
        assert(this.reduce_callback.isInitialized()) : "Unexpected error for " + this;
        
        // Initialize the TransactionCleanupCallback if this txn's base partition
        // is not at this HStoreSite. 
        if (this.hstore_site.isLocalPartition(base_partition) == false) {
            this.cleanup_callback.init(this, this.hstore_site.getLocalPartitionIds());
        }
        
        if (debug.val) LOG.info("Invoked MapReduceTransaction.init() -> " + this);
        return (this);
    }

    public MapReduceTransaction init(Long txn_id,
                                     long initiateTime,
                                     long client_handle,
                                     int base_partition,
                                     Procedure catalog_proc,
                                     ParameterSet params) {
        this.init(txn_id,
                  initiateTime,
                  client_handle,
                  base_partition,
                  hstore_site.getCatalogContext().getAllPartitionIds(),
                  false,
                  true,
                  catalog_proc,
                  params,
                  null);
        LOG.info("Invoked MapReduceTransaction.init() -> " + this);
        assert(this.map_callback.isInitialized()) : "Unexpected error for " + this;
        //assert(this.sendData_callback.isInitialized()) : "Unexpected error for " + this;
        return (this);
    }
    
    @Override
    public void finish() {
        super.finish();
//        for (int i = 0; i < this.partitions_size; i++) {
//            this.local_txns[i].finish();
//        } // FOR
        this.mr_state = null;
        
        this.map_callback.finish();
        this.mapWrapper_callback.finish();
        this.sendData_callback.finish();
        this.reduce_callback.finish();
        this.reduceWrapper_callback.finish();
        
        this.basePartition_map_exec = false;
        this.basePartition_reduce_exec = false;
        
        // TODO(xin): Only call TransactionCleanupCallback.finish() if this txn's base
        //            partition is not at this HStoreSite. 
        if (!this.hstore_site.isLocalPartition(this.base_partition)) {
            this.cleanup_callback.finish();
        }
        
        
        if(debug.val) LOG.debug("<MapReduceTransaction> this.reduceWrapper_callback.finish().......................");
        this.mapEmit = null;
        this.reduceEmit = null;
        this.mapOutput = null;
        this.reduceInput = null;
        this.reduceOutput = null;
    }
    /**
     * Store Data from MapOutput table into reduceInput table
     * ReduceInput table is the result of all incoming mapOutput table from other partitions
     * @see edu.brown.hstore.txns.AbstractTransaction#storeData(int, org.voltdb.VoltTable)
     */
    @Override
    public synchronized Status storeData(int partition, VoltTable vt) {
        VoltTable input = this.getReduceInputByPartition(partition);
        
        assert(input != null);
        if (debug.val)
            LOG.debug(String.format("StoreData into Partition #%d: RowCount=%d ",
                    partition, vt.getRowCount()));
        
        if (debug.val)
            LOG.debug(String.format("<StoreData, change to ReduceInputTable> to Partition:%d>\n %s",partition,vt));
        while (vt.advanceRow()) {
            VoltTableRow row = vt.fetchRow(vt.getActiveRowIndex());
            assert(row != null);
            input.add(row);
        }
        vt.resetRowPosition();
        
        return Status.OK;
    }
    
    /**
     * Get a LocalTransaction handle for a local partition
     * 
     * @param partition
     * @return
     */
    public LocalTransaction getLocalTransaction(int partition) {
        return (this.local_txns[partition]);
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isDeletable() {
        if (this.cleanup_callback.allCallbacksFinished() == false) {
            if (trace.val)
                LOG.warn(String.format("%s - %s is not finished", this,
                         this.cleanup_callback.getClass().getSimpleName()));
            return (false);
        }
        return (super.isDeletable());
    }
    
    public boolean isBasePartitionMapExec() {
        return this.basePartition_map_exec;
    }

    public void markBasePartitionMapExec() {
        assert(this.basePartition_map_exec == false);
        this.basePartition_map_exec = true;
    }

    public boolean isBasePartitionReduceExec() {
        return this.basePartition_reduce_exec;
    }

    public void markBasePartitionReduceExec() {
        assert(this.basePartition_reduce_exec == false);
        this.basePartition_reduce_exec = true;
    }
    
    /*
     * Return the MapOutput Table schema 
     */
    
    public boolean isMapPhase() {
        return (this.mr_state == State.MAP);
    }
   
    public boolean isShufflePhase() {
        return (this.mr_state == State.SHUFFLE); 
    }
    
    public boolean isReducePhase() {
        return (this.mr_state == State.REDUCE);
    }
    
    public boolean isFinishPhase() {
        return (this.mr_state == State.FINISH);
    }
    
    public void setMapPhase() {
        assert (this.mr_state == null);
        this.mr_state = State.MAP;
    }

    public void setShufflePhase() {
        assert(this.isMapPhase());
        this.mr_state = State.SHUFFLE;
    }
    
    public void setReducePhase() {
        assert(this.isShufflePhase());
        this.mr_state = State.REDUCE;
    }
    
    public void setFinishPhase() {
        assert(this.isReducePhase());
        this.mr_state = State.FINISH;
    }
    /*
     * return the size of partitions that MapReduce Transaction will touch 
     */
    public int getSize() {
        return partitions_size;
    }    
    public Table getMapEmit() {
        return mapEmit;
    }
    /*
     * Return the ReduceOutput Table schema 
     */
    
    public Table getReduceEmit() {
        
        return reduceEmit;
    }

    public State getState() {
        return (this.mr_state);
    }
        
    public VoltTable[] getReduceOutput() {
        return this.reduceOutput;
    }
    
    public TransactionMapCallback getTransactionMapCallback() {
        return (this.map_callback);
    }

    public TransactionMapWrapperCallback getTransactionMapWrapperCallback() {
        assert(this.mapWrapper_callback.isInitialized());
        return (this.mapWrapper_callback);
    }
    
    public SendDataCallback getSendDataCallback() {
        return sendData_callback;
    }

    public TransactionReduceCallback getTransactionReduceCallback() {
        return (this.reduce_callback);
    }
    
    public TransactionReduceWrapperCallback getTransactionReduceWrapperCallback() {
        assert(this.reduceWrapper_callback.isInitialized());
        return (this.reduceWrapper_callback);
    }
    
    public void initTransactionMapWrapperCallback(RpcCallback<TransactionMapResponse> orig_callback) {
        if (debug.val) LOG.debug("Trying to intialize TransactionMapWrapperCallback for " + this);
        assert (this.mapWrapper_callback.isInitialized() == false);
        this.mapWrapper_callback.init(this, orig_callback);
    }
    
    public void initTransactionReduceWrapperCallback(RpcCallback<TransactionReduceResponse> orig_callback) {
        if (debug.val) LOG.debug("Trying to initialize TransactionReduceWrapperCallback for " + this);
        //assert (this.reduceWrapper_callback.isInitialized() == false);
        this.reduceWrapper_callback.init(this, orig_callback);
    }

    /**
     * Get the TransactionCleanupCallback for this txn.
     */
    public RemoteFinishCallback getCleanupCallback() {
        // TODO(xin): This should return null if this handle is located at
        //            the txn's basePartition HStoreSite
        if (this.hstore_site.isLocalPartition(base_partition)) return null;
        
        assert(this.cleanup_callback.isInitialized()) :
            String.format("Trying to grab the %s for %s before it has been initialized",
                          this.cleanup_callback.getClass().getSimpleName(), this);
        return (this.cleanup_callback);
    }

    @Override
    public String toStringImpl() {
        return String.format("%s-%s #%d/%d", this.getProcedure().getName(),
                                             (this.getState() == null ? "null" : this.getState().toString()),
                                             this.txn_id, this.base_partition);
    }


    @Override
    public void initRound(int partition, long undoToken) {
        throw new RuntimeException("initRound should not be invoked on " + this.getClass());
    }

    @Override
    public void startRound(int partition) {
        throw new RuntimeException("startRound should not be invoked on " + this.getClass());
    }

    @Override
    public void finishRound(int partition) {
        throw new RuntimeException("finishRound should not be invoked on " + this.getClass());
    }
    
    public VoltTable getMapOutputByPartition( int partition ) {
        if (debug.val) LOG.debug("Trying to getMapOutputByPartition: [ " + partition + " ]");
        return this.mapOutput[partition];
    }
    
    public VoltTable getReduceInputByPartition ( int partition ) {
        if (debug.val) LOG.debug("Trying to getReduceInputByPartition: [ " + partition + " ]");
        return this.reduceInput[partition];
        //return this.reduceInput[partition];
    }
    
    public VoltTable getReduceOutputByPartition ( int partition ) {
        if (debug.val) LOG.debug("Trying to getReduceOutputByPartition: [ " + partition + " ]");
        return this.reduceOutput[partition];
        //return this.reduceOutput[partition];
    }

    /**
     * Reset variables in sub transactions to allow for re-execution.
     */
	public void resetTransaction() {
		for (LocalTransaction local_txn : this.local_txns) {
			local_txn.resetControlCodeExecuted();
			local_txn.resetClientResponse();
		}
	}
    
}
