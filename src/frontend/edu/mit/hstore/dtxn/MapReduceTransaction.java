package edu.mit.hstore.dtxn;

import java.util.Collection;
import java.util.Collections;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import sun.tools.tree.ThisExpression;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.SendDataCallback;
import edu.mit.hstore.callbacks.SendDataWrapperCallback;
import edu.mit.hstore.callbacks.TransactionMapCallback;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.callbacks.TransactionReduceCallback;
import edu.mit.hstore.callbacks.TransactionReduceWrapperCallback;

/**
 * Special transaction state object for MapReduce jobs
 * 
 * @author pavlo
 */
public class MapReduceTransaction extends LocalTransaction {
    private static final Logger LOG = Logger.getLogger(MapReduceTransaction.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final LocalTransaction local_txns[];
    public int size;
    
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
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------

    
    /**
     */
    private final TransactionMapCallback map_callback;

    private final TransactionMapWrapperCallback mapWrapper_callback;
    
    private final SendDataCallback sendData_callback;
    
    //private final SendDataWrapperCallback sendDataWrapper_callback;
    
    private final TransactionReduceCallback reduce_callback;
    
    private final TransactionReduceWrapperCallback reduceWrapper_callback;
    
    
    /**
     * Constructor 
     * @param hstore_site
     */
    public MapReduceTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        // new local_txns
        this.size = this.hstore_site.getAllPartitionIds().size();
        this.local_txns = new LocalTransaction[this.size];
        for (int i = 0; i < this.size; i++) {
            this.local_txns[i] = new LocalTransaction(hstore_site) {
                @Override
                public String toString() {
                    if (this.isInitialized()) {
                        return MapReduceTransaction.this.toString() + "/" + this.base_partition;
                    } else {
                        return ("<Uninitialized>");
                    }
                }
            };
        } // FOR
        
        // new mapout and reduce output talbes for each partition it wants to touch
        this.mapOutput = new VoltTable[this.size];
        this.reduceInput = new VoltTable[this.size];
        this.reduceOutput = new VoltTable[this.size];
                
        this.map_callback = new TransactionMapCallback(hstore_site);
        this.mapWrapper_callback = new TransactionMapWrapperCallback(hstore_site);
        
        this.sendData_callback = new SendDataCallback(hstore_site);
        //this.sendDataWrapper_callback = new SendDataWrapperCallback(hstore_site);
        
        this.reduce_callback = new TransactionReduceCallback(hstore_site);
        this.reduceWrapper_callback = new TransactionReduceWrapperCallback(hstore_site);
    }
    
    
    @Override
    public MapReduceTransaction init(long txnId, long clientHandle, int base_partition,
                                     Collection<Integer> predict_touchedPartitions, boolean predict_readOnly, boolean predict_canAbort,
                                     TransactionEstimator.State estimator_state, Procedure catalog_proc, StoredProcedureInvocation invocation, RpcCallback<byte[]> client_callback) {
        assert (invocation != null) : "invalid StoredProcedureInvocation parameter for MapReduceTransaction.init()";
        assert (catalog_proc != null) : "invalid Procedure parameter for MapReduceTransaction.init()";
        
        super.init(txnId, clientHandle, base_partition,
                   predict_touchedPartitions, predict_readOnly, predict_canAbort,
                   estimator_state, catalog_proc, invocation, client_callback);
        
        Database catalog_db = CatalogUtil.getDatabase(this.catalog_proc);
        this.mapEmit = catalog_db.getTables().get(this.catalog_proc.getMapemittable());
        this.reduceEmit = catalog_db.getTables().get(this.catalog_proc.getReduceemittable());
        LOG.info(" CatalogUtil.getVoltTable(thisMapEmit): -> " + this.catalog_proc.getMapemittable());
        
        // Get the Table catalog object for the map/reduce outputs
        // For each partition there should be a map/reduce output voltTable
        for (int partition : this.hstore_site.getAllPartitionIds()) {
            //int offset = hstore_site.getLocalPartitionOffset(partition);
            int offset = partition;
            if (trace.get()) LOG.trace(String.format("Partition[%d] -> Offset[%d]", partition, offset));
            this.local_txns[offset].init(this.txn_id, this.client_handle, partition,
                                         Collections.singleton(partition),
                                         this.predict_readOnly, this.predict_abortable,
                                         null, catalog_proc, invocation, null);
            
            // init map/reduce Output for each partition
            assert(this.mapEmit != null): "mapEmit has not been initialized\n ";
            assert(this.reduceEmit != null): "reduceEmit has not been initialized\n ";
            this.mapOutput[offset] = CatalogUtil.getVoltTable(this.mapEmit);
            this.reduceInput[offset] = CatalogUtil.getVoltTable(this.mapEmit);
            this.reduceOutput[offset] = CatalogUtil.getVoltTable(this.reduceEmit);
            
        } // FOR
        
        this.setMapPhase();
        this.map_callback.init(this);
        assert(this.map_callback.isInitialized()) : "Unexpected error for " + this;
        this.reduce_callback.init(this);
        assert(this.reduce_callback.isInitialized()) : "Unexpected error for " + this;
        
        LOG.info("Invoked MapReduceTransaction.init() -> " + this);
        return (this);
    }

    public MapReduceTransaction init(long txnId, int base_partition, Procedure catalog_proc, StoredProcedureInvocation invocation) {
        this.init(txnId, invocation.getClientHandle(), base_partition, hstore_site.getAllPartitionIds(), false, true, null, catalog_proc, invocation, null);
        LOG.info("Invoked MapReduceTransaction.init() -> " + this);
        assert(this.map_callback.isInitialized()) : "Unexpected error for " + this;
        //assert(this.sendData_callback.isInitialized()) : "Unexpected error for " + this;
        return (this);
    }
    
    @Override
    public void finish() {
        //super.finish();
        for (int i = 0; i < this.size; i++) {
            this.local_txns[i].finish();
        } // FOR
        this.mr_state = null;
        
        this.map_callback.finish();
        this.mapWrapper_callback.finish();
        this.sendData_callback.finish();
        this.reduce_callback.finish();
        this.reduceWrapper_callback.finish();
        
        this.mapEmit = null;
        this.reduceEmit = null;
        this.mapOutput = null;
        this.reduceInput = null;
        this.reduceOutput = null;
    }
    /*
     * Store Data from MapOutput table into reduceInput table
     * ReduceInput table is the result of all incoming mapOutput table from other partitions
     * @see edu.mit.hstore.dtxn.AbstractTransaction#storeData(int, org.voltdb.VoltTable)
     */
    @Override
    public synchronized Hstore.Status storeData(int partition, VoltTable vt) {
        VoltTable input = this.getReduceInputByPartition(partition);
        
        assert(input != null);
        if (debug.get())
            LOG.debug(String.format("StoreData into Partition #%d: RowCount=%d ",
                    partition, vt.getRowCount()));
        
        if (debug.get())
            LOG.debug(String.format("<StoreData, change to ReduceInputTable> to Partition:%d>\n %s",partition,vt));
        while (vt.advanceRow()) {
            VoltTableRow row = vt.fetchRow(vt.getActiveRowIndex());
            assert(row != null);
            input.add(row);
        }
        vt.resetRowPosition();
        
        return Hstore.Status.OK;
    }
    
    /**
     * Get a LocalTransaction handle for a local partition
     * 
     * @param partition
     * @return
     */
    public LocalTransaction getLocalTransaction(int partition) {
        //int offset = hstore_site.getLocalPartitionOffset(partition);
        int offset = partition;
        return (this.local_txns[offset]);
    }
    
    // ----------------------------------------------------------------------------
    // ACCESS METHODS
    // ----------------------------------------------------------------------------
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
        
        for (int i = 0; i < this.size; i++) {
            this.local_txns[i].resetExecutionState();
        }
        
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
        return size;
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
    
    public StoredProcedureInvocation getInvocation() {
        return this.invocation;
    }

    public String getProcedureName() {
        return (this.catalog_proc != null ? this.catalog_proc.getName() : null);
    }

    public Collection<Integer> getPredictTouchedPartitions() {
        return (this.hstore_site.getAllPartitionIds());
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
    
    public void initTransactionMapWrapperCallback(RpcCallback<Hstore.TransactionMapResponse> orig_callback) {
        if (debug.get()) LOG.debug("Trying to intialize TransactionMapWrapperCallback for " + this);
        assert (this.mapWrapper_callback.isInitialized() == false);
        this.mapWrapper_callback.init(this, orig_callback);
    }
    
    public void initTransactionReduceWrapperCallback(RpcCallback<Hstore.TransactionReduceResponse> orig_callback) {
        if (debug.get()) LOG.debug("Trying to initialize TransactionReduceWrapperCallback for " + this);
        assert (this.reduceWrapper_callback.isInitialized() == false);
        this.reduceWrapper_callback.init(this, orig_callback);
    }
    
    

    @Override
    public String toString() {
        if (this.isInitialized()) {
            
            return String.format("%s-%s #%d/%d", this.getProcedureName(), (this.getState().toString()), this.txn_id, this.base_partition);
        } else {
            return ("<Uninitialized>");
        }
    }

    @Override
    public String debug() {
        return (StringUtil.formatMaps(this.getDebugMap()));
    }
    

    @Override
    public void initRound(int partition, long undoToken) {
        assert (false) : "initRound should not be invoked on " + this.getClass();
    }

    @Override
    public void startRound(int partition) {
        assert (false) : "startRound should not be invoked on " + this.getClass();
    }

    @Override
    public void finishRound(int partition) {
        assert (false) : "finishRound should not be invoked on " + this.getClass();
    }
    
    public VoltTable getMapOutputByPartition( int partition ) {
        if (debug.get()) LOG.debug("Trying to getMapOutputByPartition: [ " + partition + " ]");
        //return this.mapOutput[hstore_site.getLocalPartitionOffset(partition)];
        return this.mapOutput[partition];
    }
    
    public VoltTable getReduceInputByPartition ( int partition ) {
        if (debug.get()) LOG.debug("Trying to getReduceInputByPartition: [ " + partition + " ]");
        //return this.reduceInput[hstore_site.getLocalPartitionOffset(partition)];
        return this.reduceInput[partition];
    }
    
    public VoltTable getReduceOutputByPartition ( int partition ) {
        //return this.reduceOutput[hstore_site.getLocalPartitionOffset(partition)];
        return this.reduceOutput[partition];
    }
    
}
