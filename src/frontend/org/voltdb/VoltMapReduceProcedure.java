package org.voltdb;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.ReduceInputIterator;
import org.voltdb.utils.VoltTableUtil;

import com.google.protobuf.ByteString;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.callbacks.TransactionReduceWrapperCallback;
import edu.mit.hstore.dtxn.MapReduceTransaction;

public abstract class VoltMapReduceProcedure<K> extends VoltProcedure {
    public static final Logger LOG = Logger.getLogger(VoltMapReduceProcedure.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private SQLStmt mapInputQuery;
    private SQLStmt reduceInputQuery;

    // Thread-local data
    private MapReduceTransaction mr_ts;
    private VoltTable map_output;
    
    private VoltTable reduce_input;
    private VoltTable reduce_output;
    
    // -----------------------------------------------------------------
    // MAP REDUCE API
    // -----------------------------------------------------------------

    /**
     * Returns the schema of the MapOutput table
     * @return
     */
    public abstract VoltTable.ColumnInfo[] getMapOutputSchema();    
    /**
     * Returns the schema of the ReduceOutput table
     * @return
     */
    public abstract VoltTable.ColumnInfo[] getReduceOutputSchema();
    
    /**
     * @param tuple
     */
    public abstract void map(VoltTableRow tuple);

    /**
     * @param r
     */
    public abstract void reduce(K key, Iterator<VoltTableRow> rows);
    
    // -----------------------------------------------------------------
    // INTERNAL METHODS
    // -----------------------------------------------------------------
    
    @Override
    public void globalInit(ExecutionSite site, Procedure catalogProc, BackendTarget eeType, HsqlBackend hsql,
            PartitionEstimator pEstimator) {
        super.globalInit(site, catalogProc, eeType, hsql, pEstimator);
        
        // Get the SQLStmt handles for the input queries
        this.mapInputQuery = this.getSQLStmt(catalogProc.getMapinputquery());
        assert (this.mapInputQuery != null) : "Missing MapInputQuery " + catalogProc.getMapinputquery();
        this.reduceInputQuery = this.getSQLStmt(catalogProc.getReduceinputquery());
    }
    
    /**
     * 
     * @return
     */
    public final VoltTable run(Object params[]) {
        assert (this.hstore_site != null) : "error in VoltMapReduceProcedure...for hstore_site..........";

        VoltTable result = null;

        // The MapReduceTransaction handle will have all the key information we need about this txn
        long txn_id = this.getTransactionId();
        this.mr_ts = this.hstore_site.getTransaction(txn_id);
        assert (mr_ts != null) : "Unexpected null MapReduceTransaction handle for " + this.m_localTxnState;


        // If this invocation is at the txn's base partition, then it is
        // responsible for sending out the coordination messages to the other partitions
        boolean is_local = (this.partitionId == mr_ts.getBasePartition());

        if (mr_ts.isMapPhase()) {
            
            this.map_output = mr_ts.getMapOutputByPartition(this.partitionId);
            assert(this.map_output != null);
            
            // If this is the base partition, then we'll send the out the MAP
            // initialization requests to all of the partitions
            if (is_local) {
                // Send out network messages to all other partitions to tell them to
                // execute the MAP phase of this job
                this.executor.hstore_coordinator.transactionMap(mr_ts, mr_ts.getTransactionMapCallback());
            }

            if (debug.get())
                LOG.debug("<VoltMapReduceProcedure.run> is executing ..<MAP>..\n");
            // XXX: Execute the map
            voltQueueSQL(mapInputQuery, params);
            VoltTable mapResult[] = voltExecuteSQLForceSinglePartition();
            assert (mapResult.length == 1);

            if (debug.get())
                LOG.debug(String.format("MAP: About to process %d records for %s on partition %d",
                          mapResult[0].getRowCount(), this.m_localTxnState, this.partitionId));

            if (debug.get())
                LOG.debug(String.format("<MapInputTable> Partition:%d\n %s", this.partitionId,mapResult[0]));

            while (mapResult[0].advanceRow()) {
                this.map(mapResult[0].getRow());
            } // WHILE
            
            if (debug.get())
                LOG.debug(String.format("MAP: %s generated %d results on partition %d",
                          this.m_localTxnState, this.map_output.getRowCount(), this.partitionId));
            if (debug.get())
                LOG.debug(String.format("<MapOutputTable> Partition:%d\n %s", this.partitionId,this.map_output));
            
            result = mr_ts.getMapOutputByPartition(this.partitionId);

            // Always invoke the TransactionMapWrapperCallback to let somebody know that
            // we finished the MAP phase at this partition
            TransactionMapWrapperCallback callback = mr_ts.getTransactionMapWrapperCallback();
            assert (callback != null) : "Unexpected null callback for " + mr_ts;
            assert (callback.isInitialized()) : "Unexpected uninitalized callback for " + mr_ts;
            callback.run(this.partitionId);
        }

        else if (mr_ts.isReducePhase()) {
            this.reduce_input = null; // 
            this.reduce_input = mr_ts.getReduceInputByPartition(this.partitionId);
            assert(this.reduce_input != null);
            if(debug.get()) 
                LOG.debug("__FILE__:__LINE__ " + String.format("TXN: %s, [Stage] \n<VoltMapReduceProcedure.run> is executing <Reduce>..",mr_ts)); 
            if (debug.get())
                LOG.debug(String.format("<ReduceInputTable> Partition:%d\n %s", this.partitionId,this.reduce_input));
            
            // If this is the local/base partition, send out the start REDUCE message 
            if (is_local) {
                if (debug.get())
                    LOG.debug("<VoltMapReduceProcedure.run> is executing ..<Reduce>...local!!!....\n");
                // Send out network messages to all other partitions to tell them to execute the Reduce phase of this job
                this.executor.hstore_coordinator.transactionReduce(mr_ts, mr_ts.getTransactionReduceCallback());
            } 
            
            // Sort the the MAP_OUTPUT table
            // Build an "smart" iterator that loops through the MAP_OUTPUT table key-by-key
            VoltTable sorted = VoltTableUtil.sort(this.reduce_input, Pair.of(0, SortDirectionType.ASC));
            assert(sorted != null);
            if (debug.get())
                LOG.debug(String.format("<Sorted_ReduceInputTable> Partition:%d\n %s", this.partitionId,sorted));
            
            this.reduce_output = mr_ts.getReduceOutputByPartition(this.partitionId);
            assert(this.reduce_output != null);
  
            // Make a Hstore.PartitionResult
            ReduceInputIterator<K> rows = new ReduceInputIterator<K>(sorted);

            // Loop over that iterator and call runReduce
            if (debug.get())
                LOG.debug(String.format("REDUCE: About to process %d records for %s on partition %d",
                          sorted.getRowCount(), this.m_localTxnState, this.partitionId));
            
            while (rows.hasNext()) {
                K key = rows.getKey();
                this.reduce(key, rows); 
            }
            
            if (debug.get())
                LOG.debug(String.format("<ReduceOutputTable> Partition:%d\n %s", this.partitionId,this.reduce_output));
            
            // Loop over that iterator and call runReduce
            if (debug.get())
                LOG.debug(String.format("REDUCE: %s generated %d results on partition %d",
                          this.m_localTxnState, this.reduce_output.getRowCount(), this.partitionId));
            ByteString reduceOutData = null;
            try {
                ByteBuffer b = ByteBuffer.wrap(FastSerializer.serialize(reduce_output));
                reduceOutData = ByteString.copyFrom(b.array()); 
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Unexpected error when serializing %s reduceOutput data for partition %d",
                                                         mr_ts, this.partitionId), ex);
            }
            Hstore.PartitionResult.Builder builder = Hstore.PartitionResult.newBuilder()
                                                        .setData(reduceOutData)
                                                        .setPartitionId(this.partitionId)
                                                        .setStatus(Status.OK);
           
            TransactionReduceWrapperCallback callback = mr_ts.getTransactionReduceWrapperCallback();
            assert (callback != null) : "Unexpected null TransactionReduceWrapperCallback for " + mr_ts;
            assert (callback.isInitialized()) : "Unexpected uninitalized TransactionReduceWrapperCallback for " + mr_ts;
            callback.run(builder.build());
        }
        return (result);
    }
    
    /**
     * 
     * @param key
     * @param row
     */
    public final void mapEmit(K key, Object row[]) {
        assert(key == row[0]);
        this.map_output.addRow(row);       
    }

    /**
     * 
     * @param row
     */
    public final void reduceEmit(Object row[]) {
        this.reduce_output.addRow(row);
    }
    
    @Override
    public void finish() {
        for (int i = 0; i < this.mr_ts.getSize(); i++) {
            this.mr_ts.getLocalTransaction(i).finish();
        } // FOR
        
    }
    

}

