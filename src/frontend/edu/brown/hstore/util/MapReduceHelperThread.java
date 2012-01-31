package edu.brown.hstore.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.ReduceInputIterator;
import org.voltdb.utils.VoltTableUtil;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.hstore.callbacks.SendDataCallback;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.interfaces.Shutdownable;

public class MapReduceHelperThread implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(MapReduceHelperThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final LinkedBlockingDeque<MapReduceTransaction> queue = new LinkedBlockingDeque<MapReduceTransaction>();
    private final HStoreSite hstore_site;
    private final PartitionEstimator p_estimator;
    private Thread self = null;
    private boolean stop = false;
    
    public MapReduceHelperThread(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.p_estimator = hstore_site.getPartitionEstimator();
    }

    public void queue(MapReduceTransaction ts) {
        this.queue.offer(ts);
    }

    /**
     * @see PartitionExecutorPostProcessor
     */
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreSite.getThreadName(hstore_site, "MR"));
        if (hstore_site.getHStoreConf().site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        if (debug.get())
            LOG.debug("Starting transaction post-processing thread");

        MapReduceTransaction ts = null;
        while (this.self.isInterrupted() == false) {
            // Grab a MapReduceTransaction from the queue
            // Figure out what you need to do with it
            // (1) Take all of the Map output tables and perform the shuffle operation
            try {
                ts = this.queue.take();
            } catch (InterruptedException ex) {
                // Ignore!
                break;
            }
            assert (ts != null);

            if (ts.isShufflePhase()) {
                this.shuffle(ts);
            }
//            if (ts.isReducePhase()) {
//                this.reduce(ts);
//            }
            
        } // WHILE

    }

    protected void shuffle(final MapReduceTransaction ts) {
        /**
         * TODO(xin): Loop through all of the MAP output tables from the txn handle For each of those, iterate through
         * the table row-by-row and use the PartitionEstimator to determine what partition you need to send the row to.
         * 
         * @see LoadMultipartitionTable.createNonReplicatedPlan()
         * Partitions
         *      Then you will use HStoreCoordinator.sendData() to send the partitioned table data to each of the
         *      partitions.
         * 
         *      Once that is all done, clean things up and invoke the network-outbound callback stored in the
         *      TransactionMapWrapperCallback
         */

        // create a table for each partition
        Map<Integer, VoltTable> partitionedTables = new HashMap<Integer, VoltTable>();
        for (int partition : hstore_site.getAllPartitionIds()) {
            partitionedTables.put(partition, CatalogUtil.getVoltTable(ts.getMapEmit()));
        } // FOR
        if (debug.get()) LOG.debug(String.format("Created %d VoltTables for SHUFFLE phase of %s", partitionedTables.size(), ts));
        
        VoltTable table = null;
        int rp=-1;
        for (int partition : this.hstore_site.getAllPartitionIds()) {
            
            table = ts.getMapOutputByPartition(partition);
            
            assert (table != null) : String.format("Missing MapOutput table for txn #%d", ts.getTransactionId());

            while (table.advanceRow()) {
                VoltTableRow row = table.fetchRow(table.getActiveRowIndex());
                int rowPartition = -1;
                try {
                    rowPartition = p_estimator.getTableRowPartition(ts.getMapEmit(), row);
                } catch (Exception e) {
                    LOG.fatal("Failed to split input table into partitions", e);
                    throw new RuntimeException(e.getMessage());
                }
                if (trace.get()) LOG.trace(Arrays.toString(table.getRowArray()) + " => " + rowPartition);
                assert (rowPartition >= 0);
                // this adds the active row from table
                partitionedTables.get(rowPartition).add(row);
                rp = rowPartition;
            } // WHILE
            if (debug.get())
                LOG.debug(String.format("<SendTable to Dest Partition>:%d\n %s", rp, partitionedTables.get(rp)));
            
        } // FOR
        
        // TODO(xin): The SendDataCallback should invoke the TransactionMapCallback to tell it that 
        //            the SHUFFLE phase is complete and that we need to send a message back to the
        //            transaction's base partition to let it know that the MAP phase is complete
        SendDataCallback sendData_callback = ts.getSendDataCallback();
        sendData_callback.init(ts, new RpcCallback<AbstractTransaction>() {
            @Override
            public void run(AbstractTransaction parameter) {
                ts.getTransactionMapWrapperCallback().runOrigCallback();
            }
        });
        
        
        
        this.hstore_site.getCoordinator().sendData(ts, partitionedTables, sendData_callback);
    }
    
    public <K> void reduce (final MapReduceTransaction ts) {
        Map<Integer, VoltTable> partitionedTables = new HashMap<Integer, VoltTable>();
        for (int partition : hstore_site.getAllPartitionIds()) {
            partitionedTables.put(partition, CatalogUtil.getVoltTable(ts.getMapEmit()));
        }
        VoltTable reduceInput = null;
        VoltTable reduceOutput = null;
        
        //get a handler 
        //VoltMapReduceProcedure<K> mr_proc = ts.get;
        
        // Execute the reduce job for each part serially
        for (int partition : hstore_site.getAllPartitionIds()) {
            reduceInput = ts.getReduceInputByPartition(partition);
            assert (reduceInput != null);
            VoltTable sorted = VoltTableUtil.sort(reduceInput, Pair.of(0, SortDirectionType.ASC));
            
            reduceOutput = ts.getReduceOutputByPartition(partition);
            assert(reduceOutput != null);
            // Make a Hstore.PartitionResult
            ReduceInputIterator<K> rows = new ReduceInputIterator<K>(sorted);
            
            
            while (rows.hasNext()) {
                K key = rows.getKey();
                
                // how to get a handler to run reduce function defined by users
                //this.reduce(key, rows); 
            }
            
            
        } // End of for
        
        // finalResults to be sent to client
        VoltTable finalResults[];
        finalResults = new VoltTable[hstore_site.getAllPartitionIds().size()];
        
        ClientResponseImpl cresponse = new ClientResponseImpl(ts.getTransactionId(),
                ts.getClientHandle(), 
                ts.getBasePartition(), 
                Status.OK, 
                finalResults, 
                "");
        
        hstore_site.sendClientResponse(ts, cresponse);
        
        TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(Hstore.Status.OK);
        hstore_site.getCoordinator().transactionFinish(ts, Hstore.Status.OK, finish_callback);
        
        
    }
    // public void resultBackToClient(MapReduceTransaction ts) {
//        Map<Integer, VoltTable> partitionedTables = new HashMap<Integer, VoltTable>();
//        for (int partition : hstore_site.getAllPartitionIds()) {
//            partitionedTables.put(partition, CatalogUtil.getVoltTable(ts.getMapEmit()));
//        } // FOR
//        if (debug.get()) LOG.debug(String.format("Created %d VoltTables for SHUFFLE phase of %s", partitionedTables.size(), ts));
//        
//        int destPartition = ts.getBasePartition();
//        VoltTable table = null;
//        
//        for (int partition : this.hstore_site.getLocalPartitionIds()) {
//            table = ts.getReduceOutputByPartition(partition);
//            assert(table != null);
//            
//            while(table.advanceRow()) {
//                VoltTableRow row = table.fetchRow(table.getActiveRowIndex());
//                partitionedTables.get(destPartition).add(row);
//            }
//        }
//        
//        SendDataCallback sendData_callback = ts.getSendDataCallback();
//        ts.getTransactionReduceWrapperCallback().runOrigCallback();
//                
//        this.hstore_site.getCoordinator().sendData(ts, partitionedTables, sendData_callback);
//    }

    @Override
    public boolean isShuttingDown() {

        return (this.stop);
    }

    @Override
    public void prepareShutdown(boolean error) {
        this.queue.clear();

    }

    @Override
    public void shutdown() {

        if (debug.get())
            LOG.debug(String.format("MapReduce Transaction helper Thread should be shutdown now ..."));
        this.stop = true;
        if (this.self != null)
            this.self.interrupt();
    }

}
