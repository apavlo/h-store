package edu.mit.hstore.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.ExecutionSitePostProcessor;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.SendDataCallback;
import edu.mit.hstore.callbacks.TransactionMapWrapperCallback;
import edu.mit.hstore.dtxn.AbstractTransaction;
import edu.mit.hstore.dtxn.MapReduceTransaction;
import edu.mit.hstore.interfaces.Shutdownable;

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
     * @see ExecutionSitePostProcessor
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
