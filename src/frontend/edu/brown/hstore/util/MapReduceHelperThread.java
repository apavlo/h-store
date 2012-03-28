package edu.brown.hstore.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.callbacks.SendDataCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.ExecutionState;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;

public class MapReduceHelperThread implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(MapReduceHelperThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final LinkedBlockingDeque<MapReduceTransaction> queue = new LinkedBlockingDeque<MapReduceTransaction>();
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final PartitionEstimator p_estimator;
    private Thread self = null;
    private boolean stop = false;
    private ExecutionState execState = null;

    private PartitionExecutor executor;

    public MapReduceHelperThread(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.p_estimator = hstore_site.getPartitionEstimator();
    }

    protected PartitionExecutor initPartitionExecutor() {
        PartitionExecutor executor = new PartitionExecutor(0, this.hstore_site.getDatabase().getCatalog(), BackendTarget.NATIVE_EE_JNI, this.p_estimator, null);
        executor.initHStoreSite(this.hstore_site);
        
        return (executor);
    }

    public void queue(MapReduceTransaction ts) {
        this.queue.offer(ts);
    }
    public PartitionExecutor getExecutor() {
        return executor;
    }

    /**
     * @see PartitionExecutorPostProcessor
     */
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(hstore_site, "MR"));
        if (hstore_conf.site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        if (!hstore_conf.site.mr_reduce_blocking) {
            // Initialization
            this.executor = this.initPartitionExecutor();
            execState = new ExecutionState(this.executor);
        }

        if (debug.get())
            LOG.debug("Starting transaction post-processing thread");

        MapReduceTransaction ts = null;
        while (this.self.isInterrupted() == false) {
            // Grab a MapReduceTransaction from the queue
            // Take all of the Map output tables and perform the shuffle
            // operation
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
            if (ts.isReducePhase() && !hstore_conf.site.mr_reduce_blocking) {
                this.reduce(ts);
            }

        } // WHILE

    }
    
//    public void map(final MapReduceTransaction mr_ts) {
//        // Runtime
//
//        VoltProcedure volt_proc = this.executor.getVoltProcedure(mr_ts.getInvocation().getProcName());
//
//        if (hstore_site.getLocalPartitionIds().contains(mr_ts.getBasePartition()) && !mr_ts.isBasePartition_map_runed()) {
//            if (debug.get())
//                LOG.debug(String.format("TXN: %s $$$1 non-blocking map, partition:%d", mr_ts, volt_proc.getPartitionId()));
//            volt_proc.setPartitionId(mr_ts.getBasePartition());
//            if (debug.get())
//                LOG.debug(String.format("TXN: %s $$$2 non-blocking map, partition:%d", mr_ts, volt_proc.getPartitionId()));
//            
//            assert(execState != null);
//            execState.clear();
//            mr_ts.setExecutionState(execState);
//            
//            volt_proc.call(mr_ts, mr_ts.getInitiateTaskMessage().getParameters());
//
//        } else {
//
//            for (int partition : hstore_site.getLocalPartitionIds()) {
//                if (debug.get())
//                    LOG.debug(String.format("TXN: %s $$$3 non-blocking map, partition called on:%d", mr_ts, partition));
//
//                if (partition != mr_ts.getBasePartition()) {
//                    LocalTransaction ts = mr_ts.getLocalTransaction(partition);
//                    if (debug.get())
//                        LOG.debug(String.format("TXN: %s $$$4 non-blocking map, partition called on:%d", mr_ts, partition));
//                    volt_proc.setPartitionId(partition);
//                    execState.clear();
//                    ts.setExecutionState(execState);
//                    volt_proc.call(ts, mr_ts.getInitiateTaskMessage().getParameters());
//                }
//            }
//        }
//
//    }

    protected void shuffle(final MapReduceTransaction ts) {
        /**
         * Loop through all of the MAP output tables from the txn handle For
         * each of those, iterate through the table row-by-row and use the
         * PartitionEstimator to determine what partition you need to send the
         * row to.
         * 
         * @see LoadMultipartitionTable.createNonReplicatedPlan() Partitions
         *      Then you will use HStoreCoordinator.sendData() to send the
         *      partitioned table data to each of the partitions. Once that is
         *      all done, clean things up and invoke the network-outbound
         *      callback stored in the TransactionMapWrapperCallback
         */

        // create a table for each partition
        Map<Integer, VoltTable> partitionedTables = new HashMap<Integer, VoltTable>();
        for (int partition : hstore_site.getAllPartitionIds()) {
            partitionedTables.put(partition, CatalogUtil.getVoltTable(ts.getMapEmit()));
        } // FOR
        if (debug.get())
            LOG.debug(String.format("Created %d VoltTables for SHUFFLE phase of %s", partitionedTables.size(), ts));

        VoltTable table = null;
        int rp = -1;
        for (int partition : this.hstore_site.getLocalPartitionIds()) {

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
                if (trace.get())
                    LOG.trace(Arrays.toString(table.getRowArray()) + " => " + rowPartition);
                assert (rowPartition >= 0);
                // this adds the active row from table
                partitionedTables.get(rowPartition).add(row);
                rp = rowPartition;
            } // WHILE
            if (debug.get())
                LOG.debug(String.format("<SendTable to Dest Partition>:%d\n %s", rp, partitionedTables.get(rp)));

        } // FOR

        // The SendDataCallback should invoke the TransactionMapCallback to tell
        // it that
        // the SHUFFLE phase is complete and that we need to send a message back
        // to the
        // transaction's base partition to let it know that the MAP phase is
        // complete
        SendDataCallback sendData_callback = ts.getSendDataCallback();
        sendData_callback.init(ts, new RpcCallback<AbstractTransaction>() {
            @Override
            public void run(AbstractTransaction parameter) {
                ts.getTransactionMapWrapperCallback().runOrigCallback();
            }
        });

        this.hstore_site.getHStoreCoordinator().sendData(ts, partitionedTables, sendData_callback);
    }

    public void reduce(final MapReduceTransaction mr_ts) {
        // Runtime

        VoltMapReduceProcedure<?> volt_proc = (VoltMapReduceProcedure<?>)this.executor.getVoltProcedure(mr_ts.getInvocation().getProcName());
        if (hstore_site.isLocalPartition(mr_ts.getBasePartition()) && !mr_ts.isBasePartition_reduce_runed()) {
            if (debug.get())
                LOG.debug(String.format("TXN: %s $$$1 non-blocking reduce, partition:%d", mr_ts, volt_proc.getPartitionId()));
            volt_proc.setPartitionId(mr_ts.getBasePartition());
            if (debug.get())
                LOG.debug(String.format("TXN: %s $$$2 non-blocking reduce, partition:%d", mr_ts, volt_proc.getPartitionId()));
            volt_proc.call(mr_ts, mr_ts.getInitiateTaskMessage().getParameters());

        } else {

            for (int partition : hstore_site.getLocalPartitionIds()) {
                if (debug.get())
                    LOG.debug(String.format("TXN: %s $$$3 non-blocking reduce, partition called on:%d", mr_ts, partition));

                if (partition != mr_ts.getBasePartition()) {
                    LocalTransaction ts = mr_ts.getLocalTransaction(partition);
                    if (debug.get())
                        LOG.debug(String.format("TXN: %s $$$4 non-blocking reduce, partition called on:%d", mr_ts, partition));
                    volt_proc.setPartitionId(partition);
                    volt_proc.call(ts, ts.getInitiateTaskMessage().getParameters());
                }
            }

        }

    }
    
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
