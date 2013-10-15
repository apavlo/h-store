package edu.brown.hstore.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Procedure;
import org.voltdb.exceptions.ServerFaultException;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.callbacks.SendDataCallback;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;

/**
 * Special helper thread for executing non-blocking operations in MapReduce transactions.
 * @author pavlo
 * @author xin
 */
public class MapReduceHelperThread extends AbstractProcessingRunnable<MapReduceTransaction> {
    private static final Logger LOG = Logger.getLogger(MapReduceHelperThread.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final PartitionEstimator p_estimator;

    public MapReduceHelperThread(HStoreSite hstore_site) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_MAPREDUCE,
              new LinkedBlockingDeque<MapReduceTransaction>(),
              false);
        this.p_estimator = hstore_site.getPartitionEstimator();
    }

    public void queue(MapReduceTransaction ts) {
        this.queue.offer(ts);
    }

    @Override
    protected void processingCallback(MapReduceTransaction ts) {
        // Take all of the Map output tables and perform the shuffle operation
        if (ts.isShufflePhase()) {
            this.shuffle(ts);
        }
        if (ts.isReducePhase() && hstore_conf.site.mr_reduce_blocking == false) {
            this.reduce(ts);
        }
    }
    
//    public void map(final MapReduceTransaction mr_ts) {
//        // Runtime
//
//        VoltProcedure volt_proc = this.executor.getVoltProcedure(mr_ts.getInvocation().getProcName());
//
//        if (hstore_site.getLocalPartitionIds().contains(mr_ts.getBasePartition()) && !mr_ts.isBasePartition_map_runed()) {
//            if (debug.val)
//                LOG.debug(String.format("TXN: %s $$$1 non-blocking map, partition:%d", mr_ts, volt_proc.getPartitionId()));
//            volt_proc.setPartitionId(mr_ts.getBasePartition());
//            if (debug.val)
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
//                if (debug.val)
//                    LOG.debug(String.format("TXN: %s $$$3 non-blocking map, partition called on:%d", mr_ts, partition));
//
//                if (partition != mr_ts.getBasePartition()) {
//                    LocalTransaction ts = mr_ts.getLocalTransaction(partition);
//                    if (debug.val)
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
    protected void shuffle(final MapReduceTransaction ts) {
        // create a table for each partition
        Map<Integer, VoltTable> partitionedTables = new HashMap<Integer, VoltTable>();
        for (Integer partition : hstore_site.getCatalogContext().getAllPartitionIds()) {
            partitionedTables.put(partition, CatalogUtil.getVoltTable(ts.getMapEmit()));
        } // FOR
        if (debug.val)
            LOG.debug(String.format("Created %d VoltTables for SHUFFLE phase of %s", partitionedTables.size(), ts));

        VoltTable table = null;
        int rp = -1;
        for (int partition : this.hstore_site.getLocalPartitionIds()) {

            table = ts.getMapOutputByPartition(partition);

            assert (table != null) : String.format("Missing MapOutput table for txn #%d", ts.getTransactionId());

            while (table.advanceRow()) {
                int rowPartition = -1;
                try {
                    rowPartition = p_estimator.getTableRowPartition(ts.getMapEmit(), table);
                } catch (Exception e) {
                    LOG.fatal("Failed to split input table into partitions", e);
                    throw new RuntimeException(e.getMessage());
                }
                if (trace.val)
                    LOG.trace(Arrays.toString(table.getRowArray()) + " => " + rowPartition);
                assert (rowPartition >= 0);
                // this adds the active row from table
                partitionedTables.get(rowPartition).add(table);
                rp = rowPartition;
            } // WHILE
            if (debug.val)
                LOG.debug(String.format("<SendTable to Dest Partition>:%d\n %s", rp, partitionedTables.get(rp)));

        } // FOR

        // The SendDataCallback should invoke the TransactionMapCallback to tell it that 
        // the SHUFFLE phase is complete and that we need to send a message back to the
        // transaction's base partition to let it know that the MAP phase is complete
        SendDataCallback sendData_callback = ts.getSendDataCallback();
        sendData_callback.init(ts, new RpcCallback<AbstractTransaction>() {
            @Override
            public void run(AbstractTransaction parameter) {
                ts.getTransactionMapWrapperCallback().runOrigCallback();
            }
        });

        this.hstore_site.getCoordinator().sendData(ts, partitionedTables, sendData_callback);
    }

    /**
     * Non-blocking REDUCE phase execution.
     * @param mr_ts
     */
    public void reduce(final MapReduceTransaction mr_ts) {
        // Runtime

        int basePartition = mr_ts.getBasePartition();
        Procedure catalog_proc = mr_ts.getProcedure();

        // XXX: Why do we need to have a distinction between the base partition and all 
        //      of the other partitions? 
        if (hstore_site.isLocalPartition(basePartition) && mr_ts.isBasePartitionReduceExec() == false) {
            if (debug.val)
                LOG.debug(String.format("TXN: %s $$$2 non-blocking reduce, partition:%d",
                          mr_ts, basePartition));
            
            VoltMapReduceProcedure<?> volt_proc = this.getVoltMapReduceProcedure(catalog_proc, basePartition);
            volt_proc.call(mr_ts, mr_ts.getProcedureParameters());
        }
        // Local partitions at this site that are not the base partition
        else {

            for (int partition : hstore_site.getLocalPartitionIds().values()) {
                if (debug.val)
                    LOG.debug(String.format("TXN: %s $$$3 non-blocking reduce, partition called on:%d",
                              mr_ts, partition));
                if (partition != basePartition) {
                    LocalTransaction ts = mr_ts.getLocalTransaction(partition);
                    if (debug.val)
                        LOG.debug(String.format("TXN: %s $$$4 non-blocking reduce, partition called on:%d",
                                  mr_ts, partition));
                    VoltMapReduceProcedure<?> volt_proc = this.getVoltMapReduceProcedure(catalog_proc, partition);
                    volt_proc.call(ts, ts.getProcedureParameters());
                }
            } // FOR
        }
    }
    
    /**
     * Returns the VoltMapReduceProcedure handle for the given Procedure that
     * is initialized for the specific partition.
     * @param catalog_proc
     * @param partition
     * @return
     */
    @SuppressWarnings("unchecked")
    protected VoltMapReduceProcedure<?> getVoltMapReduceProcedure(Procedure catalog_proc, int partition) {
        assert(catalog_proc.getMapreduce());
        assert(catalog_proc.getHasjava());
        assert(hstore_site.isLocalPartition(partition));
        
        PartitionExecutor executor = hstore_site.getPartitionExecutor(partition);
        VoltMapReduceProcedure<?> volt_proc = null;

        // TODO: We are creating a new instance every single time per partition.
        //       We can probably cache these...
        // Only try to load the Java class file for the SP if it has one
        Class<? extends VoltProcedure> p_class = null;
        final String className = catalog_proc.getClassname();
        try {
            p_class = (Class<? extends VoltMapReduceProcedure<?>>)Class.forName(className);
            volt_proc = (VoltMapReduceProcedure<?>)p_class.newInstance();
        } catch (Exception e) {
            throw new ServerFaultException("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
        }
        volt_proc.init(executor, catalog_proc, executor.getBackendTarget());
        return (volt_proc);
    }
}
