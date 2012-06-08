package edu.brown.hstore.util;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.estimators.AbstractEstimator;
import edu.brown.hstore.estimators.SEATSEstimator;
import edu.brown.hstore.estimators.TM1Estimator;
import edu.brown.hstore.estimators.TPCCEstimator;
import edu.brown.hstore.estimators.TransactionInitializer;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.StringUtil;

public class TransactionDispatcher implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(TransactionInitializer.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private static boolean d;
    private static boolean t;
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
        d = debug.get();
        t = trace.get();
    }
    
    // ----------------------------------------------------------------------------
    // INSTANCE MEMBERS
    // ----------------------------------------------------------------------------

    private final HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private final LinkedBlockingQueue<Pair<byte[], RpcCallback<byte[]>>> queue;
    private Shutdownable.ShutdownState state = null;
    
    private final Collection<Integer> all_partitions;
    private final Integer local_partitions_arr[];
    private EstimationThresholds thresholds;
    
    /**
     * Fixed Markov Estimator
     */
    private final AbstractEstimator fixed_estimator;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public TransactionDispatcher(HStoreSite hstore_site,
                                  LinkedBlockingQueue<Pair<byte[],
                                  RpcCallback<byte[]>>> queue) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.queue = queue;
        
        this.all_partitions = hstore_site.getAllPartitionIds();
        this.local_partitions_arr = hstore_site.getLocalPartitionIdArray();
        this.thresholds = hstore_site.getThresholds();
        
        // HACK
        if (hstore_conf.site.exec_neworder_cheat) {
            Database catalog_db = hstore_site.getDatabase();
            if (catalog_db.getProcedures().containsKey("neworder")) {
                this.fixed_estimator = new TPCCEstimator(this.hstore_site);
            } else if (catalog_db.getProcedures().containsKey("UpdateLocation")) {
                this.fixed_estimator = new TM1Estimator(this.hstore_site);
            } else if (catalog_db.getProcedures().containsKey("FindOpenSeats")) {
                this.fixed_estimator = new SEATSEstimator(this.hstore_site);
            } else {
                this.fixed_estimator = null;
            }
        } else {
            this.fixed_estimator = null;
        }
    }
    
    @Override
    public void run() {
        Thread self = Thread.currentThread();
        self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_DISPATCHER));
        if (hstore_conf.site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        
        Pair<byte[], RpcCallback<byte[]>> pair = null;
        while (this.state != ShutdownState.SHUTDOWN) {
            try {
                pair = this.queue.take();
            } catch (InterruptedException ex) {
                break;
            }
            if (this.state == ShutdownState.PREPARE_SHUTDOWN) {
                // TODO: Send back rejection
            } else {
                this.hstore_site.procedureInvocation(pair.getFirst(), pair.getSecond());
            }
        } // WHILE

    }
    
    @Override
    public void prepareShutdown(boolean error) {
        this.state = ShutdownState.PREPARE_SHUTDOWN;
    }

    @Override
    public void shutdown() {
        this.state = ShutdownState.SHUTDOWN;
    }

    @Override
    public boolean isShuttingDown() {
        return (this.state == ShutdownState.PREPARE_SHUTDOWN);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION PROCESSING METHODS
    // ----------------------------------------------------------------------------

    protected int calculateBasePartition(long client_handle,
                                           int base_partition,
                                           Procedure catalog_proc,
                                           ParameterSet procParams) {
        // Simple sanity check to make sure that we're not being told a bad partition
        if (base_partition < 0 || base_partition >= this.local_partitions_arr.length) {
            base_partition = -1;
        }
        
        // -------------------------------
        // DB2-style Transaction Redirection
        // -------------------------------
        if (base_partition != -1 && hstore_conf.site.exec_db2_redirects) {
            if (d) LOG.debug(String.format("Using embedded base partition from %s request " +
            		                       "[basePartition=%d]",
                                           catalog_proc.getName(), base_partition));
        }
        // -------------------------------
        // System Procedure
        // -------------------------------
        else if (catalog_proc.getSystemproc()) {
            // If it's a sysproc, then it doesn't need to go to a specific partition
            // We'll set it to -1 so that we'll pick a random one down below
            base_partition = -1;
        }
        // -------------------------------
        // PartitionEstimator
        // -------------------------------
        else if (hstore_conf.site.exec_force_localexecution == false) {
            if (d) LOG.debug(String.format("Using PartitionEstimator for %s request", catalog_proc.getName()));
            try {
                Integer p = hstore_site.getPartitionEstimator()
                                       .getBasePartition(catalog_proc, procParams.toArray(), false);
                if (p != null) base_partition = p.intValue(); 
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // If we don't have a partition to send this transaction to, then we will just pick
        // one our partitions at random. This can happen if we're forcing txns to execute locally
        // or if there are no input parameters <-- this should be in the paper!!!
        if (base_partition == -1) {
            if (t) LOG.trace(String.format("Selecting a random local partition to execute %s request [force_local=%s]",
                                           catalog_proc.getName(), hstore_conf.site.exec_force_localexecution));
            int idx = (int)(Math.abs(client_handle) % this.local_partitions_arr.length);
            base_partition = this.local_partitions_arr[idx].intValue();
        }
        
        return (base_partition);
    }
    
    
    public void populateProperties(LocalTransaction ts,
                                    Long txn_id,
                                    long client_handle,
                                    int base_partition,
                                    Procedure catalog_proc,
                                    ParameterSet params,
                                    RpcCallback<byte[]> client_callback) {
        
        boolean predict_abortable = (hstore_conf.site.exec_no_undo_logging_all == false);
        boolean predict_readOnly = catalog_proc.getReadonly();
        Collection<Integer> predict_touchedPartitions = null;
        TransactionEstimator.State t_state = null; 
        Object args[] = null; // FIXME
        
        // -------------------------------
        // CALCULATE EXECUTION PROPERTIES
        // -------------------------------
        
        // Sysprocs can be either all partitions or single-partitioned
        if (ts.isSysProc()) {
            // TODO: It would be nice if the client could pass us a hint when loading the tables
            // It would be just for the loading, and not regular transactions
            if (catalog_proc.getSinglepartition()) {
                predict_touchedPartitions = this.hstore_site.getSingletonPartitionList(base_partition);
            } else {
                predict_touchedPartitions = this.all_partitions;
            }
        }
        // MapReduceTransactions always need all partitions
        else if (ts.isMapReduce()) {
            if (t) LOG.trace(String.format("New request is for MapReduce %s, so it has to be multi-partitioned [clientHandle=%d]",
                                           catalog_proc.getName(), ts.getClientHandle()));
            predict_touchedPartitions = this.all_partitions;
        }
        // Force all transactions to be single-partitioned
        else if (hstore_conf.site.exec_force_singlepartitioned) {
            if (t) LOG.trace(String.format("The \"Always Single-Partitioned\" flag is true. Marking new %s transaction as single-partitioned on partition %d [clientHandle=%d]",
                                           catalog_proc.getName(), base_partition, ts.getClientHandle()));
            predict_touchedPartitions = this.hstore_site.getSingletonPartitionList(base_partition);
        }
        // Use the @ProcInfo flags in the catalog
        else if (hstore_conf.site.exec_voltdb_procinfo) {
            if (t) LOG.trace(String.format("Using the catalog information to determine whether the %s transaction is single-partitioned [clientHandle=%d, singleP=%s]",
                                            catalog_proc.getName(), ts.getClientHandle(), catalog_proc.getSinglepartition()));
            if (catalog_proc.getSinglepartition()) {
                predict_touchedPartitions = this.hstore_site.getSingletonPartitionList(base_partition);
            } else {
                predict_touchedPartitions = this.all_partitions;
            }
        }
        // Assume we're executing TPC-C neworder. Manually examine the input parameters and figure
        // out what partitions it's going to need to touch
        else if (hstore_conf.site.exec_neworder_cheat) {
            if (t) LOG.trace(String.format("Using fixed transaction estimator [clientHandle=%d]", ts.getClientHandle()));
            if (this.fixed_estimator != null)
                predict_touchedPartitions = this.fixed_estimator.initializeTransaction(catalog_proc, args);
            if (predict_touchedPartitions == null)
                predict_touchedPartitions = this.hstore_site.getSingletonPartitionList(base_partition);
        }    
        // Otherwise, we'll try to estimate what the transaction will do (if we can)
        else {
            if (d) LOG.debug(String.format("Using TransactionEstimator to check whether new %s request is single-partitioned [clientHandle=%d]",
                                           catalog_proc.getName(), ts.getClientHandle()));
            
            // Grab the TransactionEstimator for the destination partition and figure out whether
            // this mofo is likely to be single-partition or not. Anything that we can't estimate
            // will just have to be multi-partitioned. This includes sysprocs
            TransactionEstimator t_estimator = this.hstore_site.getPartitionExecutor(base_partition).getTransactionEstimator();
            
            try {
                // HACK: Convert the array parameters to object arrays...
                ParameterMangler mangler = this.hstore_site.getParameterMangler(catalog_proc); 
                Object cast_args[] = mangler.convert(args);
                if (t) LOG.trace(String.format("Txn #%d Parameters:\n%s", txn_id, mangler.toString(cast_args)));
                
                if (hstore_conf.site.txn_profiling) ts.profiler.startInitEstimation();
                t_state = t_estimator.startTransaction(txn_id, base_partition, catalog_proc, cast_args);
                
                // If there is no TransactinEstimator.State, then there is nothing we can do
                // It has to be executed as multi-partitioned
                if (t_state == null) {
                    if (d) LOG.debug(String.format("No TransactionEstimator.State was returned for %s. Executing as multi-partitioned",
                                                   AbstractTransaction.formatTxnName(catalog_proc, txn_id))); 
                    predict_touchedPartitions = this.all_partitions;
                    
                // We have a TransactionEstimator.State, so let's see what it says...
                } else {
                    if (t) LOG.trace("\n" + StringUtil.box(t_state.toString()));
                    MarkovEstimate m_estimate = t_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a MarkovEstimate for some reason...
                    if (m_estimate == null) {
                        if (d) LOG.debug(String.format("No MarkovEstimate was found for %s. Executing as multi-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                        predict_touchedPartitions = this.all_partitions;
                        
                    // Invalid MarkovEstimate. Stick with defaults
                    } else if (m_estimate.isValid() == false) {
                        if (d) LOG.warn(String.format("Invalid MarkovEstimate for %s. Marking as not read-only and multi-partitioned.\n%s",
                                AbstractTransaction.formatTxnName(catalog_proc, txn_id), m_estimate));
                        predict_readOnly = catalog_proc.getReadonly();
                        predict_abortable = true;
                        predict_touchedPartitions = this.all_partitions;
                        
                    // Use MarkovEstimate to determine things
                    } else {
                        if (d) {
                            LOG.debug(String.format("Using MarkovEstimate for %s to determine if single-partitioned", AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                            LOG.debug(String.format("%s MarkovEstimate:\n%s", AbstractTransaction.formatTxnName(catalog_proc, txn_id), m_estimate));
                        }
                        predict_touchedPartitions = m_estimate.getTouchedPartitions(this.thresholds);
                        predict_readOnly = m_estimate.isReadOnlyAllPartitions(this.thresholds);
                        predict_abortable = (predict_touchedPartitions.size() == 1 || m_estimate.isAbortable(this.thresholds)); // || predict_readOnly == false
                        
                    }
                }
            } catch (Throwable ex) {
                if (t_state != null) {
                    MarkovGraph markov = t_state.getMarkovGraph();
                    GraphvizExport<MarkovVertex, MarkovEdge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(t_state.getInitialPath()));
                    gv.highlightPath(markov.getPath(t_state.getActualPath()), "blue");
                    LOG.warn("WROTE MARKOVGRAPH: " + gv.writeToTempFile(catalog_proc));
                }
                LOG.error(String.format("Failed calculate estimate for %s request", AbstractTransaction.formatTxnName(catalog_proc, txn_id)), ex);
                predict_touchedPartitions = this.all_partitions;
                predict_readOnly = false;
                predict_abortable = true;
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopInitEstimation();
            }
        }
        
        // -------------------------------
        // SET EXECUTION PROPERTIES
        // -------------------------------
        
        ts.init(txn_id,
                client_handle,
                base_partition,
                predict_touchedPartitions,
                predict_readOnly,
                predict_abortable,
                catalog_proc,
                params,
                client_callback);
        if (t_state != null) ts.setEstimatorState(t_state);
        if (d) {
            LOG.debug(String.format("Initializing %s on partition %d [clientHandle=%d, partitions=%s, readOnly=%s, abortable=%s]",
                      ts, base_partition,
                      client_handle,
                      ts.getPredictTouchedPartitions(),
                      ts.isPredictReadOnly(),
                      ts.isPredictAbortable()));
        }
    }

    

}
