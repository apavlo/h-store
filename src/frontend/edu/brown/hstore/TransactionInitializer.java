/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import com.google.protobuf.RpcCallback;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.AbstractEstimator;
import edu.brown.hstore.estimators.SEATSEstimator;
import edu.brown.hstore.estimators.TM1Estimator;
import edu.brown.hstore.estimators.TPCCEstimator;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
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
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;

/**
 * This class is responsible for figuring out everything about a txn before it 
 * starts running. It can figure out what partition to execute the txn's control
 * code (i.e., program logic) on. It can also figure out additional properties, such 
 * as what partitions the txn will need to access, whether it is read-only at a 
 * partition, and whether it is likely to abort.
 * <B>Note:</B> It is thread-safe so it can be used by all of the PartitionExecutors with locking 
 * @author pavlo
 */
public class TransactionInitializer {
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
    private final HStoreConf hstore_conf;
    private final Collection<Integer> all_partitions;
    private final PartitionEstimator p_estimator;
    private final AbstractEstimator fixed_estimator;
    private EstimationThresholds thresholds;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public TransactionInitializer(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        
        this.all_partitions = hstore_site.getAllPartitionIds();
        this.thresholds = hstore_site.getThresholds();
        this.p_estimator = hstore_site.getPartitionEstimator();
        
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

    // ----------------------------------------------------------------------------
    // TRANSACTION PROCESSING METHODS
    // ----------------------------------------------------------------------------

    /**
     * Calculate what partition the txn should be executed on.
     * The provided base_partition argument is the "suggestion" that
     * was embedded in the original StoredProcedureInvocation from the client
     * @param client_handle
     * @param catalog_proc
     * @param procParams
     * @param base_partition
     * @return
     */
    public int calculateBasePartition(long client_handle,
                                       Procedure catalog_proc,
                                       ParameterSet procParams,
                                       int base_partition) {
        
        // Simple sanity check to make sure that we're not being told a bad partition
        if (base_partition < 0 || base_partition >= hstore_site.local_partitions_arr.length) {
            base_partition = HStoreConstants.NULL_PARTITION_ID;
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
            // We'll set it to NULL_PARTITION_ID so that we'll pick a random one down below
            base_partition = HStoreConstants.NULL_PARTITION_ID;
        }
        // -------------------------------
        // PartitionEstimator
        // -------------------------------
        else if (hstore_conf.site.exec_force_localexecution == false) {
            if (d) LOG.debug(String.format("Using PartitionEstimator for %s request", catalog_proc.getName()));
            try {
                Integer p = this.p_estimator.getBasePartition(catalog_proc, procParams.toArray(), false);
                if (p != null) base_partition = p.intValue(); 
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        // If we don't have a partition to send this transaction to, then we will just pick
        // one our partitions at random. This can happen if we're forcing txns to execute locally
        // or if there are no input parameters <-- this should be in the paper!!!
        if (base_partition == HStoreConstants.NULL_PARTITION_ID) {
            if (t) LOG.trace(String.format("Selecting a random local partition to execute %s request [force_local=%s]",
                                           catalog_proc.getName(), hstore_conf.site.exec_force_localexecution));
            int idx = (int)(Math.abs(client_handle) % hstore_site.local_partitions_arr.length);
            base_partition = hstore_site.local_partitions_arr[idx].intValue();
        }
        
        return (base_partition);
    }
    
    
    /**
     * 
     * @param serializedRequest
     * @param client_handle
     * @param base_partition
     * @param catalog_proc
     * @param procParams
     * @param done
     * @return
     */
    public LocalTransaction initInvocation(ByteBuffer serializedRequest, 
                                     long client_handle,
                                     int base_partition,
                                     Procedure catalog_proc,
                                     ParameterSet procParams,
                                     RpcCallback<byte[]> done) {
        
        if (d) LOG.debug(String.format("Incoming %s transaction request " +
        		                       "[handle=%d, partition=%d]",
                                       catalog_proc.getName(), client_handle, base_partition));

        // -------------------------------
        // TRANSACTION STATE INITIALIZATION
        // -------------------------------
        
        // Grab a new LocalTransactionState object from the target base partition's
        // PartitionExecutor object pool. This will be the handle that is used all
        // throughout this txn's lifespan to keep track of what it does
        LocalTransaction ts = null;
        try {
            if (catalog_proc.getMapreduce()) {
                ts = hstore_site.getObjectPools()
                                .getMapReduceTransactionPool(base_partition)
                                .borrowObject();
            } else {
                ts = hstore_site.getObjectPools()
                                .getLocalTransactionPool(base_partition)
                                .borrowObject();
            }
        } catch (Throwable ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for " + catalog_proc.getName());
            throw new RuntimeException(ex);
        }
        
        // Initialize our LocalTransaction handle
        Long txn_id = hstore_site.getTransactionIdManager(base_partition)
                                 .getNextUniqueTransactionId();
        
        this.populateProperties(ts,
                                txn_id,
                                client_handle,
                                base_partition,
                                catalog_proc,
                                procParams,
                                done);

        // Check whether this guy has already been restarted before
        int restartCounter = StoredProcedureInvocation.getRestartCounter(serializedRequest);
        if (restartCounter > 0) {
            ts.setRestartCounter(restartCounter);
        }
        
        // Disable transaction profiling for sysprocs
        if (hstore_conf.site.txn_profiling && ts.isSysProc()) {
            ts.profiler.disableProfiling();
        }
        
        // FIXME if (hstore_conf.site.txn_profiling) ts.profiler.startTransaction(timestamp);

        return (ts);
    }
    

    /**
     * Initialize the execution properties for a new tansaction
     * @param ts
     * @param txn_id
     * @param client_handle
     * @param base_partition
     * @param catalog_proc
     * @param params
     * @param client_callback
     */
    protected void populateProperties(LocalTransaction ts,
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
