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
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TransactionIdManager;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.TransactionProfiler;
import edu.brown.utils.EventObservable;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringBoxUtil;

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
    private final HStoreObjectPools objectPools;
    private final CatalogContext catalogContext;
    private final PartitionEstimator p_estimator;
    private final PartitionSet local_partitions;
    private final TransactionEstimator t_estimators[];
    private EstimationThresholds thresholds;
    
    /**
     * HACK: This is the internal map used to keep track of TxnId->TxnHandles
     * inside of the HStoreSite.
     */
    private final Map<Long, AbstractTransaction> inflight_txns;
    
    /**
     * This is fired whenever we create a new txn handle is grabbed from the
     * the object pools.
     * It is only used for debugging+testing 
     */
    protected final EventObservable<LocalTransaction> newTxnObservable = 
                    new EventObservable<LocalTransaction>(); 
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public TransactionInitializer(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.objectPools = hstore_site.getObjectPools();
        this.local_partitions = hstore_site.getLocalPartitionIds();
        this.catalogContext = hstore_site.getCatalogContext();
        this.inflight_txns = hstore_site.getInflightTxns();
        
        this.thresholds = hstore_site.getThresholds();
        this.p_estimator = hstore_site.getPartitionEstimator();
        this.t_estimators = new TransactionEstimator[catalogContext.numberOfPartitions];
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
        if (base_partition < 0 || base_partition >= this.local_partitions.size()) {
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
            // HACK: If they don't have enough parameters, we'll just throw them to 
            // a random local partition and then let VoltProcedure give them back the proper error
            if (procParams.size() < catalog_proc.getParameters().size()) {
                if (d) LOG.warn(String.format("Not enough parameters for %s. Not calculating base partition", catalog_proc.getName()));
            } else {
                if (d) LOG.debug(String.format("Using PartitionEstimator for %s request", catalog_proc.getName()));
                try {
                    base_partition = this.p_estimator.getBasePartition(catalog_proc, procParams.toArray(), false);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        // If we don't have a partition to send this transaction to, then we will just pick
        // one our partitions at random. This can happen if we're forcing txns to execute locally
        // or if there are no input parameters <-- this should be in the paper!!!
        if (base_partition == HStoreConstants.NULL_PARTITION_ID) {
            if (t) LOG.trace(String.format("Selecting a random local partition to execute %s request [force_local=%s]",
                                           catalog_proc.getName(), hstore_conf.site.exec_force_localexecution));
            int idx = (int)(Math.abs(client_handle) % this.local_partitions.size());
            base_partition = this.local_partitions.values()[idx];
        }
        
        return (base_partition);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION HANDLE CREATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Create a MapReduceTransaction handle. This should only be invoked on a remote site.
     * @param txn_id
     * @param invocation
     * @param base_partition
     * @return
     */
    public MapReduceTransaction createMapReduceTransaction(Long txn_id,
                                                           long initiateTime,
                                                           long client_handle,
                                                           int base_partition,
                                                           int procId,
                                                           ByteBuffer paramsBuffer) {
        Procedure catalog_proc = this.catalogContext.getProcedureById(procId);
        if (catalog_proc == null) {
            throw new RuntimeException("Unknown procedure id '" + procId + "'");
        }
        
        // Initialize the ParameterSet
        FastDeserializer incomingDeserializer = new FastDeserializer();
        ParameterSet procParams = new ParameterSet();
        try {
            incomingDeserializer.setBuffer(StoredProcedureInvocation.getParameterSet(paramsBuffer));
            procParams.readExternal(incomingDeserializer);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } 
        assert(procParams != null) :
            "The parameters object is null for new txn from client #" + client_handle;
        
        MapReduceTransaction ts = null;
        try {
            ts = objectPools.getMapReduceTransactionPool(base_partition).borrowObject();
            assert(ts.isInitialized() == false);
        } catch (Throwable ex) {
            LOG.fatal(String.format("Failed to instantiate new MapReduceTransaction state for %s txn #%s",
                                    catalog_proc.getName(), txn_id));
            throw new RuntimeException(ex);
        }
        // We should never already have a transaction handle for this txnId
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
        assert(dupe == null) : "Trying to create multiple transaction handles for " + dupe;

        ts.init(txn_id, initiateTime, client_handle, base_partition, catalog_proc, procParams);
        if (d) LOG.debug(String.format("Created new MapReduceTransaction state %s from remote partition %d",
                                       ts, base_partition));
        return (ts);
    }
    
    
    /**
     * Create a RemoteTransaction handle. This obviously only for a remote site.
     * @param txn_id
     * @param request
     * @return
     */
    public RemoteTransaction createRemoteTransaction(Long txn_id,
                                                     PartitionSet partitions,
                                                     int base_partition,
                                                     int proc_id) {
        RemoteTransaction ts = null;
        Procedure catalog_proc = this.catalogContext.getProcedureById(proc_id);
        try {
            // Remote Transaction
            // XXX ts = new RemoteTransaction(hstore_site);
            ts = objectPools.getRemoteTransactionPool(base_partition).borrowObject();
            ts.init(txn_id, base_partition, null, catalog_proc, partitions, true);
            if (d) LOG.debug(String.format("Creating new RemoteTransactionState %s from remote partition %d [partitions=%s, hashCode=%d]",
                             ts, base_partition, partitions, ts.hashCode()));
        } catch (Exception ex) {
            LOG.fatal("Failed to construct TransactionState for txn #" + txn_id, ex);
            throw new RuntimeException(ex);
        }
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
        assert(dupe == null) : "Trying to create multiple transaction handles for " + dupe;
        
        if (t) LOG.trace(String.format("Stored new transaction state for %s", ts));
        return (ts);
    }
    
    /**
     * Create and initialize a LocalTransaction from a serialized StoredProcedureInvocation
     * request sent in from the client.  
     * @param serializedRequest
     * @param client_handle
     * @param base_partition
     * @param catalog_proc
     * @param procParams
     * @param clientCallback
     * @return
     */
    public LocalTransaction createLocalTransaction(ByteBuffer serializedRequest,
                                                   long initiateTime,
                                                   long client_handle,
                                                   int base_partition,
                                                   Procedure catalog_proc,
                                                   ParameterSet procParams,
                                                   RpcCallback<ClientResponseImpl> clientCallback) {
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
                ts = this.objectPools.getMapReduceTransactionPool(base_partition)
                                .borrowObject();
            } else {
                // XXX ts = new LocalTransaction(hstore_site);
                ts = this.objectPools.getLocalTransactionPool(base_partition).borrowObject();
            }
        } catch (Throwable ex) {
            LOG.fatal("Failed to instantiate new LocalTransactionState for " + catalog_proc.getName());
            throw new RuntimeException(ex);
        }
        
        // Initialize our LocalTransaction handle
        this.populateProperties(ts,
                                initiateTime,
                                client_handle,
                                base_partition,
                                catalog_proc,
                                procParams,
                                clientCallback);
        // Check whether this guy has already been restarted before
        if (serializedRequest != null) {
            int restartCounter = StoredProcedureInvocation.getRestartCounter(serializedRequest);
            if (restartCounter > 0) {
                ts.setRestartCounter(restartCounter);
            }
        }
        
        if (hstore_conf.site.txn_profiling && ts.profiler != null) {
            // Disable transaction profiling for sysprocs
            if (ts.isSysProc()) {
                ts.profiler.disableProfiling();
            }
        }
        // Notify anybody that cares about this new txn
        this.newTxnObservable.notifyObservers(ts);
        
        assert(ts.isSysProc() == catalog_proc.getSystemproc()) :
            "Unexpected sysproc mismatch for " + ts;
        return (ts);
    }
    
    /**
     * Create a new LocalTransaction handle from a restart txn
     * @param orig_ts
     * @param base_partition
     * @param predict_touchedPartitions
     * @param predict_readOnly
     * @param predict_abortable
     * @return
     */
    public LocalTransaction createLocalTransaction(LocalTransaction orig_ts,
                                                   int base_partition,
                                                   PartitionSet predict_touchedPartitions,
                                                   boolean predict_readOnly,
                                                   boolean predict_abortable) {
        
        LocalTransaction new_ts = null;
        try {
            // XXX new_ts = new LocalTransaction(hstore_site);
            new_ts = objectPools.getLocalTransactionPool(base_partition).borrowObject();
        } catch (Exception ex) {
            LOG.fatal(String.format("Failed to instantiate new %s for mispredicted %s",
                      orig_ts.getClass().getSimpleName(), orig_ts));
            throw new RuntimeException(ex);
        }
        
        // Setup TransactionProfiler
        if (hstore_conf.site.txn_profiling) {
            if (new_ts.profiler == null) {
                new_ts.setProfiler(new TransactionProfiler());
            }
            new_ts.profiler.enableProfiling();
            
            // Since we're restarting the txn, we should probably include
            // the original profiler information the original txn.
//            if (orig_ts.profiler.isDisabled() == false) {
//                new_ts.profiler.copy(orig_ts.profiler);
//            } else {
                new_ts.profiler.startTransaction(ProfileMeasurement.getTime());
                new_ts.profiler.setSingledPartitioned(predict_touchedPartitions.size() == 1);
//            }
        } else if (new_ts.profiler != null) {
            new_ts.profiler.disableProfiling();
        }
        
        Long new_txn_id = this.registerTransaction(new_ts, base_partition);
        new_ts.init(new_txn_id,
                    orig_ts.getInitiateTime(),
                    orig_ts.getClientHandle(),
                    base_partition,
                    predict_touchedPartitions,
                    predict_readOnly,
                    predict_abortable,
                    orig_ts.getProcedure(),
                    orig_ts.getProcedureParameters(),
                    orig_ts.getClientCallback()
        );
        
        // Make sure that we remove the ParameterSet from the original LocalTransaction
        // so that they don't get returned back to the object pool when it is deleted
        orig_ts.removeProcedureParameters();
        
        // Increase the restart counter in the new transaction
        new_ts.setRestartCounter(orig_ts.getRestartCounter() + 1);
        
        // Notify anybody that cares about this new txn
        this.newTxnObservable.notifyObservers(new_ts);
        
        return (new_ts);
    }
                                
    
    /**
     * Register a new LocalTransaction handle with this HStoreSite
     * We will return a txnId that is guaranteed to be globally unique
     * @param ts
     * @param base_partition
     * @return
     */
    protected Long registerTransaction(LocalTransaction ts, int base_partition) {
        TransactionIdManager idManager = hstore_site.getTransactionIdManager(base_partition); 
        Long txn_id = idManager.getNextUniqueTransactionId();
        
        // For some odd reason we sometimes get duplicate transaction ids from the VoltDB id generator
        // So we'll just double check to make sure that it's unique, and if not, we'll just ask for a new one
        LocalTransaction dupe = (LocalTransaction)this.inflight_txns.put(txn_id, ts);
        if (dupe != null) {
            // HACK!
            this.inflight_txns.put(txn_id, dupe);
            Long new_txn_id = idManager.getNextUniqueTransactionId();
            if (new_txn_id.equals(txn_id)) {
                String msg = "Duplicate transaction id #" + txn_id;
                LOG.fatal("ORIG TRANSACTION:\n" + dupe);
                LOG.fatal("NEW TRANSACTION:\n" + ts);
                Exception error = new Exception(msg);
                this.hstore_site.getCoordinator().shutdownClusterBlocking(error);
            }
            LOG.warn(String.format("Had to fix duplicate txn ids: %d -> %d", txn_id, new_txn_id));
            txn_id = new_txn_id;
            this.inflight_txns.put(txn_id, ts);
        }
        
        return (txn_id);
    }

    /**
     * Initialize the execution properties for a new transaction.
     * This is the important part where we try to figure out:
     * <ol>
     *   <li> Where should we execute the transaction (base partition).
     *   <li> What partitions the transaction will touch.
     *   <li> Whether the transaction could abort.
     *   <li> Whether the transaction is read-only.
     * </ol>
     * @param ts
     * @param client_handle
     * @param base_partition
     * @param catalog_proc
     * @param params
     * @param client_callback
     */
    protected void populateProperties(LocalTransaction ts,
                                      long initiateTime,
                                      long client_handle,
                                      int base_partition,
                                      Procedure catalog_proc,
                                      ParameterSet params,
                                      RpcCallback<ClientResponseImpl> client_callback) {
        
        Long txn_id = this.registerTransaction(ts, base_partition);
        boolean predict_abortable = (hstore_conf.site.exec_no_undo_logging_all == false);
        boolean predict_readOnly = catalog_proc.getReadonly();
        PartitionSet predict_partitions = null;
        EstimatorState t_state = null; 
        
        // Setup TransactionProfiler
        if (hstore_conf.site.txn_profiling) {
            if (ts.profiler == null) {
                ts.setProfiler(new TransactionProfiler());
            }
            ts.profiler.enableProfiling();
            ts.profiler.startTransaction(ProfileMeasurement.getTime());
        } else if (ts.profiler != null) {
            ts.profiler.disableProfiling();
        }
        
        // -------------------------------
        // SYSTEM PROCEDURES
        // -------------------------------
        if (catalog_proc.getSystemproc()) {
            // Sysprocs can be either all partitions or single-partitioned
            // TODO: It would be nice if the client could pass us a hint when loading the tables
            // It would be just for the loading, and not regular transactions
            if (catalog_proc.getSinglepartition() && catalog_proc.getEverysite() == false) {
                predict_partitions = catalogContext.getPartitionSetSingleton(base_partition);
            } else {
                predict_partitions = catalogContext.getAllPartitionIds();
            }
        }
        // -------------------------------
        // MAPREDUCE TRANSACTIONS
        // -------------------------------
        else if (catalog_proc.getMapreduce()) {
            // MapReduceTransactions always need all partitions
            if (d) LOG.debug(String.format("New request is for MapReduce %s, so it has to be multi-partitioned [clientHandle=%d]",
                                           catalog_proc.getName(), ts.getClientHandle()));
            predict_partitions = catalogContext.getAllPartitionIds();
        }
        // -------------------------------
        // VOLTDB @PROCINFO
        // -------------------------------
        else if (hstore_conf.site.exec_voltdb_procinfo) {
            if (d) LOG.debug(String.format("Using the catalog information to determine whether the %s transaction is single-partitioned [clientHandle=%d, singleP=%s]",
                                            catalog_proc.getName(), ts.getClientHandle(), catalog_proc.getSinglepartition()));
            if (catalog_proc.getSinglepartition()) {
                predict_partitions = catalogContext.getPartitionSetSingleton(base_partition);
            } else {
                predict_partitions = catalogContext.getAllPartitionIds();
            }
        }
        // -------------------------------
        // FORCE DISTRIBUTED
        // -------------------------------
        else if (hstore_conf.site.exec_force_allpartitions) {
            predict_partitions = catalogContext.getAllPartitionIds();
        }
        // -------------------------------
        // TRANSACTION ESTIMATORS
        // -------------------------------
        else if (hstore_conf.site.markov_enable || hstore_conf.site.markov_fixed) {
            if (d) LOG.debug(String.format("%s - Using TransactionEstimator to check whether txn is single-partitioned " +
            		         "[clientHandle=%d]",
            		         AbstractTransaction.formatTxnName(catalog_proc, txn_id), ts.getClientHandle()));
            
            // Grab the TransactionEstimator for the destination partition and figure out whether
            // this mofo is likely to be single-partition or not. Anything that we can't estimate
            // will just have to be multi-partitioned. This includes sysprocs
            TransactionEstimator t_estimator = this.t_estimators[base_partition];
            if (t_estimator == null) {
                t_estimator = this.hstore_site.getPartitionExecutor(base_partition).getTransactionEstimator();
                this.t_estimators[base_partition] = t_estimator;
            }
            
            try {
                if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startInitEstimation();
                if (t_estimator != null) {
                    t_state = t_estimator.startTransaction(txn_id, base_partition, catalog_proc, params.toArray());
                }
                
                // If there is no TransactinEstimator.State, then there is nothing we can do
                // It has to be executed as multi-partitioned
                if (t_state == null) {
                    if (d) LOG.debug(String.format("%s - No EstimationState was returned. Using default estimate.",
                                     AbstractTransaction.formatTxnName(catalog_proc, txn_id))); 
                }
                // We have a TransactionEstimator, so let's see what it says...
                else {
                    if (t) LOG.trace("\n" + StringBoxUtil.box(t_state.toString()));
                    Estimate t_estimate = t_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a Estimation for some reason...
                    if (t_estimate == null) {
                        if (d) LOG.debug(String.format("%s - No Estimation was recieved. Using default estimate.",
                                         AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                    }
                    // Invalid Estimation. Stick with defaults
                    else if (t_estimate.isValid() == false) {
                        if (d) LOG.debug(String.format("%s - Estimation is invalid. Using default estimate.\n%s",
                                         AbstractTransaction.formatTxnName(catalog_proc, txn_id), t_estimate));
                    }    
                    // Use Estimation to determine things
                    else {
                        if (d) {
                            LOG.debug(String.format("%s - Using Estimation to determine if txn is single-partitioned",
                                      AbstractTransaction.formatTxnName(catalog_proc, txn_id)));
                            LOG.trace(String.format("%s %s:\n%s",
                                      AbstractTransaction.formatTxnName(catalog_proc, txn_id),
                                      t_estimate.getClass().getSimpleName(), t_estimate));
                        }
                        predict_partitions = t_estimate.getTouchedPartitions(this.thresholds);
                        predict_readOnly = t_estimate.isReadOnlyAllPartitions(this.thresholds);
                        predict_abortable = (predict_partitions.size() == 1 || t_estimate.isAbortable(this.thresholds)); // || predict_readOnly == false
                        
                        if (predict_partitions.size() == 1) {
                            if (hstore_conf.site.markov_singlep_updates == false) t_state.disableUpdates();
                        }
                        else if (hstore_conf.site.markov_dtxn_updates == false) {
                            t_state.disableUpdates();
                        }
                        
                        if (d && predict_partitions.isEmpty()) {
                            LOG.warn(String.format("%s - Unexpected empty predicted PartitonSet from %s\n%s",
                            		AbstractTransaction.formatTxnName(catalog_proc, txn_id),
                            		t_estimator, t_estimate));
                        }
                    }
                }
            } catch (Throwable ex) {
                if (t_state != null && t_state instanceof MarkovEstimatorState) {
                    LOG.warn("WROTE MARKOVGRAPH: " + ((MarkovEstimatorState)t_state).dumpMarkovGraph());
                }
                LOG.error(String.format("Failed calculate estimate for %s request\nParameters: %s",
                                        AbstractTransaction.formatTxnName(catalog_proc, txn_id),
                                        params), ex);
                ex.printStackTrace();
                predict_partitions = catalogContext.getAllPartitionIds();
                predict_readOnly = false;
                predict_abortable = true;
            } finally {
                if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.stopInitEstimation();
            }
        }
        
        if (predict_partitions == null || predict_partitions.isEmpty()) {
            // -------------------------------
            // FORCE SINGLE-PARTITIONED
            // -------------------------------
            if (hstore_conf.site.exec_force_singlepartitioned) {
                if (d) LOG.debug(String.format("The \"Always Single-Partitioned\" flag is true. Marking new %s transaction as single-partitioned on partition %d [clientHandle=%d]",
                                               catalog_proc.getName(), base_partition, ts.getClientHandle()));
                predict_partitions = catalogContext.getPartitionSetSingleton(base_partition);
            }
            // -------------------------------
            // FORCE MULTI-PARTITIONED
            // -------------------------------
            else {
                predict_partitions = catalogContext.getAllPartitionIds();
            }
        }
        assert(predict_partitions != null);
        assert(predict_partitions.isEmpty() == false);
        
        // -------------------------------
        // SET EXECUTION PROPERTIES
        // -------------------------------
        
        ts.init(txn_id,
                initiateTime,
                client_handle,
                base_partition,
                predict_partitions,
                predict_readOnly,
                predict_abortable,
                catalog_proc,
                params,
                client_callback);
        if (t_state != null) ts.setEstimatorState(t_state);
        if (hstore_conf.site.txn_profiling && ts.profiler != null) 
            ts.profiler.setSingledPartitioned(ts.isPredictSinglePartition());
        
        if (d) {
            LOG.debug(String.format("Initializing %s on partition %d " +
            		                "[clientHandle=%d, partitions=%s, singlePartitioned=%s, readOnly=%s, abortable=%s]",
                      ts, base_partition,
                      client_handle,
                      ts.getPredictTouchedPartitions(),
                      ts.isPredictSinglePartition(),
                      ts.isPredictReadOnly(),
                      ts.isPredictAbortable()));
        }
    }

    

}
