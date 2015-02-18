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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

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
import edu.brown.hstore.txns.TransactionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.profilers.TransactionProfiler;
import edu.brown.utils.EventObservable;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringBoxUtil;
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
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // INSTANCE MEMBERS
    // ----------------------------------------------------------------------------

    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final CatalogContext catalogContext;
    private final PartitionEstimator p_estimator;
    private final PartitionSet local_partitions;
    private final TransactionEstimator t_estimators[];
    private final TransactionIdManager txnIdManagers[];
    private final Random rng = new Random();
    private EstimationThresholds thresholds;
    
    /**
     * HACK: This is the internal map used to keep track of TxnId->TxnHandles
     * inside of the HStoreSite.
     */
    private final Map<Long, AbstractTransaction> inflight_txns;
    
    /**
     * This is fired whenever we create a new txn handle is initialized.
     * It is only used for debugging+testing 
     */
    private EventObservable<LocalTransaction> newTxnObservable; 
    
    // Local Catalog Cache
    private final boolean isMapReduce[];
    private final boolean isSysProc[];
    private final boolean isReadOnly[];
    private final int expectedParams[];
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public TransactionInitializer(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.local_partitions = hstore_site.getLocalPartitionIds();
        this.catalogContext = hstore_site.getCatalogContext();
        this.inflight_txns = hstore_site.getInflightTxns();
        
        this.thresholds = hstore_site.getThresholds();
        this.p_estimator = hstore_site.getPartitionEstimator();
        this.t_estimators = new TransactionEstimator[catalogContext.numberOfPartitions];
        
        int num_procs = this.catalogContext.procedures.size() + 1;
        this.isMapReduce = new boolean[num_procs];
        this.isSysProc = new boolean[num_procs];
        this.isReadOnly = new boolean[num_procs];
        this.expectedParams = new int[num_procs];
        for (Procedure proc : this.catalogContext.procedures) {
            int id = proc.getId();
            this.isMapReduce[id] = proc.getMapreduce();
            this.isSysProc[id] = proc.getSystemproc();
            this.isReadOnly[id] = proc.getReadonly();
            this.expectedParams[id] = proc.getParameters().size();
        } // FOR
        
        this.txnIdManagers = new TransactionIdManager[this.catalogContext.numberOfPartitions];
        for (int partition : this.local_partitions.values()) {
            this.txnIdManagers[partition] = hstore_site.getTransactionIdManager(partition);
        } // FOR
    }
    
    public synchronized EventObservable<LocalTransaction> getNewTxnObservable() {
        if (this.newTxnObservable == null) {
            this.newTxnObservable = new EventObservable<LocalTransaction>();
        }
        return (this.newTxnObservable);
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
        final int procId = catalog_proc.getId();
        
        // Simple sanity check to make sure that we're not being told a bad partition
        if (base_partition < 0 || base_partition >= this.local_partitions.size()) {
            base_partition = HStoreConstants.NULL_PARTITION_ID;
        }
        
        // -------------------------------
        // DB2-style Transaction Redirection
        // -------------------------------
        if (base_partition != HStoreConstants.NULL_PARTITION_ID && hstore_conf.site.exec_db2_redirects) {
            if (debug.val)
                LOG.debug(String.format("Using embedded base partition from %s request " +
                                        "[basePartition=%d]",
                                        catalog_proc.getName(), base_partition));
        }
        // -------------------------------
        // System Procedure
        // -------------------------------
        else if (this.isSysProc[procId]) {
            if (catalog_proc.getSinglepartition() &&
                catalog_proc.getEverysite() == false &&
                catalog_proc.getPartitionparameter() >= 0) {
                if (debug.val)
                    LOG.debug(String.format("Using PartitionEstimator for %s request",
                              catalog_proc.getName()));
                try {
                    base_partition = this.p_estimator.getBasePartition(catalog_proc, procParams.toArray(), false);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
            else {
                // If it's a sysproc, then it doesn't need to go to a specific partition
                // We'll set it to NULL_PARTITION_ID so that we'll pick a random one down below
                if (debug.val)
                    LOG.debug(String.format("Using random local partition for %s request",
                              catalog_proc.getName()));
                base_partition = HStoreConstants.NULL_PARTITION_ID;
            }
        }
        // -------------------------------
        // PartitionEstimator
        // -------------------------------
        else if (hstore_conf.site.exec_force_localexecution == false) {
            // HACK: If they don't have enough parameters, we'll just throw them to 
            // a random local partition and then let VoltProcedure give them back the proper error
            if (procParams.size() < this.expectedParams[procId]) {
                if (debug.val)
                    LOG.warn(String.format("Not enough parameters for %s. Not calculating base partition",
                             catalog_proc.getName()));
            } else {
                if (debug.val)
                    LOG.debug(String.format("Using PartitionEstimator for %s request",
                              catalog_proc.getName()));
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
            if (trace.val)
                LOG.trace(String.format("Selecting a random local partition to execute %s request [force_local=%s]",
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
        final int procId = catalog_proc.getId();
        if (debug.val)
            LOG.debug(String.format("Incoming %s transaction request " +
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
            if (this.isMapReduce[procId]) {
                ts = new MapReduceTransaction(this.hstore_site);
            } else {
                ts = new LocalTransaction(this.hstore_site);
            }
            assert(ts.isInitialized() == false);
        } catch (Throwable ex) {
            String msg = "Failed to instantiate new local transaction handle for " + catalog_proc.getName();
            throw new RuntimeException(msg, ex);
        }
        
        // Initialize our LocalTransaction handle
        Long txn_id = this.registerTransaction(ts, base_partition);
        this.populateProperties(ts,
                                txn_id,
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
        
        // Notify anybody that cares about this new txn
        if (this.newTxnObservable != null) this.newTxnObservable.notifyObservers(ts);
        
        assert(ts.isSysProc() == this.isSysProc[procId]) :
            "Unexpected sysproc mismatch for " + ts;
        return (ts);
    }
    
    /**
     * Create a new LocalTransaction handle from a restarted txn
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
        
        LocalTransaction new_ts = new LocalTransaction(hstore_site);
        
        // Setup TransactionProfiler
        if (hstore_conf.site.txn_profiling) {
            if (this.setupTransactionProfiler(new_ts, orig_ts.isSysProc())) {
                // Since we're restarting the txn, we should probably include
                // the original profiler information the original txn.
                // new_ts.profiler.startTransaction(ProfileMeasurement.getTime());
                new_ts.profiler.setSingledPartitioned(predict_touchedPartitions.size() == 1); 
                if (trace.val) LOG.trace(new_ts + " => " + new_ts.profiler);
            }
        }
        else if (new_ts.profiler != null) {
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
        if (this.newTxnObservable != null) this.newTxnObservable.notifyObservers(new_ts);
        
        if (debug.val)
            LOG.debug(String.format("Restarted %s as %s [handle=%d, basePartition=%d]",
                      orig_ts, new_ts, orig_ts.getClientHandle(), base_partition));
        
        return (new_ts);
    }
    
    /**
     * Create a RemoteTransaction handle. This obviously only for a remote site.
     * @param txn_id
     * @param request
     * @return
     */
    public RemoteTransaction createRemoteTransaction(Long txn_id,
                                                     PartitionSet partitions,
                                                     ParameterSet procParams,
                                                     int base_partition,
                                                     int proc_id) {
        RemoteTransaction ts = null;
        Procedure catalog_proc = this.catalogContext.getProcedureById(proc_id);
        try {
            ts = new RemoteTransaction(this.hstore_site);
            assert(ts.isInitialized() == false);
            ts.init(txn_id, base_partition, procParams, catalog_proc, partitions, true);
            if (debug.val)
                LOG.debug(String.format("Creating new RemoteTransactionState %s from " +
                		  "remote partition %d [partitions=%s, hashCode=%d]",
                          ts, base_partition, partitions, ts.hashCode()));
        } catch (Throwable ex) {
            String msg = "Failed to instantiate new remote transaction handle for " + TransactionUtil.formatTxnName(catalog_proc, txn_id);
            throw new RuntimeException(msg, ex);
        }
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
        assert(dupe == null) : "Trying to create multiple transaction handles for " + dupe;
        
        if (trace.val)
            LOG.trace(String.format("Stored new transaction state for %s", ts));
        return (ts);
    }
    
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
        
        MapReduceTransaction ts = new MapReduceTransaction(hstore_site);
        
        // We should never already have a transaction handle for this txnId
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
        assert(dupe == null) : "Trying to create multiple transaction handles for " + dupe;

        ts.init(txn_id, initiateTime, client_handle, base_partition, catalog_proc, procParams);
        if (debug.val)
            LOG.debug(String.format("Created new MapReduceTransaction state %s from remote partition %d",
                      ts, base_partition));
        return (ts);
    }
   
    // ----------------------------------------------------------------------------
    // TRANSACTION HANDLE INITIALIZATION METHODS
    // These don't normally need to be invoked from outside of this class
    // ----------------------------------------------------------------------------
    
    /**
     * This method allows you to reset the txnId for an already initialized LocalTransaction handle.
     * This is primarily needed for the AntiCacheManager stuff
     * @param ts
     * @param base_partition
     * @return
     */
    protected Long resetTransactionId(AbstractTransaction ts, int base_partition) {
        Long oldTxnId = ts.getTransactionId();
        assert(oldTxnId != null);
        AbstractTransaction removed = this.inflight_txns.remove(oldTxnId);
        assert(ts == removed);
        
        Long newTxnId = this.registerTransaction(ts, base_partition);
        ts.setTransactionId(newTxnId);
        
        if (debug.val)
            LOG.debug(String.format("Changed txnId from %d to %d: %s", oldTxnId, newTxnId, ts)); 
        return (newTxnId);
    }
    
    /**
     * Register a new LocalTransaction handle with this HStoreSite
     * We will return a txnId that is guaranteed to be globally unique
     * @param ts
     * @param base_partition
     * @return
     */
    protected Long registerTransaction(AbstractTransaction ts, int base_partition) {
        TransactionIdManager idManager = this.txnIdManagers[base_partition]; 
        Long txn_id = idManager.getNextUniqueTransactionId();
        
        // For some odd reason we sometimes get duplicate transaction ids from the VoltDB id generator
        // So we'll just double check to make sure that it's unique, and if not, we'll just ask for a new one
        AbstractTransaction dupe = this.inflight_txns.put(txn_id, ts);
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
     * Register a new LocalTransaction handle with this HStoreSite with a new id
     * @param ts
     * @param oldTxnId
     * @param newTxnId
     * @return
     */
    protected void registerTransactionRestartWithId(AbstractTransaction ts, Long oldTxnId, Long newTxnId) {
    	AbstractTransaction removed = this.inflight_txns.remove(oldTxnId);
        assert(ts == removed);
        ts.setTransactionId(newTxnId);
        
        this.inflight_txns.put(newTxnId, ts);   
        
        if (debug.val)
            LOG.debug(String.format("Changed txnId from %d to %d: %s", oldTxnId, newTxnId, ts));
             
    }

    /**
     * Initialize the TransactionProfiler for the given txn handle.
     * Returns true if profiling is enabled for this txn.
     * @param ts
     * @param sysproc
     * @return
     */
    private boolean setupTransactionProfiler(LocalTransaction ts, boolean sysproc) {
        if (hstore_conf.site.txn_profiling && sysproc == false &&
            (hstore_conf.site.txn_profiling_sample >= 1 || this.rng.nextDouble() < hstore_conf.site.txn_profiling_sample)) {
            if (ts.profiler == null) {
                ts.setProfiler(new TransactionProfiler());
            }
            ts.profiler.enableProfiling();
            ts.profiler.startTransaction(ProfileMeasurement.getTime());
            
            return (true);
        } else if (ts.profiler != null) {
            ts.profiler.disableProfiling();
        }
        return (false);
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
    private void populateProperties(LocalTransaction ts,
                                    Long txn_id,
                                    long initiateTime,
                                    long client_handle,
                                    int base_partition,
                                    Procedure catalog_proc,
                                    ParameterSet params,
                                    RpcCallback<ClientResponseImpl> client_callback) {
        final int procId = catalog_proc.getId();
        boolean predict_abortable = (hstore_conf.site.exec_no_undo_logging_all == false);
        boolean predict_readOnly = this.isReadOnly[procId];
        PartitionSet predict_partitions = null;
        EstimatorState t_state = null; 
        
        // Setup TransactionProfiler
        if (hstore_conf.site.txn_profiling) {
            boolean ret = this.setupTransactionProfiler(ts, this.isSysProc[procId]);
            if (trace.val && ret) LOG.trace("Enabling profiling for new txn " + ts);
        }
        
        // -------------------------------
        // SYSTEM PROCEDURES
        // -------------------------------
        if (this.isSysProc[procId]) {
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
        else if (this.isMapReduce[procId]) {
            // MapReduceTransactions always need all partitions
            if (debug.val)
                LOG.debug(String.format("New request is for MapReduce %s, so it has to be " +
                		  "multi-partitioned [clientHandle=%d]",
                          catalog_proc.getName(), ts.getClientHandle()));
            predict_partitions = catalogContext.getAllPartitionIds();
        }
        // -------------------------------
        // VOLTDB @PROCINFO
        // -------------------------------
        else if (hstore_conf.site.exec_voltdb_procinfo) {
            if (debug.val)
                LOG.debug(String.format("Using the catalog information to determine whether the %s transaction " +
                		  "is single-partitioned [clientHandle=%d, singleP=%s]",
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
                    if (debug.val)
                        LOG.debug(String.format("%s - Using %s to populate txn properties [clientHandle=%d]",
                                  TransactionUtil.formatTxnName(catalog_proc, txn_id),
                                  t_estimator.getClass().getSimpleName(), client_handle));
                    t_state = t_estimator.startTransaction(txn_id, base_partition, catalog_proc, params.toArray());
                }
                
                // If there is no EstimatorState, then there is nothing we can do
                // It has to be executed as multi-partitioned
                if (t_state == null) {
                    if (debug.val) {
                        LOG.debug(String.format("%s - No %s was returned. Using default estimate.",
                                  TransactionUtil.formatTxnName(catalog_proc, txn_id),
                                  EstimatorState.class.getSimpleName()));
                    }
                }
                // We have a EstimatorState handle, so let's see what it says...
                else {
                    if (trace.val)
                        LOG.trace("\n" + StringBoxUtil.box(t_state.toString()));
                    Estimate t_estimate = t_state.getInitialEstimate();
                    
                    // Bah! We didn't get back a Estimation for some reason...
                    if (t_estimate == null) {
                        if (debug.val)
                            LOG.debug(String.format("%s - No %s handle was return. Using default estimate.",
                                      TransactionUtil.formatTxnName(catalog_proc, txn_id),
                                      Estimate.class.getSimpleName()));
                    }
                    // Invalid Estimation. Stick with defaults
                    else if (t_estimate.isValid() == false) {
                        if (debug.val)
                            LOG.debug(String.format("%s - %s is marked as invalid. Using default estimate.\n%s",
                                      TransactionUtil.formatTxnName(catalog_proc, txn_id),
                                      t_estimate.getClass().getSimpleName(), t_estimate));
                    }    
                    // Use Estimation to determine things
                    else {
                        if (debug.val) {
                            LOG.debug(String.format("%s - Using %s to determine if txn is single-partitioned",
                                      TransactionUtil.formatTxnName(catalog_proc, txn_id),
                                      t_estimate.getClass().getSimpleName()));
                            LOG.trace(String.format("%s %s:\n%s",
                                      TransactionUtil.formatTxnName(catalog_proc, txn_id),
                                      t_estimate.getClass().getSimpleName(), t_estimate));
                        }
                        predict_partitions = t_estimate.getTouchedPartitions(this.thresholds);
                        predict_readOnly = t_estimate.isReadOnlyAllPartitions(this.thresholds);
                        predict_abortable = (predict_partitions.size() == 1 ||
                                             predict_readOnly == false ||
                                             t_estimate.isAbortable(this.thresholds));
                        
                        // Check whether the TransactionEstimator *really* thinks that we should 
                        // give it updates about this txn. If the flag is false, then we'll
                        // check whether the updates are enabled in the HStoreConf parameters
                        if (t_state.shouldAllowUpdates() == false) {
                            if (predict_partitions.size() == 1) {
                                if (hstore_conf.site.markov_singlep_updates == false) t_state.disableUpdates();
                            }
                            else if (hstore_conf.site.markov_dtxn_updates == false) {
                                t_state.disableUpdates();
                            }
                        }
                        
                        if (debug.val && predict_partitions.isEmpty()) {
                            LOG.warn(String.format("%s - Unexpected empty predicted %s from %s [updatesEnabled=%s]\n%s",
                            		TransactionUtil.formatTxnName(catalog_proc, txn_id),
                            		PartitionSet.class.getSimpleName(),
                            		t_estimator.getClass().getSimpleName(),
                            		t_state.isUpdatesEnabled(), t_estimate));
                            ts.setAllowEarlyPrepare(false);
//                            System.err.println("WROTE MARKOVGRAPH: " + ((MarkovEstimatorState)t_state).dumpMarkovGraph());
//                            System.err.flush();
//                            HStore.crashDB();
                        }
                    }
                }
            } catch (Throwable ex) {
                if (t_state != null && t_state instanceof MarkovEstimatorState) {
                    LOG.warn("WROTE MARKOVGRAPH: " + ((MarkovEstimatorState)t_state).dumpMarkovGraph());
                }
                LOG.error(String.format("Failed calculate estimate for %s request\nParameters: %s",
                          TransactionUtil.formatTxnName(catalog_proc, txn_id),
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
                if (debug.val)
                    LOG.debug(String.format("The \"Always Single-Partitioned\" flag is true. " +
                    		  "Marking new %s transaction as single-partitioned on partition %d [clientHandle=%d]",
                              catalog_proc.getName(), base_partition, client_handle));
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
        if (hstore_conf.site.txn_profiling && ts.profiler != null) {
            ts.profiler.setSingledPartitioned(ts.isPredictSinglePartition());
//            LOG.info("Profiling is enabled for " + ts);
        }
        
        if (debug.val) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            m.put("ClientHandle", client_handle);
            m.put("Partitions", ts.getPredictTouchedPartitions());
            m.put("Single Partition", ts.isPredictSinglePartition());
            m.put("Read Only", ts.isPredictReadOnly());
            m.put("Abortable", ts.isPredictAbortable()); 
            LOG.debug(String.format("Initializing %s on partition %d\n%s",
                      ts, base_partition, StringUtil.formatMaps(m).trim()));
        }
    }

}
