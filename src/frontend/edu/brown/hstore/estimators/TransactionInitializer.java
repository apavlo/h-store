package edu.brown.hstore.estimators;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import com.google.protobuf.RpcCallback;

import edu.brown.graphs.GraphvizExport;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.LocalTransaction;
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
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final Collection<Integer> all_partitions;
    private EstimationThresholds thresholds;
    
    /**
     * Fixed Markov Estimator
     */
    private final AbstractEstimator fixed_estimator;
    
    public TransactionInitializer(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.all_partitions = hstore_site.getAllPartitionIds();
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
    
    public void populateProperties(LocalTransaction ts,
                                    Long txn_id,
                                    long client_handle,
                                    int base_partition,
                                    Procedure catalog_proc,
                                    ParameterSet params,
                                    RpcCallback<byte[]> client_callback){

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
