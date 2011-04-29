/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
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
package edu.brown.costmodel;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.DesignerHints;
import edu.brown.statistics.Histogram;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.Workload;
import edu.brown.workload.TransactionTrace;

/**
 * 
 * @author pavlo
 *
 */
public abstract class AbstractCostModel implements Cloneable {
    private static final Logger LOG = Logger.getLogger(AbstractCostModel.class);
    
    /**
     * Child Class (keep this around just in case...)
     */
    protected final Class<? extends AbstractCostModel> child_class;

    /**
     * Keep track of the last PartitionPlan that was used so that we can automatically invalidate our own cache?
     * Really? Do we really want to always be able to do that ourselves?
     * Why not? It's not working the way we have it now? Go fuck yourself!
     */
    // protected PartitionPlan last_pplan = null;
    
    /**
     * Caching Parameter
     */
    protected boolean use_caching = true;

    /**
     * Enable Execution Calculation (if supported)
     */
    protected boolean use_execution = true;
    
    /**
     * Enable Entropy Calculations (if supported)
     */
    protected boolean use_entropy = true;
    protected boolean use_entropy_txns = true;
    protected boolean use_entropy_java = false;
    
    /**
     * Enable Multipartition Txn Penalty (if supported)
     */
    protected boolean use_multitpartition_penalty = true;
    
    
    /**
     * Weights
     */
    protected double execution_weight = 1.0;
    protected double entropy_weight = 1.0;
    protected double entropy_weight_txn = 1.0;
    protected int entropy_weight_java = 1;
    protected double multipartition_penalty = 1.0;
    
    /**
     * PartitionEstimator
     * This does all the heavy lifting for us
     */
    protected final PartitionEstimator p_estimator;
    protected int num_partitions;
    
    /**
     * Which partitions executed the actual the java of the VoltProcedure
     */
    protected final Histogram histogram_java_partitions = new Histogram();
    
    /**
     * How many times did we execute each procedure
     */
    protected final Histogram<String> histogram_procs = new Histogram<String>();
    
    /**
     * How many times did we execute each procedure when it was either single- or multi-partition?
     */
    protected final Histogram histogram_sp_procs = new Histogram();
    protected final Histogram histogram_mp_procs = new Histogram();
    
    /**
     * This histogram keeps track of how many times txns touched a partition at least once
     * Note that this will only record an entry once per txn per partition. If you want the data
     * on the total number times the txns touched the partitions, you want the query access histogram 
     */
    protected final Histogram<Integer> histogram_txn_partitions = new Histogram<Integer>();
    
    /**
     * This histogram keeps track of how many times partitions were touched by any query in the txns
     * If a single txn has multiple queries that touch a particular partition, there will be an entry
     * added for each of those queries.
     */
    protected final Histogram<Integer> histogram_query_partitions = new Histogram<Integer>();
    
    /**
     * Since we have an iterative cost-model, keep track of the number of queries and txns that
     * we have examined. 
     */
    protected final AtomicLong query_ctr = new AtomicLong(0);
    protected final AtomicLong txn_ctr = new AtomicLong(0);
    
    /**
     * Debugging switch
     */
    private boolean enable_debugging = false;
    private final Level orig_log_level;
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    protected class DynamicLogger extends Logger {
        private final Logger logger;
        private StringBuilder sb;
        
        public DynamicLogger(Class<? extends AbstractCostModel> _class) {
            super(_class.getSimpleName());
            this.logger = Logger.getLogger(_class);
            this.clear();
        }
        
        public void clear() {
            this.sb = new StringBuilder();
        }
        
        @Override
        public String toString() {
            return this.sb.toString();
        }
        
        @Override
        public void debug(Object message) {
            this.sb.append(message).append("\n");
            this.logger.debug(message);
        }
        
        @Override
        public void info(Object message) {
            this.sb.append(message).append("\n");
            this.logger.info(message);
        }
        
        @Override
        public void warn(Object message) {
            this.sb.append(message).append("\n");
            this.logger.warn(message);
        }
        
        @Override
        public void error(Object message) {
            this.sb.append(message).append("\n");
            this.logger.error(message);
        }
        
        @Override
        public void fatal(Object message) {
            this.sb.append(message).append("\n");
            this.logger.fatal(message);
        }
        
        @Override
        public boolean isDebugEnabled() {
            return this.logger.isDebugEnabled();
        }
    }
    
    /**
     * Constructor
     */
    public AbstractCostModel(final Class<? extends AbstractCostModel> child_class, final Database catalog_db, final PartitionEstimator p_estimator) {
        this.child_class = child_class;
        this.p_estimator = p_estimator;
        this.orig_log_level = LOG.getLevel();
    }
    
    public final void clear() {
        this.clear(false);
    }
    
    /**
     * Clear out some of the internal counters 
     */
    public void clear(boolean force) {
        this.histogram_procs.clear();
        this.histogram_mp_procs.clear();
        this.histogram_sp_procs.clear();
        this.histogram_java_partitions.clear();
        this.histogram_query_partitions.clear();
        this.histogram_txn_partitions.clear();
        this.query_ctr.set(0);
        this.txn_ctr.set(0);
        if (LOG instanceof DynamicLogger) ((DynamicLogger)LOG).clear();
    }
    
    /**
     * 
     * @param catalog_db
     */
    public final void prepare(final Database catalog_db) {
        // final boolean trace = LOG.isTraceEnabled();
        this.prepareImpl(catalog_db);
        
        // Construct a PartitionPlan for the current state of the catalog so that we 
        // know how to invalidate ourselves
        
        /* I don't think we need this anymore...
        PartitionPlan new_pplan = PartitionPlan.createFromCatalog(catalog_db);
        if (this.last_pplan != null) {
            Set<CatalogType> changed = new_pplan.getChangedEntries(this.last_pplan);
            if (!changed.isEmpty()) {
                if (trace) LOG.trace("Invalidating " + changed.size() + " catalog items that have changed from the last PartitionPlan");
                this.invalidateCache(changed);
            }
        }
        this.last_pplan = new_pplan;
        */
    }
    
    public PartitionEstimator getPartitionEstimator() {
        return p_estimator;
    }
    
    // ----------------------------------------------------------------------------
    // ABSTRACT METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Must be called before the next round of cost estimations for a new catalog
     */
    public abstract void prepareImpl(final Database catalog_db);
    
    
    /**
     * 
     * @param workload TODO
     * @param xact
     * @return
     * @throws Exception
     */
    public abstract double estimateTransactionCost(Database catalog_db, Workload workload, Workload.Filter filter, TransactionTrace xact) throws Exception;
    
    /**
     * Invalidate cache entries for the given CatalogKey
     * @param catalog_key
     */
    public abstract void invalidateCache(String catalog_key);
    
    /**
     *  Invalidate the cache entries for all of the given CatalogKeys
     * @param keys
     */
    public void invalidateCache(Iterable<String> keys) {
        for (String catalog_key : keys) {
            this.invalidateCache(catalog_key);
        }
    }
    
    /**
     * 
     */
    public abstract AbstractCostModel clone(Database catalog_db) throws CloneNotSupportedException;

    
    // ----------------------------------------------------------------------------
    // DEBUGGING METHODS
    // ----------------------------------------------------------------------------

    /**
     * Dynamic switch to enable DEBUG log level
     * If enable_debugging is false, then LOG's level will be set back to its original level
     * @param enable_debugging
     */
    public void setDebuggingEnabled(boolean enable_debugging) {
        this.enable_debugging = enable_debugging;
        if (this.enable_debugging) {
            LOG.setLevel(Level.DEBUG);
            if (LOG.isDebugEnabled()) LOG.debug("Enabling dynamic logging in " + this.child_class.getSimpleName());
        } else {
            LOG.setLevel(this.orig_log_level);
        }
    }
    
    public String getLastDebugMessages() {
        return (LOG.toString());
    }


    // ----------------------------------------------------------------------------
    // BASE METHODS
    // ----------------------------------------------------------------------------
    
    public void applyDesignerHints(DesignerHints hints) {
        this.setCachingEnabled(hints.enable_costmodel_caching);
        
        this.setEntropyEnabled(hints.enable_costmodel_entropy);
        this.setEntropyWeight(hints.weight_costmodel_entropy);
        
        this.setExecutionCostEnabled(hints.enable_costmodel_execution);
        this.setExecutionWeight(hints.weight_costmodel_execution);
        
        this.setMultiPartitionPenaltyEnabled(hints.enable_costmodel_multipartition_penalty);
        this.setMultiPartitionPenalty(hints.weight_costmodel_multipartition_penalty);
        
        this.setJavaExecutionWeightEnabled(hints.enable_costmodel_java_execution);
        this.setJavaExecutionWeight(hints.weight_costmodel_java_execution);
    }

    /**
     * Returns true if this procedure is only executed as a single-partition procedure
     * Returns false if this procedure was executed as a multi-partition procedure at least once
     * Returns null if there is no information about this procedure
     * @param catalog_proc
     * @return
     */
    public Boolean isAlwaysSinglePartition(Procedure catalog_proc) {
        assert(catalog_proc != null);
        String proc_key = CatalogKey.createKey(catalog_proc);
        Boolean ret = null;
        if (!this.histogram_mp_procs.contains(proc_key)) {
            if (this.histogram_sp_procs.contains(proc_key)) {
                ret = true;
            }
        } else {
            ret = false;
        }
        return (ret);
    }
    
    public boolean isCachingEnabled() {
        return use_caching;
    }
    /**
     * 
     * @param caching
     */
    public void setCachingEnabled(boolean caching) {
        LOG.debug("Cost Model Caching: " + (caching ? "ENABLED" : "DISABLED"));
        this.use_caching = caching;
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION COSTS
    // ----------------------------------------------------------------------------
    
    public boolean isExecutionCostEnabled() {
        return use_execution;
    }
    public void setExecutionCostEnabled(boolean execution) {
        LOG.debug("Cost Model Execution: " + (execution ? "ENABLED" : "DISABLED"));
        this.use_execution = execution;
    }
    public void setExecutionWeight(double weight) {
        LOG.debug("Execution Cost Weight: " + weight);
        this.execution_weight = weight;
    }
    public double getExecutionWeight() {
        return (this.execution_weight);
    }
    
    // ----------------------------------------------------------------------------
    // ENTROPY COST
    // ----------------------------------------------------------------------------
    
    public boolean isEntropyEnabled() {
        return use_entropy;
    }
    public void setEntropyEnabled(boolean entropy) {
        LOG.debug("Cost Model Entropy: " + (entropy ? "ENABLED" : "DISABLED"));
        this.use_entropy = entropy;
    }
    public void setEntropyWeight(double weight) {
        LOG.debug("Entropy Cost Weight: " + weight);
        this.entropy_weight = weight;
    }
    public double getEntropyWeight() {
        return (this.entropy_weight);
    }
    
    // ----------------------------------------------------------------------------
    // MULTIPARTITION PENALTY
    // ----------------------------------------------------------------------------
    
    public boolean isMultiPartitionPenaltyEnabled() {
        return this.use_multitpartition_penalty;
    }
    public void setMultiPartitionPenaltyEnabled(boolean enable) {
        LOG.debug("Cost Model MultiPartition Penalty: " + (enable ? "ENABLED" : "DISABLED"));
        this.use_multitpartition_penalty = enable;
    }
    public void setMultiPartitionPenalty(double penalty) {
        LOG.debug("MultiPartition Penalty: " + penalty);
        this.multipartition_penalty = penalty;
    }
    public double getMultiPartitionPenalty() {
        return (this.multipartition_penalty);
    }
    
    
    // ----------------------------------------------------------------------------
    // JAVA EXECUTION WEIGHT (SKEW)
    // ----------------------------------------------------------------------------
    
    public boolean isJavaExecutionWeightEnabled() {
        return this.use_entropy_java;
    }
    public void setJavaExecutionWeightEnabled(boolean enable) {
        LOG.debug("Cost Model Java Execution: " + (enable ? "ENABLED" : "DISABLED"));
        this.use_entropy_java = enable;
    }
    public void setJavaExecutionWeight(int weight) {
        LOG.debug("Java Execution Weight: " + weight);
        this.entropy_weight_java = weight;
    }
    public int getJavaExecutionWeight() {
        return (this.entropy_weight_java);
    }

    // ----------------------------------------------------------------------------
    // PARTITION EXECUTION WEIGHT (SKEW)
    // ----------------------------------------------------------------------------
    
    public boolean isEntropyTxnWeightEnabled() {
        return this.use_entropy_java;
    }
    public void setEntropyTxnWeightEnabled(boolean enable) {
        LOG.debug("Cost Model Entropy Txn: " + (enable ? "ENABLED" : "DISABLED"));
        this.use_entropy_java = enable;
    }
    public void setEntropyTxnWeight(int weight) {
        LOG.debug("Entropy Txn Weight: " + weight);
        this.entropy_weight_java = weight;
    }
    public int getEntropyTxnWeight() {
        return (this.entropy_weight_java);
    }
    
    
    public Histogram getProcedureHistogram() {
        return this.histogram_procs;
    }
    
    public Histogram getSinglePartitionProcedureHistogram() {
        return this.histogram_sp_procs;
    }
    public Histogram getMultiPartitionProcedureHistogram() {
        return this.histogram_mp_procs;
    }
    
    /**
     * Returns the histogram of how often a particular partition has to execute
     * the Java code for a transaction
     * @return
     */
    public Histogram getJavaExecutionHistogram() {
        return this.histogram_java_partitions;
    }
    
    /**
     * Returns the histogram for how often partitions are accessed for txns
     * @return
     */
    public Histogram getTxnPartitionAccessHistogram() {
        return this.histogram_txn_partitions;
    }
    
    /**
     * Returns the histogram for how often partitions are accessed for queries 
     * @return
     */
    public Histogram getQueryPartitionAccessHistogram() {
        return this.histogram_query_partitions;
    }
    
    /**
     * Invalidate a table's cache entry
     * @param catalog_tbl
     */
    public <T extends CatalogType> void invalidateCache(T catalog_item) {
        this.invalidateCache(CatalogKey.createKey(catalog_item));
    }
    
    /**
     * 
     * @param catalog_tbls
     */
    public <T extends CatalogType> void invalidateCache(Collection<T> catalog_items) {
        for (T catalog_item : catalog_items) {
            this.invalidateCache(CatalogKey.createKey(catalog_item));
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // ESTIMATION METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param workload
     * @return
     * @throws Exception
     */
    public double estimateCost(Database catalog_db, Workload workload, Workload.Filter filter) throws Exception {
        this.prepare(catalog_db);
        double cost = 0.0d;
        
        Iterator<AbstractTraceElement<?>> it = workload.iterator(filter);
        while (it.hasNext()) {
            AbstractTraceElement<?> element = it.next();
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                //System.out.println(xact.debug(this.catalog_db) + "\n");
                try {
                    cost += this.estimateTransactionCost(catalog_db, workload, filter, xact);
                } catch (Exception ex) {
                    LOG.error("Failed to estimate cost for " + xact.getCatalogItemName());
                    CatalogUtil.saveCatalog(catalog_db.getCatalog(), "catalog.txt");
                    throw ex;
                }
            }
        } // WHILE
        return (cost);
    }

    /**
     * Estimate the cost of a single TransactionTrace object
     * @param catalog_db
     * @param xact
     * @return
     * @throws Exception
     */
    public final double estimateTransactionCost(Database catalog_db, TransactionTrace xact) throws Exception {
        return (this.estimateTransactionCost(catalog_db, null, null, xact));
    }
    
    /**
     * 
     * @param workload
     * @return
     * @throws Exception
     */
    public final double estimateCost(Database catalog_db, Workload workload) throws Exception {
        return (this.estimateCost(catalog_db, workload, null));
    }

    /**
     * Return the set of untouched partitions for the last costmodel estimate
     * @param num_partitions
     * @return
     */
    public Set<Integer> getUntouchedPartitions(int num_partitions) {
        Set<Integer> untouched = new HashSet<Integer>();
        for (int i = 0; i < num_partitions; i++) {
            // For now only consider where the java executes. Ideally we will want to
            // consider where the queries execute too, but we would need to isolate 
            // the single-partition txns from the multi-partition txns that are always 
            // going to touch every partition
            if (!(this.histogram_java_partitions.contains(i))) {
//                  this.histogram_txn_partitions.contains(i) ||
//                  this.histogram_query_partitions.contains(i))) {
                untouched.add(i);
            }
        } // FOR
        return (untouched);
    }
    
    /**
     * Debug string of all the histograms
     * @return
     */
    public String debugHistograms() {
        StringBuilder sb = new StringBuilder();
        
        // Execution
        sb.append("Java Execution Partitions Histogram\n")
          .append(StringUtil.SINGLE_LINE)
          .append(this.histogram_java_partitions)
          .append("\n");

        // Transaction Access
        sb.append("Txn Partition Access Histogram\n")
          .append(StringUtil.SINGLE_LINE)
          .append(this.histogram_txn_partitions)
          .append("\n");

        // Query Access
        sb.append("Query Partition Access Histogram\n")
          .append(StringUtil.SINGLE_LINE)
          .append(this.histogram_query_partitions)
          .append("\n");
        
        return (sb.toString());
    }
}
