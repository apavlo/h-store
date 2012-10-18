package org.voltdb.sysprocs;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.estimators.MarkovEstimator;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.FileUtil;

@ProcInfo(singlePartition = false)
public class MarkovUpdate extends VoltSystemProcedure {
    
    private static final Logger LOG = Logger.getLogger(MarkovUpdate.class);

    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_recomputeMarkovsDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_recomputeMarkovsAggregate, this);
    }
    
    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        final boolean debug = LOG.isDebugEnabled();
        
        // Return the path to the files
        VoltTable[] result = new VoltTable[1];
        result[0] = new VoltTable(new VoltTable.ColumnInfo("SiteId", VoltType.BIGINT),
                                  new VoltTable.ColumnInfo("PartitionId", VoltType.BIGINT),
                                  new VoltTable.ColumnInfo("OutputFile", VoltType.STRING),
                                  new VoltTable.ColumnInfo("IsGlobal", VoltType.INTEGER));
        
        if (fragmentId == SysProcFragmentId.PF_recomputeMarkovsDistribute) {
            
            boolean save_to_file = (Boolean)params.toArray()[0];
            HStoreSite hstore_site = this.executor.getHStoreSite();
            
            // Check whether the MarkovsGraphsContainer is global or not.
            // If it is, then we only need to write out a single file
            
            if ((this.executor.getTransactionEstimator() instanceof MarkovEstimator) == false) {
                String msg = String.format("Cannot recompute markov graphs because the estimator " +
                		                   "at partition %d is not a MarkovEstimator", partitionId);
                throw new VoltAbortException(msg);
            }
                
            MarkovEstimator t_estimator = (MarkovEstimator)this.executor.getTransactionEstimator();
            assert(t_estimator != null);
            MarkovGraphsContainer markovs = t_estimator.getMarkovGraphsContainer();
            
            if (t_estimator.getMarkovGraphsContainer() != null) {
                boolean is_global = t_estimator.getMarkovGraphsContainer().isGlobal();

                // We will only write out our file if we are the first partition in the list at this site
                if (is_global == false ||
                    (is_global == true && Collections.min(hstore_site.getLocalPartitionIds()).equals(this.partitionId))) {
                    
                    if (debug) LOG.debug(String.format("Recalculating MarkovGraph probabilities at partition %d [save=%s, global=%s]",
                                                       this.partitionId, save_to_file, is_global));
                    
                    for (MarkovGraph m : markovs.getAll()) {
                        try {
                             m.calculateProbabilities();
                        } catch (Throwable ex) {
                            LOG.fatal(String.format("Failed to recalculate probabilities for %s MarkovGraph #%d: %s", m.getProcedure().getName(), m.getGraphId(), ex.getMessage()));
                            File output = MarkovUtil.exportGraphviz(m, true, false, true, null).writeToTempFile();
                            LOG.fatal("Wrote out invalid MarkovGraph: " + output.getAbsolutePath());
                            this.executor.crash(ex);
                            assert(false) : "I shouldn't have gotten here!";
                        }
                    } // FOR
                    
                    if (save_to_file) {
                        File f = FileUtil.getTempFile("markovs-" + this.partitionId, true);
                        LOG.info(String.format("Saving updated MarkovGraphs to '" + f + "'"));
                        try {
                            markovs.save(f);
                        } catch (Throwable ex) {
                            throw new RuntimeException("Failed to save MarkovGraphContainer for site " + HStoreThreadManager.formatSiteName(this.executor.getSiteId()), ex);
                        }
                        result[0].addRow(this.executor.getSiteId(), this.partitionId, f.getAbsolutePath(), is_global ? 1 : 0);
                    }
                }
            }
            return new DependencySet(new int[] { (int)SysProcFragmentId.PF_recomputeMarkovsDistribute }, result);
            
        } else if (fragmentId == SysProcFragmentId.PF_recomputeMarkovsAggregate) {
            if (debug) LOG.debug("Aggregating results from recomputing models fragments in txn #" + txn_id);
            for (List<VoltTable> l : dependencies.values()) {
                for (VoltTable vt : l) {
                    while (vt != null && vt.advanceRow()) {
                        result[0].add(vt.getRow());
                    } // WHILE
                } // FOR
            } // FOR
            return new DependencySet(new int[] { (int)SysProcFragmentId.PF_recomputeMarkovsAggregate }, result);
        }
        assert(false) : "Unexpected FragmentId " + fragmentId;
        return null;
    }
    
    /**
     * 
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(boolean save_to_file) throws VoltAbortException {
        return this.executeOncePerPartition(SysProcFragmentId.PF_recomputeMarkovsDistribute,
                                            SysProcFragmentId.PF_recomputeMarkovsAggregate,
                                            new ParameterSet(save_to_file));
    }

}
