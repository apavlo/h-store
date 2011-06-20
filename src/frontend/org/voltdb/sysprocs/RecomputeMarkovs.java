package org.voltdb.sysprocs;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencySet;
import org.voltdb.ExecutionSite;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovGraphsContainer;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.mit.hstore.HStoreSite;

@ProcInfo(singlePartition = false)
public class RecomputeMarkovs extends VoltSystemProcedure {
    
    private static final Logger LOG = Logger.getLogger(RecomputeMarkovs.class);

    @Override
    public void globalInit(ExecutionSite site, Procedure catalog_proc,
            BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator,
            Integer local_partition) {
        super.globalInit(site, catalog_proc, eeType, hsql, p_estimator, local_partition);
        site.registerPlanFragment(SysProcFragmentId.PF_recomputeMarkovsDistribute, this);
        site.registerPlanFragment(SysProcFragmentId.PF_recomputeMarkovsAggregate, this);
    }
    
    @Override
    public DependencySet executePlanFragment(long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        final boolean debug = LOG.isDebugEnabled();
        
        // need to return something ..
        VoltTable[] result = new VoltTable[1];
        result[0] = new VoltTable(new VoltTable.ColumnInfo("Updated", VoltType.BIGINT));
        
        int updated = 0;
        if (fragmentId == SysProcFragmentId.PF_recomputeMarkovsDistribute) {
            
            boolean save_to_file = (Boolean)params.toArray()[0];
            File save_path = (save_to_file ? new File(params.toArray()[1].toString() + "-" + this.executor.getSiteId()) : null);
            
            TransactionEstimator t_estimator = this.executor.getTransactionEstimator();
            assert(t_estimator != null);
            
            MarkovGraphsContainer markovs = t_estimator.getMarkovs();
            if (markovs != null) {
                for (MarkovGraph m : markovs.getAll()) {
                    m.recalculateProbabilities();
                    updated++;
                } // FOR
                if (save_to_file) {
                    LOG.info(String.format("Saving updated MarkovGraphs to '" + save_path + "'"));
                    try {
                        markovs.save(save_path.getAbsolutePath());
                    } catch (Throwable ex) {
                        throw new RuntimeException("Failed to save MarkovGraphContainer for site " + HStoreSite.getSiteName(this.executor.getSiteId()), ex);
                    }
                }
            }
            result[0].addRow(updated);
            return new DependencySet(new int[] { (int)SysProcFragmentId.PF_recomputeMarkovsDistribute }, result);
            
        } else if (fragmentId == SysProcFragmentId.PF_recomputeMarkovsAggregate) {
            if (debug) LOG.debug("Aggregating results from recomputing models fragments in txn #" + txn_id);
            for (List<VoltTable> l : dependencies.values()) {
                for (VoltTable vt : l) {
                    if (vt != null && vt.advanceRow()) {
                        updated += vt.getLong(0);
                    }
                } // FOR
            } // FOR
            result[0].addRow(updated);
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
    public VoltTable[] run(boolean save_to_file, String save_path) throws VoltAbortException {
//        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        VoltTable[] results;
        SynthesizedPlanFragment pfs[];

        // Generate a plan fragment for each site
        // We only need to send this to one partition per HStoreSite
        Cluster catalog_clus = this.database.getParent();
        Set<Partition> targets = new HashSet<Partition>();
        for (Site catalog_site : catalog_clus.getSites()) {
            targets.add(CollectionUtil.getFirst(catalog_site.getPartitions()));
        } // FOR
        
        pfs = new SynthesizedPlanFragment[targets.size()  + 1];
        int i = 0;
        for (Partition catalog_part : targets) {
            int partition = catalog_part.getId();
            ParameterSet params = new ParameterSet();
            params.setParameters(new Object[]{save_to_file, save_path});
            
            pfs[++i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = SysProcFragmentId.PF_recomputeMarkovsDistribute;
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_recomputeMarkovsDistribute };
            pfs[i].multipartition = true;
            pfs[i].nonExecSites = false;
            pfs[i].destPartitionId = partition;
            pfs[i].parameters = params;
            pfs[i].last_task = true;
        } // FOR

        // a final plan fragment to aggregate the results
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].destPartitionId = base_partition;
        pfs[0].fragmentId = SysProcFragmentId.PF_recomputeMarkovsAggregate;
        pfs[0].inputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpDistribute };
        pfs[0].outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_recomputeMarkovsAggregate };
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].parameters = new ParameterSet();

        // send these forth in to the world .. and wait
        if (debug) LOG.debug("Passing " + pfs.length + " sysproc fragments to executeSysProcPlanFragments()");
        results = executeSysProcPlanFragments(pfs, (int)SysProcFragmentId.PF_recomputeMarkovsAggregate);
        return results;
    }

}
