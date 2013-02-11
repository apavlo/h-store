package org.voltdb.sysprocs;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.profilers.AbstractProfiler;
import edu.brown.profilers.ProfileMeasurement;

/** 
 * Reset internal profiling statistics
 */
@ProcInfo(singlePartition = false)
public class ResetProfiling extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(ResetProfiling.class);

    public static final ColumnInfo nodeResultsColumns[] = {
        new ColumnInfo("SITE", VoltType.STRING),
        new ColumnInfo("STATUS", VoltType.STRING),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP),
    };
    
    private final ProfileMeasurement gcTime = new ProfileMeasurement(this.getClass().getSimpleName());

    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_resetProfilingAggregate, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_resetProfilingDistribute, this);
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        DependencySet result = null;
        switch (fragmentId) {
            // Reset Stats
            case SysProcFragmentId.PF_resetProfilingDistribute: {
                LOG.debug("Resetting internal profiling counters");
                HStoreConf hstore_conf = hstore_site.getHStoreConf();
                
                PartitionExecutor.Debug executorDebug = this.executor.getDebugContext();
                Collection<AbstractProfiler> profilers = new HashSet<AbstractProfiler>();
                
                // EXECUTOR
                if (hstore_conf.site.exec_profiling) {
                    executorDebug.getProfiler().reset();
                }
                
                // SPEC EXEC
                if (hstore_conf.site.specexec_profiling) {
                    for (AbstractProfiler p : executorDebug.getSpecExecScheduler().getDebugContext().getProfilers()) {
                        profilers.add(p);
                    } // FOR
                }
                                
                // MARKOV
                if (hstore_conf.site.markov_profiling) {
                    TransactionEstimator est = executor.getTransactionEstimator();
                    if (est instanceof MarkovEstimator) {
                        profilers.add(((MarkovEstimator)est).getDebugContext().getProfiler());
                    }
                }
                
                // ANTI-CACHE
                if (hstore_conf.site.anticache_enable) {
                    profilers.add(hstore_site.getAntiCacheManager().getDebugContext().getProfiler(this.partitionId));
                }

                // QUEUE
                if (hstore_conf.site.queue_profiling) {
                    profilers.add(hstore_site.getTransactionQueueManager().getDebugContext().getProfiler(this.partitionId));
                }
                
                // The first partition at this HStoreSite will have to reset
                // any global profiling parameters
                if (this.isFirstLocalPartition()) {
                    // COMMAND LOGGER
                    CommandLogWriter commandLog = hstore_site.getCommandLogWriter();
                    if (hstore_conf.site.commandlog_profiling && commandLog.getProfiler() != null) {
                        profilers.add(commandLog.getProfiler());
                    }
                    
                    // Reset the StartWorkload flag in the HStoreSite
                    hstore_site.getDebugContext().resetStartWorkload();
                }
                
                for (AbstractProfiler profiler : profilers) {
                    profiler.reset();
                } // FOR
                
                VoltTable vt = new VoltTable(nodeResultsColumns);
                vt.addRow(this.executor.getHStoreSite().getSiteName(),
                          this.gcTime.getTotalThinkTimeMS() + " ms",
                          new TimestampType());
                result = new DependencySet(SysProcFragmentId.PF_resetProfilingDistribute, vt);
                break;
            }
            // Aggregate Results
            case SysProcFragmentId.PF_resetProfilingAggregate:
                LOG.debug("Combining results");
                List<VoltTable> siteResults = dependencies.get(SysProcFragmentId.PF_resetProfilingDistribute);
                if (siteResults == null || siteResults.isEmpty()) {
                    String msg = "Missing site results";
                    throw new ServerFaultException(msg, txn_id);
                }
                
                VoltTable vt = VoltTableUtil.union(siteResults);
                result = new DependencySet(SysProcFragmentId.PF_resetProfilingAggregate, vt);
                break;
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);
        } // SWITCH
        // Invalid!
        return (result);
    }

    public VoltTable[] run() {
        return this.executeOncePerSite(SysProcFragmentId.PF_resetProfilingDistribute,
                                       SysProcFragmentId.PF_resetProfilingAggregate);
    }
}
