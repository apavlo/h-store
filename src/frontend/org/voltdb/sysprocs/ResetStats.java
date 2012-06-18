package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencySet;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;

/** 
 * Reset internal profiling statistics
 */
@ProcInfo(singlePartition = false)
public class ResetStats extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(ResetStats.class);

    public static final ColumnInfo nodeResultsColumns[] = {
        new ColumnInfo("SITE", VoltType.STRING),
        new ColumnInfo("STATUS", VoltType.STRING),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP),
    };
    
    private final ProfileMeasurement gcTime = new ProfileMeasurement(this.getClass().getSimpleName());

    @Override
    public void globalInit(PartitionExecutor site, Procedure catalog_proc,
            BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator) {
        super.globalInit(site, catalog_proc, eeType, hsql, p_estimator);
        site.registerPlanFragment(SysProcFragmentId.PF_resetStatsAggregate, this);
        site.registerPlanFragment(SysProcFragmentId.PF_resetStatsDistribute, this);
    }

    @Override
    public DependencySet executePlanFragment(long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        DependencySet result = null;
        switch (fragmentId) {
            // Reset Stats
            case SysProcFragmentId.PF_resetStatsDistribute: {
                LOG.info("Resetting internal profiling counters");
                HStoreConf hstore_conf = hstore_site.getHStoreConf();
                
                // EXECUTOR
                if (hstore_conf.site.exec_profiling) {
                    
                }
                
                // The first partition at this HStoreSite will have to reset
                // any global profling parameters
                if (this.isFirstLocalPartition()) {
                    
                    // NETWORK
                    if (hstore_conf.site.network_profiling) {
                        
                    }
                    
                    // COMMAND LOGGER
                    if (hstore_conf.site.exec_command_logging_profile) {
                        
                    }
                    
                }
                
                
                VoltTable vt = new VoltTable(nodeResultsColumns);
                vt.addRow(this.executor.getHStoreSite().getSiteName(),
                          this.gcTime.getTotalThinkTimeMS() + " ms",
                          new TimestampType());
                result = new DependencySet(SysProcFragmentId.PF_resetStatsDistribute, vt);
                break;
            }
            // Aggregate Results
            case SysProcFragmentId.PF_resetStatsAggregate:
                List<VoltTable> siteResults = dependencies.get(SysProcFragmentId.PF_resetStatsDistribute);
                if (siteResults == null || siteResults.isEmpty()) {
                    String msg = "Missing site results";
                    throw new ServerFaultException(msg, txn_id);
                }
                
                VoltTable vt = VoltTableUtil.combine(siteResults);
                result = new DependencySet(SysProcFragmentId.PF_resetStatsAggregate, vt);
                break;
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);
        } // SWITCH
        // Invalid!
        return (result);
    }

    public VoltTable[] run() {
        // Send a gc request to the first partition at each HStoreSite
        final int num_sites = CatalogUtil.getNumberOfSites(this.database);
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[num_sites + 1];
        final ParameterSet params = new ParameterSet();
        
        int i = 0;
        for (Site catalog_site : CatalogUtil.getAllSites(this.database)) {
            Partition catalog_part = CollectionUtil.first(catalog_site.getPartitions());
            pfs[i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = SysProcFragmentId.PF_resetStatsDistribute;
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].outputDependencyIds = new int[] { SysProcFragmentId.PF_resetStatsDistribute };
            pfs[i].multipartition = true;
            pfs[i].nonExecSites = false;
            pfs[i].destPartitionId = catalog_part.getId();
            pfs[i].parameters = params;
            pfs[i].last_task = (catalog_site.getId() == hstore_site.getSiteId());
            i += 1;
        } // FOR

        // a final plan fragment to aggregate the results
        pfs[i] = new SynthesizedPlanFragment();
        pfs[i].fragmentId = SysProcFragmentId.PF_resetStatsAggregate;
        pfs[i].inputDependencyIds = new int[] { SysProcFragmentId.PF_resetStatsDistribute };
        pfs[i].outputDependencyIds = new int[] { SysProcFragmentId.PF_resetStatsAggregate };
        pfs[i].multipartition = false;
        pfs[i].nonExecSites = false;
        pfs[i].destPartitionId = CollectionUtil.first(hstore_site.getLocalPartitionIds());
        pfs[i].parameters = params;
        pfs[i].last_task = true;
        
        return executeSysProcPlanFragments(pfs, SysProcFragmentId.PF_resetStatsAggregate);
    }
}
