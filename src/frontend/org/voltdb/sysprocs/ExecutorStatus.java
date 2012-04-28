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
import org.voltdb.catalog.Procedure;
import org.voltdb.types.TimestampType;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.util.ThrottlingQueue;
import edu.brown.utils.PartitionEstimator;

/** 
 * Get a status snapshot of the PartitionExecutors in the cluster
 * @author pavlo
 */
@ProcInfo(singlePartition = true)
public class ExecutorStatus extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(ExecutorStatus.class);

    public static final ColumnInfo nodeResultsColumns[] = {
        new ColumnInfo("SITE",          VoltType.INTEGER),
        new ColumnInfo("PARTITION",     VoltType.INTEGER),
        new ColumnInfo("TXNS_TOTAL",    VoltType.INTEGER),
        new ColumnInfo("TXNS_QUEUED",   VoltType.INTEGER),
        new ColumnInfo("TXNS_BLOCKED",  VoltType.INTEGER),
        new ColumnInfo("TXNS_WAITING",  VoltType.INTEGER),
        new ColumnInfo("CURRENT_TXN",   VoltType.BIGINT),
        new ColumnInfo("CURRENT_DTXN",  VoltType.BIGINT),
        new ColumnInfo("LAST_EXECUTED", VoltType.BIGINT),
        new ColumnInfo("LAST_COMMITTED", VoltType.BIGINT),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP),
    };
    
    @Override
    public void globalInit(PartitionExecutor site, Procedure catalog_proc,
            BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator) {
        super.globalInit(site, catalog_proc, eeType, hsql, p_estimator);
        site.registerPlanFragment(SysProcFragmentId.PF_execStatus, this);
    }

    @Override
    public DependencySet executePlanFragment(long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        assert(fragmentId == SysProcFragmentId.PF_execStatus);
        
        // Hit up all of the PartitionExecutors at this HStore and figure out what
        // they got going on
        VoltTable vt = new VoltTable(nodeResultsColumns);
        for (Integer p : hstore_site.getLocalPartitionIdArray()) {
            PartitionExecutor es = hstore_site.getPartitionExecutor(p.intValue());
            ThrottlingQueue<?> es_queue = this.executor.getThrottlingQueue();
                
            Long currentTxnId = es.getCurrentTxnId();
            Long currentDtxnId = es.getCurrentDtxnId();
            Long lastCommitted = es.getLastCommittedTxnId();
            Long lastExecuted = es.getLastExecutedTxnId(); 
            
            vt.addRow(es.getSiteId(),
                      es.getPartitionId(),
                      es_queue.size(),
                      es.getWorkQueueSize(),
                      es.getBlockedQueueSize(),
                      es.getWaitingQueueSize(),
                      (currentTxnId != null  ? currentTxnId.longValue()  : VoltType.NULL_BIGINT),
                      (currentDtxnId != null ? currentDtxnId.longValue() : VoltType.NULL_BIGINT),
                      (lastExecuted != null  ? lastExecuted.longValue()  : VoltType.NULL_BIGINT),
                      (lastCommitted != null ? lastCommitted.longValue() : VoltType.NULL_BIGINT),
                      new TimestampType());
        } // FOR
        
        DependencySet result = new DependencySet(SysProcFragmentId.PF_execStatus, vt);
        return (result);
    }

    public VoltTable[] run(int partitionId) {
        // Hopefully this put us on the right partition...
        
        // Send a gc request to the first partition at each HStoreSite
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        final ParameterSet params = new ParameterSet();
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_execStatus;
        pfs[0].inputDependencyIds = new int[] { };
        pfs[0].outputDependencyIds = new int[] { SysProcFragmentId.PF_execStatus };
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].destPartitionId = hstore_site.getHasher().hash(partitionId);
        pfs[0].parameters = params;
        pfs[0].last_task = true;
        
        return executeSysProcPlanFragments(pfs, SysProcFragmentId.PF_execStatus);
    }
}
