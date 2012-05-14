package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

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
import edu.brown.utils.PartitionEstimator;

/** 
 * Get a status snapshot of the PartitionExecutors in the cluster
 * @author pavlo
 */
@ProcInfo(singlePartition = true)
public class GetCatalog extends VoltSystemProcedure {

    public static final ColumnInfo nodeResultsColumns[] = {
        new ColumnInfo("CATALOG",   VoltType.STRING),
        new ColumnInfo("CREATED",   VoltType.TIMESTAMP),
    };
    
    @Override
    public void globalInit(PartitionExecutor site, Procedure catalog_proc,
                            BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator) {
        super.globalInit(site, catalog_proc, eeType, hsql, p_estimator);
        site.registerPlanFragment(SysProcFragmentId.PF_getCatalog, this);
    }

    @Override
    public DependencySet executePlanFragment(long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        assert(fragmentId == SysProcFragmentId.PF_getCatalog);
        
        // Serialize the catalog and throw it back to the client
        VoltTable vt = new VoltTable(nodeResultsColumns);
        vt.addRow(database.getCatalog().serialize(),
                  new TimestampType());
        DependencySet result = new DependencySet(SysProcFragmentId.PF_getCatalog, vt);
        return (result);
    }

    public VoltTable[] run() {
        // Blast that mofo and get the catalog for the client
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        final ParameterSet params = new ParameterSet();
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_getCatalog;
        pfs[0].inputDependencyIds = new int[] { };
        pfs[0].outputDependencyIds = new int[] { SysProcFragmentId.PF_getCatalog };
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].destPartitionId = this.partitionId;
        pfs[0].parameters = params;
        pfs[0].last_task = true;
        
        return executeSysProcPlanFragments(pfs, SysProcFragmentId.PF_getCatalog);
    }
}
