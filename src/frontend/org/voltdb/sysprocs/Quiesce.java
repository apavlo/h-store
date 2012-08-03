package org.voltdb.sysprocs;

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

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.PartitionExecutor;

/** 
 * Flush out and reject all of the txns queued up at each PartitionExecutor
 */
@ProcInfo(singlePartition = false)
public class Quiesce extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(Quiesce.class);

    private static final ColumnInfo ResultsColumns[] = {
        new ColumnInfo("PARTITION", VoltType.STRING),
        new ColumnInfo("STATUS", VoltType.STRING),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP),
    };
    
    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_quiesce_sites, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_quiesce_processed_sites, this);
    }

    @Override
    public DependencySet executePlanFragment(long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        DependencySet result = null;
        switch (fragmentId) {
            case SysProcFragmentId.PF_quiesce_sites: {
                LOG.debug("Clearing out work queue at partition " + executor.getPartitionId());
                executor.haltProcessing();
                VoltTable vt = new VoltTable(ResultsColumns);
                vt.addRow(this.executor.getHStoreSite().getSiteName(),
                          Status.OK,
                          new TimestampType());
                result = new DependencySet(SysProcFragmentId.PF_quiesce_sites, vt);
                break;
            }
            // Aggregate Results
            case SysProcFragmentId.PF_quiesce_processed_sites:
                List<VoltTable> siteResults = dependencies.get(SysProcFragmentId.PF_quiesce_sites);
                if (siteResults == null || siteResults.isEmpty()) {
                    String msg = "Missing site results";
                    throw new ServerFaultException(msg, txn_id);
                }
                VoltTable vt = VoltTableUtil.combine(siteResults);
                result = new DependencySet(SysProcFragmentId.PF_quiesce_processed_sites, vt);
                break;
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);
        } // SWITCH
        // Invalid!
        return (result);
    }
    
    public VoltTable[] run() {
        return this.executeOncePerPartition(SysProcFragmentId.PF_quiesce_sites,
                                            SysProcFragmentId.PF_quiesce_processed_sites,
                                            new ParameterSet());
    }
}
