/**
 * 
 */
package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;

/**
 * @author aelmore
 */
@ProcInfo(singlePartition = false)
public class StopCopy extends VoltSystemProcedure {

    private static final Logger LOG = Logger.getLogger(StopCopy.class);

    public static final ColumnInfo nodeResultsColumns[] = { new ColumnInfo("PARTITION", VoltType.INTEGER) };

    /*
     * (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#initImpl()
     */
    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_stopCopyDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_stopCopyAggregate, this);
    }

    /*
     * (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#executePlanFragment(java.lang.Long,
     * java.util.Map, int, org.voltdb.ParameterSet,
     * edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext)
     */
    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        DependencySet result = null;
        switch (fragmentId) {
            case SysProcFragmentId.PF_stopCopyDistribute: {
                int coordinator = (int) params.toArray()[0];
                String partition_plan = (String) params.toArray()[1];
                ReconfigurationProtocols reconfig_protocol = ReconfigurationProtocols.STOPCOPY;

                
                try {
                    //TODO do work
                    LOG.info(String.format("sleeping"));
                    Thread.sleep(1000);
                } catch (Exception ex) {
                    throw new ServerFaultException(ex.getMessage(), txn_id);
                }

                VoltTable vt = new VoltTable(nodeResultsColumns);

                vt.addRow(executor.getPartitionId());

                result = new DependencySet(SysProcFragmentId.PF_stopCopyDistribute, vt);
                break;
            }
            case SysProcFragmentId.PF_stopCopyAggregate: {
                LOG.info("Combining results");
                List<VoltTable> partitionResults = dependencies.get(SysProcFragmentId.PF_stopCopyDistribute);
                if (partitionResults == null || partitionResults.isEmpty()) {
                    String msg = "Missing partition results";
                    throw new ServerFaultException(msg, txn_id);
                }

                VoltTable vt = VoltTableUtil.union(partitionResults);
                result = new DependencySet(SysProcFragmentId.PF_stopCopyAggregate, vt);
                break;
            }
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);

        }

        return (result);
    }

    public VoltTable[] run(int coordinator, String partition_plan) {

        LOG.info(String.format("RUN : Init stopCopy. Coordinator:%s  Partition plan to %s. Protocol:%s", coordinator, partition_plan));
        ParameterSet params = new ParameterSet();

        params.setParameters(coordinator, partition_plan);
        return this.executeOncePerPartition(SysProcFragmentId.PF_stopCopyDistribute, SysProcFragmentId.PF_stopCopyAggregate, params);
    }

}
