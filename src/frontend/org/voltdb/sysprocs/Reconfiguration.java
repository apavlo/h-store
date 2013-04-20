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
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;

/**
 * Initiate a reconfiguration
 * 
 * @author aelmore
 * 
 */
@ProcInfo(singlePartition = false)
public class Reconfiguration extends VoltSystemProcedure {

  private static final Logger LOG = Logger.getLogger(Reconfiguration.class);

  public static final ColumnInfo nodeResultsColumns[] = { new ColumnInfo("SITE", VoltType.INTEGER) };

  /*
   * (non-Javadoc)
   * 
   * @see org.voltdb.VoltSystemProcedure#initImpl()
   */
  @Override
  public void initImpl() {
    executor.registerPlanFragment(SysProcFragmentId.PF_reconfigurationDistribute, this);
    executor.registerPlanFragment(SysProcFragmentId.PF_reconfigurationAggregate, this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.voltdb.VoltSystemProcedure#executePlanFragment(java.lang.Long,
   * java.util.Map, int, org.voltdb.ParameterSet,
   * edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext)
   */
  @Override
  public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params,
      SystemProcedureExecutionContext context) {
    DependencySet result = null;
    switch (fragmentId) {
    case SysProcFragmentId.PF_reconfigurationDistribute: {
      int coordinator = (int) params.toArray()[0];
      String partition_plan = (String) params.toArray()[1];
      String reconfiguration_protocol_string = (String) params.toArray()[2];

      LOG.info(String.format(""));

      try {
        ReconfigurationProtocols reconfig_protocol = ReconfigurationProtocols.valueOf(reconfiguration_protocol_string.toUpperCase());
        hstore_site.initReconfiguration(coordinator, partition_plan, reconfig_protocol);
      } catch (Exception ex) {
        throw new ServerFaultException(ex.getMessage(), txn_id);
      }

      VoltTable vt = new VoltTable(nodeResultsColumns);

      vt.addRow(hstore_site.getSiteId());

      result = new DependencySet(SysProcFragmentId.PF_reconfigurationDistribute, vt);
      break;
    }
    case SysProcFragmentId.PF_reconfigurationAggregate: {
      LOG.info("Combining results");
      List<VoltTable> siteResults = dependencies.get(SysProcFragmentId.PF_reconfigurationDistribute);
      if (siteResults == null || siteResults.isEmpty()) {
        String msg = "Missing site results";
        throw new ServerFaultException(msg, txn_id);
      }

      VoltTable vt = VoltTableUtil.union(siteResults);
      result = new DependencySet(SysProcFragmentId.PF_reconfigurationAggregate, vt);
      break;
    }
    default:
      String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
      throw new ServerFaultException(msg, txn_id);

    }

    return (result);
  }

  public VoltTable[] run(int coordinator, String partition_plan, String protocol) {

    LOG.info(String.format("RUN : Init reconfiguration. Coordinator:%s  Partition plan to %s. Protocol:%s", coordinator, partition_plan,
        protocol));
    ParameterSet params = new ParameterSet();

    params.setParameters(coordinator, partition_plan, protocol);
    return this.executeOncePerSite(SysProcFragmentId.PF_reconfigurationDistribute, SysProcFragmentId.PF_reconfigurationAggregate,
        params);
  }
}
