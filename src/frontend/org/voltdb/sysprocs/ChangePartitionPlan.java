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

import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.PlannedHasher;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.utils.ThreadUtil;

/**
 * Change the partition plan for each PartitionExecutor
 * 
 * @author aelmore
 * 
 */
@ProcInfo(singlePartition = false)
public class ChangePartitionPlan extends VoltSystemProcedure {
  private static final Logger LOG = Logger.getLogger(ChangePartitionPlan.class);

  public static final ColumnInfo nodeResultsColumns[] = { new ColumnInfo("SITE", VoltType.INTEGER) };

  /*
   * (non-Javadoc)
   * 
   * @see org.voltdb.VoltSystemProcedure#initImpl()
   */
  @Override
  public void initImpl() {
    executor.registerPlanFragment(SysProcFragmentId.PF_changePartitionPlanDistribute, this);
    executor.registerPlanFragment(SysProcFragmentId.PF_changePartitionPlanAggregate, this);
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
    case SysProcFragmentId.PF_changePartitionPlanDistribute: {
      String partition_plan = (String) params.toArray()[0];
      LOG.info(String.format("executePlanFragment : Changing partition plan. Site: %s Partition Plan: %s", hstore_site.getSiteId(),
          partition_plan));
      AbstractHasher hasher = hstore_site.getHasher();
      if (hasher instanceof PlannedHasher) {
        LOG.info("Updating plannedHasher");
        //((PlannedHasher)hasher)ChangePartitionPlan.
        try{
          ((PlannedHasher)hasher).changePartitionPhase(partition_plan);
        }
        catch(Exception ex){
          throw new ServerFaultException(ex.getMessage(), txn_id);
        }
      }
      VoltTable vt = new VoltTable(nodeResultsColumns);

      vt.addRow(hstore_site.getSiteId());
      /*
       * for (int p : hstore_site.getLocalPartitionIds().values()) {
       * AbstractHasher hasher = hstore_site.getHasher(); if (hasher instanceof
       * PlannedHasher) { LOG.info("Updating plannedHasher"); // hasher.set } }
       */
      result = new DependencySet(SysProcFragmentId.PF_changePartitionPlanDistribute, vt);
      break;
    }
    case SysProcFragmentId.PF_changePartitionPlanAggregate: {
      LOG.info("Combining results");
      List<VoltTable> siteResults = dependencies.get(SysProcFragmentId.PF_changePartitionPlanDistribute);
      if (siteResults == null || siteResults.isEmpty()) {
        String msg = "Missing site results";
        throw new ServerFaultException(msg, txn_id);
      }

      VoltTable vt = VoltTableUtil.union(siteResults);
      result = new DependencySet(SysProcFragmentId.PF_changePartitionPlanAggregate, vt);
      break;
    }
    default:
      String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
      throw new ServerFaultException(msg, txn_id);

    }

    return (result);
  }

  public VoltTable[] run(String partition_plan) {

    LOG.info(String.format("RUN : Changing partition plan to %s", partition_plan));
    ParameterSet params = new ParameterSet();
    params.setParameters(partition_plan);
    return this.executeOncePerSite(SysProcFragmentId.PF_changePartitionPlanDistribute, SysProcFragmentId.PF_changePartitionPlanAggregate,
        params);
  }
}
