/**
 * 
 */
package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;
import edu.brown.hstore.reconfiguration.ReconfigurationCoordinator;

/**
 * @author aelmore
 */
@ProcInfo(singlePartition = false)
public class StopCopy extends VoltSystemProcedure {

    private static final Logger LOG = Logger.getLogger(StopCopy.class);

    public static final ColumnInfo nodeResultsColumns[] = { new ColumnInfo("PARTITION", VoltType.INTEGER) };

    // ******** REMOVE *********** TODO (ae)

    public static final Random rand = new Random();

    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return StopCopy.randomString(minimum_length, maximum_length, 'A', 26);
    }

    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return StopCopy.randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = (int) StopCopy.number(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + StopCopy.number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    /*******
     * END REMOVE **************************************************** /*
     * (non-Javadoc)
     * 
     * @see org.voltdb.VoltSystemProcedure#initImpl()
     */
    @Override
    public void initImpl() {
        this.executor.registerPlanFragment(SysProcFragmentId.PF_stopCopyDistribute, this);
        this.executor.registerPlanFragment(SysProcFragmentId.PF_stopCopyAggregate, this);
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
                    // TODO do work
                    ReconfigurationCoordinator rc = this.hstore_site.getReconfigurationCoordinator();
                    // Set RC to start migration, may not be the first one. get
                    // partition plan.
                    ReconfigurationPlan plan = rc.initReconfiguration(coordinator, reconfig_protocol, partition_plan, this.partitionId);
                    if(plan != null){
                        assert plan.getOutgoing_ranges() != null : "reconfig plan outgoing_ranges is null";
                        List<ReconfigurationRange<? extends Comparable<?>>> outgoing_ranges = plan.getOutgoing_ranges().get(this.partitionId);
                        if (outgoing_ranges != null && outgoing_ranges.size() > 0) {
                            Table catalog_tbl = null;
                            VoltTable table = null;
                            Object row[] = null;
                            for (ReconfigurationRange<? extends Comparable<?>> range : outgoing_ranges) {
                                catalog_tbl = this.catalogContext.getTableByName(range.table_name);
                                table = org.voltdb.utils.CatalogUtil.getVoltTable(catalog_tbl);
                                row = new Object[table.getColumnCount()];
                                table.clearRowData();
                                int sleep_sec = rand.nextInt(10);
                                LOG.info(String.format("sleeping for %s seconds", sleep_sec));
                                Thread.sleep(sleep_sec * 1000);
                                // TODO range.table_name (ae)
    
                                // TODO (ae) how to iterate? or do we even need to
                                // since
                                // we will push down range
                                // to ee to get table
                                assert (range.getMin_inclusive() instanceof Long);
                                for (Long i = (Long) range.getMin_inclusive(); i < (Long) range.getMax_exclusive(); i++) {
                                    row[0] = i;
    
                                    // randomly generate strings for each column
                                    for (int col = 2; col < row.length; col++) {
                                        row[col] = StopCopy.astring(100, 100);
                                    } // FOR
                                    table.addRow(row);
                                }
                                rc.pushTuples(range.old_partition, range.new_partition, range.table_name, table);
                            }
                        } else {
                            LOG.info("No outgoing ranges for this partition");
                        }
                        rc.finishReconfiguration(this.partitionId);
                    }
                    else{
                        LOG.info("No reconfiguration to initiate");
                    }
                } catch (Exception ex) {
                    LOG.error("Failure on stop and copy", ex);
                    throw new ServerFaultException(ex.getMessage(), txn_id);
                }

                VoltTable vt = new VoltTable(nodeResultsColumns);

                vt.addRow(this.executor.getPartitionId());

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

        LOG.info(String.format("RUN : Init stopCopy. Coordinator:%s  Partition plan to %s. ", coordinator, partition_plan));
        ParameterSet params = new ParameterSet();

        params.setParameters(coordinator, partition_plan);
        return this.executeOncePerPartition(SysProcFragmentId.PF_stopCopyDistribute, SysProcFragmentId.PF_stopCopyAggregate, params);
    }

}
