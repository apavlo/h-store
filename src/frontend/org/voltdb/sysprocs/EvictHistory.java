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
import org.voltdb.types.SortDirectionType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.profilers.AntiCacheManagerProfiler.EvictionHistory;

/** 
 * Gather the eviction history from each partition
 */
@ProcInfo(singlePartition = false)
public class EvictHistory extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(EvictHistory.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    private static final ColumnInfo ResultsColumns[] = {
        new ColumnInfo("TIMESTAMP", VoltType.TIMESTAMP),
        new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
        new ColumnInfo("HOSTNAME", VoltType.STRING),
        new ColumnInfo("PARTITION", VoltType.INTEGER),
        new ColumnInfo("START", VoltType.BIGINT), // TIMESTAMP (MS)
        new ColumnInfo("STOP", VoltType.BIGINT), // TIMESTAMP (MS)
        new ColumnInfo("ANTICACHE_TUPLES_EVICTED", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BLOCKS_EVICTED", VoltType.INTEGER),
        new ColumnInfo("ANTICACHE_BYTES_EVICTED", VoltType.BIGINT),
    };
    
    private static final int DISTRIBUTE_ID = SysProcFragmentId.PF_anitCacheHistoryDistribute;
    private static final int AGGREGATE_ID = SysProcFragmentId.PF_anitCacheHistoryAggregate;

    @Override
    public void initImpl() {
        executor.registerPlanFragment(AGGREGATE_ID, this);
        executor.registerPlanFragment(DISTRIBUTE_ID, this);
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        DependencySet result = null;
        switch (fragmentId) {
            case DISTRIBUTE_ID: {
                VoltTable vt = new VoltTable(ResultsColumns);
                AntiCacheManagerProfiler profiler = hstore_site.getAntiCacheManager().getDebugContext().getProfiler(this.partitionId);
                assert(profiler != null);
                for (EvictionHistory eh : profiler.eviction_history) { 
                    Object row[] = {
                        new TimestampType(),
                        this.hstore_site.getSiteId(),
                        this.hstore_site.getSiteName(),
                        this.partitionId,
                        eh.startTimestamp,
                        eh.stopTimestamp,
                        eh.tuplesEvicted,
                        eh.blocksEvicted,
                        eh.bytesEvicted,
                    };
                    vt.addRow(row);
                } // FOR
                result = new DependencySet(DISTRIBUTE_ID, vt);
                if (debug.val)
                    LOG.info(String.format("%s - Sending back result for partition %d",
                             hstore_site.getTransaction(txn_id), this.executor.getPartitionId()));
                break;
            }
            // Aggregate Results
            case AGGREGATE_ID:
                List<VoltTable> siteResults = dependencies.get(DISTRIBUTE_ID);
                if (siteResults == null || siteResults.isEmpty()) {
                    String msg = "Missing site results";
                    throw new ServerFaultException(msg, txn_id);
                }
                
                Pair<Integer, SortDirectionType> sortCol = Pair.of(3, SortDirectionType.ASC);
                @SuppressWarnings("unchecked")
                VoltTable vt = VoltTableUtil.sort(VoltTableUtil.union(siteResults), sortCol);
                result = new DependencySet(AGGREGATE_ID, vt);
                break;
            default:
                String msg = "Unexpected sysproc fragmentId '" + fragmentId + "'";
                throw new ServerFaultException(msg, txn_id);
        } // SWITCH
        return (result);
    }
    
    public VoltTable[] run() {
        if (hstore_conf.site.anticache_enable == false) {
            String msg = "Unable to collect eviction history because 'site.anticache_enable' is disabled";
            throw new VoltAbortException(msg);
        }
        else if (hstore_conf.site.anticache_profiling == false) {
            String msg = "Unable to collect eviction history because 'site.anticache_profiling' is disabled";
            throw new VoltAbortException(msg);
        }
        
        ParameterSet params = new ParameterSet();
        return this.executeOncePerPartition(DISTRIBUTE_ID, AGGREGATE_ID, params);
    }
}
