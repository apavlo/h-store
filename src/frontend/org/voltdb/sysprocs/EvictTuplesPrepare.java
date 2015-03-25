package org.voltdb.sysprocs;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.profilers.AntiCacheManagerProfiler;
import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.catalog.Table;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.types.TimestampType;

import java.util.List;
import java.util.Map;

import static edu.brown.logging.LoggerUtil.LoggerBoolean;
import static edu.brown.profilers.AntiCacheManagerProfiler.EvictionHistory;
import static org.voltdb.VoltTable.ColumnInfo;

/**
 * Created by sam on 1/12/15.
 */
@ProcInfo(
        partitionParam = 0,
        singlePartition = true
)
public class EvictTuplesPrepare extends VoltSystemProcedure {

    private static final Logger LOG = Logger.getLogger(EvictTuplesPrepare.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    public static final ColumnInfo ResultsColumns[] = {
            new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo("PARTITION", VoltType.INTEGER),
            new ColumnInfo("TABLE", VoltType.STRING),
            new ColumnInfo("ANTICACHE_TUPLES_EVICTED", VoltType.INTEGER),
            new ColumnInfo("ANTICACHE_BLOCKS_EVICTED", VoltType.INTEGER),
            new ColumnInfo("ANTICACHE_BYTES_EVICTED", VoltType.BIGINT),
            new ColumnInfo("CREATED", VoltType.TIMESTAMP),
    };

    @Override
    public void initImpl() {
        // TODO: change the fragment id?
        executor.registerPlanFragment(SysProcFragmentId.PF_antiCacheEviction, this);
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer,List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        // TODO: change the fragment id?
        assert(fragmentId == SysProcFragmentId.PF_antiCacheEviction);
        throw new IllegalAccessError("Invalid invocation of " + this.getClass() + ".executePlanFragment()");
    }

    public VoltTable[] run(int partition,
                           String tableNames[],
                           String childrenTableNames[],
                           long blockSizes[],
                           int numBlocks[]) {
        ExecutionEngine ee = executor.getExecutionEngine();
        assert(tableNames.length == blockSizes.length);

        // Check Input
        if (tableNames.length == 0) {
            throw new VoltAbortException("No tables to evict were given");
        }
        Table tables[] = new Table[tableNames.length];
        Table childTables[] = new Table[tableNames.length];
        for (int i = 0; i < tableNames.length; i++) {
            tables[i] = catalogContext.database.getTables().getIgnoreCase(tableNames[i]);
            if (tables[i] == null) {
                String msg = String.format("Unknown table '%s'", tableNames[i]);
                throw new VoltAbortException(msg);
            }
            else if (!tables[i].getEvictable()) {
                String msg = String.format("Trying to evict tuples from table '%s' but it is not marked as evictable", tables[i].getName());
                throw new VoltAbortException(msg);
            }
            else if (blockSizes[i] <= 0) {
                String msg = String.format("Invalid block eviction size '%d' for table '%s'", blockSizes[i], tables[i].getName());
                throw new VoltAbortException(msg);
            }
            else if (numBlocks[i] <= 0) {
                String msg = String.format("Invalid number of blocks to evict '%d' for table '%s'", numBlocks[i], tables[i].getName());
                throw new VoltAbortException(msg);
            }
        }

        Long prepareTxnId = getTransactionId();
        if (prepareTxnId == null) {
            throw new VoltAbortException("Unexpected error: anticache eviction prepare transaction id is null.");
        }
        // TODO: check error condition. throws EEException as in trackingEnable()?
        ee.antiCacheEvictBlockPrepareInit(prepareTxnId);

        for (int i = 0; i < tableNames.length; i++) {
            if (hstore_conf.site.anticache_batching){
                if (childrenTableNames.length!=0 && !childrenTableNames[i].isEmpty()){
                    childTables[i] = catalogContext.database.getTables().getIgnoreCase(childrenTableNames[i]);
                    ee.antiCacheEvictBlockPrepareInBatch(prepareTxnId, tables[i], childTables[i], blockSizes[i], numBlocks[i]);
                } else {
                    ee.antiCacheEvictBlockPrepare(prepareTxnId, tables[i], blockSizes[i], numBlocks[i]);
                }
            }else{
                ee.antiCacheEvictBlockPrepare(prepareTxnId, tables[i], blockSizes[i], numBlocks[i]);
            }
        }

        //return new VoltTable[] {};
        return null;
    }
}
