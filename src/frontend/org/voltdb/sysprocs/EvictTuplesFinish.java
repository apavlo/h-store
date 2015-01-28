package org.voltdb.sysprocs;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.jni.ExecutionEngine;

import java.util.List;
import java.util.Map;

/**
 * Created by sam on 1/22/15.
 */
public class EvictTuplesFinish extends VoltSystemProcedure {

    private static final Logger LOG = Logger.getLogger(EvictTuplesFinish.class);
    private static final LoggerUtil.LoggerBoolean debug = new LoggerUtil.LoggerBoolean();

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

    public VoltTable[] run(int partition, long prepareTxnId) {
        ExecutionEngine ee = executor.getExecutionEngine();
        ee.anticacheEvictBlockFinish(prepareTxnId);
        return null;
    }
}
