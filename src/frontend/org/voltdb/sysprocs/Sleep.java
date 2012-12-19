package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.utils.ThreadUtil;

@ProcInfo(singlePartition = false)
public class Sleep extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(Sleep.class);
    
    @Override
    public void initImpl() {
        // Nothing
    }
    
    @Override
    public DependencySet executePlanFragment(Long txnId,
                                             Map<Integer,
                                             List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             SystemProcedureExecutionContext context) {
        // This should never get invoked
        return null;
    }
    
    public VoltTable[] run(long sleepTime, VoltTable data[]) {
        
        LOG.debug(String.format("BEFORE: Sleeping for %.01f seconds", sleepTime / 1000d));
        ThreadUtil.sleep(sleepTime);
        LOG.debug("BEFORE: Awake!");
        
        return HStoreConstants.EMPTY_RESULT;
    }
}
