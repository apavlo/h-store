package edu.mit.hstore;

import org.apache.log4j.Logger;

import edu.brown.utils.ArgumentsParser;

public final class HStoreConf {
    private static final Logger LOG = Logger.getLogger(HStoreConf.class);

    /**
     * Whether to not use the Dtxn.Coordinator
     */
    public boolean ignore_dtxn = false;

    /**
     * Whether to force all transactions to be executed as single-partitioned
     */
    public boolean force_singlepartitioned = false;
    
    /**
     * Whether all transactions should execute at the local HStoreSite (i.e., they are never redirected)
     */
    public boolean force_localexecution = false;
    
    /**
     * Assume all txns are TPC-C neworder and look directly at the parameters to figure out
     * whether it is single-partitioned or not 
     */
    public boolean force_neworder_hack = false;
    
    /**
     * Enable txn profiling
     */
    public boolean enable_profiling = false;
    
    /**
     * How many ms to wait before the ExecutionSiteHelper executes again to clean up txns
     */
    public int helper_interval = 1000;
    
    /**
     * How many txns can the ExecutionSiteHelper clean-up per Partition per Round
     */
    public int helper_txn_per_round = -1;
    
    /**
     * How long should the ExecutionSiteHelper wait before cleaning up a txn's state
     */
    public int helper_txn_expire = 1000;
    
    /**
     * Whether the VoltProcedure should crash the HStoreSite on a mispredict
     */
    public boolean mispredict_crash = false;
    
    /**
     * Constructor
     */
    private HStoreConf() {
        
    }
    
    private static HStoreConf conf;
    
    public synchronized static HStoreConf init(ArgumentsParser args) {
        if (conf != null) return (conf);
        conf = new HStoreConf();
        
        if (args != null) {
            // Force all transactions to be single-partitioned
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION)) {
                conf.force_singlepartitioned = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_SINGLEPARTITION);
                if (conf.force_singlepartitioned) LOG.info("Forcing all transactions to execute as single-partitioned");
            }
            // Force all transactions to be executed at the first partition that the request arrives on
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION)) {
                conf.force_localexecution = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_LOCALEXECUTION);
                if (conf.force_localexecution) LOG.info("Forcing all transactions to execute at the partition they arrive on");
            }
            // Enable the "neworder" parameter hashing hack for the VLDB paper
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT)) {
                conf.force_neworder_hack = args.getBooleanParam(ArgumentsParser.PARAM_NODE_FORCE_NEWORDERINSPECT);
                if (conf.force_neworder_hack) LOG.info("Enabling the inspection of incoming neworder parameters");
            }
            // Clean-up Interval
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL)) {
                conf.helper_interval = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_INTERVAL);
                LOG.info("Setting Cleanup Interval = " + conf.helper_interval + "ms");
            }
            // Txn Expiration Time
            if (args.hasIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE)) {
                conf.helper_txn_expire = args.getIntParam(ArgumentsParser.PARAM_NODE_CLEANUP_TXN_EXPIRE);
                LOG.info("Setting Cleanup Txn Expiration = " + conf.helper_txn_expire + "ms");
            }
            // Profiling
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING)) {
                conf.enable_profiling = args.getBooleanParam(ArgumentsParser.PARAM_NODE_ENABLE_PROFILING);
                if (conf.enable_profiling) LOG.info("Enabling procedure profiling");
            }
            // Mispredict Crash
            if (args.hasBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH)) {
                conf.mispredict_crash = args.getBooleanParam(ArgumentsParser.PARAM_NODE_MISPREDICT_CRASH);
                if (conf.mispredict_crash) LOG.info("Enabling crashing HStoreSite on mispredict");
            }
        }
        return (conf);
    }
    
    public static HStoreConf singleton() {
        return (HStoreConf.init(null));
    }
}
