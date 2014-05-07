package edu.brown.api;

import java.io.File;

import org.apache.log4j.Logger;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class BenchmarkCompiler {
    private static final Logger LOG = Logger.getLogger(BenchmarkCompiler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final BenchmarkConfig m_config;
    private final AbstractProjectBuilder m_projectBuilder;
    private final HStoreConf hstore_conf;
    
    public BenchmarkCompiler(BenchmarkConfig config, AbstractProjectBuilder projectBuilder, HStoreConf hstore_conf) {
        this.m_config = config;
        this.m_projectBuilder = projectBuilder;
        this.hstore_conf = hstore_conf;
    }
    
    /**
     * COMPILE BENCHMARK JAR
     */
    public boolean compileBenchmark(File m_jarFileName) {
        LOG.info(String.format("Compiling %s benchmark project jar",
                 this.m_projectBuilder.getProjectName().toUpperCase()));
        
        if (m_config.hosts.length == 0) {
            m_config.hosts = new String[] { hstore_conf.global.defaulthost };
        }
        
        if (m_config.deferrable != null) {
            for (String entry : m_config.deferrable) {
                String parts[] = entry.split("\\.");
                assert(parts.length == 2) :
                    "Invalid deferrable entry '" + entry + "'";
                
                String procName = parts[0];
                String stmtName = parts[1];
                assert(procName.isEmpty() == false) :
                    "Invalid procedure name in deferrable entry '" + entry + "'";
                assert(stmtName.isEmpty() == false) :
                    "Invalid statement name in deferrable entry '" + entry + "'";
                m_projectBuilder.markStatementDeferrable(procName, stmtName);
                if (debug.val) LOG.debug(String.format("Marking %s.%s as deferrable in %s",
                                                         procName, stmtName, m_projectBuilder.getProjectName())); 
            } // FOR
        }
        if (m_config.evictable != null) {
            for (String tableName : m_config.evictable) {
                if (tableName.isEmpty()) continue;
                m_projectBuilder.markTableEvictable(tableName);
                if (debug.val) LOG.debug(String.format("Marking table %s as evictable in %s",
                                                         tableName, m_projectBuilder.getProjectName())); 
            } // FOR
        }
        if (m_config.batchEvictable != null) {
            for (String tableName : m_config.batchEvictable) {
                if (tableName.isEmpty()) continue;
                m_projectBuilder.markTableBatchEvictable(tableName);
                LOG.info(String.format("Marking table %s as batch evictable in %s",
                                                         tableName, m_projectBuilder.getProjectName())); 
            } // FOR
        }
        
        boolean success = m_projectBuilder.compile(m_jarFileName.getAbsolutePath(),
                                                   m_config.sitesPerHost,
                                                   m_config.hosts.length,
                                                   m_config.k_factor,
                                                   m_config.hosts[0]);
        return (success);
    }
    
}
