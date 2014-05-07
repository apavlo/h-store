/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ProcedureProfiler;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.Database;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.estimators.fixed.AbstractFixedEstimator;
import edu.brown.hstore.estimators.markov.MarkovEstimator;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.Workload;

/**
 * Main Wrapper Class for an HStoreSite
 * @author pavlo
 */
public abstract class HStore {
    private static final Logger LOG = Logger.getLogger(HStore.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static HStoreSite singleton;
    private static String buildString;
    private static String versionString;
    private static boolean m_ariesRecovery;
    
    
    /**
     * Retrieve a reference to the main HStoreSite running in this JVM. 
     * When running a real server (and not a test harness), this instance will only
     * be useful after calling HStore.initialize().
     *
     * @return A reference to the underlying HStoreSite object.
     */
    public static final HStoreSite instance() {
        return singleton;
    }
    
    /**
     * Exit the process, dumping any useful info and notifying any
     * important parties beforehand.
     *
     * For now, just die.
     */
    public static void crashDB() {
//        if (HStore.instance().ignoreCrash()) {
//            return;
//        }
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        StackTraceElement[] myTrace = traces.get(Thread.currentThread());
        for (StackTraceElement t : myTrace) {
            System.err.println(t.toString());
        }
    
        HStoreSite handle = instance();
        if (handle != null) {
            handle.shutdown();
            System.exit(-1);
            // FIXME handle.getHStoreCoordinator().shutdownCluster();
        } else {
            System.err.println("H-Store has encountered an unrecoverable error and is exiting.");
            System.err.println("The log may contain additional information.");
            System.exit(-1);
        }
    }
    
    
    /**
     * Initialize the HStore server.
     */
    public synchronized static final HStoreSite initialize(CatalogContext catalogContext, int site_id, HStoreConf hstore_conf) {
        singleton = new HStoreSite(site_id, catalogContext, hstore_conf);
        
        // For every partition in our local site, we want to setup a new ExecutionSite
        // Thankfully I had enough sense to have PartitionEstimator take in the local partition
        // as a parameter, so we can share a single instance across all ExecutionSites
        PartitionEstimator p_estimator = singleton.getPartitionEstimator();
        
        // ----------------------------------------------------------------------------
        // MarkovGraphs
        // ----------------------------------------------------------------------------
        Map<Integer, MarkovGraphsContainer> markovs = null;
        if (hstore_conf.site.markov_path != null) {
            File path = new File(hstore_conf.site.markov_path);
            if (path.exists()) {
                long start = System.currentTimeMillis();
                try {
                    markovs = MarkovGraphsContainerUtil.loadIds(catalogContext, path, 
                                                                singleton.getLocalPartitionIds());
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                MarkovGraphsContainerUtil.setHasher(markovs, singleton.getPartitionEstimator().getHasher());
                long load_time = System.currentTimeMillis() - start;
                LOG.info(String.format("Finished loading %s '%s' in %.1f sec",
                         MarkovGraphsContainer.class.getSimpleName(), path, (load_time / 1000d)));
            } else if (debug.val) LOG.warn("The Markov Graphs file '" + path + "' does not exist");
            ThreadUtil.shutdownGlobalPool(); // HACK
        }
        
        // ----------------------------------------------------------------------------
        // ParameterMappings
        // ----------------------------------------------------------------------------
        ParameterMappingsSet mappings = new ParameterMappingsSet(); 
        if (hstore_conf.site.mappings_path != null) {
            File path = new File(hstore_conf.site.mappings_path);
            if (path.exists()) {
                Database catalog_db = catalogContext.database;
                try {
                    mappings.load(path, catalog_db);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else if (debug.val) LOG.warn("The ParameterMappings file '" + path + "' does not exist");
        }
        
        // ----------------------------------------------------------------------------
        // PartitionExecutor Initialization
        // ----------------------------------------------------------------------------
        boolean first = true;
        for (int local_partition : singleton.getLocalPartitionIds().values()) {
            MarkovGraphsContainer local_markovs = null;
            if (markovs != null) {
                if (markovs.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) {
                    local_markovs = markovs.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
                } else {
                    local_markovs = markovs.get(local_partition);
                }
                assert(local_markovs != null) : 
                    "Failed to retrieve MarkovGraphsContainer for partition #" + local_partition;
            }

            // Initialize TransactionEstimator stuff
            // Load the Markov models if we were given an input path and pass them to t_estimator
            // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
            // stick them into the HStoreSite
            if (debug.val)
                LOG.debug(String.format("Creating %s for %s",
                          TransactionEstimator.class.getSimpleName(), singleton.getSiteName()));
            TransactionEstimator t_estimator = null;
            if (hstore_conf.site.markov_enable) {
                if (hstore_conf.site.markov_fixed == false && markovs != null) {
                    t_estimator = new MarkovEstimator(catalogContext, p_estimator, local_markovs);
                } else if (hstore_conf.site.markov_fixed) {
                    t_estimator = AbstractFixedEstimator.factory(p_estimator, singleton.getCatalogContext());
                }
            }
            if (first && t_estimator != null)
                LOG.info(String.format("All incoming txn requests will be processed with %s at this site",
                         t_estimator.getClass().getSimpleName()));

            // setup the EE
            if (debug.val)
                LOG.debug(String.format("Creating %s for Partition #%02d",
                          PartitionExecutor.class.getSimpleName(), local_partition));
            PartitionExecutor executor = new PartitionExecutor(
                                                local_partition,
                                                singleton.getCatalogContext(),
                                                BackendTarget.NATIVE_EE_JNI, // BackendTarget.NULL,
                                                p_estimator,
                                                t_estimator);
            singleton.addPartitionExecutor(local_partition, executor);
            first = false;
        } // FOR
        
        TheHashinator.initialize(catalogContext.catalog);
        
        return (singleton);
    }
    
    /**
     * Main Start-up Method
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        if (ClassUtil.isAssertsEnabled()) {
            LOG.warn("\n" + HStore.getAssertWarning());
        }
        
        ArgumentsParser args = ArgumentsParser.load(vargs,
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_SITE_ID,
            ArgumentsParser.PARAM_CONF
        );
        
        // HStoreSite Stuff
        final int site_id = args.getIntParam(ArgumentsParser.PARAM_SITE_ID);
        Thread t = Thread.currentThread();
        t.setName(HStoreThreadManager.getThreadName(site_id, null, "main"));
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args);
        if (debug.val) 
            LOG.debug("HStoreConf Parameters:\n" + HStoreConf.singleton().toString(true));
        
        HStoreSite hstore_site = HStore.initialize(args.catalogContext, site_id, hstore_conf);

        // ----------------------------------------------------------------------------
        // Workload Trace Output
        // ----------------------------------------------------------------------------
        if (args.hasParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT)) {
            ProcedureProfiler.profilingLevel = ProcedureProfiler.Level.INTRUSIVE;
            String traceClass = Workload.class.getName();
            String tracePath = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT) + "-" + site_id;
            String traceIgnore = args.getParam(ArgumentsParser.PARAM_WORKLOAD_PROC_EXCLUDE);
            ProcedureProfiler.initializeWorkloadTrace(args.catalog, traceClass, tracePath, traceIgnore);
            LOG.info("Enabled workload logging '" + tracePath + "'");
        }
        
        if (args.thresholds != null) hstore_site.setThresholds(args.thresholds);
        
        // ----------------------------------------------------------------------------
        // Bombs Away!
        // ----------------------------------------------------------------------------
        if (debug.val)
            LOG.debug("Instantiating HStoreSite network connections for " + hstore_site.getSiteName());
        hstore_site.run();
    }
    
    public static String getAssertWarning() {
        String url = HStoreConstants.HSTORE_WEBSITE + "/documentation/deployment/client-configuration";
        String msg = "!!! WARNING !!!\n" +
                     "H-Store is executing with JVM asserts enabled. This will degrade runtime performance.\n" +
                     "You can disable them by setting the config option 'site.jvm_asserts' to FALSE\n" +
                     "See the online documentation for more information:\n   " + url;
        return StringBoxUtil.heavyBox(msg);
    }
    
    public static String getBuildString() {
        if (buildString == null) {
            synchronized (HStore.class) {
                if (buildString == null) readBuildInfo();
            } // SYNCH
        }
        return (buildString);
    }
    
    public static String getVersionString() {
        if (versionString == null) {
            synchronized (HStore.class) {
                if (versionString == null) readBuildInfo();
            } // SYNCH
        }
        return (versionString);
    }
    
    private static void readBuildInfo() {
        StringBuilder sb = new StringBuilder(64);
        byte b = -1;
        try {
            InputStream buildstringStream =
                ClassLoader.getSystemResourceAsStream("buildstring.txt");
            while ((b = (byte) buildstringStream.read()) != -1) {
                sb.append((char)b);
            }
            sb.append("\n");
            String parts[] = sb.toString().split(" ", 2);
            if (parts.length != 2) {
                throw new RuntimeException("Invalid buildstring.txt file.");
            }
            versionString = parts[0].trim();
            buildString = parts[1].trim();
        } catch (Exception ignored) {
            try {
                InputStream buildstringStream = new FileInputStream("version.txt");
                while ((b = (byte) buildstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                versionString = sb.toString().trim();
            }
            catch (Exception ignored2) {
                throw new RuntimeException(ignored);
            }
        } finally {
            if (buildString == null) buildString = "H-Store";
        }
        LOG.debug("Build: " + versionString + " " + buildString);
    }
}
