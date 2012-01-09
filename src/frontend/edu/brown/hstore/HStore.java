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
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ExecutionSite;
import org.voltdb.ProcedureProfiler;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.containers.MarkovGraphContainersUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.Workload;

/**
 * Main Wrapper Class for an HStoreSite
 * @author pavlo
 */
public abstract class HStore {
    public static final Logger LOG = Logger.getLogger(HStore.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static HStoreSite singleton;
    
    /**
     * Initialize the HStore server.
     */
    public synchronized static HStoreSite initialize(Site catalog_site, HStoreConf hstore_conf) {
        singleton = new HStoreSite(catalog_site, hstore_conf);
        return (singleton);
    }
    
    /**
     * Retrieve a reference to the object implementing VoltDBInterface.  When
     * running a real server (and not a test harness), this instance will only
     * be useful after calling VoltDB.initialize().
     *
     * @return A reference to the underlying VoltDBInterface object.
     */
    public static HStoreSite instance() {
        return singleton;
    }
    
    
    /**
     * Main Start-up Method
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                    ArgumentsParser.PARAM_CATALOG,
                    ArgumentsParser.PARAM_SITE_ID,
                    ArgumentsParser.PARAM_CONF
        );
        
        // HStoreSite Stuff
        final int site_id = args.getIntParam(ArgumentsParser.PARAM_SITE_ID);
        Thread t = Thread.currentThread();
        t.setName(HStoreSite.getThreadName(site_id, "main", null));
        
        final Site catalog_site = CatalogUtil.getSiteFromId(args.catalog_db, site_id);
        if (catalog_site == null) throw new RuntimeException("Invalid site #" + site_id);
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args, catalog_site);
        if (debug.get()) LOG.debug("HStoreConf Parameters:\n" + HStoreConf.singleton().toString(true));
        
        HStoreSite hstore_site = HStore.initialize(catalog_site, hstore_conf);
        
        // For every partition in our local site, we want to setup a new ExecutionSite
        // Thankfully I had enough sense to have PartitionEstimator take in the local partition
        // as a parameter, so we can share a single instance across all ExecutionSites
        PartitionEstimator p_estimator = hstore_site.getPartitionEstimator();

        // ----------------------------------------------------------------------------
        // MarkovGraphs
        // ----------------------------------------------------------------------------
        Map<Integer, MarkovGraphsContainer> markovs = null;
        if (args.hasParam(ArgumentsParser.PARAM_MARKOV)) {
            File path = new File(args.getParam(ArgumentsParser.PARAM_MARKOV));
            if (path.exists()) {
                markovs = MarkovGraphContainersUtil.loadIds(args.catalog_db, path.getAbsolutePath(), CatalogUtil.getLocalPartitionIds(catalog_site));
                MarkovGraphContainersUtil.setHasher(markovs, p_estimator.getHasher());
                LOG.info("Finished loading MarkovGraphsContainer '" + path + "'");
            } else if (debug.get()) LOG.warn("The Markov Graphs file '" + path + "' does not exist");
        }

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
        
        // ----------------------------------------------------------------------------
        // Partition Initialization
        // ----------------------------------------------------------------------------
        for (Partition catalog_part : catalog_site.getPartitions()) {
            int local_partition = catalog_part.getId();
            MarkovGraphsContainer local_markovs = null;
            if (markovs != null) {
                if (markovs.containsKey(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID)) {
                    local_markovs = markovs.get(MarkovUtil.GLOBAL_MARKOV_CONTAINER_ID);
                } else {
                    local_markovs = markovs.get(local_partition);
                }
                assert(local_markovs != null) : "Failed to get the proper MarkovGraphsContainer that we need for partition #" + local_partition;
            }

            // Initialize TransactionEstimator stuff
            // Load the Markov models if we were given an input path and pass them to t_estimator
            // HACK: For now we have to create a TransactionEstimator for all partitions, since
            // it is written under the assumption that it was going to be running at just a single partition
            // I'm not proud of this...
            // Load in all the partition-specific TransactionEstimators and ExecutionSites in order to 
            // stick them into the HStoreSite
            if (debug.get()) LOG.debug("Creating Estimator for " + HStoreSite.formatSiteName(site_id));
            TransactionEstimator t_estimator = new TransactionEstimator(p_estimator, args.param_mappings, local_markovs);

            // setup the EE
            if (debug.get()) LOG.debug("Creating ExecutionSite for Partition #" + local_partition);
            ExecutionSite executor = new ExecutionSite(
                    local_partition,
                    args.catalog,
                    BackendTarget.NATIVE_EE_JNI, // BackendTarget.NULL,
                    p_estimator,
                    t_estimator);
            hstore_site.addExecutionSite(local_partition, executor);
        } // FOR
        
        
        if (args.thresholds != null) hstore_site.setThresholds(args.thresholds);
        
        // ----------------------------------------------------------------------------
        // Bombs Away!
        // ----------------------------------------------------------------------------
        LOG.info("Instantiating HStoreSite network connections...");
        hstore_site.run();
    }
}
