/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
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
/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package edu.brown.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.processtools.ProcessSetManager;
import org.voltdb.processtools.SSHTools;
import org.voltdb.sysprocs.DatabaseDump;
import org.voltdb.sysprocs.EvictHistory;
import org.voltdb.sysprocs.EvictedAccessHistory;
import org.voltdb.sysprocs.ExecutorStatus;
import org.voltdb.sysprocs.GarbageCollection;
import org.voltdb.sysprocs.MarkovUpdate;
import org.voltdb.sysprocs.NoOp;
import org.voltdb.sysprocs.Quiesce;
import org.voltdb.sysprocs.ResetProfiling;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.api.BenchmarkControllerUtil.ProfilingOutput;
import edu.brown.api.results.BenchmarkResults;
import edu.brown.api.results.CSVResultsPrinter;
import edu.brown.api.results.JSONResultsPrinter;
import edu.brown.api.results.MemoryStatsPrinter;
import edu.brown.api.results.AnticacheMemoryStatsPrinter;
import edu.brown.api.results.TableStatsPrinter;
import edu.brown.api.results.ResponseEntries;
import edu.brown.api.results.ResultsChecker;
import edu.brown.api.results.ResultsPrinter;
import edu.brown.api.results.ResultsUploader;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

public class BenchmarkController {
    public static final Logger LOG = Logger.getLogger(BenchmarkController.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ============================================================================
    // STATIC CONFIGURATION
    // ============================================================================

    /**
     * HStoreConf parameters to not forward to clients 
     */
    private static final String EXCLUDED_CLIENT_PARAMS[] = {
        "client.jvm_args"
    };
    
    // ============================================================================
    // INSTANCE MEMBERS
    // ============================================================================
    
    /** Clients **/
    final ProcessSetManager clientPSM;
    
    /** Server Sites **/
    final ProcessSetManager sitePSM;
    
    Thread self = null;
    boolean stop = false;
    boolean failed = false;
    boolean cleaned = false;
    final BenchmarkConfig m_config;
    HStoreConf hstore_conf;
    BenchmarkResults currentResults = null;
    
    // ----------------------------------------------------------------------------
    // CLIENT INFORMATION
    // ----------------------------------------------------------------------------
    final List<String> m_clients = new ArrayList<String>();
    final List<String> m_clientThreads = new ArrayList<String>();
    final AtomicInteger m_clientsNotReady = new AtomicInteger(0);
    final Collection<BenchmarkInterest> m_interested = new HashSet<BenchmarkInterest>();
    
    // ----------------------------------------------------------------------------
    // STATUS THREADS
    // ----------------------------------------------------------------------------
    final List<ClientStatusThread> m_statusThreads = new ArrayList<ClientStatusThread>();
    final AtomicBoolean m_statusThreadShouldContinue = new AtomicBoolean(true);

    
    /**
     * BenchmarkResults Processing
     */
    protected int m_pollIndex = 0;
    protected long m_maxCompletedPoll = 0;
    protected final long m_pollCount;
    private int totalNumClients;
    protected CountDownLatch resultsToRead;
    ResultsUploader resultsUploader = null;
    protected PeriodicEvictionThread evictorThread;
    private final ScheduledThreadPoolExecutor threadPool;

    Class<? extends BenchmarkComponent> m_clientClass = null;
    Class<? extends AbstractProjectBuilder> m_builderClass = null;
    Class<? extends BenchmarkComponent> m_loaderClass = null;

    final AbstractProjectBuilder projectBuilder;
    final File jarFileName;
    
    /**
     * SiteId -> Set[Host, Port]
     */
    Map<Integer, Set<Pair<String, Integer>>> m_launchHosts;
    
    private CatalogContext catalogContext;
    
    /**
     * Keeps track of any files to send to clients
     */
    private final BenchmarkClientFileUploader m_clientFileUploader = new BenchmarkClientFileUploader();
    private final AtomicInteger m_clientFilesUploaded = new AtomicInteger(0);
    
    // ----------------------------------------------------------------------------
    // FAILURE HANDLERS
    // ----------------------------------------------------------------------------
    
    /**
     * UncaughtExceptionHandler
     * This just forwards the error message to the failure EventObserver
     */
    private final EventObservableExceptionHandler exceptionHandler = new EventObservableExceptionHandler();
    {
        this.exceptionHandler.addObserver(new EventObserver<Pair<Thread,Throwable>>() {
            final EventObservable<String> inner = new EventObservable<String>();
            {
                inner.addObserver(failure_observer);
            }
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> arg) {
                Thread thread = arg.getFirst();
                Throwable throwable = arg.getSecond();
                LOG.error(String.format("Unexpected error from %s", thread.getName()), throwable);
                String msg = String.format("%s - %s", thread.getName(), throwable.getMessage());
                inner.notifyObservers(msg);
            }
        });
    }
    
    /**
     * Failure EventObserver 
     */
    private final EventObserver<String> failure_observer = new EventObserver<String>() {
        final AtomicBoolean lock = new AtomicBoolean(false);
        
        @Override
        public void update(EventObservable<String> o, String msg) {
            if (lock.compareAndSet(false, true)) {
                LOG.fatal(msg);
                BenchmarkController.this.stop = true;
                BenchmarkController.this.failed = true;
                
                // Stop all the benchmark interests
                for (BenchmarkInterest bi : m_interested) {
                    bi.stop();
                } // FOR
                
                ThreadUtil.sleep(1500);
                clientPSM.prepareShutdown(false);
                sitePSM.prepareShutdown(false);
                if (self != null) BenchmarkController.this.self.interrupt();
            }
        }
    };
    
    @SuppressWarnings("unchecked")
    public BenchmarkController(BenchmarkConfig config, CatalogContext catalogContext) {
        this.m_config = config;
        this.self = Thread.currentThread();
        this.hstore_conf = HStoreConf.singleton();
        if (catalogContext != null) this.initializeCatalog(catalogContext);
        
        // Setup ProcessSetManagers...
        this.clientPSM = new ProcessSetManager(hstore_conf.client.log_dir,
                                               hstore_conf.client.log_backup,
                                               0,
                                               this.failure_observer);
        this.sitePSM = new ProcessSetManager(hstore_conf.site.log_dir,
                                             hstore_conf.site.log_backup,
                                             config.client_initialPollingDelay,
                                             this.failure_observer);

        Map<String, Field> builderFields = new HashMap<String, Field>();
        builderFields.put("m_clientClass", null);
        builderFields.put("m_loaderClass", null);
        
        try {
            m_builderClass = (Class<? extends AbstractProjectBuilder>)Class.forName(m_config.projectBuilderClass);
        } catch (Exception ex) {
            LOG.fatal(String.format("Failed load class for ProjectBuilder '%s'", m_config.projectBuilderClass), ex);
            throw new RuntimeException(ex);
        }
            
        for (String fieldName : builderFields.keySet()) {
            try {
                //Hackish, client expected to have these field as a static member
                Field f = m_builderClass.getField(fieldName);
                builderFields.put(fieldName, f);
            } catch (NoSuchFieldError ex) {
                LOG.fatal(String.format("ProjectBuilder '%s' is missing field '%s'", m_config.projectBuilderClass, fieldName), ex);
                throw new RuntimeException(ex);
            } catch (Exception ex) {
                LOG.fatal(String.format("Unexpected error in ProjectBuilder '%s' when retrieving field '%s'", m_config.projectBuilderClass, fieldName), ex);
                throw new RuntimeException(ex);
            }
        }
        try {
            m_clientClass = (Class<? extends BenchmarkComponent>)builderFields.get("m_clientClass").get(null);
            m_loaderClass = (Class<? extends BenchmarkComponent>)builderFields.get("m_loaderClass").get(null);
//            if (m_config.localmode == false) {
//                this.jarFileName = config.hosts[0] + "." + this.jarFileName;
//            }
        } catch (Exception e) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_ErrorDuringReflectionForClient;
            LOG.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { m_config.projectBuilderClass }, e);
            System.exit(-1);
        }

        m_pollCount = hstore_conf.client.duration / hstore_conf.client.interval;
        resultsUploader = new ResultsUploader(m_config.projectBuilderClass, config);

        AbstractProjectBuilder tempBuilder = null;
        try {
            tempBuilder = m_builderClass.newInstance();
        } catch (Exception e) {
            LogKeys logkey =
                LogKeys.benchmark_BenchmarkController_UnableToInstantiateProjectBuilder;
            LOG.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { m_builderClass.getSimpleName() }, e);
            System.exit(-1);
        }
        assert(tempBuilder != null);
        this.projectBuilder = tempBuilder;
        this.projectBuilder.addAllDefaults();
        this.jarFileName = FileUtil.join(hstore_conf.client.jar_dir, this.projectBuilder.getJarName(false));
        assert(this.jarFileName.getPath().isEmpty() == false) :
            "Invalid Project jar file name '" + this.jarFileName + "'";

        if (config.snapshotFrequency != null
                && config.snapshotPath != null
                && config.snapshotPrefix != null
                && config.snapshotRetain > 0) {
            this.projectBuilder.setSnapshotSettings(
                    config.snapshotFrequency,
                    config.snapshotRetain,
                    config.snapshotPath,
                    config.snapshotPrefix);
        }
        
        this.threadPool = ThreadUtil.getScheduledThreadPoolExecutor(
                "benchmark",
                this.exceptionHandler,
                2, 1024 * 128);
    }
    
    private void initializeCatalog(CatalogContext catalogContext) {
        assert(catalogContext != null);
        this.catalogContext = catalogContext;
        int total_num_clients = m_config.clients.length * hstore_conf.client.threads_per_host;
        if (hstore_conf.client.processesperclient_per_partition) {
            total_num_clients *= catalogContext.numberOfPartitions;
        }
        this.totalNumClients = total_num_clients;
        int num_results = (int)(m_pollCount * this.totalNumClients);
        if (hstore_conf.client.output_responses != null) num_results += this.totalNumClients;
        this.resultsToRead = new CountDownLatch(num_results);
    }
    
    private String makeHeader(String label) {
        return (StringUtil.SET_BOLD_TEXT +
                StringUtil.header(label.toUpperCase() + " :: " + this.getProjectName()) +
                StringUtil.SET_PLAIN_TEXT);
    }
    
    public String getProjectName() {
        return (this.projectBuilder.getProjectName().toUpperCase());
    }
    
    public Catalog getCatalog() {
        return (this.catalogContext.catalog);
    }
    
    protected BenchmarkResults getBenchmarkResults() {
        return (this.currentResults);
    }
    protected CountDownLatch getResultsToReadLatch() {
        return (this.resultsToRead);
    }
    protected ProcessSetManager getClientProcessSetManager() {
        return (this.clientPSM);
    }
    protected ProcessSetManager getSiteProcessSetManager() {
        return (this.sitePSM);
    }
    protected void clientIsReady(String clientName) {
        m_clientsNotReady.decrementAndGet();
    }
    protected Collection<BenchmarkInterest> getBenchmarkInterests() {
        return (this.m_interested);
    }

    public void registerInterest(BenchmarkInterest interest) {
        synchronized(m_interested) {
            m_interested.add(interest);
        }
    }
    
    /**
     * SETUP BENCHMARK 
     */
    public void setupBenchmark() {
        // Load the catalog that we just made
        if (debug.val) LOG.debug("Loading catalog from '" + this.jarFileName + "'");
        this.initializeCatalog(CatalogUtil.loadCatalogContextFromJar(this.jarFileName));
        
        // Clear out any HStoreConf parameters that we're not suppose to
        // forward to the sites and clients
        for (String key : EXCLUDED_CLIENT_PARAMS)
            m_config.clientParameters.remove(key);
        
        // Now figure out which hosts we really want to launch this mofo on
        Set<String> unique_hosts = new HashSet<String>();
        if (m_config.useCatalogHosts == false) {
            if (debug.val) LOG.debug("Creating host information from BenchmarkConfig");
            m_launchHosts = new HashMap<Integer, Set<Pair<String,Integer>>>();
            int site_id = HStoreConstants.FIRST_PARTITION_ID;
            for (String host : m_config.hosts) {
                if (trace.val) LOG.trace(String.format("Creating host info for %s: %s:%d",
                                                         HStoreThreadManager.formatSiteName(site_id), host, HStoreConstants.DEFAULT_PORT));
                
                Set<Pair<String, Integer>> s = new HashSet<Pair<String,Integer>>();
                s.add(Pair.of(host, HStoreConstants.DEFAULT_PORT));
                m_launchHosts.put(site_id, s);
                unique_hosts.add(host);
                site_id++;
            } // FOR
        } else {
            if (debug.val) LOG.debug("Retrieving host information from catalog");
            m_launchHosts = CatalogUtil.getExecutionSites(this.catalogContext.catalog);
            for (Entry<Integer, Set<Pair<String, Integer>>> e : m_launchHosts.entrySet()) {
                Pair<String, Integer> p = CollectionUtil.first(e.getValue());
                assert(p != null);
                if (trace.val)
                    LOG.trace(String.format("Retrieved host info for %s from catalog: %s:%d",
                                           HStoreThreadManager.formatSiteName(e.getKey()),
                                           p.getFirst(), p.getSecond()));
                unique_hosts.add(p.getFirst());
            } // FOR
        }

        // copy the catalog to the servers, but don't bother in local mode
        if (m_config.localmode == false) {
            // HACK
            m_config.hosts = new String[unique_hosts.size()];
            unique_hosts.toArray(m_config.hosts);
            
            Set<String> copyto_hosts = new HashSet<String>();
            CollectionUtil.addAll(copyto_hosts, unique_hosts);
            CollectionUtil.addAll(copyto_hosts, m_config.clients);
            Collection<Thread> threads = new ArrayList<Thread>();
            
            // HStoreSite
            // IMPORTANT: Don't try to kill things if we're going to profile... for obvious reasons... duh!
            if (m_config.profileSiteIds.isEmpty() && m_config.noSites == false) {
                for (String host : unique_hosts) {
                    KillStragglers ks = new KillStragglers(m_config.remoteUser, host, m_config.remotePath, m_config.sshOptions).enableKillAll();
                    threads.add(new Thread(ks));
                    Runtime.getRuntime().addShutdownHook(new Thread(ks));
                } // FOR
            }
            // Client
            for (String host : m_config.clients) {
                KillStragglers ks = new KillStragglers(m_config.remoteUser, host, m_config.remotePath, m_config.sshOptions).enableKillClient();
                threads.add(new Thread(ks));
                Runtime.getRuntime().addShutdownHook(new Thread(ks));
            } // FOR

            
            if (debug.val) LOG.debug("Killing stragglers on " + threads.size() + " hosts");
            try {
                ThreadUtil.runNewPool(threads, Math.min(25, threads.size())); 
            } catch (Exception e) {
                LogKeys logkey = LogKeys.benchmark_BenchmarkController_UnableToRunRemoteKill;
                LOG.l7dlog(Level.FATAL, logkey.name(), e);
                LOG.fatal("Couldn't run remote kill operation.", e);
                System.exit(-1);
            }
            
            // START THE SERVERS
            if (m_config.noSites == false) {
                this.startSites();
            }
            
        } else {
            // START A SERVER LOCALLY IN-PROCESS
//            VoltDB.Configuration localconfig = new VoltDB.Configuration();
//            localconfig.m_pathToCatalog = this.jarFileName.getAbsolutePath();
//            m_localserver = null;//new ServerThread(localconfig);
//            m_localserver.start();
//            m_localserver.waitForInitialization();
        }

        
        if (m_loaderClass != null && m_config.noLoader == false) {
            ProfileMeasurement stopwatch = new ProfileMeasurement("load").start();
            this.startLoader();
            stopwatch.stop();
            if (this.failed) System.exit(1);
            LOG.info(String.format("Completed %s loading phase in %.2f sec",
                                   this.projectBuilder.getProjectName().toUpperCase(),
                                   stopwatch.getTotalThinkTimeSeconds()));
        } else if (debug.val && m_config.noLoader) {
            LOG.debug("Skipping data loading phase");
        }

        // Start the clients
        if (m_config.noExecute == false) {
            this.startClients();
        }
        
        // registerInterest(uploader);
    }
    
    /**
     * Deploy the HStoreSites on the remote nodes
     */
    public void startSites() {
        LOG.info(makeHeader("BENCHMARK INITIALIZE"));
        if (debug.val) LOG.debug("Number of hosts to start: " + m_launchHosts.size());
        int hosts_started = 0;
        
        // For each client output option, we'll enable the corresponding
        // site config parameter so that we can collect the proper data
        for (ProfilingOutput po : BenchmarkControllerUtil.PROFILING_OUTPUTS) {
            if (po.siteParam == null) continue;
            
            assert(HStoreConf.isConfParameter(po.clientParam)) : "Invalid Client Param: " + po;
            assert(HStoreConf.isConfParameter(po.siteParam)) : "Invalid Site Param: " + po;
            String clientOptVal = (String)hstore_conf.get(po.clientParam);
            if (clientOptVal != null && clientOptVal.isEmpty() == false) {
                if (clientOptVal.equalsIgnoreCase("true")) {
                    LOG.warn(String.format("The HStoreConf parameter '%s' should be a file path, not a boolean value", po.clientParam));
                } else {
                    m_config.siteParameters.put(po.siteParam, Boolean.TRUE.toString());
                }
            }
        } // FOR
        
        List<String> siteBaseCommand = new ArrayList<String>();
        if (hstore_conf.global.sshprefix != null &&
            hstore_conf.global.sshprefix.isEmpty() == false) {
            siteBaseCommand.add(hstore_conf.global.sshprefix + " && ");
        }
        siteBaseCommand.add("ant hstore-site");
        siteBaseCommand.add("-Dconf=" + m_config.hstore_conf_path);
        siteBaseCommand.add("-Dproject=" + this.projectBuilder.getProjectName());
        for (Entry<String, String> e : m_config.siteParameters.entrySet()) {
            String value = e.getValue();
            if (value.startsWith("\"") == false) {
                value = '"' + value + '"';
            }
            String opt = String.format("-D%s=%s", e.getKey(), value);
            siteBaseCommand.add(opt);
            if (trace.val) 
                LOG.trace("  " + opt);
        } // FOR

        for (Entry<Integer, Set<Pair<String, Integer>>> e : m_launchHosts.entrySet()) {
            Integer site_id = e.getKey();
            Pair<String, Integer> p = CollectionUtil.first(e.getValue());
            assert(p != null);
            String host = p.getFirst();
            String host_id = String.format("site-%02d-%s", site_id, host);
            
            // Check whether this one of the sites that will be started externally
            if (m_config.profileSiteIds.contains(site_id)) {
                LOG.info(String.format("Skipping HStoreSite %s because it will be started by profiler",
                                       HStoreThreadManager.formatSiteName(site_id)));
                continue;
            }
            
            LOG.info(String.format("Starting HStoreSite %s on %s",
                                   HStoreThreadManager.formatSiteName(site_id), host));

//            String debugString = "";
//            if (m_config.listenForDebugger) {
//                debugString =
//                    " -agentlib:jdwp=transport=dt_socket,address=8001,server=y,suspend=n ";
//            }
            // -agentlib:hprof=cpu=samples,
            // depth=32,interval=10,lineno=y,monitor=y,thread=y,force=y,
            // file=" + host + "_hprof_tpcc.txt"
            
            // Site Specific Parameters
            List<String> siteCommand = new ArrayList<String>(siteBaseCommand);
            siteCommand.add("-Dsite.id=" + site_id);

            String exec_command[] = SSHTools.convert(m_config.remoteUser, host, m_config.remotePath, m_config.sshOptions, siteCommand);
            String fullCommand = StringUtil.join(" ", exec_command);
            resultsUploader.setCommandLineForHost(host, fullCommand);
            if (trace.val) LOG.trace("START " + HStoreThreadManager.formatSiteName(site_id) + ": " + fullCommand);
            sitePSM.startProcess(host_id, exec_command);
            hosts_started++;
        } // FOR

        // WAIT FOR SERVERS TO BE READY
        int waiting = hosts_started;
        if (waiting > 0) {
            LOG.info(String.format("Waiting for %d HStoreSite%s with %d partition%s to finish initialization",
                                   waiting, (waiting > 1 ? "s" : ""),
                                   catalogContext.numberOfPartitions, (catalogContext.numberOfPartitions > 1 ? "s" : "")));
            do {
                ProcessSetManager.OutputLine line = sitePSM.nextBlocking();
                if (line == null) break;
                if (line.value.contains(HStoreConstants.SITE_READY_MSG)) {
                    waiting--;
                }
            } while (waiting > 0);
            if (waiting > 0) {
                throw new RuntimeException("Failed to start all HStoreSites. Halting benchmark");
            }
        }
        // If one of them was started on the outside, let's just sleep for a 
        // few seconds to make sure that our clients can connect
        if (m_config.profileSiteIds.isEmpty() == false) {
            int sleep = 5000;
            LOG.info(String.format("Sleeping for %.1f seconds to let the profiler come online", sleep/1000d)); 
            ThreadUtil.sleep(sleep);
        }
        
        if (debug.val) LOG.debug("All remote HStoreSites are initialized");
    }
    
    /**
     * Invoke the benchmark loader
     */
    public void startLoader() {
        LOG.info(makeHeader("BENCHMARK LOAD"));
        String title = String.format("Starting %s Benchmark Loader - %s / ScaleFactor %.2f",
                                     this.projectBuilder.getProjectName().toUpperCase(),
                                     m_loaderClass.getSimpleName(),
                                     hstore_conf.client.scalefactor);
        if (hstore_conf.client.blocking_loader) title += " / Blocking";
        LOG.info(title);
        
        final List<String> allLoaderArgs = new ArrayList<String>();
        final List<String> loaderCommand = new ArrayList<String>();

        // set loader max heap to MAX(1M,6M) based on thread count.
        int lthreads = 2;
        if (m_config.clientParameters.containsKey("loadthreads")) {
            lthreads = Integer.parseInt(m_config.clientParameters.get("loadthreads"));
            if (lthreads < 1) lthreads = 1;
            if (lthreads > 6) lthreads = 6;
        }
        int loaderheap = 1024 * lthreads;
        if (trace.val) LOG.trace("LOADER HEAP " + loaderheap);

        String debugString = "";
        if (m_config.listenForDebugger) {
            debugString = " -agentlib:jdwp=transport=dt_socket,address=8002,server=y,suspend=n ";
        }

        if (hstore_conf.global.sshprefix != null &&
            hstore_conf.global.sshprefix.isEmpty() == false) {
            loaderCommand.add(hstore_conf.global.sshprefix + " && ");
        }
        loaderCommand.add("java");
        loaderCommand.add("-Dhstore.tag=loader");
        loaderCommand.add("-XX:-ReduceInitialCardMarks");
        loaderCommand.add("-XX:-ReduceInitialCardMarks");
        loaderCommand.add("-XX:+HeapDumpOnOutOfMemoryError");
        loaderCommand.add("-XX:HeapDumpPath=" + hstore_conf.global.temp_dir);
        loaderCommand.add(String.format("-Xmx%dm", loaderheap));

        if (hstore_conf.client.jvm_args != null) {
            for (String arg : hstore_conf.client.jvm_args.split(" ")) {
                loaderCommand.add(arg);        
            } // FOR
        }
        
        if (debugString.isEmpty() == false) loaderCommand.add(debugString); 
        
        String classpath = ""; // Disable this so that we just pull from the build dir -> "hstore.jar" + ":" + this.jarFileName;
        if (System.getProperty("java.class.path") != null) {
            classpath = classpath + ":" + System.getProperty("java.class.path");
        }
        loaderCommand.add("-cp \"" + classpath + "\"");
        loaderCommand.add(m_loaderClass.getCanonicalName());
        
        this.addHostConnections(allLoaderArgs);
        allLoaderArgs.add("CONF=" + m_config.hstore_conf_path);
        allLoaderArgs.add("NAME=" + this.projectBuilder.getProjectName());
        allLoaderArgs.add("BENCHMARK.CONF=" + m_config.benchmark_conf_path);
        allLoaderArgs.add("NUMCLIENTS=" + totalNumClients);
        allLoaderArgs.add("LOADER=true");
        allLoaderArgs.add("EXITONCOMPLETION=false");
        
        if (m_config.statsDatabaseURL != null) {
            allLoaderArgs.add("STATSDATABASEURL=" + m_config.statsDatabaseURL);
            allLoaderArgs.add("STATSDATABASEUSER=" + m_config.statsDatabaseUser);
            allLoaderArgs.add("STATSDATABASEPASS=" + m_config.statsDatabasePass);
            allLoaderArgs.add("STATSDATABASEJDBC=" + m_config.statsDatabaseJDBC);
            allLoaderArgs.add("STATSPOLLINTERVAL=" + m_config.statsPollInterval);
            // LOG.info("Loader Stats Database: " + m_config.statsDatabaseURL);
        }

        for (Entry<String,String> e : m_config.clientParameters.entrySet()) {
            String arg = String.format("%s=%s", e.getKey(), e.getValue());
            allLoaderArgs.add(arg);
        } // FOR

        // RUN THE LOADER
//        if (true || m_config.localmode) {
        
        try {
            if (trace.val) {
                LOG.trace("Loader Class: " + m_loaderClass);
                LOG.trace("Parameters: " + StringUtil.join(" ", allLoaderArgs));
            }
            BenchmarkComponent.main(m_loaderClass,
                                    m_clientFileUploader,
                                    allLoaderArgs.toArray(new String[0]),
                                    true);
        } catch (Throwable ex) {
            this.failed = true;
            throw new RuntimeException("Failed to load data using " + m_loaderClass.getSimpleName(), ex);
        }
            
//        }
//        else {
//            if (debug.val) LOG.debug("Loader Command: " + loaderCommand.toString());
//            String[] command = SSHTools.convert(
//                    m_config.remoteUser,
//                    m_config.clients[0],
//                    m_config.remotePath,
//                    m_config.sshOptions,
//                    loaderCommand.toString());
//            status = ShellTools.cmdToStdOut(command);
//            assert(status);
//        }
    }

    
    /**
     * Invoke the benchmark clients on the remote nodes
     */
    private void startClients() {
        final ArrayList<String> allClientArgs = new ArrayList<String>();
        if (hstore_conf.global.sshprefix != null &&
            hstore_conf.global.sshprefix.isEmpty() == false) {
            allClientArgs.add(hstore_conf.global.sshprefix + " && ");
        }
        allClientArgs.add("java");
        if (m_config.listenForDebugger) {
            allClientArgs.add(""); //placeholder for agent lib
        }
        allClientArgs.add("-Dhstore.tag=client");
        allClientArgs.add("-ea");
        allClientArgs.add("-XX:-ReduceInitialCardMarks");
        allClientArgs.add("-XX:+HeapDumpOnOutOfMemoryError");
        allClientArgs.add("-XX:HeapDumpPath=/tmp");
        allClientArgs.add(String.format("-Xmx%dm", m_config.clientHeapSize));

        // JProfiler
        // allClientArgs.add("-agentpath:/home/pavlo/Programs/jprofiler/bin/linux-x64/libjprofilerti.so=port=8849");
        
        if (hstore_conf.client.jvm_args != null) {
            for (String arg : hstore_conf.client.jvm_args.split(" ")) {
                allClientArgs.add(arg);        
            } // FOR
        }
        
        /*
         * This is needed to do database verification at the end of the run. In
         * order load the snapshot tables, we need the checksum stuff in the
         * native library.
         */
        allClientArgs.add("-Djava.library.path=.");

        String classpath = ""; // "voltdbfat.jar" + ":" + this.jarFileName;
        if (System.getProperty("java.class.path") != null) {
            classpath = classpath + ":" + System.getProperty("java.class.path");
        }
        allClientArgs.add("-cp");
        allClientArgs.add("\"" + classpath + "\"");

        // The first parameter must be the BenchmarkComponentSet class name
        // Follow by the Client class name.
        allClientArgs.add(BenchmarkComponentSet.class.getCanonicalName());
        allClientArgs.add(m_clientClass.getCanonicalName());
        
        for (Entry<String,String> e : m_config.clientParameters.entrySet()) {
            String value = e.getValue();
            if (value.startsWith("\"") == false) {
                value = '"' + value + '"';
            }
            String opt = String.format("%s=%s", e.getKey(), value);
            allClientArgs.add(opt);
            if (trace.val) 
                LOG.trace("  " + opt);
        } // FOR

        this.addHostConnections(allClientArgs);
        allClientArgs.add("CONF=" + m_config.hstore_conf_path);
        allClientArgs.add("NAME=" + this.projectBuilder.getProjectName());
        allClientArgs.add("CHECKTRANSACTION=" + m_config.checkTransaction);
        allClientArgs.add("CHECKTABLES=" + m_config.checkTables);
        allClientArgs.add("LOADER=false");
        
        if (m_config.statsDatabaseURL != null) {
            allClientArgs.add("STATSDATABASEURL=" + m_config.statsDatabaseURL);
            allClientArgs.add("STATSDATABASEUSER=" + m_config.statsDatabaseUser);
            allClientArgs.add("STATSDATABASEPASS=" + m_config.statsDatabasePass);
            allClientArgs.add("STATSDATABASEJDBC=" + m_config.statsDatabaseJDBC);
            allClientArgs.add("STATSPOLLINTERVAL=" + m_config.statsPollInterval);
            // allClientArgs.add("STATSTAG=" + m_config.statsTag);
        }
        
        int threads_per_client = hstore_conf.client.threads_per_host;
        if (hstore_conf.client.processesperclient_per_partition) {
            threads_per_client *= catalogContext.numberOfPartitions;
        }

        final Map<String, Map<File, File>> sent_files = new ConcurrentHashMap<String, Map<File,File>>();
        final AtomicInteger clientIndex = new AtomicInteger(0);
        List<Runnable> runnables = new ArrayList<Runnable>();
        final Client local_client = (m_clientFileUploader.hasFilesToSend() ? getClientConnection(): null);
        
        // Add all of the client hostnames in our list first so that we can check
        // to see whether we have duplicates. If so, then we'll make sure that
        // their hostnames are unique in our results print outs
        Histogram<String> clientNames = new ObjectHistogram<String>();
        for (String clientHost : m_config.clients) {
            clientNames.put(clientHost.trim());
        } // FOR
        Histogram<String> clientNamesIdxs = new ObjectHistogram<String>();
        
        for (int host_idx = 0; host_idx < m_config.clients.length; host_idx++) {
            // The clientHost is the hostname that we actaully need to run on
            final String clientHost = m_config.clients[host_idx].trim();
            
            // The clientHostId is a unique identifier for a single invocation
            // on the clientHost. This is needed so that we can have multiple 
            // JVMs running at the same host
            String _hostId = clientHost;
            if (clientNames.get(clientHost) > 1) {
                int ctr = (int)clientNamesIdxs.get(clientHost, 0l);
                _hostId = String.format("%s-%02d", clientHost, ctr);
                clientNamesIdxs.put(clientHost);
            }
            final String clientHostId = "client-" + _hostId;
            m_clients.add(clientHostId);
            
            final List<String> curClientArgs = new ArrayList<String>(allClientArgs);
            final List<Integer> clientIds = new ArrayList<Integer>();
            for (int j = 0; j < threads_per_client; j++) {
                int clientId = clientIndex.getAndIncrement();
                m_clientThreads.add(BenchmarkControllerUtil.getClientName(clientHost, clientId));   
                clientIds.add(clientId);
            } // FOR
            
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0, cnt = clientIds.size(); i < cnt; i++) {
                        if (m_config.listenForDebugger) {
                            String arg = "-agentlib:jdwp=transport=dt_socket,address="
                                + (8003 + i) + ",server=y,suspend=n ";
                            curClientArgs.set(1, arg);
                        }
                        
                        // Check whether we need to send files to this client
                        int clientId = clientIds.get(i);
                        if (m_clientFileUploader.hasFilesToSend(clientId)) {
                            Collection<String> uploadArgs = processClientFileUploads(clientHost, clientId, sent_files);
                            if (uploadArgs.isEmpty() == false) curClientArgs.addAll(uploadArgs);
                            
                            if (local_client != null && i % 3 == 0) {
                                String procName = VoltSystemProcedure.procCallName(NoOp.class); 
                                try {
                                    local_client.callProcedure(new NullCallback(), procName); 
                                } catch (Exception ex) {
                                    throw new RuntimeException(ex);
                                }        
                            }
                        }
                    } // FOR
                    
                    curClientArgs.add("ID=" + StringUtil.join(",", clientIds));
                    
                    String args[] = SSHTools.convert(m_config.remoteUser, clientHost, m_config.remotePath, m_config.sshOptions, curClientArgs);
                    String fullCommand = StringUtil.join(" ", args);
    
                    resultsUploader.setCommandLineForClient(clientHostId, fullCommand);
                    if (trace.val) LOG.trace("Client Commnand: " + fullCommand);
                    clientPSM.startProcess(clientHostId, args);
                }
            });
        } // FOR
        m_clientsNotReady.set(m_clientThreads.size());
        assert(m_clientThreads.size() == totalNumClients) :
            String.format("%d != %d", m_clientThreads.size(), totalNumClients);
        
        // Let 'er rip!
        ThreadUtil.runGlobalPool(runnables);

        // ----------------------------------------------------------------------------
        // RESULT PRINTING SETUP
        // ----------------------------------------------------------------------------
        
        // JSON Output
        if (hstore_conf.client.output_results_json) {
            this.registerInterest(new JSONResultsPrinter(hstore_conf));
        }
        // CSV Output
        if (hstore_conf.client.output_results_csv != null && hstore_conf.client.output_results_csv.isEmpty() == false) {
            if (hstore_conf.client.output_results_csv.equalsIgnoreCase("true")) {
                LOG.warn("The HStoreConf parameter 'hstore_conf.client.output_csv' should be a file path, not a boolean value");
            }
            File f = new File(hstore_conf.client.output_results_csv);
            this.registerInterest(new CSVResultsPrinter(f));
        }
        // Default Table Output
        if (hstore_conf.client.output_results_table) {
            this.registerInterest(new ResultsPrinter(hstore_conf));
        }
        
        // Memory Stats Output
        if (hstore_conf.client.output_memory_stats != null && hstore_conf.client.output_memory_stats.isEmpty() == false) {
            if (hstore_conf.client.output_memory_stats.equalsIgnoreCase("true")) {
                LOG.warn("The HStoreConf parameter 'hstore_conf.client.output_memory_stats' should be a file path, not a boolean value");
            }
            File f = new File(hstore_conf.client.output_memory_stats);
            this.registerInterest(new MemoryStatsPrinter(this.getClientConnection(), f));
        }
        
        // Anticache Memory Stats Output
        if (hstore_conf.client.output_anticache_memory_stats != null && hstore_conf.client.output_anticache_memory_stats.isEmpty() == false) {
            if (hstore_conf.client.output_anticache_memory_stats.equalsIgnoreCase("true")) {
                LOG.warn("The HStoreConf parameter 'hstore_conf.client.output_anticache_memory_stats' should be a file path, not a boolean value");
            }
            File f = new File(hstore_conf.client.output_anticache_memory_stats);
            this.registerInterest(new AnticacheMemoryStatsPrinter(this.getClientConnection(), f));
        }
        
        // Table Stats Output
        if (hstore_conf.client.output_table_stats != null && hstore_conf.client.output_table_stats.isEmpty() == false) {
            if (hstore_conf.client.output_table_stats.equalsIgnoreCase("true")) {
                LOG.warn("The HStoreConf parameter 'hstore_conf.client.output_table_stats' should be a file path, not a boolean value");
            }
            File f = new File(hstore_conf.client.output_table_stats);
            this.registerInterest(new TableStatsPrinter(this.getClientConnection(), f));
        }

        // Kill Benchmark on Zero Results
        if (m_config.killOnZeroResults) {
            if (debug.val) LOG.debug("Will kill benchmark if results are zero for two poll intervals");
            ResultsChecker checker = new ResultsChecker(this.failure_observer);
            this.registerInterest(checker);
        }
        
        // ----------------------------------------------------------------------------
        // RESULT PRINTING SETUP
        // ----------------------------------------------------------------------------
        
        if (hstore_conf.client.anticache_enable &&  catalogContext.getEvictableTables().isEmpty() == false) {
            this.evictorThread = new PeriodicEvictionThread(catalogContext, getClientConnection(),
                                                            hstore_conf.client.anticache_evict_size, m_interested);
        }
    }
    
    private void addHostConnections(Collection<String> params) {
        for (Site catalog_site : catalogContext.sites) {
            for (Pair<String, Integer> p : m_launchHosts.get(catalog_site.getId())) {
                String address = String.format("%s:%d:%d", p.getFirst(), p.getSecond(), catalog_site.getId());
                params.add("HOST=" + address);
                if (trace.val) 
                    LOG.trace(String.format("HStoreSite %s: %s", HStoreThreadManager.formatSiteName(catalog_site.getId()), address));
                break;
            } // FOR
        } // FOR
    }
    
    private Client getClientConnection() {
        // Connect to random host and using a random port that it's listening on
        Integer site_id = CollectionUtil.random(m_launchHosts.keySet());
        assert(site_id != null);
        Pair<String, Integer> p = CollectionUtil.random(m_launchHosts.get(site_id));
        assert(p != null);
        if (debug.val)
            LOG.debug(String.format("Creating new client connection to HStoreSite %s",
                      HStoreThreadManager.formatSiteName(site_id)));
        
        Client new_client = ClientFactory.createClient(128, null, false, null);
        try {
            new_client.createConnection(null, p.getFirst(), p.getSecond(), "user", "password");
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Failed to connect to HStoreSite %s at %s:%d",
                                                     HStoreThreadManager.formatSiteName(site_id), p.getFirst(), p.getSecond()));
        }
        return (new_client);
    }
    
    private List<String> processClientFileUploads(String clientHost, int clientId, Map<String, Map<File, File>> sent_files) {
        List<String> newArgs = new ArrayList<String>();
        Map<File, File> files = null;
        for (Entry<String, Pair<File, File>> e : m_clientFileUploader.getFilesToSend(clientId).entrySet()) {
            String param = e.getKey();
            File local_file = e.getValue().getFirst();
            File remote_file = e.getValue().getSecond();
            if (local_file.exists() == false) {
                LOG.warn(String.format("Not sending %s file to client %d. The local file '%s' does not exist", param, clientId, local_file));
                continue;
            }
            boolean skip = false;
            synchronized (sent_files) {
                files = sent_files.get(clientHost);
                if (files == null) {
                    files = new HashMap<File, File>();
                    sent_files.put(clientHost, files);
                }
                // Check whether we have already written to this remote file on the client host
                // If we have, then we need to check whether it's the same local file.
                // If it is, then we're ok. If it's not, well then that's a paddlin'...
                File previous = files.get(remote_file);
                if (previous != null) {
                    if (previous.equals(local_file)) {
                        skip = true;
                    } else {
                        throw new RuntimeException(String.format("Trying to write two different local files ['%s', '%s'] to the same remote file '%s' on client host '%s'",
                                                                 local_file, previous, remote_file, clientHost));
                    }
                }
            } // SYNCH
            
            if (skip) {
                if (debug.val)
                    LOG.warn(String.format("Skipping duplicate file '%s' on client host '%s'",
                             local_file, clientHost));
            } else {
                if (debug.val)
                    LOG.debug(String.format("Copying %s file '%s' to '%s' on client %s [clientId=%d]",
                              param, local_file, remote_file, clientHost, clientId)); 
                SSHTools.copyToRemote(local_file, m_config.remoteUser, clientHost, remote_file, m_config.sshOptions);
                files.put(remote_file, local_file);
                m_clientFilesUploaded.incrementAndGet();
            }
            if (debug.val) LOG.debug(String.format("Uploaded File Parameter '%s': %s", param, remote_file));
            newArgs.add(param + "=" + remote_file.getPath());
        } // FOR
        return (newArgs);
    }

    /**
     * RUN BENCHMARK
     */
    public void runBenchmark() throws Exception {
        if (this.stop) return;
        LOG.info(makeHeader("BENCHMARK EXECUTE"));
        
        int threadsPerHost = hstore_conf.client.threads_per_host;
        if (hstore_conf.client.processesperclient_per_partition) {
            threadsPerHost *= catalogContext.numberOfPartitions; 
        }
                
        String debugOpts = String.format("[hosts=%d, perhost=%d, txnrate=%s", m_config.clients.length,
                                                                              threadsPerHost,
                                                                              hstore_conf.client.txnrate);
        if (hstore_conf.client.blocking) {
            debugOpts += ", concurrent=" + hstore_conf.client.blocking_concurrent; 
        }
        debugOpts += "]";
        
        ProfileMeasurement stopwatch = new ProfileMeasurement("clients").start();
        LOG.info(String.format("Starting %s execution with %d %sclient%s %s",
                 this.projectBuilder.getProjectName().toUpperCase(),
                 m_clientThreads.size(), 
                 (hstore_conf.client.blocking ? "blocking " : ""),
                 (m_clientThreads.size() > 1 ? "s" : ""),
                 debugOpts));
        if (m_config.statsDatabaseURL != null) {
            LOG.info("Client Stats Database: " + m_config.statsDatabaseURL);
        }
        
        // ---------------------------------------------------------------------------------
        
        // HACK
        int gdb_sleep = 0;
        if (gdb_sleep > 0) {
            LOG.info("Sleeping for " + gdb_sleep + " waiting for GDB");
            ThreadUtil.sleep(gdb_sleep*1000);
        }
        
        currentResults = new BenchmarkResults(hstore_conf.client.interval,
                                                hstore_conf.client.duration,
                                                m_clientThreads.size());
        currentResults.setEnableBasePartitions(hstore_conf.client.output_basepartitions);
        
        
        long nextIntervalTime = hstore_conf.client.interval;
        for (int i = 0; i < m_clients.size(); i++) {
            ClientStatusThread t = new ClientStatusThread(this, i);
            t.setUncaughtExceptionHandler(this.exceptionHandler);
            t.start();
            m_statusThreads.add(t);
        } // FOR
        if (debug.val)
            LOG.debug(String.format("Started %d %s",
                      m_statusThreads.size(), ClientStatusThread.class.getSimpleName()));

        // Get a connection to the cluster
        Client local_client = this.getClientConnection();
        
        // Spin on whether all clients are ready
        while (m_clientsNotReady.get() > 0 && this.stop == false) {
            if (debug.val)
                LOG.debug(String.format("Waiting for %d clients to come online", m_clientsNotReady.get()));
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                if (this.stop == false) throw ex;
                return;
            }
        } // WHILE
        stopwatch.stop();
        LOG.info(String.format("Initialized %d %s client threads in %.2f sec",
                 m_clientThreads.size(), 
                 this.projectBuilder.getProjectName().toUpperCase(),
                 stopwatch.getTotalThinkTimeSeconds()));
        
        if (this.stop) {
            if (debug.val) LOG.debug("Stop flag is set to true");
            return;
        }
        if (debug.val)
            LOG.debug("All clients are on-line and ready to go!");
        if (m_clientFilesUploaded.get() > 0) {
            LOG.info(String.format("Uploaded %d files to clients", m_clientFilesUploaded.get()));
        }
        
        // Reset some internal information at the cluster
        this.resetCluster(local_client);
        
        // Start up all the clients
        if (debug.val)
            LOG.debug(String.format("Telling %d clients to start executing", m_clients.size()));
        for (String clientName : m_clients) {
            if (debug.val)
                LOG.debug(String.format("Sending %s to %s", ControlCommand.START, clientName)); 
            clientPSM.writeToProcess(clientName, ControlCommand.START.name());
        } // FOR

        // Warm-up
        if (hstore_conf.client.warmup > 0) {
            LOG.info(String.format("Letting system warm-up for %.01f seconds", hstore_conf.client.warmup / 1000.0));
            
            try {
                Thread.sleep(hstore_conf.client.warmup);
            } catch (InterruptedException e) {
                if (debug.val) LOG.debug("Warm-up was interrupted!");
            }
            
            if (this.stop == false) {
                // Recompute Markovs
                // We don't need to save them to a file though
                if (m_config.markovRecomputeAfterWarmup) {
                    
                    // Note that this won't work the way that we want it to because it will get queued up
                    // in the Dtxn.Coordinator and we have no way to set the priority of it
                    // This means that it will have to wait until *all* of the previously submitted multi-partition
                    // transactions get executed before it will get executed... we really need to write our
                    // own transaction coordinator...
                    LOG.info("Requesting HStoreSites to recalculate Markov models after warm-up");
                    try {
                        this.recomputeMarkovs(local_client, false);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to recompute Markov models", ex);
                    }
                }
                
                // Schedule PeriodicEvictionThread
                if (this.evictorThread != null) {
                    int interval = hstore_conf.client.anticache_evict_interval;
                    this.threadPool.scheduleWithFixedDelay(this.evictorThread,
                                                           interval, interval, TimeUnit.MILLISECONDS);
                }
                
                // Reset the counters
                for (String clientName : m_clients)
                    clientPSM.writeToProcess(clientName, ControlCommand.CLEAR.name());
                
                LOG.info("Starting benchmark stats collection");
            }
        }
        
        // 
        long startTime = System.currentTimeMillis();
        nextIntervalTime += startTime;
        long nowTime = startTime;
        while (m_pollIndex < m_pollCount && this.stop == false) {
            // wait some time
            long sleep = nextIntervalTime - nowTime;
            try {
                if (this.stop == false && sleep > 0) {
                    if (debug.val) LOG.debug(String.format("Sleeping for %.1f sec [pollIndex=%d]",
                                               sleep / 1000d, m_pollIndex));
                    Thread.sleep(sleep);
                }
            } catch (InterruptedException e) {
                // Ignore...
                if (debug.val) LOG.debug(String.format("Interrupted! [pollIndex=%d / stop=%s]",
                                           m_pollIndex, this.stop));
            } finally {
                if (debug.val) LOG.debug(String.format("Awake! [pollIndex=%d / stop=%s]",
                                           m_pollIndex, this.stop));
            }
            nowTime = System.currentTimeMillis();
            
            // check if the next interval time has arrived
            if (nowTime >= nextIntervalTime) {
                m_pollIndex++;

                // make all the clients poll
                if (debug.val) LOG.debug(String.format("Sending %s to %d clients", ControlCommand.POLL, m_clients.size()));
                for (String clientName : m_clients)
                    clientPSM.writeToProcess(clientName, ControlCommand.POLL.name() + " " + m_pollIndex);

                // get ready for the next interval
                nextIntervalTime = hstore_conf.client.interval * (m_pollIndex + 1) + startTime;
            }
        } // WHILE
        
        if (this.stop == false) {
            this.postProcessBenchmark(local_client);
        }
//        else if (this.failed == true) {
//            this.invokeStatusSnapshot(local_client);
//        }
        
        this.stop = true;
        if (m_config.noShutdown == false && this.failed == false) sitePSM.prepareShutdown(false);
        
        // shut down all the clients
        clientPSM.prepareShutdown(false);
        boolean first = true;
        for (String clientName : m_clients) {
            if (first && m_config.noShutdown == false) {
                clientPSM.writeToProcess(clientName, ControlCommand.SHUTDOWN.name());
                first = false;
            } else {
                clientPSM.writeToProcess(clientName, ControlCommand.STOP.name());
            }
        } // FOR
        LOG.info("Waiting for " + m_clients.size() + " clients to finish");
        clientPSM.joinAll();

        if (this.failed == false) {
            if (this.resultsToRead.getCount() > 0) {
                LOG.info(String.format("Waiting for %d status threads to finish [remaining=%d]",
                         m_statusThreads.size(), this.resultsToRead.getCount()));
                try {
                    this.resultsToRead.await();
                    for (ClientStatusThread t : m_statusThreads) {
                        if (t.isFinished() == false) {
    //                        if (debug.val) 
                                LOG.info(String.format("ClientStatusThread '%s' asked to finish up", t.getName()));
    //                        System.err.println(StringUtil.join("\n", t.getStackTrace()));
                            t.interrupt();
                            t.join();
                        }
                    } // FOR
                } catch (InterruptedException e) {
                    LOG.warn(e);
                }
            }
            
            // Print out the final results
//            if (debug.val)
            if (hstore_conf.client.output_basepartitions || hstore_conf.client.output_status) {
                LOG.info("Computing final benchmark results...");
            }
            for (BenchmarkInterest interest : m_interested) {
                String finalResults = interest.formatFinalResults(currentResults);
                if (finalResults != null) System.out.println(finalResults);
            } // FOR
        } else if (debug.val) {
            LOG.debug("Benchmark failed. Not displaying final results");
        }
    }
    
    /**
     * Perform various post-processing tasks that we may need
     * @param client
     * @throws Exception
     */
    private void postProcessBenchmark(Client client) throws Exception {
        if (debug.val) LOG.debug("Performing post-processing on benchmark");
        
        // We have to tell all our clients to pause first
        clientPSM.writeToAll(ControlCommand.PAUSE.name());
        
        // Then tell the cluster to drain all txns
        if (debug.val) LOG.debug("Draining execution queues on cluster");
        ClientResponse cresponse = null;
        String procName = VoltSystemProcedure.procCallName(Quiesce.class);
        try {
            cresponse = client.callProcedure(procName);
        } catch (Exception ex) {
            throw new Exception("Failed to execute " + procName, ex);
        }
        assert(cresponse.getStatus() == Status.OK) :
            String.format("Failed to quiesce cluster!\n%s", cresponse);
        
        // DUMP RESPONSE ENTRIES
        if (hstore_conf.client.output_responses != null) {
            File outputPath = new File(hstore_conf.client.output_responses);
            this.writeResponseEntries(client, outputPath);
        }
        
        // DUMP DATABASE
        if (m_config.dumpDatabase && this.stop == false) {
            assert(m_config.dumpDatabaseDir != null);
            try {
                client.callProcedure(VoltSystemProcedure.procCallName(DatabaseDump.class),
                                     m_config.dumpDatabaseDir);
            } catch (Exception ex) {
                throw new Exception("Failed to dump database contents", ex);
            }
        }

        // DUMP PROFILING INFORMATION
        for (ProfilingOutput po : BenchmarkControllerUtil.PROFILING_OUTPUTS) {
            Object output = hstore_conf.get(po.clientParam);
            if (output != null) {
                this.writeProfilingData(client, po.key, new File(output.toString()));
            }
        } // FOR
        
        // Recompute MarkovGraphs
        if (m_config.markovRecomputeAfterEnd && this.stop == false) {
            this.recomputeMarkovs(client, true);
        }
    }
    
    private void writeResponseEntries(Client client, File outputPath) throws Exception {
        // We know how many clients that we need to get results back from
        CountDownLatch latch = new CountDownLatch(this.totalNumClients);
        for (ClientStatusThread t : m_statusThreads) {
            t.addResponseEntriesLatch(latch);
        } // FOR
        
        // Now tell everybody that part is over and we want them to dump their 
        // results back to us
        clientPSM.writeToAll(ControlCommand.DUMP_TXNS.name());
        
        // Wait until we get all of the responses that we need
        boolean result = latch.await(10, TimeUnit.SECONDS);
        if (result == false) {
            LOG.warn(String.format("Only got %d out of %d response dumps from clients",
                    this.totalNumClients-latch.getCount(), this.totalNumClients));
        }
        
        // Merge sort them
        int count = 0;
        for (ClientStatusThread t : m_statusThreads) {
            count += t.getResponseEntries().size();
        } // FOR
        LOG.info(String.format("Merging %d ClientResponse entries together...", count));
        ResponseEntries fullDump = new ResponseEntries();
        for (ClientStatusThread t : m_statusThreads) {
            fullDump.addAll(t.getResponseEntries());
        } // FOR
        if (fullDump.isEmpty()) {
            LOG.warn("No ClientResponse results were returned!");
            return;
        }
        
        // Convert to a VoltTable and then write out to a CSV file
        LOG.info(String.format("Writing %d ClientResponse entries to '%s'", fullDump.size(), outputPath));
        String txnNames[] = currentResults.getTransactionNames();
        FileWriter out = new FileWriter(outputPath);
        VoltTable vt = ResponseEntries.toVoltTable(fullDump, txnNames);
        VoltTableUtil.csv(out, vt, true);
        out.close();
        if (debug.val)
            LOG.debug(String.format("Wrote %d response entries information to '%s'",
                      fullDump.size(), outputPath));
    }
    
    private void writeProfilingData(Client client, SysProcSelector sps, File outputPath) throws Exception {
        Object params[];
        String sysproc;
        
        // The AntiCache history is as special case here.
        // We need to use the correct sysproc instead of @Statistics
        if (sps == SysProcSelector.ANTICACHEEVICTIONS) {
            sysproc = VoltSystemProcedure.procCallName(EvictHistory.class);
            params = new Object[]{ };
        }
        else if (sps == SysProcSelector.ANTICACHEACCESS) {
            sysproc = VoltSystemProcedure.procCallName(EvictedAccessHistory.class);
            params = new Object[]{ };
        }
        else {
            sysproc = VoltSystemProcedure.procCallName(Statistics.class);
            params = new Object[]{ sps.name(), 0 };
        }
        
        // Grab the data that we need from the cluster
        ClientResponse cresponse;
        try {
            cresponse = client.callProcedure(sysproc, params);
        } catch (Exception ex) {
            throw new Exception("Failed to execute " + sysproc, ex);
        }
        assert(cresponse.getStatus() == Status.OK) :
            String.format("Failed to get %s stats\n%s", sps, cresponse); 
        assert(cresponse.getResults().length == 1) :
            String.format("Failed to get %s stats\n%s", sps, cresponse);
        VoltTable vt = cresponse.getResults()[0];
        
        // Combine results (optional)
        boolean needCombine = false;
        if (sps == SysProcSelector.TXNPROFILER) needCombine = hstore_conf.client.output_txn_profiling_combine;
        if (sps == SysProcSelector.PLANNERPROFILER) needCombine = hstore_conf.client.output_planner_profiling_combine;
        if (sps == SysProcSelector.TXNCOUNTER) needCombine = hstore_conf.client.output_txn_counters_combine;
        if (sps == SysProcSelector.SPECEXECPROFILER) needCombine = hstore_conf.client.output_specexec_profiling_combine;
        if (needCombine) {
            String combineColumn;
            if (sps == SysProcSelector.SPECEXECPROFILER) {
                combineColumn = "SPECULATE_TYPE";
            } else {
                combineColumn = "PROCEDURE";
            }
            int offset = vt.getColumnIndex(combineColumn);
            
            VoltTable.ColumnInfo cols[] = Arrays.copyOfRange(VoltTableUtil.extractColumnInfo(vt), offset, vt.getColumnCount());
            Map<String, Object[]> totalRows = new TreeMap<String, Object[]>();
            Map<String, List<Double>[]> stdevRows = new HashMap<String, List<Double>[]>();
            
            while (vt.advanceRow()) {
                String procName = vt.getString(offset);
                Object row[] = totalRows.get(procName);
                List<Double> stdevs[] = stdevRows.get(procName);
                if (row == null) {
                    row = new Object[cols.length];
                    row[0] = procName;
                    for (int i = 1; i < row.length; i++) {
                        row[i] = new Long(0l);
                    } // FOR
                    totalRows.put(procName, row);
                
                    @SuppressWarnings("unchecked")
                    List<Double> temp[] = (List<Double>[])new ArrayList<?>[cols.length]; 
                    stdevs = temp;
                    stdevRows.put(procName, stdevs);
                }
                
                for (int i = 1; i < row.length; i++) {
                    if (vt.getColumnName(offset + i).endsWith("STDEV")) {
                        if (stdevs[i] == null) stdevs[i] = new ArrayList<Double>();
                        stdevs[i].add(vt.getDouble(offset + i));
                        // stdevs[i].put(vt.getDouble(offset + i), vt.getLong(offset + 1));
                    } else if (vt.getColumnType(offset + i) == VoltType.STRING) {
                        row[i] = vt.getString(offset + i);
                    } else if (vt.getColumnType(offset + i) == VoltType.FLOAT) {
                        row[i] = vt.getDouble(offset + i);
                    } else {
                    	//LOG.info(String.format("offset: %d, i: %d, VoltTable: %s,",offset,i, vt));
                    	//System.err.println(String.format("offset: %d, i: %d, VoltTable: %s,",offset,i, vt));
                        row[i] = ((Long)row[i]) + vt.getLong(offset + i);
                    }
                } // FOR
            } // WHILE
            
            // HACK: Take the weighted average stddev.
            for (String procName : totalRows.keySet()) {
                // Histogram<Double> stdevs[] = stdevRows.get(procName);
                List<Double> stdevs[] = stdevRows.get(procName);
                Object row[] = totalRows.get(procName);
                for (int i = 1; i < row.length; i++) {
                    if (stdevs[i] != null) {
                        row[i] = MathUtil.arithmeticMean(CollectionUtil.toDoubleArray(stdevs[i]));
                        if (trace.val) 
                            LOG.trace(String.format("%s STDEV -> %s -> %s", procName, stdevs[i], row[i]));
                    }
                } // FOR
            } // FOR
            
            vt = new VoltTable(cols);
            for (Object[] row : totalRows.values()) {
                vt.addRow(row);
            } // FOR
        }
        
        // Write out CSV
        FileWriter out = new FileWriter(outputPath);
        VoltTableUtil.csv(out, vt, true);
        out.close();
        LOG.info(String.format("Wrote %s information to '%s'", sps, outputPath));
        return;
    }
    
    private ClientResponse[] invokeSysProcs(Client client, Class<? extends VoltSystemProcedure> sysprocs[], Object params[][]) {
        ClientResponse cr[] = new ClientResponse[sysprocs.length];
        for (int i = 0; i < sysprocs.length; i++) {
            String procName = VoltSystemProcedure.procCallName(sysprocs[i]);
            try {
                cr[i] = client.callProcedure(procName, params[i]);
            } catch (Exception ex) {
                LOG.error("Failed to execute sysproc " + procName, ex);
                return (null);
            }
            assert(cr[i].getStatus() == Status.OK) :
                String.format("Unexpected response status %s for sysproc %s", cr[i].getStatus(), procName); 
        } // FOR
        return (cr);
    }
    
    @SuppressWarnings("unused")
    private void invokeStatusSnapshot(Client client) {
        @SuppressWarnings("unchecked")
        Class<VoltSystemProcedure> sysprocs[] = (Class<VoltSystemProcedure>[])new Class<?>[]{
            ExecutorStatus.class,
        };
        Object params[][] = { { 0 } };
        this.invokeSysProcs(client, sysprocs, params);
    }
    
    private void resetCluster(Client client) {
        if (debug.val)
            LOG.debug("Resetting internal profiling and invoking garbage collection on cluster");
        
        @SuppressWarnings("unchecked")
        Class<VoltSystemProcedure> sysprocs[] = (Class<VoltSystemProcedure>[])new Class<?>[]{
            ResetProfiling.class,
            GarbageCollection.class
        };
        Object params[][] = { { }, { } };
        this.invokeSysProcs(client, sysprocs, params);
    }
    
    /**
     * Instruct the cluster to recompute the edge weight probabilities for its markov models.
     * If retrieveFiles is set to true, then the BenchmarkController automatically will download
     * the updated models and write them to a temp directory on the BenchmarkController's host. 
     * @param client
     * @param retrieveFiles
     */
    private void recomputeMarkovs(Client client, boolean retrieveFiles) {
        LOG.info("Requesting HStoreSites to recalculate Markov models");
        ClientResponse cr = null;
        String procName = VoltSystemProcedure.procCallName(MarkovUpdate.class);
        try {
            cr = client.callProcedure(procName, true);
        } catch (Exception ex) {
            LOG.error("Failed to recompute MarkovGraphs", ex);
            return;
        }
        assert(cr != null);
        if (retrieveFiles == false) return;

        File outputDir = FileUtil.join(hstore_conf.global.temp_dir, "markovs",
                                       this.projectBuilder.getProjectName());
        FileUtil.makeDirIfNotExists(outputDir);
        
        // The return should be a list of SiteIds->RemotePath
        // We just need to then pull down the files and then combine them into
        // a single MarkovGraphContainer
        Map<Integer, File> markovs = new HashMap<Integer, File>();
        List<Pair<String, File>> files_to_remove = new ArrayList<Pair<String, File>>();
        VoltTable results[] = cr.getResults();
        assert(results.length == 1);
        while (results[0].advanceRow()) {
            int site_id = (int)results[0].getLong(0);
            int partition_id = (int)results[0].getLong(1);
            File remote_path = new File(results[0].getString(2));
//            boolean is_global = (results[0].getLong(3) == 1);
            
            Pair<String, Integer> p = CollectionUtil.first(m_launchHosts.get(site_id));
            assert(p != null) : "Invalid SiteId " + site_id;
            
            if (debug.val)
                LOG.debug(String.format("Retrieving %s file '%s' from %s",
                          MarkovGraphsContainer.class.getSimpleName(),
                          remote_path, HStoreThreadManager.formatSiteName(site_id)));
            SSHTools.copyFromRemote(outputDir, m_config.remoteUser, p.getFirst(), remote_path, m_config.sshOptions);
            File local_file = new File(outputDir + "/" + remote_path.getName());
            markovs.put(partition_id, local_file);
            files_to_remove.add(Pair.of((String)null, local_file));
            files_to_remove.add(Pair.of(p.getFirst(), remote_path));
        } // FOR
        
        File new_output = FileUtil.join(outputDir.getPath(),
                                        this.projectBuilder.getProjectName() + "-new.markovs");
        if (debug.val)
            LOG.debug(String.format("Writing %d updated MarkovGraphsContainers to '%s'",
                      markovs.size(),  new_output));
        MarkovGraphsContainerUtil.combine(markovs, new_output, catalogContext.database);
        
        // Clean up the remote files
        for (Pair<String, File> p : files_to_remove) {
            if (p.getFirst() == null) {
                p.getSecond().delete();
            } else {
                SSHTools.deleteFile(m_config.remoteUser, p.getFirst(), p.getSecond(), m_config.sshOptions);
            }
        } // FOR
    }
    
    /**
     * Cleanup Benchmark
     */
    public synchronized void cleanUpBenchmark() {
        // if (this.cleaned) return;
        
        if (m_config.noExecute == false) {
            if (debug.val) 
                LOG.warn("Killing clients");
            clientPSM.shutdown();
        }
        
        if (m_config.noShutdown == false && this.failed == false) {
            if (debug.val) 
                LOG.warn("Killing HStoreSites");
            sitePSM.shutdown();
        }
        
        this.cleaned = true;
    }


    /**
     *
     * @return A ResultSet instance for the ongoing or just finished benchmark run.
     */
    public BenchmarkResults getResults() {
        assert(currentResults != null);
        synchronized(currentResults) {
            return currentResults.copy();
        }
    }


    /** Call dump on each of the servers */
    public void tryDumpAll() {
        Client dumpClient = ClientFactory.createClient();
        for (String host : m_config.hosts) {
            try {
                dumpClient.createConnection(null, host, HStoreConstants.DEFAULT_PORT, "program", "password");
                dumpClient.callProcedure("@dump");
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ProcCallException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Read a MySQL connection URL from a file named "mysqlp".
     * Look for the file in a few places, then try to read the first,
     * and hopefully only, line from the file.
     *
     * @param remotePath Path to the volt binary files.
     * @return Two connection string URLs (can't be null).
     * @throws RuntimeException with an error message on failure.
     */
    static String[] readConnectionStringFromFile(String remotePath) {
        String filename = "mysqlp";
        // try the current dir
        File f = new File(filename);
        if (f.isFile() == false) {
            // try voltbin from the current dir
            f = new File(remotePath + filename);
            if (f.isFile() == false) {
                // try the home voltbin
                String path = System.getProperty("user.home");
                path += "/" + remotePath + filename;
                f = new File(path);
            }
        }
        if (f.isFile() == false) {
            String msg = "Cannot find suitable reporting database connection string file";
            throw new RuntimeException(msg);
        }
        if (f.canRead() == false) {
            String msg = "Reporting database connection string file at \"" +
                f.getPath() + "\" cannot be read (permissions).";
            throw new RuntimeException(msg);
        }

        String[] retval = new String[2];
        try {
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            retval[0] = br.readLine().trim();
            retval[1] = br.readLine().trim();
        } catch (IOException e) {
            String msg = "Reporting database connection string file at \"" +
                f.getPath() + "\" cannot be read (read error).";
            throw new RuntimeException(msg);
        }
        if ((retval[0].length() == 0) || (retval[1].length() == 0)){
            String msg = "Reporting database connection string file at \"" +
                f.getPath() + "\" seems to be (partly) empty.";
            throw new RuntimeException(msg);
        }

        return retval;
    }

    public static void main(final String[] vargs) throws Exception {
        int hostCount = 1;
        int sitesPerHost = 2;
        int k_factor = 0;
        int clientInitialPollingDelay = 10000;
        String sshOptions = "";
        String remotePath = "voltbin/";
        String remoteUser = null; // null implies current local username
        String projectBuilderClassname = TPCCProjectBuilder.class.getCanonicalName();
        File catalogPath = null;
        String partitionPlanPath = null;
        boolean listenForDebugger = false;
        int serverHeapSize = 2048;
        int clientHeapSize = 1024;
        boolean localmode = false;
        boolean compileBenchmark = true;
        boolean compileOnly = false;
        boolean useCatalogHosts = false;
        String workloadTrace = null;
        int num_partitions = 0;
        String backend = "jni";
        String snapshotPath = null;
        String snapshotFrequency = null;
        String snapshotPrefix = null;
        int snapshotRetain = -1;
        float checkTransaction = 0;
        boolean checkTables = false;
        
        String statsTag = null;
        String statsDatabaseURL = null;
        String statsDatabaseUser = null;
        String statsDatabasePass = null;
        String statsDatabaseJDBC = null;
        int statsPollInterval = 1000;
        
        String applicationName = null;
        String subApplicationName = null;
        
        boolean noSites = false;
        boolean noLoader = false;
        boolean noUploading = false;
        boolean noExecute = false;
        boolean noShutdown = false;
        
        boolean killOnZeroResults = false; 
        
        CatalogContext catalogContext = null;
        
        // HStoreConf Path
        File hstore_conf_path = null;
        
        // Benchmark Conf Path
        String benchmark_conf_path = null;
        
        // Markov Stuff
        String markov_path = null;
        String markov_thresholdsPath = null;
        Double markov_thresholdsValue = null;
        boolean markov_recomputeAfterEnd = false;
        boolean markov_recomputeAfterWarmup = false;
        
        // Deferrable Queries
        String deferrable[] = null;

        // Evictable Tables
        String evictable[] = null;
        // Batch Evictable Tables        
        String[] batchEvictable = null;
        
        boolean dumpDatabase = false;
        String dumpDatabaseDir = null;
        
        // List of SiteIds that we won't start because they'll be started by the profiler
        Set<Integer> profileSiteIds = new HashSet<Integer>();

        Map<String, String> clientParams = new LinkedHashMap<String, String>();
        List<String> clientHosts = new ArrayList<String>();
        
        Map<String, String> siteParams = new LinkedHashMap<String, String>();
        List<String> siteHosts = new ArrayList<String>();
        
        
        for (String arg : vargs) {
            String[] parts = arg.split("=",2);
            for (int i = 0; i < parts.length; i++)
                parts[1] = parts[1].trim();
            
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            /* HStoreConf File Path */
            } else if (parts[0].equalsIgnoreCase("CONF")) {
                hstore_conf_path = new File(parts[1]);
            /* Benchmark Configuration File Path */
            } else if (parts[0].equalsIgnoreCase(HStoreConstants.BENCHMARK_PARAM_PREFIX + "CONF")) {
                benchmark_conf_path = parts[1];

            /* Whether to kill the benchmark if we get consecutive intervals with zero results */
            } else if (parts[0].equalsIgnoreCase("KILLONZERO")) {
                killOnZeroResults = Boolean.parseBoolean(parts[1]);
                
            /*
             * Whether or not to check the result of each transaction.
             */
            } else if (parts[0].equalsIgnoreCase("CHECKTRANSACTION")) {
                checkTransaction = Float.parseFloat(parts[1]);
            /*
             * Whether or not to check all the tables at the end.
             */
            } else if (parts[0].equalsIgnoreCase("CHECKTABLES")) {
                checkTables = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equalsIgnoreCase("LOCAL")) {
                /*
                 * The number of Volt servers to start.
                 * Can be less then the number of provided hosts
                 */
                localmode = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equalsIgnoreCase("HOSTCOUNT")) {
                /*
                 * The number of Volt servers to start.
                 * Can be less then the number of provided hosts
                 */
                hostCount = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("SITESPERHOST")) {
                /*
                 * The number of execution sites per host
                 */
                sitesPerHost = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("KFACTOR")) {
                /*
                 * The number of partition replicas (k-factor)
                 */
                k_factor = Integer.parseInt(parts[1]);
            }
                
            else if (parts[0].equalsIgnoreCase("CLIENTHEAP")) {
                /*
                 * The number of client processes per client host
                 */
                clientHeapSize = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("SERVERHEAP")) {
                /*
                 * The number of client processes per client host
                 */
                serverHeapSize = Integer.parseInt(parts[1]);
            }
            // Name of the ProjectBuilder class for this benchmark.
            else if (parts[0].equalsIgnoreCase(HStoreConstants.BENCHMARK_PARAM_PREFIX +  "BUILDER")) {
                projectBuilderClassname = parts[1];
            }
            // Options used when logging into client/server hosts
            else if (parts[0].equalsIgnoreCase("SSHOPTIONS")) {
                sshOptions = parts[1];
            }
            // Directory on the NFS host where the H-Store files are stored
            else if (parts[0].equalsIgnoreCase("REMOTEPATH")) {
                remotePath = parts[1];
            }
            // User that runs volt on remote client and host machines
            else if (parts[0].equalsIgnoreCase("REMOTEUSER")) {
                remoteUser =  parts[1];
            }
            // Name of a host to be used for Volt servers
            else if (parts[0].equalsIgnoreCase("HOST")) {
                String hostnport[] = parts[1].split("\\:",2);
                siteHosts.add(hostnport[0]);
                
            } else if (parts[0].equalsIgnoreCase("LISTENFORDEBUGGER")) {
                listenForDebugger = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equalsIgnoreCase("BACKEND")) {
                backend = parts[1];
            } else if (parts[0].equalsIgnoreCase("SNAPSHOTPATH")) {
                snapshotPath = parts[1];
            } else if (parts[0].equalsIgnoreCase("SNAPSHOTFREQUENCY")) {
                snapshotFrequency = parts[1];
            } else if (parts[0].equalsIgnoreCase("SNAPSHOTPREFIX")) {
                snapshotPrefix = parts[1];
            } else if (parts[0].equalsIgnoreCase("SNAPSHOTRETAIN")) {
                snapshotRetain = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("NUMCONNECTIONS")) {
                clientParams.put(parts[0], parts[1]);
                
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEURL")) {
                statsDatabaseURL = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEJDBC")) {
                statsDatabaseJDBC = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEUSER")) {
                statsDatabaseUser = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEPASS")) {
                statsDatabasePass = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASETAG")) {
                statsTag = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("CATALOG")) {
                catalogPath = new File(parts[1]);
                catalogContext = CatalogUtil.loadCatalogContextFromJar(catalogPath);
                assert(catalogContext != null);
                num_partitions = catalogContext.numberOfPartitions;
            }
            else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_PARTITION_PLAN)) {
                partitionPlanPath = parts[1];
                clientParams.put(ArgumentsParser.PARAM_PARTITION_PLAN, parts[1]);
                siteParams.put(ArgumentsParser.PARAM_PARTITION_PLAN, parts[1]);
                siteParams.put(ArgumentsParser.PARAM_PARTITION_PLAN_APPLY, "true");
                
            }
            else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_PARTITION_PLAN_NO_SECONDARY)) {
                clientParams.put(ArgumentsParser.PARAM_PARTITION_PLAN_NO_SECONDARY, parts[1]);
                siteParams.put(ArgumentsParser.PARAM_PARTITION_PLAN_NO_SECONDARY, parts[1]);
            }
            else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_PARTITION_PLAN_IGNORE_MISSING)) {
                clientParams.put(ArgumentsParser.PARAM_PARTITION_PLAN_IGNORE_MISSING, parts[1]);
                siteParams.put(ArgumentsParser.PARAM_PARTITION_PLAN_IGNORE_MISSING, parts[1]);
                
            }
            /*
             * Whether to compile the benchmark jar
             */
            else if (parts[0].equalsIgnoreCase("COMPILE")) {
                compileBenchmark = Boolean.parseBoolean(parts[1]);
            }
            /*
             * Whether to compile only the benchmark jar and then quit
             */
            else if (parts[0].equalsIgnoreCase("COMPILEONLY")) {
                compileOnly = Boolean.parseBoolean(parts[1]);
            }
            /*
             * Launch the ExecutionSites using the hosts that are in the catalog
             */
            else if (parts[0].equalsIgnoreCase("CATALOGHOSTS")) {
                useCatalogHosts = Boolean.parseBoolean(parts[1]);
            }
            /*
             * List of evictable tables
             */
            else if (parts[0].equalsIgnoreCase("EVICTABLE")) {
                LOG.info("EVICTABLE: " + parts[1]);
                evictable = parts[1].split(",");
            }
            /*
             * List of batch evictable tables
             */
            else if (parts[0].equalsIgnoreCase("BATCHEVICTABLE")) {
                LOG.info("BATCHEVICTABLE: " + parts[1]);
                batchEvictable = parts[1].split(",");
            }
                        
            /*
             * List of deferrable queries
             * Format: <ProcedureName>.<StatementName>
             */
            else if (parts[0].equalsIgnoreCase("DEFERRABLE")) {
                if (debug.val) LOG.debug("DEFERRABLE: " + parts[1]);
                deferrable = parts[1].split(",");
            }
            /* Disable starting the database cluster  */
            else if (parts[0].equalsIgnoreCase("NOSITES") || parts[0].equalsIgnoreCase("NOSTART")) {
                // HACK: If they stay "nostart=true", then we'll allow that
                if (parts[1].equalsIgnoreCase("true")) {
                    noSites = true;
                }
                // Otherwise we expect a list of siteIds
                else {
                    for (String s : parts[1].split(",")) {
                        profileSiteIds.add(Integer.valueOf(s));
                    } // FOR
                }
            /* Disable executing the data loader */
            } else if (parts[0].equalsIgnoreCase("NOLOADER")) {
                noLoader = Boolean.parseBoolean(parts[1]);
//                LOG.info("NOLOADER = " + noLoader);
            /* Run the loader but disable uploading tuples */
            } else if (parts[0].equalsIgnoreCase("NOUPLOADING")) {
                noUploading = Boolean.parseBoolean(parts[1]);
                clientParams.put(parts[0].toUpperCase(), parts[1]);
            /* Disable workload execution */
            } else if (parts[0].equalsIgnoreCase("NOEXECUTE")) {
                noExecute = Boolean.parseBoolean(parts[1]);
                
            // Disable sending the shutdown command at the end of the benchmark run
            } else if (parts[0].equalsIgnoreCase("NOSHUTDOWN")) {
                noShutdown = Boolean.parseBoolean(parts[1]);
                siteParams.put("site.status_kill_if_hung", "false");
                
            /* Workload Trace Output */
            } else if (parts[0].equalsIgnoreCase("TRACE")) {
                workloadTrace = parts[1];
                siteParams.put(ArgumentsParser.PARAM_WORKLOAD_OUTPUT, parts[1]);
            /* Markov Model File */
            } else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_MARKOV)) {
                markov_path = parts[1];
                siteParams.put(parts[0], parts[1]);
            /* Markov Confidence Thresholds File */
            } else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_MARKOV_THREADS)) {
                markov_thresholdsPath = parts[1];
                siteParams.put(parts[0].toLowerCase(), parts[1]);
            /* Markov Confidence Thresholds Value */
            } else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_MARKOV_THRESHOLDS_VALUE)) {
                markov_thresholdsValue = Double.valueOf(parts[1]);
                siteParams.put(parts[0].toLowerCase(), parts[1]);
            /* Recompute Markovs After End */
            } else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_MARKOV_RECOMPUTE_END)) {
                markov_recomputeAfterEnd = Boolean.parseBoolean(parts[1]);
            /* Recompute Markovs After Warmup Period*/
            } else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_MARKOV_RECOMPUTE_WARMUP)) {
                markov_recomputeAfterWarmup = Boolean.parseBoolean(parts[1]);

            } else if (parts[0].equalsIgnoreCase("DUMPDATABASE")) {
                dumpDatabase = Boolean.parseBoolean(parts[1]);                
            } else if (parts[0].equalsIgnoreCase("DUMPDATABASEDIR")) {
                dumpDatabaseDir = parts[1];
                
            } else if (parts[0].equalsIgnoreCase(HStoreConstants.BENCHMARK_PARAM_PREFIX +  "INITIAL_POLLING_DELAY")) {
                clientInitialPollingDelay = Integer.parseInt(parts[1]);
            } else {
                clientParams.put(parts[0].toLowerCase(), parts[1]);
            }
        }

        // Initialize HStoreConf
        assert(hstore_conf_path != null) : "Missing HStoreConf file";
        HStoreConf hstore_conf = HStoreConf.init(hstore_conf_path, vargs);
        if (trace.val) 
            LOG.trace("HStore Conf '" + hstore_conf_path.getAbsolutePath() + "'\n" + hstore_conf.toString(true));
        
        if (hstore_conf.client.duration < 1000) {
            LOG.error("Duration is specified in milliseconds");
            System.exit(-1);
        }
        else if (hstore_conf.client.threads_per_host <= 0) {
            LOG.error("Invalid number of threads per host '" + hstore_conf.client.threads_per_host + "'");
            System.exit(-1);
        }
        
        // The number of client hosts to place client processes on
        int clientCount = hstore_conf.client.count;
        // Comma or colon separated list of the hostnames to be used for benchmark clients
        for (String host : hstore_conf.client.hosts.split("[,;]")) {
            clientHosts.add(host);
        } // FOR
        if (clientHosts.isEmpty())
            clientHosts.add(hstore_conf.global.defaulthost);
        
        // If no hosts were given, then use the defaults 
        if (siteHosts.size() == 0)
            siteHosts.add(hstore_conf.global.defaulthost);
        

        if (compileOnly == false && clientHosts.size() < clientCount) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_NotEnoughClients;
            LOG.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { clientHosts.size(), clientCount }, null);
            System.exit(-1);
        }
        
        String[] hostNames = null;
        if (! (useCatalogHosts || compileOnly) ) {
            if (siteHosts.size() < hostCount) {
                LogKeys logkey = LogKeys.benchmark_BenchmarkController_NotEnoughHosts;
                LOG.l7dlog( Level.FATAL, logkey.name(),
                        new Object[] { siteHosts.size(), hostCount }, null);
                LOG.fatal("Don't have enough hosts(" + siteHosts.size()
                        + ") for host count " + hostCount);
                System.exit(-1);
            }
    
            // copy the lists of hostnames into array of the right lengths
            // (this truncates the list to the right number)
            hostNames = new String[hostCount];
            for (int i = 0; i < hostCount; i++)
                hostNames[i] = siteHosts.get(i);
        } else {
            hostNames = new String[0];
        }
        String[] clientNames = new String[clientCount];
        if (compileOnly == false) {
            for (int i = 0; i < clientCount; i++)
                clientNames[i] = clientHosts.get(i);
        }

        
		// create a config object, mostly for the results uploader at this point
        BenchmarkConfig config = new BenchmarkConfig(
                hstore_conf,
                hstore_conf_path,
                benchmark_conf_path,
                projectBuilderClassname,
                backend, 
                hostNames,
                sitesPerHost, 
                k_factor, 
                clientNames, 
                sshOptions,
                remotePath, 
                remoteUser, 
                listenForDebugger, 
                serverHeapSize, 
                clientHeapSize,
                clientInitialPollingDelay,
                localmode, 
                checkTransaction, 
                checkTables, 
                snapshotPath, 
                snapshotPrefix,
                snapshotFrequency, 
                snapshotRetain, 
                statsDatabaseURL, 
                statsDatabaseUser,
                statsDatabasePass,
                statsDatabaseJDBC,
                statsPollInterval,
                statsTag,
                applicationName, 
                subApplicationName,
                compileBenchmark, 
                compileOnly, 
                useCatalogHosts,
                noSites,
                noLoader,
                noUploading,
                noExecute,
                noShutdown,
                killOnZeroResults,
                workloadTrace,
                profileSiteIds,
                partitionPlanPath,
                markov_path,
                markov_thresholdsPath,
                markov_thresholdsValue,
                markov_recomputeAfterEnd,
                markov_recomputeAfterWarmup,
                evictable,
                batchEvictable,
                deferrable,
                dumpDatabase,
                dumpDatabaseDir
        );
        
        // Always pass these parameters
        if (catalogPath != null) {
            clientParams.put("CATALOG", catalogPath.getAbsolutePath());
            clientParams.put("NUMPARTITIONS", Integer.toString(num_partitions));
        }
        int total_num_clients = clientCount * hstore_conf.client.threads_per_host;
        if (hstore_conf.client.processesperclient_per_partition) {
            total_num_clients *= num_partitions;
        }
        clientParams.put("NUMCLIENTS", Integer.toString(total_num_clients));
        clientParams.putAll(hstore_conf.getParametersLoadedFromArgs());
        
        // Make sure we put in the parameters passed from the command-line into both components
        Map<String, String> loadedArgs = hstore_conf.getParametersLoadedFromArgs(); 
        clientParams.putAll(loadedArgs);
        siteParams.putAll(loadedArgs);
        config.clientParameters.putAll(clientParams);
        config.siteParameters.putAll(siteParams);
        if (trace.val) LOG.trace("Benchmark Configuration\n" + config.toString());
        
        // ACTUALLY RUN THE BENCHMARK
        BenchmarkController controller = new BenchmarkController(config, catalogContext);
        boolean failed = false;
        
        // Check CodeSpeed Parameters
        if (hstore_conf.client.codespeed_url != null) {
            assert(hstore_conf.client.codespeed_project != null) : "Missing CodeSpeed Project";
            assert(hstore_conf.client.codespeed_environment != null) : "Missing CodeSpeed Environment";
            assert(hstore_conf.client.codespeed_executable != null) : "Missing CodeSpeed Executable";
            assert(hstore_conf.client.codespeed_commitid != null) : "Missing CodeSpeed CommitId";
        }
        
        
        // COMPILE BENCHMARK
        if (config.compileBenchmark) {
            boolean success = false;
            BenchmarkCompiler compiler = new BenchmarkCompiler(controller.m_config,
                                                               controller.projectBuilder,
                                                               hstore_conf);
            try {
                // Actually compile and write the catalog to disk
                success = compiler.compileBenchmark(controller.jarFileName);
                assert(controller.jarFileName.exists()) : 
                    "Failed to create jar file '" + controller.jarFileName + "'";
            } catch (Throwable ex) {
                LOG.error(String.format("Unexected error when trying to compile %s benchmark",
                                        controller.projectBuilder.getProjectName()), ex);
                System.exit(1);
            }
            if (config.compileOnly) {
                if (success) {
                    LOG.info("Compilation Complete. Exiting [" + controller.jarFileName.getAbsolutePath() + "]");
                } else {
                    LOG.info("Compilation Failed");
                }
                System.exit(success ? 0 : -1);
            }
        } else {
            if (debug.val) LOG.debug("Skipping benchmark project compilation");
        }

        // EXECUTE BENCHMARK
        try {
            controller.setupBenchmark();
            if (config.noExecute == false) controller.runBenchmark();
        } catch (Throwable ex) {
            LOG.fatal("Failed to complete benchmark", ex);
            failed = true;
        } finally {
            controller.cleanUpBenchmark();
        }
        if (config.noShutdown == false && (failed || controller.failed)) System.exit(1);
        
        // Upload Results to CodeSpeed
        if (hstore_conf.client.codespeed_url != null) {
            String codespeed_benchmark = controller.projectBuilder.getProjectName();
            double txnrate = controller.getResults().getFinalResult().getTotalTxnPerSecond();
            
            BenchmarkResultsUploader uploader = new BenchmarkResultsUploader(new URL(hstore_conf.client.codespeed_url),
                                                                             hstore_conf.client.codespeed_project,
                                                                             hstore_conf.client.codespeed_executable,
                                                                             codespeed_benchmark,
                                                                             hstore_conf.client.codespeed_environment,
                                                                             hstore_conf.client.codespeed_commitid);
            if (hstore_conf.client.codespeed_branch != null && hstore_conf.client.codespeed_branch.isEmpty() == false) {
                uploader.setBranch(hstore_conf.client.codespeed_branch);
            }
            
            uploader.post(txnrate);
            LOG.info("Uploaded benchmarks results to " + hstore_conf.client.codespeed_url);
        }
        
        if (config.noShutdown && config.noSites == false) {
            // Wait indefinitely
            LOG.info("H-Store cluster remaining online until killed");
            while (true) {
                Thread.sleep(1000);
            } // WHILE
        }

    }
}
