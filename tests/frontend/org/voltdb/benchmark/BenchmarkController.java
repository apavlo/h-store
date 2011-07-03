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

package org.voltdb.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.BenchmarkResults.Result;
import org.voltdb.benchmark.ClientMain.Command;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.processtools.ProcessSetManager;
import org.voltdb.processtools.SSHTools;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.MarkovUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreSite;

public class BenchmarkController {
    private static final Logger LOG = Logger.getLogger(BenchmarkController.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static final String BENCHMARK_PARAM_PREFIX = "benchmark.";

    // ProcessSetManager Failure Callback
    final EventObserver failure_observer = new EventObserver() {
        @Override
        public void update(Observable o, Object arg) {
            assert(arg != null);
            assert(arg instanceof String); // No generics :-(

            String processName = (String)arg;
            synchronized (BenchmarkController.this) {
                if (BenchmarkController.this.stop == false) {
                    LOG.fatal(String.format("Process '%s' failed. Halting benchmark!", processName));
                    BenchmarkController.this.stop = true;
                    if (self != null) {
                        BenchmarkController.this.self.interrupt();
                    }
                    cleanUpBenchmark();
                }
            } // SYNCH
        }
    };
    
    /** Dtxn.Coordinator **/
    final ProcessSetManager m_coordPSM;
    
    /** Clients **/
    final ProcessSetManager m_clientPSM;
    
    /** Server Sites **/
    final ProcessSetManager m_sitePSM;
    
    BenchmarkResults m_currentResults = null;
    Set<String> m_clients = new HashSet<String>();
    ClientStatusThread m_statusThread = null;
    Set<BenchmarkInterest> m_interested = new HashSet<BenchmarkInterest>();
    long m_maxCompletedPoll = 0;
    long m_pollCount = 0;
    Thread self = null;
    boolean stop = false;
    boolean cleaned = false;
    HStoreConf hstore_conf;
    AtomicBoolean m_statusThreadShouldContinue = new AtomicBoolean(true);
    AtomicInteger m_clientsNotReady = new AtomicInteger(0);
    AtomicInteger m_pollIndex = new AtomicInteger(0);

    final static String m_tpccClientClassName = "org.voltdb.benchmark.tpcc.TPCCClient"; // DEFAULT

    // benchmark parameters
    final BenchmarkConfig m_config;
    ResultsUploader resultsUploader = null;

    Class<? extends ClientMain> m_clientClass = null;
    Class<? extends VoltProjectBuilder> m_builderClass = null;
    Class<? extends ClientMain> m_loaderClass = null;

    VoltProjectBuilder m_projectBuilder;
    String m_jarFileName = null;
    ServerThread m_localserver = null;
    
    /**
     * SiteId -> <Host, Port>
     */
    Map<Integer, Pair<String, Integer>> m_launchHosts;
    
    private Catalog catalog;
    
    /**
     * Keeps track of any files to send to clients
     */
    private final BenchmarkClientFileUploader m_clientFileUploader = new BenchmarkClientFileUploader();

    public static interface BenchmarkInterest {
        public void benchmarkHasUpdated(BenchmarkResults currentResults);
    }

    class ClientStatusThread extends Thread {

        @Override
        public void run() {
            long resultsToRead = m_pollCount * m_clients.size();

            while (resultsToRead > 0) {
                ProcessSetManager.OutputLine line = m_clientPSM.nextBlocking();
                if (line.stream == ProcessSetManager.Stream.STDERR) {
                    System.err.printf("(%s): \"%s\"\n", line.processName, line.value);
                    continue;
                }

                // assume stdout at this point

                // General Debug Output
                if (line.value.startsWith(ClientMain.CONTROL_PREFIX) == false) {
                    System.out.println(line.value);
                    
                // BenchmarkController Coordination Message
                } else {
                    // split the string on commas and strip whitespace
                    String control_line = line.value.substring(ClientMain.CONTROL_PREFIX.length());
                    String[] parts = control_line.split(",");
                    for (int i = 0; i < parts.length; i++)
                        parts[i] = parts[i].trim();
    
                    // expect at least time and status
                    if (parts.length < 2) {
                        if (line.value.startsWith("Listening for transport dt_socket at address:") ||
                                line.value.contains("Attempting to load") ||
                                line.value.contains("Successfully loaded native VoltDB library")) {
                            LOG.info(line.processName + ": " + control_line + "\n");
                            continue;
                        }
    //                    m_clientPSM.killProcess(line.processName);
    //                    LogKeys logkey =
    //                        LogKeys.benchmark_BenchmarkController_ProcessReturnedMalformedLine;
    //                    LOG.l7dlog( Level.ERROR, logkey.name(),
    //                            new Object[] { line.processName, line.value }, null);
                        continue;
                    }
    
                    long time = -1;
                    try {
                        time = Long.parseLong(parts[0]);
                    } catch (NumberFormatException ex) {
                        continue; // IGNORE
                    }
                    String status = parts[1];
                    
                    if (trace.get()) LOG.trace(String.format("Client '%s' Status: %s", line.processName, status));
    
                    if (status.equals("READY")) {
//                        LogKeys logkey = LogKeys.benchmark_BenchmarkController_GotReadyMessage;
//                        LOG.l7dlog( Level.INFO, logkey.name(),
//                                new Object[] { line.processName }, null);
                        if (debug.get()) LOG.debug(String.format("Got ready message for '%s'.", line.processName));
                        m_clientsNotReady.decrementAndGet();
                    }
                    else if (status.equals("ERROR")) {
                        m_clientPSM.killProcess(line.processName);
//                        LogKeys logkey = LogKeys.benchmark_BenchmarkController_ReturnedErrorMessage;
//                        LOG.l7dlog( Level.ERROR, logkey.name(),
//                                new Object[] { line.processName, parts[2] }, null);
                        LOG.error(
                                "(" + line.processName + ") Returned error message:\n"
                                + " \"" + parts[2] + "\"\n");
                        continue;
                    }
                    else if (status.equals("RUNNING")) {
                        // System.out.println("Got running message: " + Arrays.toString(parts));
                        HashMap<String, Long> results = new HashMap<String, Long>();
                        if ((parts.length % 2) != 0) {
                            m_clientPSM.killProcess(line.processName);
                            LogKeys logkey =
                                LogKeys.benchmark_BenchmarkController_ProcessReturnedMalformedLine;
                            LOG.l7dlog( Level.ERROR, logkey.name(),
                                    new Object[] { line.processName, control_line }, null);
                            continue;
                        }
                        for (int i = 2; i < parts.length; i += 2) {
                            String txnName = parts[i];
                            long txnCount = Long.valueOf(parts[i+1]);
                            results.put(txnName, txnCount);
                        }
                        resultsToRead--;
                        setPollResponseInfo(line.processName, time, results, null);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public BenchmarkController(BenchmarkConfig config, Catalog catalog) {
        m_config = config;
        self = Thread.currentThread();
        hstore_conf = HStoreConf.singleton();
        this.catalog = catalog;
        
        // Setup ProcessSetManagers...
        m_clientPSM = new ProcessSetManager(hstore_conf.client.log_dir, this.failure_observer);
        m_sitePSM = new ProcessSetManager(hstore_conf.site.log_dir, this.failure_observer);
        m_coordPSM = new ProcessSetManager(hstore_conf.coordinator.log_dir, this.failure_observer);

        try {
            m_clientClass = (Class<? extends ClientMain>)Class.forName(m_config.client);
            //Hackish, client expected to have these field as a static member
            Field builderClassField = m_clientClass.getField("m_projectBuilderClass");
            Field loaderClassField = m_clientClass.getField("m_loaderClass");
            Field jarFileNameField = m_clientClass.getField("m_jarFileName");
            m_builderClass = (Class<? extends VoltProjectBuilder>)builderClassField.get(null);
            m_loaderClass = (Class<? extends ClientMain>)loaderClassField.get(null);
            m_jarFileName = (String)jarFileNameField.get(null);
//            if (m_config.localmode == false) {
//                m_jarFileName = config.hosts[0] + "." + m_jarFileName;
//            }
        } catch (Exception e) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_ErrorDuringReflectionForClient;
            LOG.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { m_config.client }, e);
            System.exit(-1);
        }

        resultsUploader = new ResultsUploader(m_config.client, config);

        try {
            m_projectBuilder = m_builderClass.newInstance();
        } catch (Exception e) {
            LogKeys logkey =
                LogKeys.benchmark_BenchmarkController_UnableToInstantiateProjectBuilder;
            LOG.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { m_builderClass.getSimpleName() }, e);
            System.exit(-1);
        }
        m_projectBuilder.addAllDefaults();

        if (config.snapshotFrequency != null
                && config.snapshotPath != null
                && config.snapshotPrefix != null
                && config.snapshotRetain > 0) {
            m_projectBuilder.setSnapshotSettings(
                    config.snapshotFrequency,
                    config.snapshotRetain,
                    config.snapshotPath,
                    config.snapshotPrefix);
        }
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
        // actually compile and write the catalog to disk
        if (m_config.compileBenchmark) {
            if (m_config.hosts.length == 0) m_config.hosts = new String[] { "localhost" };
            
        m_projectBuilder.compile(
                m_jarFileName,
                m_config.sitesPerHost,
                m_config.hosts.length,
                m_config.k_factor,
                m_config.hosts[0]);
        } else {
            if (debug.get()) LOG.debug("Skipping benchmark project compilation");
        }
        if (m_config.compileOnly) {
            assert(FileUtil.exists(m_jarFileName)) : "Failed to create jar file '" + m_jarFileName + "'";
            LOG.info("Compilation complete. Exiting [" + m_jarFileName + "]");
            System.exit(0);
        }
        
        // Load the catalog that we just made
        if (debug.get()) LOG.debug("Loading catalog from '" + m_jarFileName + "'");
        this.catalog = CatalogUtil.loadCatalogFromJar(m_jarFileName);
        assert(catalog != null);
        
        // Now figure out which hosts we really want to launch this mofo on
        Set<String> unique_hosts = new HashSet<String>();
        if (m_config.useCatalogHosts == false) {
            if (debug.get()) LOG.debug("Creating host information from BenchmarkConfig");
            m_launchHosts = new HashMap<Integer, Pair<String,Integer>>();
            int site_id = VoltDB.FIRST_SITE_ID;
            for (String host : m_config.hosts) {
                if (trace.get()) LOG.trace(String.format("Creating host info for %s: %s:%d",
                                                         HStoreSite.formatSiteName(site_id), host, VoltDB.DEFAULT_PORT));
                m_launchHosts.put(site_id, Pair.of(host, VoltDB.DEFAULT_PORT));
                unique_hosts.add(host);
                site_id++;
            } // FOR
        } else {
            if (debug.get()) LOG.debug("Collecting host information from catalog");
            m_launchHosts = CatalogUtil.getExecutionSites(catalog);
            for (Entry<Integer, Pair<String, Integer>> e : m_launchHosts.entrySet()) {
                if (trace.get()) LOG.trace(String.format("Retrieved host info for %s from catalog: %s:%d",
                                                         HStoreSite.formatSiteName(e.getKey()), e.getValue().getFirst(), e.getValue().getSecond()));
                unique_hosts.add(e.getValue().getFirst());
            } // FOR
        }

        // copy the catalog to the servers, but don't bother in local mode
//        boolean status;
        if (m_config.localmode == false) {
            // HACK
            m_config.hosts = new String[unique_hosts.size()];
            unique_hosts.toArray(m_config.hosts);
            
            HashSet<String> copyto_hosts = new HashSet<String>();
            CollectionUtil.addAll(copyto_hosts, unique_hosts);
            CollectionUtil.addAll(copyto_hosts, m_config.clients);
            
            Set<Thread> threads = new HashSet<Thread>();
            
            // Dtxn.Coordinator
            if (m_config.noCoordinator == false) {
                KillStragglers ks = new KillStragglers(m_config.remoteUser, m_config.coordinatorHost, m_config.remotePath, m_config.sshOptions)
                                            .enableKillClient()
                                            .enableKillCoordinator();
                threads.add(new Thread(ks));
                Runtime.getRuntime().addShutdownHook(new Thread(ks));
            }
            // HStoreSite
            // IMPORTANT: Don't try to kill things if we're going to profile... for obvious reasons... duh!
            if (m_config.profileSiteIds.isEmpty()) {
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

            
            if (debug.get()) LOG.debug("Killing stragglers on " + threads.size() + " hosts");
            try {
                ThreadUtil.runNewPool(threads, Math.min(25, threads.size())); 
            } catch (Exception e) {
                LogKeys logkey = LogKeys.benchmark_BenchmarkController_UnableToRunRemoteKill;
                LOG.l7dlog(Level.FATAL, logkey.name(), e);
                LOG.fatal("Couldn't run remote kill operation.", e);
                System.exit(-1);
            }
            
            // START THE SERVERS
            this.startSites(catalog);
            
        } else {
            // START A SERVER LOCALLY IN-PROCESS
            VoltDB.Configuration localconfig = new VoltDB.Configuration();
            localconfig.m_pathToCatalog = m_jarFileName;
            m_localserver = new ServerThread(localconfig);
            m_localserver.start();
            m_localserver.waitForInitialization();
        }

        final int numClients = (m_config.clients.length * m_config.processesPerClient);
        if (m_loaderClass != null && !m_config.noDataLoad) {
            this.startLoader(catalog, numClients);
        } else if (m_config.noDataLoad) {
            LOG.info("Skipping data loading phase");
        }
        LOG.info("Completed loading phase");

        //Start the clients
        this.startClients(numClients);
        
        // registerInterest(uploader);
    }
    
    public void startSites(final Catalog catalog) {
        if (debug.get()) LOG.debug("Number of hosts to start: " + m_launchHosts.size());
        int hosts_started = 0;
        
        List<String> siteBaseCommand = new ArrayList<String>();
        siteBaseCommand.add("ant hstore-site");
        siteBaseCommand.add("-Dproperties=" + m_config.hstore_conf_path);
        siteBaseCommand.add("-Dcoordinator.host=" + m_config.coordinatorHost);
        siteBaseCommand.add("-Dproject=" + m_projectBuilder.getProjectName());
        for (Entry<String, String> e : m_config.siteParameters.entrySet()) {
            siteBaseCommand.add(String.format("-D%s=%s", e.getKey(), e.getValue()));
        } // FOR

        for (Entry<Integer, Pair<String, Integer>> e : m_launchHosts.entrySet()) {
            Integer site_id = e.getKey();
            String host = e.getValue().getFirst();
            Integer port = e.getValue().getSecond();
            String host_id = String.format("site-%02d-%s", site_id, host);
            
            // Check whether this one of the sites that will be started externally
            if (m_config.profileSiteIds.contains(site_id)) {
                LOG.info(String.format("Skipping HStoreSite %s because it will be started by profiler", HStoreSite.formatSiteName(site_id)));
                continue;
            }
            
            LOG.info(String.format("Starting HStoreSite %s on %s:%d", HStoreSite.formatSiteName(site_id), host, port));

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
            if (trace.get()) LOG.trace("START " + HStoreSite.formatSiteName(site_id) + ": " + fullCommand);
            m_sitePSM.startProcess(host_id, exec_command);
            hosts_started++;
        } // FOR

        // START: Dtxn.Coordinator
        if (m_config.noCoordinator == false) {
            String host = m_config.coordinatorHost;
            List<String> dtxnCommand = new ArrayList<String>();
            dtxnCommand.add("ant dtxn-coordinator");
            dtxnCommand.add("-Dproject=" + m_projectBuilder.getProjectName());
            dtxnCommand.add("-Dcoordinator.delay=" + hstore_conf.coordinator.delay);
            dtxnCommand.add("-Dcoordinator.port=" + hstore_conf.coordinator.port);

            String command[] = SSHTools.convert(m_config.remoteUser, host, m_config.remotePath, m_config.sshOptions, dtxnCommand);
            String fullCommand = StringUtil.join(" ", command);
            if (trace.get()) LOG.trace("START COORDINATOR: " + fullCommand);
            m_coordPSM.startProcess("dtxn-" + host, command);
            LOG.info("Started Dtxn.Coordinator on " + host + ":" + hstore_conf.coordinator.port);
        }
        
        // WAIT FOR SERVERS TO BE READY
        int waiting = hosts_started;
        if (waiting > 0) {
            LOG.info("Waiting for " + waiting + " HStoreSites to finish initialization");
            do {
                ProcessSetManager.OutputLine line = m_sitePSM.nextBlocking();
                if (line == null) break;
                if (line.value.contains(HStoreSite.SITE_READY_MSG)) {
                    waiting--;
                }
            } while (waiting > 0);
            if (waiting > 0) {
                throw new RuntimeException("Failed to start all HStoreSites. Halting benchmark");
            }
        }
        LOG.info("All remote HStoreSites are initialized");

    }
    
    public void startLoader(final Catalog catalog, final int numClients) {
        if (debug.get()) LOG.debug("Starting loader: " + m_loaderClass);
        final ArrayList<String> allArgs = new ArrayList<String>();
        final ArrayList<String> loaderCommand = new ArrayList<String>();

        // set loader max heap to MAX(1M,6M) based on thread count.
        int lthreads = 2;
        if (m_config.clientParameters.containsKey("loadthreads")) {
            lthreads = Integer.parseInt(m_config.clientParameters.get("loadthreads"));
            if (lthreads < 1) lthreads = 1;
            if (lthreads > 6) lthreads = 6;
        }
        int loaderheap = 1024 * lthreads;
        if (trace.get()) LOG.trace("LOADER HEAP " + loaderheap);

        String debugString = "";
        if (m_config.listenForDebugger) {
            debugString = " -agentlib:jdwp=transport=dt_socket,address=8002,server=y,suspend=n ";
        }

        loaderCommand.add("java");
        loaderCommand.add("-Dhstore.tag=loader");
        loaderCommand.add("-XX:-ReduceInitialCardMarks");
        loaderCommand.add("-XX:+HeapDumpOnOutOfMemoryError");
        loaderCommand.add("-XX:HeapDumpPath=" + hstore_conf.global.temp_dir);
        loaderCommand.add(String.format("-Xmx%dm", loaderheap));
        if (debugString.isEmpty() == false) loaderCommand.add(debugString); 
        
        String classpath = ""; // Disable this so that we just pull from the build dir -> "hstore.jar" + ":" + m_jarFileName;
        if (System.getProperty("java.class.path") != null) {
            classpath = classpath + ":" + System.getProperty("java.class.path");
        }
        loaderCommand.add("-cp \"" + classpath + "\"");
        loaderCommand.add(m_loaderClass.getCanonicalName());
        
        for (Site catalog_site : CatalogUtil.getCluster(catalog).getSites()) {
            String address = String.format("%s:%d", catalog_site.getHost().getIpaddr(), catalog_site.getProc_port());
            allArgs.add("HOST=" + address);
            if (trace.get()) LOG.trace(String.format("HStoreSite %s: %s", HStoreSite.formatSiteName(catalog_site.getId()), address));
        } // FOR

        allArgs.add("BENCHMARK.CONF=" + m_config.benchmark_conf_path);
        allArgs.add("NUMCLIENTS=" + numClients);
        allArgs.add("STATSDATABASEURL=" + m_config.statsDatabaseURL);
        allArgs.add("STATSPOLLINTERVAL=" + m_config.interval);

        for (Entry<String,String> e : m_config.clientParameters.entrySet()) {
            if (e.getKey().equals("TXNRATE")) {
                continue;
            }
            String arg = String.format("%s=%s", e.getKey(), e.getValue());
            allArgs.add(arg);
        } // FOR

        // RUN THE LOADER
//        if (true || m_config.localmode) {
            allArgs.add("EXITONCOMPLETION=false");
            
            ClientMain.main(m_loaderClass, m_clientFileUploader, allArgs.toArray(new String[0]), true);
            
//        }
//        else {
//            if (debug.get()) LOG.debug("Loader Command: " + loaderCommand.toString());
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
     * 
     */
    public void startClients(final int numClients) {
        
        // java -cp voltdbfat.jar org.voltdb.benchmark.tpcc.TPCCClient warehouses=X etc...
        final ArrayList<String> allClientArgs = new ArrayList<String>();
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

        /*
         * This is needed to do database verification at the end of the run. In
         * order load the snapshot tables, we need the checksum stuff in the
         * native library.
         */
        allClientArgs.add("-Djava.library.path=.");

        String classpath = ""; // "voltdbfat.jar" + ":" + m_jarFileName;
        if (System.getProperty("java.class.path") != null) {
            classpath = classpath + ":" + System.getProperty("java.class.path");
        }
        allClientArgs.add("-cp");
        allClientArgs.add("\"" + classpath + "\"");

        allClientArgs.add(m_clientClass.getCanonicalName());
        for (Entry<String,String> userParam : m_config.clientParameters.entrySet()) {
            allClientArgs.add(userParam.getKey() + "=" + userParam.getValue());
        }

        allClientArgs.add("CONF=" + m_config.hstore_conf_path);
        allClientArgs.add("BENCHMARK.CONF=" + m_config.benchmark_conf_path);
        allClientArgs.add("CHECKTRANSACTION=" + m_config.checkTransaction);
        allClientArgs.add("CHECKTABLES=" + m_config.checkTables);
        allClientArgs.add("STATSDATABASEURL=" + m_config.statsDatabaseURL);
        allClientArgs.add("STATSPOLLINTERVAL=" + m_config.interval);
        
        for (Pair<String, Integer> p : m_launchHosts.values()) {
            allClientArgs.add("HOST=" + p.getFirst() + ":" + p.getSecond());
        } // FOR

        final AtomicInteger clientIndex = new AtomicInteger(0);
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (final String clientHost : m_config.clients) {
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    
                    for (int j = 0; j < m_config.processesPerClient; j++) {
                        int clientId = clientIndex.getAndIncrement();
                        String host_id = String.format("client-%02d-%s", clientId, clientHost);
                        List<String> client_args = new ArrayList<String>(allClientArgs);
                        
                        if (m_config.listenForDebugger) {
                            String arg = "-agentlib:jdwp=transport=dt_socket,address="
                                + (8003 + j) + ",server=y,suspend=n ";
                            client_args.set(1, arg);
                        }
                        
                        // Check whether we need to send files to this client
                        if (m_clientFileUploader.hasFilesToSend(clientId)) {
                            for (Entry<String, Pair<File, File>> e : m_clientFileUploader.getFilesToSend(clientId).entrySet()) {
                                String param = e.getKey();
                                File local_file = e.getValue().getFirst();
                                File remote_file = e.getValue().getSecond();
                                if (local_file.exists() == false) {
                                    LOG.warn(String.format("Not sending %s file to client %d. The local file '%s' does not exist", param, clientId, local_file));
                                    continue;
                                }
                                if (debug.get()) LOG.debug(String.format("Copying %s file '%s' to '%s' on client %s [clientId=%d]",
                                		                                 param, local_file, remote_file, clientHost, clientId)); 
                                SSHTools.copyToRemote(local_file.getPath(), m_config.remoteUser, clientHost, remote_file.getPath(), m_config.sshOptions);
                                client_args.add(param + "=" + remote_file.getPath());
                            } // FOR
                        }
                        
                        client_args.add("ID=" + clientId);
                        client_args.add("NUMCLIENTS=" + numClients);
        
                        String args[] = SSHTools.convert(m_config.remoteUser, clientHost, m_config.remotePath, m_config.sshOptions, client_args);
                        String fullCommand = StringUtil.join(" ", args);
        
                        resultsUploader.setCommandLineForClient(host_id, fullCommand);
                        if (trace.get()) LOG.trace("Client Commnand: " + fullCommand);
                        m_clientPSM.startProcess(host_id, args);
                    } // FOR
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        String[] clientNames = m_clientPSM.getProcessNames();
        for (String name : clientNames) {
            m_clients.add(name);
        }
        m_clientsNotReady.set(m_clientPSM.size());

        registerInterest(new ResultsPrinter());
    }

    protected Client getClientConnection() {
        // Connect to the first host and tell them to dump out the database contents
        Integer site_id = CollectionUtil.getRandomValue(m_launchHosts.keySet());
        assert(site_id != null);
        Pair<String, Integer> p = m_launchHosts.get(site_id);
        assert(p != null);
        if (debug.get()) LOG.debug(String.format("Creating new client connection to HStoreSite %s", HStoreSite.formatSiteName(site_id)));
        
        Client new_client = ClientFactory.createClient(128, null, false, null);
        try {
            new_client.createConnection(p.getFirst(), p.getSecond(), "user", "password");
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Failed to connect to HStoreSite %s at %s:%d",
                                                     HStoreSite.formatSiteName(site_id), p.getFirst(), p.getSecond()));
        }
        return (new_client);
    }
    
    /**
     * RUN BENCHMARK
     */
    public void runBenchmark() {
        if (this.stop) return;
        LOG.info(String.format("Starting execution phase with %d clients [hosts=%d, perhost=%d, txnrate=%s, blocking=%s]",
                                m_clients.size(),
                                m_config.clients.length,
                                m_config.processesPerClient,
                                m_config.clientParameters.get("TXNRATE"),
                                m_config.clientParameters.get("BLOCKING")
        ));
        
        // HACK
        int gdb_sleep = 0;
        if (gdb_sleep > 0) {
            LOG.info("Sleeping for " + gdb_sleep + " waiting for GDB");
            ThreadUtil.sleep(gdb_sleep*1000);
        }
        
        m_currentResults = new BenchmarkResults(m_config.interval, m_config.duration, m_clients.size());
        m_statusThread = new ClientStatusThread();
        m_statusThread.setDaemon(true);
        m_pollCount = m_config.duration / m_config.interval;
        m_statusThread.start();

        long nextIntervalTime = m_config.interval;
        
        Client local_client = null;

        // spin on whether all clients are ready
        while (m_clientsNotReady.get() > 0)
            Thread.yield();

        // start up all the clients
        for (String clientName : m_clients)
            m_clientPSM.writeToProcess(clientName, Command.START);

        // Warm-up
        if (m_config.warmup > 0) {
            LOG.info(String.format("Letting system warm-up for %.01f seconds", m_config.warmup / 1000.0));
            
            try {
                Thread.sleep(m_config.warmup);
            } catch (InterruptedException e) {
                if (debug.get()) LOG.debug("Warm-up was interrupted!");
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
                    if (local_client == null) local_client = this.getClientConnection();
                    LOG.info("Requesting HStoreSites to recalculate Markov models after warm-up");
                    try {
                        local_client.callProcedure("@RecomputeMarkovs", false);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to recompute Markov models", ex);
                    }
                }
                
                // Reset the counters
                for (String clientName : m_clients)
                    m_clientPSM.writeToProcess(clientName, Command.CLEAR);
                
                LOG.info("Starting benchmark stats collection");
            }
        }
        
        long startTime = System.currentTimeMillis();
        nextIntervalTime += startTime;
        long nowTime = startTime;
        while (m_pollIndex.get() < m_pollCount && this.stop == false) {

            // check if the next interval time has arrived
            if (nowTime >= nextIntervalTime) {
                m_pollIndex.incrementAndGet();

                // make all the clients poll
                if (debug.get()) LOG.debug(String.format("Sending %s to %d clients", Command.POLL, m_clients.size()));
                for (String clientName : m_clients)
                {
                    m_clientPSM.writeToProcess(clientName, Command.POLL);               	
                }
 
                // get ready for the next interval
                nextIntervalTime = m_config.interval * (m_pollIndex.get() + 1) + startTime;
            }

            // wait some time
            long sleep = nextIntervalTime - nowTime;
            try {
                if (this.stop == false && sleep > 0) {
                    if (debug.get()) LOG.debug("Sleeping for " + sleep + " ms");
                    Thread.sleep(sleep);
                }
            } catch (InterruptedException e) {
                // Ignore...
            }
            nowTime = System.currentTimeMillis();
        } // WHILE
        
        // Dump database
        if (m_config.dumpDatabase && this.stop == false) {
            assert(m_config.dumpDatabaseDir != null);
            
            // We have to tell all our clients to pause first
            m_clientPSM.writeToAll(Command.PAUSE);
            
            if (local_client == null) local_client = this.getClientConnection();
            try {
                local_client.callProcedure("@DatabaseDump", m_config.dumpDatabaseDir);
            } catch (Exception ex) {
                LOG.error("Failed to dump database contents", ex);
            }
        }
        
        // Recompute MarkovGraphs
        if (m_config.markovRecomputeAfterEnd && this.stop == false) {
            // We have to tell all our clients to pause first
            m_clientPSM.writeToAll(Command.PAUSE);
            
            if (local_client == null) local_client = this.getClientConnection();
            this.recomputeMarkovs(local_client);
        }

        this.stop = true;
        m_clientPSM.prepareShutdown();
        m_sitePSM.prepareShutdown();
        m_coordPSM.prepareShutdown();
        
        // shut down all the clients
        boolean first = true;
        for (String clientName : m_clients) {
            if (first && m_config.noShutdown == false) {
                m_clientPSM.writeToProcess(clientName, Command.SHUTDOWN);
                first = false;
            } else {
                m_clientPSM.writeToProcess(clientName, Command.STOP);
            }
        }
        LOG.info("Waiting for " + m_clients.size() + " clients to finish");
        for (String clientName : m_clients)
            m_clientPSM.joinProcess(clientName);

        LOG.info("Waiting for status thread to finish");
        try {
            m_statusThread.join(1000);
        } catch (InterruptedException e) {
            LOG.warn(e);
        }
    }
    
    private void recomputeMarkovs(Client client) {
        String output_directory = hstore_conf.global.temp_dir + "/markovs/" + m_projectBuilder.getProjectName();
        FileUtil.makeDirIfNotExists(output_directory);
        Database catalog_db = CatalogUtil.getDatabase(catalog);

        ThreadUtil.sleep(60000);
        LOG.info("Requesting HStoreSites to recalculate Markov models");
        ClientResponse cr = null;
        try {
            cr = client.callProcedure("@RecomputeMarkovs", true);
        } catch (Exception ex) {
            LOG.error("Failed to recompute MarkovGraphs", ex);
            return;
        }
        assert(cr != null);
        
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
            
            Pair<String, Integer> p = m_launchHosts.get(site_id);
            assert(p != null) : "Invalid SiteId " + site_id;
            
            if (debug.get()) LOG.debug(String.format("Retrieving MarkovGraph file '%s' from %s", remote_path, HStoreSite.formatSiteName(site_id)));
            SSHTools.copyFromRemote(output_directory, m_config.remoteUser, p.getFirst(), remote_path.getPath(), m_config.sshOptions);
            File local_file = new File(output_directory + "/" + remote_path.getName());
            markovs.put(partition_id, local_file);
            files_to_remove.add(Pair.of((String)null, local_file));
            files_to_remove.add(Pair.of(p.getFirst(), remote_path));
        } // FOR
        
        String new_output = output_directory + "/" + m_projectBuilder.getProjectName() + "-new.markovs";
        if (debug.get()) LOG.debug(String.format("Writing %d updated MarkovGraphsContainers to '%s'", markovs.size(),  new_output));
        MarkovUtil.combine(markovs, new_output, catalog_db);
        
        // Clean up the remote files
        for (Pair<String, File> p : files_to_remove) {
            if (p.getFirst() == null) {
                p.getSecond().delete();
            } else {
                SSHTools.deleteFile(m_config.remoteUser, p.getFirst(), p.getSecond().getPath(), m_config.sshOptions);
            }
        } // FOR
    }
    
    /**
     * Cleanup Benchmark
     */
    public synchronized void cleanUpBenchmark() {
        // if (this.cleaned) return;
        
        if (debug.get()) LOG.debug("Killing clients");
        m_clientPSM.shutdown();

        if (debug.get()) LOG.debug("Killing nodes");
        m_sitePSM.shutdown();
        
        if (m_config.noCoordinator == false) {
            ThreadUtil.sleep(1000); // HACK
            if (debug.get()) LOG.debug("Killing Dtxn.Coordinator");
            m_coordPSM.shutdown();
        }
        this.cleaned = true;
    }


    /**
     *
     * @return A ResultSet instance for the ongoing or just finished benchmark run.
     */
    public BenchmarkResults getResults() {
        assert(m_currentResults != null);
        synchronized(m_currentResults) {
            return m_currentResults.copy();
        }
    }


    void setPollResponseInfo(
            String clientName,
            long time,
            Map<String, Long> transactionCounts,
            String errMsg)
    {
        assert(m_currentResults != null);
        BenchmarkResults resultCopy = null;
        int completedCount = 0;

        synchronized(m_currentResults) {
            m_currentResults.setPollResponseInfo(
                    clientName,
                    m_pollIndex.get() - 1,
                    time,
                    transactionCounts,
                    errMsg);
            completedCount = m_currentResults.getCompletedIntervalCount();
            resultCopy = m_currentResults.copy();
        }

        if (completedCount > m_maxCompletedPoll) {
            synchronized(m_interested) {
                // notify interested parties
                for (BenchmarkInterest interest : m_interested)
                    interest.benchmarkHasUpdated(resultCopy);
            }
            m_maxCompletedPoll = completedCount;

            // get total transactions run for this segment
            long txnDelta = 0;
            for (String client : resultCopy.getClientNames()) {
                for (String txn : resultCopy.getTransactionNames()) {
                    Result[] rs = resultCopy.getResultsForClientAndTransaction(client, txn);
                    Result r = rs[rs.length - 1];
                    txnDelta += r.transactionCount;
                }
            }

            // if nothing done this segment, dump everything
//            if (txnDelta == 0) {
//                tryDumpAll();
//                System.out.println("\nDUMPING!\n");
//            }
        }


    }

    /** Call dump on each of the servers */
    public void tryDumpAll() {
        Client dumpClient = ClientFactory.createClient();
        for (String host : m_config.hosts) {
            try {
                dumpClient.createConnection(host, Client.VOLTDB_SERVER_PORT, "program", "password");
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
        long interval = 10000;
        long duration = 60000;
        long warmup = 0;
        int hostCount = 1;
        int sitesPerHost = 2;
        int k_factor = 0;
        int clientCount = 1;
        int processesPerClient = 1;
        String sshOptions = "";
        String remotePath = "voltbin/";
        String remoteUser = null; // null implies current local username
        String clientClassname = m_tpccClientClassName;
        File catalogPath = null;
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
        String coordinatorHost = null;
        
        String statsTag = null;
        String applicationName = null;
        String subApplicationName = null;
        
        boolean noCoordinator = false;
        boolean noDataLoad = false;
        boolean noShutdown = false;
        
        Catalog catalog = null;
        
        // HStoreConf Path
        String hstore_conf_path = null;
        
        // Benchmark Conf Path
        String benchmark_conf_path = null;
        
        // Markov Stuff
        String markov_path = null;
        String markov_thresholdsPath = null;
        Double markov_thresholdsValue = null;
        boolean markov_recomputeAfterEnd = false;
        boolean markov_recomputeAfterWarmup = false;
        
        // Logging
        String clientLogDir = "/tmp";
        String siteLogDir = "/tmp";
        String coordLogDir = "/tmp";
        
        boolean dumpDatabase = false;
        String dumpDatabaseDir = null;
        
        // List of SiteIds that we won't start because they'll be started by the profiler
        Set<Integer> profileSiteIds = new HashSet<Integer>();

        // try to read connection string for reporting database
        // from a "mysqlp" file
        // set value to null on failure
        String[] databaseURL = { "localhost", "localhost" };
//        try {
//            databaseURL = readConnectionStringFromFile(remotePath);
//            assert(databaseURL.length == 2);
//        }
//        catch (RuntimeException e) {
//            databaseURL = new String[2];
//            System.out.println(e.getMessage());
//        }

        Map<String, String> clientParams = new LinkedHashMap<String, String>();
        Map<String, String> siteParams = new LinkedHashMap<String, String>();
        
        for (String arg : vargs) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
                
            } else if (parts[0].equalsIgnoreCase("CONF")) {
                hstore_conf_path = parts[1];
                
                
            } else if (parts[0].equalsIgnoreCase(BENCHMARK_PARAM_PREFIX + "CONF")) {
                benchmark_conf_path = parts[1];
                
            } else if (parts[0].equalsIgnoreCase("CHECKTRANSACTION")) {
                /*
                 * Whether or not to check the result of each transaction.
                 */
                checkTransaction = Float.parseFloat(parts[1]);
            } else if (parts[0].equalsIgnoreCase("CHECKTABLES")) {
                /*
                 * Whether or not to check all the tables at the end.
                 */
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
            } else if (parts[0].equalsIgnoreCase("CLIENTCOUNT")) {
                /*
                 * The number of client hosts to place client processes on
                 */
                clientCount = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("PROCESSESPERCLIENT")) {
                /*
                 * The number of client processes per client host
                 */
                processesPerClient = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("CLIENTHEAP")) {
                /*
                 * The number of client processes per client host
                 */
                clientHeapSize = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("SERVERHEAP")) {
                /*
                 * The number of client processes per client host
                 */
                serverHeapSize = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("INTERVAL")) {
                /*
                 * The interval to poll for results in milliseconds
                 */
                interval = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("DURATION")) {
                /*
                 * Duration of the benchmark in milliseconds (not including warmup)
                 */
                duration = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase("WARMUP")) {
                /*
                 * Amount of warmup time in milliseconds
                 */
                warmup = Integer.parseInt(parts[1]);
            } else if (parts[0].equalsIgnoreCase(BENCHMARK_PARAM_PREFIX +  "CLIENT")) {
                /*
                 * Name of the client class for this benchmark.
                 *
                 * This is a class that extends ClientMain and has associated
                 * with it a VoltProjectBuilder implementation and possibly a
                 * Loader that also extends ClientMain
                 */
                clientClassname = parts[1];
            } else if (parts[0].equalsIgnoreCase("SSHOPTIONS")) {
                /*
                 * Options used when logging into client/server hosts
                 */
                sshOptions = parts[1];

            } else if (parts[0].equalsIgnoreCase("REMOTEPATH")) {
                /*
                 * Directory on the NFS host where the VoltDB files are stored
                 */
                remotePath = parts[1];
            } else if (parts[0].equalsIgnoreCase("REMOTEUSER")) {
                /*
                 * User that runs volt on remote client and host machines
                 */
                remoteUser =  parts[1];
            } else if (parts[0].equalsIgnoreCase("HOST") || parts[0].equalsIgnoreCase("CLIENTHOST")) {
                //Do nothing, parsed later.
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
            } else if (parts[0].equalsIgnoreCase("TXNRATE")) {
                clientParams.put(parts[0], parts[1]);
            } else if (parts[0].equalsIgnoreCase("BLOCKING")) {
                clientParams.put(parts[0], parts[1]);
            } else if (parts[0].equalsIgnoreCase("THROTTLING")) {
                clientParams.put(parts[0], parts[1]);
            } else if (parts[0].equalsIgnoreCase("NUMCONNECTIONS")) {
                clientParams.put(parts[0], parts[1]);
            } else if (parts[0].equalsIgnoreCase("STATSDATABASEURL")) {
                databaseURL[0] = parts[1];
            } else if (parts[0].equalsIgnoreCase("STATSTAG")) {
                statsTag = parts[1];
            } else if (parts[0].equalsIgnoreCase("APPLICATIONNAME")) {
                applicationName = parts[1];
            } else if (parts[0].equalsIgnoreCase("SUBAPPLICATIONNAME")) {
                subApplicationName = parts[1];

            /** PAVLO **/
            } else if (parts[0].equalsIgnoreCase("COORDINATORHOST")) {
                coordinatorHost = parts[1];
            } else if (parts[0].equalsIgnoreCase("NOCOORDINATOR")) {
                noCoordinator = Boolean.valueOf(parts[1]);
                
            } else if (parts[0].equalsIgnoreCase("CATALOG")) {
                catalogPath = new File(parts[1]);
                
                catalog = CatalogUtil.loadCatalogFromJar(catalogPath.getAbsolutePath());
                assert(catalog != null);
                num_partitions = CatalogUtil.getNumberOfPartitions(catalog);
                
            } else if (parts[0].equalsIgnoreCase("COMPILE")) {
                /*
                 * Whether to compile the benchmark jar
                 */
                compileBenchmark = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equalsIgnoreCase("COMPILEONLY")) {
                /*
                 * Whether to compile only the benchmark jar and then quit
                 */
                compileOnly = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equalsIgnoreCase("CATALOGHOSTS")) {
                /*
                 * Launch the ExecutionSites using the hosts that are in the catalog
                 */
                useCatalogHosts = Boolean.parseBoolean(parts[1]);
            
            // Disable data loading
            } else if (parts[0].equalsIgnoreCase("NODATALOAD")) {
                noDataLoad = Boolean.parseBoolean(parts[1]);
                
            // Disable sending the shutdown command at the end of the benchmark run
            } else if (parts[0].equalsIgnoreCase("NOSHUTDOWN")) {
                noShutdown = Boolean.parseBoolean(parts[1]);
                LOG.info("NOSHUTDOWN = " + noShutdown);
                
            /* Workload Trace Output */
            } else if (parts[0].equalsIgnoreCase("TRACE")) {
                workloadTrace = parts[1];
                siteParams.put(ArgumentsParser.PARAM_WORKLOAD_OUTPUT, parts[1]);
            /* Disable Sites Starting */ 
            } else if (parts[0].equalsIgnoreCase("PROFILESITES")) {
                for (String s : parts[1].split(",")) {
                    profileSiteIds.add(Integer.valueOf(s));
                } // FOR
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

            /** LOGGING **/
            } else if (parts[0].equalsIgnoreCase("CLIENT.LOG_DIR")) {
                clientLogDir = parts[1];
                FileUtil.makeDirIfNotExists(clientLogDir);
            } else if (parts[0].equalsIgnoreCase("COORDINATOR.LOG_DIR")) {
                coordLogDir = parts[1];
                FileUtil.makeDirIfNotExists(coordLogDir);
            } else if (parts[0].equalsIgnoreCase("SITE.LOG_DIR")) {
                siteLogDir = parts[1];
                FileUtil.makeDirIfNotExists(siteLogDir);
            } else {
                clientParams.put(parts[0].toLowerCase(), parts[1]);
            }
        }
        assert(coordinatorHost != null) : "Missing CoordinatorHost";

        // Initialize HStoreConf
        assert(hstore_conf_path != null) : "Missing HStoreConf file";
        HStoreConf.init(new File(hstore_conf_path));
        
        if (duration < 1000) {
            System.err.println("Duration is specified in milliseconds");
            System.exit(-1);
        }

        ArrayList<String> hosts = new ArrayList<String>();
        ArrayList<String> clients = new ArrayList<String>();

        for (String arg : vargs) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
            }
            else if (parts[0].equalsIgnoreCase("HOST")) {
                /*
                 * Name of a host to be used for Volt servers
                 */
                String hostnport[] = parts[1].split("\\:",2);
                hosts.add(hostnport[0]);
            } else if (parts[0].equalsIgnoreCase("CLIENTHOST")) {
                /*
                 * Name of a host to be used for Volt clients
                 */
//                String hostnport[] = parts[1].split("\\:",2);
                for (String host : parts[1].split(",")) {
                    clients.add(host);
                }
            }
        }

        // if no hosts given, use localhost
        if (hosts.size() == 0)
            hosts.add("localhost");
        if (clients.size() == 0)
            clients.add("localhost");

        if (compileOnly == false && clients.size() < clientCount) {
            LogKeys logkey = LogKeys.benchmark_BenchmarkController_NotEnoughClients;
            LOG.l7dlog( Level.FATAL, logkey.name(),
                    new Object[] { clients.size(), clientCount }, null);
            System.exit(-1);
        }
        
        String[] hostNames = null;
        if (! (useCatalogHosts || compileOnly) ) {
            if (hosts.size() < hostCount) {
                LogKeys logkey = LogKeys.benchmark_BenchmarkController_NotEnoughHosts;
                LOG.l7dlog( Level.FATAL, logkey.name(),
                        new Object[] { hosts.size(), hostCount }, null);
                LOG.fatal("Don't have enough hosts(" + hosts.size()
                        + ") for host count " + hostCount);
                System.exit(-1);
            }
    
            // copy the lists of hostnames into array of the right lengths
            // (this truncates the list to the right number)
            hostNames = new String[hostCount];
            for (int i = 0; i < hostCount; i++)
                hostNames[i] = hosts.get(i);
        } else {
            hostNames = new String[0];
        }
        String[] clientNames = new String[clientCount];
        if (compileOnly == false) {
            for (int i = 0; i < clientCount; i++)
                clientNames[i] = clients.get(i);
        }

        // create a config object, mostly for the results uploader at this point
        BenchmarkConfig config = new BenchmarkConfig(
                hstore_conf_path,
                benchmark_conf_path,
                clientClassname,
                backend, 
                coordinatorHost,
                noCoordinator,
                hostNames,
                sitesPerHost, 
                k_factor, 
                clientNames, 
                processesPerClient, 
                interval, 
                duration,
                warmup,
                sshOptions,
                remotePath, 
                remoteUser, 
                listenForDebugger, 
                serverHeapSize, 
                clientHeapSize,
                localmode, 
                checkTransaction, 
                checkTables, 
                snapshotPath, 
                snapshotPrefix,
                snapshotFrequency, 
                snapshotRetain, 
                databaseURL[0], 
                databaseURL[1], 
                statsTag,
                applicationName, 
                subApplicationName,
                compileBenchmark, 
                compileOnly, 
                useCatalogHosts,
                noDataLoad,
                noShutdown,
                workloadTrace,
                profileSiteIds,
                markov_path,
                markov_thresholdsPath,
                markov_thresholdsValue,
                markov_recomputeAfterEnd,
                markov_recomputeAfterWarmup,
                dumpDatabase,
                dumpDatabaseDir
        );
        
        // Always pass these parameters
        clientParams.put("INTERVAL", Long.toString(interval));
        clientParams.put("DURATION", Long.toString(duration));
        if (catalogPath != null) {
            clientParams.put("CATALOG", catalogPath.getAbsolutePath());
            clientParams.put("NUMPARTITIONS", Integer.toString(num_partitions));
        }
        clientParams.put("NUMCLIENTS", Integer.toString(clientCount * processesPerClient));

        config.clientParameters.putAll(clientParams);
        config.siteParameters.putAll(siteParams);
        
        if (debug.get()) LOG.debug("Benchmark Configuration\n" + config.toString());
        
        // ACTUALLY RUN THE BENCHMARK
        BenchmarkController controller = new BenchmarkController(config, catalog);
        try {
            controller.setupBenchmark();
            controller.runBenchmark();
        } catch (Throwable ex) {
            LOG.fatal("Failed to complete benchmark", ex);
        } finally {
            controller.cleanUpBenchmark();
        }
    }
}
