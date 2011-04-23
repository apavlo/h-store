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
import java.util.Arrays;
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
import org.voltdb.benchmark.BenchmarkResults.Result;
import org.voltdb.benchmark.ClientMain.Command;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.processtools.ProcessSetManager;
import org.voltdb.processtools.SSHTools;
import org.voltdb.utils.LogKeys;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
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
    final ProcessSetManager m_serverPSM;
    
    BenchmarkResults m_currentResults = null;
    Set<String> m_clients = new HashSet<String>();
    ClientStatusThread m_statusThread = null;
    Set<BenchmarkInterest> m_interested = new HashSet<BenchmarkInterest>();
    long m_maxCompletedPoll = 0;
    long m_pollCount = 0;
    Thread self = null;
    boolean stop = false;
    boolean cleaned = false;
    AtomicBoolean m_statusThreadShouldContinue = new AtomicBoolean(true);
    AtomicInteger m_clientsNotReady = new AtomicInteger(0);
    AtomicInteger m_pollIndex = new AtomicInteger(0);

    final static String m_tpccClientClassName = "org.voltdb.benchmark.tpcc.TPCCClient"; // DEFAULT

    // benchmark parameters
    final BenchmarkConfig m_config;
    ResultsUploader uploader = null;

    Class<? extends ClientMain> m_clientClass = null;
    Class<? extends VoltProjectBuilder> m_builderClass = null;
    Class<? extends ClientMain> m_loaderClass = null;

    VoltProjectBuilder m_projectBuilder;
    String m_jarFileName = null;
    ServerThread m_localserver = null;
    
    /**
     * Triplets: <Host>, <Port>, <SiteId>
     */
    List<String[]> launch_hosts = null;

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
    public BenchmarkController(BenchmarkConfig config) {
        m_config = config;
        self = Thread.currentThread();
        
        // Setup ProcessSetManagers...
        m_clientPSM = new ProcessSetManager(m_config.clientLogDir, this.failure_observer);
        m_serverPSM = new ProcessSetManager(m_config.siteLogDir, this.failure_observer);
        m_coordPSM = new ProcessSetManager(m_config.coordLogDir, this.failure_observer);

        try {
            m_clientClass = (Class<? extends ClientMain>)Class.forName(m_config.benchmarkClient);
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
                    new Object[] { m_config.benchmarkClient }, e);
            System.exit(-1);
        }

        uploader = new ResultsUploader(m_config.benchmarkClient, config);

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
        Catalog catalog = CatalogUtil.loadCatalogFromJar(m_jarFileName);
        assert(catalog != null);
        
        // Now figure out which hosts we really want to launch this mofo on
        Set<String> unique_hosts = new HashSet<String>();
        if (m_config.useCatalogHosts == false) {
            if (debug.get()) LOG.debug("Creating host information from BenchmarkConfig");
            launch_hosts = new ArrayList<String[]>();
            Integer site_id = VoltDB.FIRST_SITE_ID;
            for (String host : m_config.hosts) {
                launch_hosts.add(new String[] {
                        host,
                        Integer.toString(VoltDB.DEFAULT_PORT),
                        site_id.toString()
                });
                unique_hosts.add(host);
                site_id++;
            } // FOR
        } else {
            if (debug.get()) LOG.debug("Collecting host information from catalog");
            launch_hosts = CatalogUtil.getExecutionSites(catalog);
            for (String[] triplet : launch_hosts) {
                if (trace.get()) LOG.trace("Retrieved execution node info from catalog: " + triplet[0] + ":" + triplet[1] + " - ExecutionSite #" + triplet[2]);
                unique_hosts.add(triplet[0]);
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
            if (debug.get()) LOG.debug("Number of hosts to start: " + launch_hosts.size());
            int hosts_started = 0;
            for (String[] triplet : launch_hosts) {
                String host = triplet[0];
                String port = triplet[1];
                int site_id = Integer.valueOf(triplet[2]);
                String host_id = String.format("site-%s-%d", host, site_id);
                
                // Check whether this one of the sites that will be started externally
                if (m_config.profileSiteIds.contains(site_id)) {
                    LOG.info(String.format("Skipping HStoreSite #%d because it will be started by profiler", site_id));
                    continue;
                }
                
                LOG.info(String.format("Starting HStoreSite on %s:%s with site id #%d", host, port, site_id));

//                String debugString = "";
//                if (m_config.listenForDebugger) {
//                    debugString =
//                        " -agentlib:jdwp=transport=dt_socket,address=8001,server=y,suspend=n ";
//                }
                // -agentlib:hprof=cpu=samples,
                // depth=32,interval=10,lineno=y,monitor=y,thread=y,force=y,
                // file=" + host + "_hprof_tpcc.txt"
                List<String> command = new ArrayList<String>();
                command.add("ant");
                command.add("hstore-site");
                command.add("-Dcoordinator.host=" + m_config.coordinatorHost);
                command.add("-Dproject=" + m_projectBuilder.getProjectName());
                command.add("-Dnode.site=" + site_id);
                if (m_config.markovPath != null) command.add("-Dmarkov=" + m_config.markovPath);
                if (m_config.thresholdsPath != null) command.add("-Dthresholds=" + m_config.thresholdsPath);
                
                // Enable workload trace outputs
                if (m_config.workloadTrace != null) {
                    command.add("-Dworkload.output=" + m_config.workloadTrace);
                }

                String exec_command[] = SSHTools.convert(m_config.remoteUser, host, m_config.remotePath, m_config.sshOptions, command.toArray(new String[]{}));
                String fullCommand = StringUtil.join(" ", exec_command);
                uploader.setCommandLineForHost(host, fullCommand);
                if (trace.get()) LOG.trace(fullCommand);
                m_serverPSM.startProcess(host_id, exec_command);
                hosts_started++;
            } // FOR

            // START: Dtxn.Coordinator
            if (m_config.noCoordinator == false) {
                String host = m_config.coordinatorHost;
                String[] command = {
                    "ant",
                    "dtxn-coordinator",
                    "-Dproject=" + m_projectBuilder.getProjectName(),
                };

                command = SSHTools.convert(m_config.remoteUser, host, m_config.remotePath, m_config.sshOptions, command);
                String fullCommand = StringUtil.join(" ", command);
                if (trace.get()) LOG.trace(fullCommand);
                m_coordPSM.startProcess("dtxn-" + host, command);
                LOG.info("Started Dtxn.Coordinator on " + host);
            }
            
            // WAIT FOR SERVERS TO BE READY
            int waiting = hosts_started;
            if (waiting > 0) {
                LOG.info("Waiting for " + waiting + " HStoreSites to finish initialization");
                
                do {
                    ProcessSetManager.OutputLine line = m_serverPSM.nextBlocking();
                    if (line == null) break;
                    if (line.value.contains(HStoreSite.SITE_READY_MSG)) {
                        waiting--;
                    }
                } while (waiting > 0);
                if (waiting > 0) {
                    LOG.fatal("Failed to start all HStoreSites. Halting benchmark");
                    return;
                }
            }
            LOG.info("All remote HStoreSites are initialized");
        }
        else {
            // START A SERVER LOCALLY IN-PROCESS
            VoltDB.Configuration localconfig = new VoltDB.Configuration();
            localconfig.m_pathToCatalog = m_jarFileName;
            m_localserver = new ServerThread(localconfig);
            m_localserver.start();
            m_localserver.waitForInitialization();
        }

        final int numClients = (m_config.clients.length * m_config.processesPerClient);
        if (m_loaderClass != null && !m_config.noDataLoad) {
            if (debug.get()) LOG.debug("Starting loader: " + m_loaderClass);
            ArrayList<String> localArgs = new ArrayList<String>();

            // set loader max heap to MAX(1M,6M) based on thread count.
            int lthreads = 2;
            if (m_config.parameters.containsKey("loadthreads")) {
                lthreads = Integer.parseInt(m_config.parameters.get("loadthreads"));
                if (lthreads < 1) lthreads = 1;
                if (lthreads > 6) lthreads = 6;
            }
            int loaderheap = 1024 * lthreads;
            if (trace.get()) LOG.trace("LOADER HEAP " + loaderheap);

            String debugString = "";
            if (m_config.listenForDebugger) {
                debugString = " -agentlib:jdwp=transport=dt_socket,address=8002,server=y,suspend=n ";
            }
            StringBuilder loaderCommand = new StringBuilder(4096);

            loaderCommand.append("java -XX:-ReduceInitialCardMarks -XX:+HeapDumpOnOutOfMemoryError " +
                    "-XX:HeapDumpPath=/tmp -Xmx" + loaderheap + "m " + debugString);
            String classpath = ""; // Disable this so that we just pull from the build dir -> "hstore.jar" + ":" + m_jarFileName;
            if (System.getProperty("java.class.path") != null) {
                classpath = classpath + ":" + System.getProperty("java.class.path");
            }
            loaderCommand.append(" -cp \"" + classpath + "\" ");
            loaderCommand.append(m_loaderClass.getCanonicalName());
            
            for (Site catalog_site : CatalogUtil.getCluster(catalog).getSites()) {
                String address = String.format("%s:%d", catalog_site.getHost().getIpaddr(), catalog_site.getProc_port());
                loaderCommand.append(" HOST=" + address);
                localArgs.add("HOST=" + address);
                if (trace.get()) LOG.trace("HStoreSite: " + address);
            }

            loaderCommand.append(" NUMCLIENTS=" + numClients + " ");
            localArgs.add(" NUMCLIENTS=1 ");
                    
            loaderCommand.append(" STATSDATABASEURL=" + m_config.statsDatabaseURL + " ");
            loaderCommand.append(" STATSPOLLINTERVAL=" + m_config.interval + " ");
            localArgs.add(" STATSDATABASEURL=" + m_config.statsDatabaseURL + " ");
            localArgs.add(" STATSPOLLINTERVAL=" + m_config.interval + " ");

            StringBuffer userParams = new StringBuffer(4096);
            for (Entry<String,String> userParam : m_config.parameters.entrySet()) {
                if (userParam.getKey().equals("TXNRATE")) {
                    continue;
                }
                userParams.append(" ");
                userParams.append(userParam.getKey());
                userParams.append("=");
                userParams.append(userParam.getValue());

                localArgs.add(userParam.getKey() + "=" + userParam.getValue());
            }

            loaderCommand.append(userParams);

            // RUN THE LOADER
//            if (true || m_config.localmode) {
                localArgs.add("EXITONCOMPLETION=false");
                ClientMain.main(m_loaderClass, localArgs.toArray(new String[0]), true);
//            }
//            else {
//                if (debug.get()) LOG.debug("Loader Command: " + loaderCommand.toString());
//                String[] command = SSHTools.convert(
//                        m_config.remoteUser,
//                        m_config.clients[0],
//                        m_config.remotePath,
//                        m_config.sshOptions,
//                        loaderCommand.toString());
//                status = ShellTools.cmdToStdOut(command);
//                assert(status);
//            }
        } else if (m_config.noDataLoad) {
            LOG.info("Skipping data loading phase");
        }
        LOG.info("Completed loading phase");

        //Start the clients
        // java -cp voltdbfat.jar org.voltdb.benchmark.tpcc.TPCCClient warehouses=X etc...
        final ArrayList<String> clArgs = new ArrayList<String>();
        clArgs.add("java");
        if (m_config.listenForDebugger) {
            clArgs.add(""); //placeholder for agent lib
        }
        clArgs.add("-ea -XX:-ReduceInitialCardMarks -XX:+HeapDumpOnOutOfMemoryError " +
                    "-XX:HeapDumpPath=/tmp -Xmx" + String.valueOf(m_config.clientHeapSize) + "m");

        /*
         * This is needed to do database verification at the end of the run. In
         * order load the snapshot tables, we need the checksum stuff in the
         * native library.
         */
        clArgs.add("-Djava.library.path=.");

        String classpath = ""; // "voltdbfat.jar" + ":" + m_jarFileName;
        if (System.getProperty("java.class.path") != null) {
            classpath = classpath + ":" + System.getProperty("java.class.path");
        }
        clArgs.add("-cp");
        clArgs.add("\"" + classpath + "\"");

        clArgs.add(m_clientClass.getCanonicalName());
        for (Entry<String,String> userParam : m_config.parameters.entrySet()) {
            clArgs.add(userParam.getKey() + "=" + userParam.getValue());
        }

        clArgs.add("CHECKTRANSACTION=" + m_config.checkTransaction);
        clArgs.add("CHECKTABLES=" + m_config.checkTables);
        clArgs.add("STATSDATABASEURL=" + m_config.statsDatabaseURL);
        clArgs.add("STATSPOLLINTERVAL=" + m_config.interval);
        
        for (String[] triplet : launch_hosts) {
            String host = triplet[0];
            String port = triplet[1];
            clArgs.add("HOST=" + host + ":" + port);
        } // FOR

        final AtomicInteger clientIndex = new AtomicInteger(0);
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (final String client : m_config.clients) {
            final List<String> client_args = new ArrayList<String>(clArgs);
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < m_config.processesPerClient; j++) {
                        int id = clientIndex.getAndIncrement();
                        String host_id = String.format("%s-%02d", client, id);
                        
                        if (m_config.listenForDebugger) {
                            clArgs.remove(1);
                            String arg = "-agentlib:jdwp=transport=dt_socket,address="
                                + (8003 + j) + ",server=y,suspend=n ";
                            clArgs.add(1, arg);
                        }
                        client_args.add("ID=" + id);
                        client_args.add("NUMCLIENTS=" + numClients);
                        String[] args = client_args.toArray(new String[0]);
        
                        args = SSHTools.convert(m_config.remoteUser, client, m_config.remotePath, m_config.sshOptions, args);
                        String fullCommand = StringUtil.join(" ", args);
        
                        uploader.setCommandLineForClient(host_id, fullCommand);
                        if (trace.get()) LOG.trace("Client Commnand: " + fullCommand);
                        m_clientPSM.startProcess(host_id, args);
                    }
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
        // registerInterest(uploader);
    }

    /**
     * RUN BENCHMARK
     */
    public void runBenchmark() {
        if (this.stop) return;
        LOG.info(String.format("Starting execution phase with %d clients [hosts=%d, perhost=%d, txnrate=%s, blocking=%s, throttling=%s]",
                                m_clients.size(),
                                m_config.clients.length,
                                m_config.processesPerClient,
                                m_config.parameters.get("TXNRATE"),
                                m_config.parameters.get("BLOCKING"),
                                m_config.parameters.get("THROTTLING")
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
                    m_clientPSM.writeToProcess(clientName, Command.POLL);

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
            
            // Connect to the first host and tell them to dump out the database contents
            String triplet[] = CollectionUtil.getRandomValue(this.launch_hosts);
            assert(triplet != null);
            LOG.info(String.format("Requesting HStoreSite #%02d to dump database contents [dir=%s]", Integer.parseInt(triplet[2]), m_config.dumpDatabaseDir));
            
            Client new_client = ClientFactory.createClient(128, null, false, null);
            try {
                new_client.createConnection(triplet[0], Integer.parseInt(triplet[1]), "user", "password");
                new_client.callProcedure("@DatabaseDump", m_config.dumpDatabaseDir);
            } catch (Exception ex) {
                LOG.error(String.format("Failed to dump database contents using '%s'", Arrays.toString(triplet)), ex);
            }
        }

        this.stop = true;
        m_clientPSM.prepareToShutdown();
        m_serverPSM.prepareToShutdown();
        m_coordPSM.prepareToShutdown();
        
        // shut down all the clients
        boolean first = true;
        for (String clientName : m_clients) {
            if (first) {
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
    
    /**
     * Cleanup Benchmark
     */
    public synchronized void cleanUpBenchmark() {
        // if (this.cleaned) return;
        
        if (debug.get()) LOG.debug("Killing clients");
        m_clientPSM.killAll();

        if (debug.get()) LOG.debug("Killing nodes");
        m_serverPSM.killAll();
        
        if (m_config.noCoordinator == false) {
            ThreadUtil.sleep(1000); // HACK
            if (debug.get()) LOG.debug("Killing Dtxn.Coordinator");
            m_coordPSM.killAll();
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

    public static void main(final String[] vargs) {
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
        String useProfile = "";
        boolean compileBenchmark = true;
        boolean compileOnly = false;
        boolean useCatalogHosts = false;
        boolean noDataLoad = false;
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
        boolean noCoordinator = false;
        String statsTag = null;
        String applicationName = null;
        String subApplicationName = null;
        
        // Markov Stuff
        String markov_path = null;
        String thresholds_path = null;
        
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

        LinkedHashMap<String, String> clientParams = new LinkedHashMap<String, String>();
        for (String arg : vargs) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1) {
                continue;
            } else if (parts[1].startsWith("${")) {
                continue;
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
            } else if (parts[0].equalsIgnoreCase("USEPROFILE")) {
                useProfile = parts[1];
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
                
                // HACK
                Catalog catalog = CatalogUtil.loadCatalogFromJar(catalogPath.getAbsolutePath());
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
            } else if (parts[0].equalsIgnoreCase("NODATALOAD")) {
                /*
                 * Disable data loading
                 */
                noDataLoad = Boolean.parseBoolean(parts[1]);
            } else if (parts[0].equalsIgnoreCase("TRACE")) {
                /*
                 * Workload Trace Output
                 */
                workloadTrace = parts[1];
            } else if (parts[0].equalsIgnoreCase("PROFILESITES")) {
                /*
                 * Profile SiteIds
                 */
                for (String s : parts[1].split(",")) {
                    profileSiteIds.add(Integer.valueOf(s));
                } // FOR

            } else if (parts[0].equalsIgnoreCase("MARKOV")) {
                markov_path = parts[1];
                LOG.debug("MARKOV PATH = " + markov_path);
            } else if (parts[0].equalsIgnoreCase("THRESHOLDS")) {
                thresholds_path = parts[1];

            } else if (parts[0].equalsIgnoreCase("DUMPDATABASE")) {
                dumpDatabase = Boolean.parseBoolean(parts[1]);                
            } else if (parts[0].equalsIgnoreCase("DUMPDATABASEDIR")) {
                dumpDatabaseDir = parts[1];

            /** PAVLO **/
                
            } else {
                clientParams.put(parts[0].toLowerCase(), parts[1]);
            }
        }
        assert(coordinatorHost != null) : "Missing CoordinatorHost";

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
                useProfile, 
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
                workloadTrace,
                profileSiteIds,
                markov_path,
                thresholds_path,
                clientLogDir,
                siteLogDir,
                coordLogDir,
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
        
        // Set all of the client params to uppercase...
        for (Entry<String, String> e : clientParams.entrySet()) {
            config.parameters.put(e.getKey().toUpperCase(), e.getValue());    
        } // FOR
        config.parameters.put("NUMCLIENTS", Integer.toString(clientCount * processesPerClient));

        if (debug.get()) LOG.debug("Benchmark Configuration\n" + config.toString());
        
        // ACTUALLY RUN THE BENCHMARK
        BenchmarkController controller = new BenchmarkController(config);
        controller.setupBenchmark();
        controller.runBenchmark();
        controller.cleanUpBenchmark();
    }
}
