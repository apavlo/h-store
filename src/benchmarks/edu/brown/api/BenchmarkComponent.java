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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.benchmark.BlockingClient;
import org.voltdb.benchmark.Verification;
import org.voltdb.benchmark.Verification.Expression;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.StatsUploaderSettings;
import org.voltdb.sysprocs.LoadMultipartitionTable;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltSampler;

import edu.brown.api.results.BenchmarkComponentResults;
import edu.brown.api.results.ResponseEntries;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

/**
 * Base class for clients that will work with the multi-host multi-process
 * benchmark framework that is driven from stdin
 */
public abstract class BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(BenchmarkComponent.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static String CONTROL_MESSAGE_PREFIX = "{HSTORE}";
    
    // ============================================================================
    // SHARED STATIC MEMBERS
    // ============================================================================
    
    private static Client globalClient;
    private static final ReentrantLock globalClientLock = new ReentrantLock();
    
    private static CatalogContext globalCatalog;
    private static final ReentrantLock globalCatalogLock = new ReentrantLock();
    
    private static PartitionPlan globalPartitionPlan;
    private static final ReentrantLock globalPartitionPlanLock = new ReentrantLock();
    
    private static final Set<Client> globalHasConnections = new HashSet<Client>(); 
    
    private static Client getClient(Catalog catalog,
                                    int messageSize,
                                    boolean heavyWeight,
                                    StatsUploaderSettings statsSettings,
                                    boolean shareConnection) {
        Client client = globalClient;
        if (shareConnection == false) {
            client = ClientFactory.createClient(
                    messageSize,
                    null,
                    heavyWeight,
                    statsSettings,
                    catalog
            );
            if (debug.val) LOG.debug("Created new Client handle");
        } else if (client == null) {
            globalClientLock.lock();
            try {
                if (globalClient == null) {
                    client = ClientFactory.createClient(
                            messageSize,
                            null,
                            heavyWeight,
                            statsSettings,
                            catalog
                    );
                    if (debug.val) LOG.debug("Created new shared Client handle");
                }
            } finally {
                globalClientLock.unlock();
            } // SYNCH
        }
        return (client);
    }
    
    private static CatalogContext getCatalogContext(File catalogPath) {
        // Read back the catalog and populate catalog object
        if (globalCatalog == null) {
            globalCatalogLock.lock();
            try {
                if (globalCatalog == null) {
                    globalCatalog = CatalogUtil.loadCatalogContextFromJar(catalogPath);
                }
            } finally {
                globalCatalogLock.unlock();
            } // SYNCH
        }
        return (globalCatalog);
    }
    
    private static void applyPartitionPlan(Database catalog_db, File partitionPlanPath) {
        if (globalPartitionPlan != null) return;
        globalPartitionPlanLock.lock();
        try {
            if (globalPartitionPlan != null) return;
            if (debug.val) LOG.debug("Loading PartitionPlan '" + partitionPlanPath + "' and applying it to the catalog");
            globalPartitionPlan = new PartitionPlan();
            try {
                globalPartitionPlan.load(partitionPlanPath, catalog_db);
                globalPartitionPlan.apply(catalog_db);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load PartitionPlan '" + partitionPlanPath + "' and apply it to the catalog", ex);
            }
        } finally {
            globalPartitionPlanLock.unlock();
        } // SYNCH
        return;
    }
    
    // ============================================================================
    // INSTANCE MEMBERS
    // ============================================================================
    
    /**
     * Client initialized here and made available for use in derived classes
     */
    private Client m_voltClient;

    /**
     * Manage input and output to the framework
     */
    private ControlPipe m_controlPipe;
    private boolean m_controlPipeAutoStart = false;

    /**
     * 
     */
    protected final ControlWorker worker = new ControlWorker(this);
    
    /**
     * State of this client
     */
    protected volatile ControlState m_controlState = ControlState.PREPARING;

    /**
     * Username supplied to the Volt client
     */
    private final String m_username;

    /**
     * Password supplied to the Volt client
     */
    private final String m_password;

    /**
     * Rate at which transactions should be generated. If set to -1 the rate
     * will be controlled by the derived class. Rate is in transactions per
     * second
     */
    final int m_txnRate;
    
    private final boolean m_blocking;

    /**
     * Number of transactions to generate for every millisecond of time that
     * passes
     */
    final double m_txnsPerMillisecond;

    /**
     * Additional parameters (benchmark specific)
     */
    protected final Map<String, String> m_extraParams = new HashMap<String, String>();

    /**
     * Storage for error descriptions
     */
    protected String m_reason = "";

    /**
     * Display names for each transaction.
     */
    private final String m_countDisplayNames[];

    /**
     * Client Id
     */
    private final int m_id;
    
    /**
     * Total # of Clients
     */
    private final int m_numClients;
    
    /**
     * If set to true, don't try to make any connections to the cluster with this client
     * This is just used for testing
     */
    private final boolean m_noConnections;
    
    /**
     * Total # of Partitions
     */
    private final int m_numPartitions;

    /**
     * Path to catalog jar
     */
    private final File m_catalogPath;
    private CatalogContext m_catalogContext;
    private final String m_projectName;
    
    final boolean m_exitOnCompletion;
    
    /**
     * Pause Lock
     */
    protected final Semaphore m_pauseLock = new Semaphore(1);

    /**
     * Data verification.
     */
    private final float m_checkTransaction;
    protected final boolean m_checkTables;
    private final Random m_checkGenerator = new Random();
    private final LinkedHashMap<Pair<String, Integer>, Expression> m_constraints;
    private final List<String> m_tableCheckOrder = new LinkedList<String>();
    protected VoltSampler m_sampler = null;
    
    protected final int m_tickInterval;
    protected final Thread m_tickThread;
    protected int m_tickCounter = 0;
    
    private final boolean m_noUploading;
    private final ReentrantLock m_loaderBlock = new ReentrantLock();
    private final ClientResponse m_dummyResponse = new ClientResponseImpl(-1, -1, -1, Status.OK, HStoreConstants.EMPTY_RESULT, "");
    
    /**
     * Keep track of the number of tuples loaded so that we can generate table statistics
     */
    private final boolean m_tableStats;
    private final File m_tableStatsDir;
    private final ObjectHistogram<String> m_tableTuples = new ObjectHistogram<String>();
    private final ObjectHistogram<String> m_tableBytes = new ObjectHistogram<String>();
    private final Map<Table, TableStatistics> m_tableStatsData = new HashMap<Table, TableStatistics>();
    protected final BenchmarkComponentResults m_txnStats = new BenchmarkComponentResults();
    
    /**
     * ClientResponse Entries
     */
    protected final ResponseEntries m_responseEntries;
    private boolean m_enableResponseEntries = false;

    private final Map<String, ProfileMeasurement> computeTime = new HashMap<String, ProfileMeasurement>();
    
    /**
     * 
     */
    private BenchmarkClientFileUploader uploader = null;
    
    /**
     * Configuration
     */
    private final HStoreConf m_hstoreConf;
    private final ObjectHistogram<String> m_txnWeights = new ObjectHistogram<String>();
    private Integer m_txnWeightsDefault = null;
    private final boolean m_isLoader;
    
    private final String m_statsDatabaseURL;
    private final String m_statsDatabaseUser;
    private final String m_statsDatabasePass;
    private final String m_statsDatabaseJDBC;
    private final int m_statsPollerInterval;

    public BenchmarkComponent(final Client client) {
        m_voltClient = client;
        m_exitOnCompletion = false;
        m_password = "";
        m_username = "";
        m_txnRate = -1;
        m_isLoader = false;
        m_blocking = false;
        m_txnsPerMillisecond = 0;
        m_catalogPath = null;
        m_projectName = null;
        m_id = 0;
        m_numClients = 1;
        m_noConnections = false;
        m_numPartitions = 0;
        m_countDisplayNames = null;
        m_checkTransaction = 0;
        m_checkTables = false;
        m_constraints = new LinkedHashMap<Pair<String, Integer>, Expression>();
        m_tickInterval = -1;
        m_tickThread = null;
        m_responseEntries = null;
        m_tableStats = false;
        m_tableStatsDir = null;
        m_noUploading = false;
        
        m_statsDatabaseURL = null; 
        m_statsDatabaseUser = null;
        m_statsDatabasePass = null;
        m_statsDatabaseJDBC = null;
        m_statsPollerInterval = -1;
        
        // FIXME
        m_hstoreConf = null;
    }

    /**
     * Constructor that initializes the framework portions of the client.
     * Creates a Volt client and connects it to all the hosts provided on the
     * command line with the specified username and password
     *
     * @param args
     */
    public BenchmarkComponent(String args[]) {
    	if (debug.val) LOG.debug("Benchmark Component debugging");
        // Initialize HStoreConf
        String hstore_conf_path = null;
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            final String[] parts = arg.split("=", 2);
            if (parts.length > 1 && parts[1].startsWith("${") == false && parts[0].equalsIgnoreCase("CONF")) {
                hstore_conf_path = parts[1];
                break;
            }
        } // FOR

        synchronized (BenchmarkComponent.class) {
            if (HStoreConf.isInitialized() == false) {
                assert(hstore_conf_path != null) : "Missing HStoreConf file";
                File f = new File(hstore_conf_path);
                if (debug.val) LOG.debug("Initializing HStoreConf from '" + f.getName() + "' along with input parameters");
                HStoreConf.init(f, args);
            } else {
                if (debug.val) LOG.debug("Initializing HStoreConf only with input parameters");
                HStoreConf.singleton().loadFromArgs(args); // XXX Why do we need to do this??
            }
        } // SYNCH
        m_hstoreConf = HStoreConf.singleton();
        if (trace.val) LOG.trace("HStore Conf\n" + m_hstoreConf.toString(true));
        
        int transactionRate = m_hstoreConf.client.txnrate;
        boolean blocking = m_hstoreConf.client.blocking;
        boolean tableStats = m_hstoreConf.client.tablestats;
        String tableStatsDir = m_hstoreConf.client.tablestats_dir;
        int tickInterval = m_hstoreConf.client.tick_interval;
        
        // default values
        String username = "user";
        String password = "password";
        ControlState state = ControlState.PREPARING; // starting state
        String reason = ""; // and error string
        int id = 0;
        boolean isLoader = false;
        int num_clients = 0;
        int num_partitions = 0;
        boolean exitOnCompletion = true;
        float checkTransaction = 0;
        boolean checkTables = false;
        boolean noConnections = false;
        boolean noUploading = false;
        File catalogPath = null;
        String projectName = null;
        String partitionPlanPath = null;
        boolean partitionPlanIgnoreMissing = false;
        long startupWait = -1;
        boolean autoStart = false;
        
        String statsDatabaseURL = null;
        String statsDatabaseUser = null;
        String statsDatabasePass = null;
        String statsDatabaseJDBC = null;
        int statsPollInterval = 10000;
        
        // scan the inputs once to read everything but host names
        Map<String, Object> componentParams = new TreeMap<String, Object>();
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            final String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                state = ControlState.ERROR;
                reason = "Invalid parameter: " + arg;
                break;
            }
            else if (parts[1].startsWith("${")) {
                continue;
            }
            else if (parts[0].equalsIgnoreCase("CONF")) {
                continue;
            }
            
            if (debug.val) componentParams.put(parts[0], parts[1]);
            
            if (parts[0].equalsIgnoreCase("CATALOG")) {
                catalogPath = new File(parts[1]);
                assert(catalogPath.exists()) : "The catalog file '" + catalogPath.getAbsolutePath() + " does not exist";
                if (debug.val) componentParams.put(parts[0], catalogPath);
            }
            else if (parts[0].equalsIgnoreCase("LOADER")) {
                isLoader = Boolean.parseBoolean(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("NAME")) {
                projectName = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("USER")) {
                username = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("PASSWORD")) {
                password = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("EXITONCOMPLETION")) {
                exitOnCompletion = Boolean.parseBoolean(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("ID")) {
                id = Integer.parseInt(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("NUMCLIENTS")) {
                num_clients = Integer.parseInt(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("NUMPARTITIONS")) {
                num_partitions = Integer.parseInt(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("CHECKTRANSACTION")) {
                checkTransaction = Float.parseFloat(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("CHECKTABLES")) {
                checkTables = Boolean.parseBoolean(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("NOCONNECTIONS")) {
                noConnections = Boolean.parseBoolean(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("NOUPLOADING")) {
                noUploading = Boolean.parseBoolean(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("WAIT")) {
                startupWait = Long.parseLong(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("AUTOSTART")) {
                autoStart = Boolean.parseBoolean(parts[1]);
            }
            
            // Procedure Stats Uploading Parameters
            else if (parts[0].equalsIgnoreCase("STATSDATABASEURL")) {
                statsDatabaseURL = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEUSER")) {
                if (parts[1].isEmpty() == false) statsDatabaseUser = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEPASS")) {
                if (parts[1].isEmpty() == false) statsDatabasePass = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSDATABASEJDBC")) {
                if (parts[1].isEmpty() == false) statsDatabaseJDBC = parts[1];
            }
            else if (parts[0].equalsIgnoreCase("STATSPOLLINTERVAL")) {
                statsPollInterval = Integer.parseInt(parts[1]); 
            }
            
            else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_PARTITION_PLAN)) {
                partitionPlanPath = parts[1];
            }
            else if (parts[0].equalsIgnoreCase(ArgumentsParser.PARAM_PARTITION_PLAN_IGNORE_MISSING)) {
                partitionPlanIgnoreMissing = Boolean.parseBoolean(parts[1]);
            }
            // If it starts with "benchmark.", then it always goes to the implementing class
            else if (parts[0].toLowerCase().startsWith(HStoreConstants.BENCHMARK_PARAM_PREFIX)) {
                if (debug.val) componentParams.remove(parts[0]);
                parts[0] = parts[0].substring(HStoreConstants.BENCHMARK_PARAM_PREFIX.length());
                m_extraParams.put(parts[0].toUpperCase(), parts[1]);
            }
        }
        if (trace.val) {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put("BenchmarkComponent", componentParams);
            m.put("Extra Client", m_extraParams);
            LOG.debug("Input Parameters:\n" + StringUtil.formatMaps(m));
        }
        
        // Thread.currentThread().setName(String.format("client-%02d", id));
        
        m_catalogPath = catalogPath;
        m_projectName = projectName;
        m_id = id;
        m_isLoader = isLoader;
        m_numClients = num_clients;
        m_numPartitions = num_partitions;
        m_exitOnCompletion = exitOnCompletion;
        m_username = username;
        m_password = password;
        m_txnRate = (isLoader ? -1 : transactionRate);
        m_txnsPerMillisecond = (isLoader ? -1 : transactionRate / 1000.0);
        m_blocking = blocking;
        m_tickInterval = tickInterval;
        m_noUploading = noUploading;
        m_noConnections = noConnections || (isLoader && m_noUploading);
        m_tableStats = tableStats;
        m_tableStatsDir = (tableStatsDir.isEmpty() ? null : new File(tableStatsDir));
        m_controlPipeAutoStart = autoStart;
        
        m_statsDatabaseURL = statsDatabaseURL; 
        m_statsDatabaseUser = statsDatabaseUser;
        m_statsDatabasePass = statsDatabasePass;
        m_statsDatabaseJDBC = statsDatabaseJDBC;
        m_statsPollerInterval = statsPollInterval;
        
        // If we were told to sleep, do that here before we try to load in the catalog
        // This is an attempt to keep us from overloading a single node all at once
        if (startupWait > 0) {
            if (debug.val) LOG.debug(String.format("Delaying client start-up by %.2f sec", startupWait/1000d));
            try {
                Thread.sleep(startupWait);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Unexpected interruption", ex);
            }
        }
        
        // HACK: This will instantiate m_catalog for us...
        if (m_catalogPath != null) {
            this.getCatalogContext();
        }
        
        // Parse workload transaction weights
        if (m_hstoreConf.client.weights != null && m_hstoreConf.client.weights.trim().isEmpty() == false) {
            for (String entry : m_hstoreConf.client.weights.split("(,|;)")) {
                String data[] = entry.split(":");
                if (data.length != 2) {
                    LOG.warn("Invalid transaction weight entry '" + entry + "'");
                    continue;
                }
                try {
                    String txnName = data[0];
                    int txnWeight = Integer.parseInt(data[1]);
                    assert(txnWeight >= 0);
                    
                    // '*' is the default value
                    if (txnName.equals("*")) {
                        this.m_txnWeightsDefault = txnWeight;
                        if (debug.val) LOG.debug(String.format("Default Transaction Weight: %d", txnWeight));
                    } else {
                        if (debug.val) LOG.debug(String.format("%s Transaction Weight: %d", txnName, txnWeight));
                        this.m_txnWeights.put(txnName.toUpperCase(), txnWeight);
                    }
                    
                    // If the weight is 100, then we'll set the default weight to zero
                    if (txnWeight == 100 && this.m_txnWeightsDefault == null) {
                        this.m_txnWeightsDefault = 0;
                        if (debug.val) LOG.debug(String.format("Default Transaction Weight: %d", this.m_txnWeightsDefault));
                    }
                } catch (Throwable ex) {
                    LOG.warn("Invalid transaction weight entry '" + entry + "'", ex);
                    continue;
                }
            } // FOR
        }
        
        if (partitionPlanPath != null) {
            boolean exists = FileUtil.exists(partitionPlanPath); 
            if (partitionPlanIgnoreMissing == false)
                assert(exists) : "Invalid partition plan path '" + partitionPlanPath + "'";
            if (exists) this.applyPartitionPlan(new File(partitionPlanPath));
        }
        
        this.initializeConnection();
        
        // report any errors that occurred before the client was instantiated
        if (state != ControlState.PREPARING)
            setState(state, reason);

        
        m_checkTransaction = checkTransaction;
        m_checkTables = checkTables;
        m_constraints = new LinkedHashMap<Pair<String, Integer>, Expression>();

        m_countDisplayNames = getTransactionDisplayNames();
        if (m_countDisplayNames != null) {
            Map<Integer, String> debugLabels = new TreeMap<Integer, String>();
            
            m_enableResponseEntries = (m_hstoreConf.client.output_responses != null);
            m_responseEntries = new ResponseEntries();
            
            for (int i = 0; i < m_countDisplayNames.length; i++) {
                m_txnStats.transactions.put(i, 0);
                m_txnStats.dtxns.put(i, 0);
                m_txnStats.specexecs.put(i, 0);
                debugLabels.put(i, m_countDisplayNames[i]);
            } // FOR
            m_txnStats.transactions.setDebugLabels(debugLabels);
            m_txnStats.setEnableBasePartitions(m_hstoreConf.client.output_basepartitions);
            m_txnStats.setEnableResponsesStatuses(m_hstoreConf.client.output_status);
            
        } else {
            m_responseEntries = null;
        }
        
        // If we need to call tick more frequently than when POLL is called,
        // then we'll want to use a separate thread
        if (m_tickInterval > 0 && isLoader == false) {
            if (debug.val)
                LOG.debug(String.format("Creating local thread that will call BenchmarkComponent.tick() every %.1f seconds",
                                        (m_tickInterval / 1000.0)));
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            BenchmarkComponent.this.invokeTickCallback(m_tickCounter++);
                            Thread.sleep(m_tickInterval);
                        } // WHILE
                    } catch (InterruptedException ex) {
                        LOG.warn("Tick thread was interrupted");
                    }
                }
            };
            m_tickThread = new Thread(r);
            m_tickThread.setDaemon(true);
        } else {
            m_tickThread = null;
        }
    }

    // ----------------------------------------------------------------------------
    // MAIN METHOD HOOKS
    // ----------------------------------------------------------------------------
    
    /**
     * Derived classes implementing a main that will be invoked at the start of
     * the app should call this main to instantiate themselves
     *
     * @param clientClass
     *            Derived class to instantiate
     * @param args
     * @param startImmediately
     *            Whether to start the client thread immediately or not.
     */
    public static BenchmarkComponent main(final Class<? extends BenchmarkComponent> clientClass, final String args[], final boolean startImmediately) {
        return main(clientClass, null, args, startImmediately);
    }
        
    protected static BenchmarkComponent main(final Class<? extends BenchmarkComponent> clientClass,
                                             final BenchmarkClientFileUploader uploader,
                                             final String args[],
                                             final boolean startImmediately) {
        BenchmarkComponent clientMain = null;
        try {
            final Constructor<? extends BenchmarkComponent> constructor =
                clientClass.getConstructor(new Class<?>[] { new String[0].getClass() });
            clientMain = constructor.newInstance(new Object[] { args });
            if (uploader != null) clientMain.uploader = uploader;
            
            clientMain.invokeInitCallback();
            
            if (startImmediately) {
                final ControlWorker worker = new ControlWorker(clientMain);
                worker.start();
                
                // Wait for the worker to finish
                if (debug.val)
                    LOG.debug(String.format("Started ControlWorker for client #%02d. Waiting until finished...",
                              clientMain.getClientId()));
                worker.join();
                clientMain.invokeStopCallback();
            }
            else {
                // if (debug.val) LOG.debug(String.format("Deploying ControlWorker for client #%02d. Waiting for control signal...", clientMain.getClientId()));
                // clientMain.start();
            }
        }
        catch (final Throwable e) {
            String name = (clientMain != null ? clientMain.getProjectName()+"." : "") + clientClass.getSimpleName(); 
            LOG.error("Unexpected error while invoking " + name, e);
            throw new RuntimeException(e);
        }
        return (clientMain);
    }
    
    // ----------------------------------------------------------------------------
    // CLUSTER CONNECTION SETUP
    // ----------------------------------------------------------------------------
    
    protected void initializeConnection() {
        StatsUploaderSettings statsSettings = null;
        if (m_statsDatabaseURL != null && m_statsDatabaseURL.isEmpty() == false) {
            try {
                statsSettings = StatsUploaderSettings.singleton(
                        m_statsDatabaseURL,
                        m_statsDatabaseUser,
                        m_statsDatabasePass,
                        m_statsDatabaseJDBC,
                        this.getProjectName(),
                        (m_isLoader ? "LOADER" : "CLIENT"),
                        m_statsPollerInterval,
                        m_catalogContext.catalog);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to initialize StatsUploader", ex);
            }
            if (debug.val)
                LOG.debug("StatsUploaderSettings:\n" + statsSettings);
        }
        Client new_client = BenchmarkComponent.getClient(
                (m_hstoreConf.client.txn_hints ? this.getCatalogContext().catalog : null),
                getExpectedOutgoingMessageSize(),
                useHeavyweightClient(),
                statsSettings,
                m_hstoreConf.client.shared_connection
        );
        if (m_blocking) { //  && isLoader == false) {
            int concurrent = m_hstoreConf.client.blocking_concurrent;
            if (debug.val) 
                LOG.debug(String.format("Using BlockingClient [concurrent=%d]",
                          m_hstoreConf.client.blocking_concurrent));
            if (this.isSinglePartitionOnly()) {
                concurrent *= 4; // HACK
            }
            m_voltClient = new BlockingClient(new_client, concurrent);
        } else {
            m_voltClient = new_client;
        }
        
        // scan the inputs again looking for host connections
        if (m_noConnections == false) {
            synchronized (globalHasConnections) {
                if (globalHasConnections.contains(new_client) == false) {
                    this.setupConnections();
                    globalHasConnections.add(new_client);
                }
            } // SYNCH
        }
    }

    private void setupConnections() {
        boolean atLeastOneConnection = false;
        for (Site catalog_site : this.getCatalogContext().sites) {
            final int site_id = catalog_site.getId();
            final String host = catalog_site.getHost().getIpaddr();
            int port = catalog_site.getProc_port();
            if (debug.val)
                LOG.debug(String.format("Creating connection to %s at %s:%d",
                                        HStoreThreadManager.formatSiteName(site_id),
                                        host, port));
            try {
                this.createConnection(site_id, host, port);
            } catch (IOException ex) {
                String msg = String.format("Failed to connect to %s on %s:%d",
                                           HStoreThreadManager.formatSiteName(site_id), host, port);
                // LOG.error(msg, ex);
                // setState(ControlState.ERROR, msg + ": " + ex.getMessage());
                // continue;
                throw new RuntimeException(msg, ex);
            }
            atLeastOneConnection = true;
        } // FOR
        if (!atLeastOneConnection) {
            setState(ControlState.ERROR, "No HOSTS specified on command line.");
            throw new RuntimeException("Failed to establish connections to H-Store cluster");
        }
    }
    
    private void createConnection(final Integer site_id, final String hostname, final int port)
        throws UnknownHostException, IOException {
        if (debug.val)
            LOG.debug(String.format("Requesting connection to %s %s:%d",
                HStoreThreadManager.formatSiteName(site_id), hostname, port));
        m_voltClient.createConnection(site_id, hostname, port, m_username, m_password);
    }

    // ----------------------------------------------------------------------------
    // CONTROLLER COMMUNICATION METHODS
    // ----------------------------------------------------------------------------
    
    protected void printControlMessage(ControlState state) {
        printControlMessage(state, null);
    }
    
    private void printControlMessage(ControlState state, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s %d,%d,%s", CONTROL_MESSAGE_PREFIX,
                                               this.getClientId(),
                                               System.currentTimeMillis(),
                                               state));
        if (message != null && message.isEmpty() == false) {
            sb.append(",").append(message);
        }
        System.out.println(sb);
    }
    
    protected void answerWithError() {
        this.printControlMessage(m_controlState, m_reason);
    }

    protected void answerPoll() {
        BenchmarkComponentResults copy = this.m_txnStats.copy();
        this.m_txnStats.clear(false);
        this.printControlMessage(m_controlState, copy.toJSONString());
    }
    
    protected void answerDumpTxns() {
        ResponseEntries copy = new ResponseEntries(this.m_responseEntries);
        this.m_responseEntries.clear();
        this.printControlMessage(ControlState.DUMPING, copy.toJSONString());
    }
    
    protected void answerOk() {
        this.printControlMessage(m_controlState, "OK");
    }

    /**
     * Implemented by derived classes. Loops indefinitely invoking stored
     * procedures. Method never returns and never receives any updates.
     */
    @Deprecated
    abstract protected void runLoop() throws IOException;
    
    /**
     * Get the display names of the transactions that will be invoked by the
     * derived class. As a side effect this also retrieves the number of
     * transactions that can be invoked.
     *
     * @return
     */
    abstract protected String[] getTransactionDisplayNames();
    
    /**
     * Increment the internal transaction counter. This should be invoked
     * after the client has received a ClientResponse from the DBMS cluster
     * The txn_index is the offset of the transaction that was executed. This offset
     * is the same order as the array returned by getTransactionDisplayNames
     * @param cresponse - The ClientResponse returned from the server
     * @param txn_idx
     */
    protected final void incrementTransactionCounter(final ClientResponse cresponse, final int txn_idx) {
        // Only include it if it wasn't rejected
        // This is actually handled in the Distributer, but it doesn't hurt to have this here
        Status status = cresponse.getStatus();
        if (status == Status.OK || status == Status.ABORT_USER) {
            
            // TRANSACTION COUNTERS
            boolean is_specexec = cresponse.isSpeculative();
            boolean is_dtxn = (cresponse.isSinglePartition() == false); 
            synchronized (m_txnStats.transactions) {
                m_txnStats.transactions.put(txn_idx);
                if (is_dtxn) m_txnStats.dtxns.put(txn_idx);
                if (is_specexec) m_txnStats.specexecs.put(txn_idx);
            } // SYNCH

            // LATENCIES COUNTERS
            // Ignore zero latencies... Not sure why this happens...
            int latency = cresponse.getClusterRoundtrip();
            if (latency > 0) {
                Map<Integer, ObjectHistogram<Integer>> latenciesMap = (is_dtxn ? m_txnStats.dtxnLatencies :
                                                                                 m_txnStats.spLatencies); 
                Histogram<Integer> latencies = latenciesMap.get(txn_idx);
                if (latencies == null) {
                    synchronized (latenciesMap) {
                        latencies = latenciesMap.get(txn_idx);
                        if (latencies == null) {
                            latencies = new ObjectHistogram<Integer>();
                            latenciesMap.put(txn_idx, (ObjectHistogram<Integer>)latencies);
                        }
                    } // SYNCH
                }
                synchronized (latencies) {
                    latencies.put(latency);
                } // SYNCH
            }
            
            // RESPONSE ENTRIES
            if (m_enableResponseEntries) {
                long timestamp = System.currentTimeMillis();
                m_responseEntries.add(cresponse, m_id, txn_idx, timestamp);
            }
            
            // BASE PARTITIONS
            if (m_txnStats.isBasePartitionsEnabled()) {
                synchronized (m_txnStats.basePartitions) {
                    m_txnStats.basePartitions.put(cresponse.getBasePartition());
                } // SYNCH
            }
        }
//        else if (status == Status.ABORT_UNEXPECTED) {
//            LOG.warn("Invalid " + m_countDisplayNames[txn_idx] + " response!\n" + cresponse);
//            if (cresponse.getException() != null) {
//                cresponse.getException().printStackTrace();
//            }
//            if (cresponse.getStatusString() != null) {
//                LOG.warn(cresponse.getStatusString());
//            }
//        }
        
        if (m_txnStats.isResponsesStatusesEnabled()) {
            synchronized (m_txnStats.responseStatuses) {
                m_txnStats.responseStatuses.put(status.ordinal());    
            } // SYNCH
        }
    }
    
    // ----------------------------------------------------------------------------
    // PUBLIC UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Return the scale factor for this benchmark instance
     * @return
     */
    public double getScaleFactor() {
        return (m_hstoreConf.client.scalefactor);
    }
    
    /**
     * This method will load a VoltTable into the database for the given tableName.
     * The database will automatically split the tuples and send to the correct partitions
     * The current thread will block until the the database cluster returns the result.
     * Can be overridden for testing purposes.
     * @param tableName
     * @param vt
     */
    public ClientResponse loadVoltTable(String tableName, VoltTable vt) {
        assert(vt != null) : "Null VoltTable for '" + tableName + "'";
        
        int rowCount = vt.getRowCount();
        long rowTotal = m_tableTuples.get(tableName, 0);
        int byteCount = vt.getUnderlyingBufferSize();
        long byteTotal = m_tableBytes.get(tableName, 0);
        
        if (trace.val) LOG.trace(String.format("%s: Loading %d new rows - TOTAL %d [bytes=%d/%d]",
                                   tableName.toUpperCase(), rowCount, rowTotal, byteCount, byteTotal));
        
        // Load up this dirty mess...
        ClientResponse cr = null;
        if (m_noUploading == false) {
            boolean locked = m_hstoreConf.client.blocking_loader;
            if (locked) m_loaderBlock.lock();
            try {
                int tries = 3;
                String procName = VoltSystemProcedure.procCallName(LoadMultipartitionTable.class);
                while (tries-- > 0) {
                    try {
                        cr = m_voltClient.callProcedure(procName, tableName, vt);
                    } catch (ProcCallException ex) {
                        // If this thing was rejected, then we'll allow us to try again. 
                        cr = ex.getClientResponse();
                        if (cr.getStatus() == Status.ABORT_REJECT && tries > 0) {
                            if (debug.val) 
                                LOG.warn(String.format("Loading data for %s was rejected. Going to try again\n%s",
                                         tableName, cr.toString()));
                            continue;
                        }
                        // Anything else needs to be thrown out of here
                        throw ex;
                    }
                    break;
                } // WHILE
            } catch (Throwable ex) {
                throw new RuntimeException("Error when trying load data for '" + tableName + "'", ex);
            } finally {
                if (locked) m_loaderBlock.unlock();
            } // SYNCH
            assert(cr != null);
            assert(cr.getStatus() == Status.OK);
            if (trace.val) LOG.trace(String.format("Load %s: txn #%d / %s / %d",
                                       tableName, cr.getTransactionId(), cr.getStatus(), cr.getClientHandle()));
        } else {
            cr = m_dummyResponse;
        }
        if (cr.getStatus() != Status.OK) {
            LOG.warn(String.format("Failed to load %d rows for '%s': %s",
                     rowCount, tableName, cr.getStatusString()), cr.getException()); 
            return (cr);
        }
        
        m_tableTuples.put(tableName, rowCount);
        m_tableBytes.put(tableName, byteCount);
        
        // Keep track of table stats
        if (m_tableStats && cr.getStatus() == Status.OK) {
            final CatalogContext catalogContext = this.getCatalogContext();
            assert(catalogContext != null);
            final Table catalog_tbl = catalogContext.getTableByName(tableName);
            assert(catalog_tbl != null) : "Invalid table name '" + tableName + "'";
            
            synchronized (m_tableStatsData) {
                TableStatistics stats = m_tableStatsData.get(catalog_tbl);
                if (stats == null) {
                    stats = new TableStatistics(catalog_tbl);
                    stats.preprocess(catalogContext.database);
                    m_tableStatsData.put(catalog_tbl, stats);
                }
                vt.resetRowPosition();
                while (vt.advanceRow()) {
                    VoltTableRow row = vt.getRow();
                    stats.process(catalogContext.database, row);
                } // WHILE
            } // SYNCH
        }
        return (cr);
    }
    
    /**
     * Return an overridden transaction weight
     * @param txnName
     * @return
     */
    protected final Integer getTransactionWeight(String txnName) {
        return (this.getTransactionWeight(txnName, null));
    }
    
    /**
     * 
     * @param txnName
     * @param weightIfNull
     * @return
     */
    protected final Integer getTransactionWeight(String txnName, Integer weightIfNull) {
        if (debug.val)
            LOG.debug(String.format("Looking for txn weight for '%s' [weightIfNull=%s]",
                      txnName, weightIfNull));
        
        Long val = this.m_txnWeights.get(txnName.toUpperCase()); 
        if (val != null) {
            return (val.intValue());
        }
        else if (m_txnWeightsDefault != null) {
            return (m_txnWeightsDefault);
        }
        return (weightIfNull);
    }
    
    /**
     * Get the number of tuples loaded into the given table thus far
     * @param tableName
     * @return
     */
    public final long getTableTupleCount(String tableName) {
        return (m_tableTuples.get(tableName, 0));
    }
    
    /**
     * Get a read-only histogram of the number of tuples loaded in all
     * of the tables
     * @return
     */
    public final Histogram<String> getTableTupleCounts() {
        return (new ObjectHistogram<String>(m_tableTuples));
    }
    
    /**
     * Get the number of bytes loaded into the given table thus far
     * @param tableName
     * @return
     */
    public final long getTableBytes(String tableName) {
        return (m_tableBytes.get(tableName, 0));
    }
    
    /**
     * Generate a WorkloadStatistics object based on the table stats that
     * were collected using loadVoltTable()
     * @return
     */
    private final WorkloadStatistics generateWorkloadStatistics() {
        assert(m_tableStatsDir != null);
        final CatalogContext catalogContext = this.getCatalogContext();
        assert(catalogContext != null);

        // Make sure we call postprocess on all of our friends
        for (TableStatistics tableStats : m_tableStatsData.values()) {
            try {
                tableStats.postprocess(catalogContext.database);
            } catch (Exception ex) {
                String tableName = tableStats.getCatalogItem(catalogContext.database).getName();
                throw new RuntimeException("Failed to process TableStatistics for '" + tableName + "'", ex);
            }
        } // FOR
        
        if (trace.val)
            LOG.trace(String.format("Creating WorkloadStatistics for %d tables [totalRows=%d, totalBytes=%d",
                                    m_tableStatsData.size(), m_tableTuples.getSampleCount(), m_tableBytes.getSampleCount()));
        WorkloadStatistics stats = new WorkloadStatistics(catalogContext.database);
        stats.apply(m_tableStatsData);
        return (stats);
    }

    /**
     * Queue a local file to be sent to the client with the given client id.
     * The file will be copied into the path specified by remote_file.
     * When the client is started it will be passed argument <parameter>=<remote_file>
     * @param client_id
     * @param parameter
     * @param local_file
     * @param remote_file
     */
    public void sendFileToClient(int client_id, String parameter, File local_file, File remote_file) throws IOException {
        assert(uploader != null);
        this.uploader.sendFileToClient(client_id, parameter, local_file, remote_file);
        LOG.debug(String.format("Queuing local file '%s' to be sent to client %d as parameter '%s' to remote file '%s'", local_file, client_id, parameter, remote_file));
    }
    
    /**
     * 
     * @param client_id
     * @param parameter
     * @param local_file
     * @throws IOException
     */
    public void sendFileToClient(int client_id, String parameter, File local_file) throws IOException {
        String suffix = FileUtil.getExtension(local_file);
        String prefix = String.format("%s-%02d-", local_file.getName().replace("." + suffix, ""), client_id);
        File remote_file = FileUtil.getTempFile(prefix, suffix, false);
        sendFileToClient(client_id, parameter, local_file, remote_file);
    }
    
    /**
     * Queue a local file to be sent to all clients
     * @param parameter
     * @param local_file
     * @throws IOException
     */
    public void sendFileToAllClients(String parameter, File local_file) throws IOException {
        for (int i = 0, cnt = this.getNumClients(); i < cnt; i++) {
            sendFileToClient(i, parameter, local_file, local_file);
//            this.sendFileToClient(i, parameter, local_file);
        } // FOR
    }
    
    protected void setBenchmarkClientFileUploader(BenchmarkClientFileUploader uploader) {
        assert(this.uploader == null);
        this.uploader = uploader;
    }
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------
    
    protected final void invokeInitCallback() {
        this.initCallback();
    }
    
    protected final void invokeStartCallback() {
        this.startCallback();
    }
    
    protected final void invokeStopCallback() {
        // If we were generating stats, then get the final WorkloadStatistics object
        // and write it out to a file for them to use
        if (m_tableStats) {
            WorkloadStatistics stats = this.generateWorkloadStatistics();
            assert(stats != null);
            
            if (m_tableStatsDir.exists() == false) m_tableStatsDir.mkdirs();
            File path = new File(m_tableStatsDir.getAbsolutePath() + "/" + this.getProjectName() + ".stats");
            LOG.info("Writing table statistics data to '" + path + "'");
            try {
                stats.save(path);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to save table statistics to '" + path + "'", ex);
            }
        }
        
        this.stopCallback();
    }
    
    protected final void invokeClearCallback() {
        m_txnStats.clear(true);
        m_responseEntries.clear();
        this.clearCallback();
    }
    
    /**
     * Internal callback for each POLL tick that we get from the BenchmarkController
     * This will invoke the tick() method that can be implemented benchmark clients 
     * @param counter
     */
    protected final void invokeTickCallback(int counter) {
        if (debug.val) LOG.debug("New Tick Update: " + counter);
        this.tickCallback(counter);
        
        if (debug.val) {
            if (this.computeTime.isEmpty() == false) {
                for (String txnName : this.computeTime.keySet()) {
                    ProfileMeasurement pm = this.computeTime.get(txnName);
                    if (pm.getInvocations() != 0) {
                        LOG.debug(String.format("[%02d] - %s COMPUTE TIME: %s", counter, txnName, pm.debug()));
                        pm.reset();
                    }
                } // FOR
            }
            LOG.debug("Client Queue Time: " + this.m_voltClient.getQueueTime().debug());
            this.m_voltClient.getQueueTime().reset();
        }
    }
    
    /**
     * Optional callback for when this BenchmarkComponent has been initialized
     */
    public void initCallback() {
        // Default is to do nothing
    }
    
    /**
     * Optional callback for when this BenchmarkComponent has been told to start
     */
    public void startCallback() {
        // Default is to do nothing
    }
    
    /**
     * Optional callback for when this BenchmarkComponent has been told to stop
     * This is not a reliable callback and should only be used for testing
     */
    public void stopCallback() {
        // Default is to do nothing
    }
    
    /**
     * Optional callback for when this BenchmarkComponent has been told to clear its 
     * internal counters.
     */
    public void clearCallback() {
        // Default is to do nothing
    }
    
    /**
     * Internal callback for each POLL tick that we get from the BenchmarkController
     * @param counter The number of times we have called this callback in the past
     */
    public void tickCallback(int counter) {
        // Default is to do nothing!
    }
    
    // ----------------------------------------------------------------------------
    // PROFILING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Profiling method to keep track of how much time is spent in the client
     * to compute the input parameters for a new txn invocation.
     * Must be called before stopComputeTime()
     * @see BenchmarkComponent.stopComputeTime
     * @param txnName
     */
    protected synchronized void startComputeTime(String txnName) {
        ProfileMeasurement pm = this.computeTime.get(txnName);
        if (pm == null) {
            pm = new ProfileMeasurement(txnName);
            this.computeTime.put(txnName, pm);
        }
        pm.start();
    }
    
    /**
     * Stop recording the compute time for a new txn invocation.
     * Must be called after startComputeTime()
     * @see BenchmarkComponent.startComputeTime
     * @param txnName
     */
    protected synchronized void stopComputeTime(String txnName) {
        ProfileMeasurement pm = this.computeTime.get(txnName);
        assert(pm != null) : "Unexpected " + txnName;
        pm.stop();
    }
    
    protected ProfileMeasurement getComputeTime(String txnName) {
        return (this.computeTime.get(txnName));
    }
    
    protected boolean useHeavyweightClient() {
        return false;
    }

    /**
     * Implemented by derived classes. Invoke a single procedure without running
     * the network. This allows BenchmarkComponent to control the rate at which
     * transactions are generated.
     *
     * @return True if an invocation was queued and false otherwise
     */
    protected boolean runOnce() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Hint used when constructing the Client to control the size of buffers
     * allocated for message serialization
     *
     * @return
     */
    protected int getExpectedOutgoingMessageSize() {
        return 128;
    }

    public ControlPipe createControlPipe(InputStream in) {
        m_controlPipe = new ControlPipe(this, in, m_controlPipeAutoStart);
        return (m_controlPipe);
    }

    /**
     * Return the number of partitions in the cluster for this benchmark invocation
     * @return
     */
    public final int getNumPartitions() {
        return (m_numPartitions);
    }
    /**
     * Return the DBMS client handle
     * This Client will already be connected to the database cluster
     * @return
     */
    public final Client getClientHandle() {
        return (m_voltClient);
    }
    /**
     * Special hook for setting the DBMS client handle
     * This should only be invoked for RegressionSuite test cases
     * @param client
     */
    protected void setClientHandle(Client client) {
        m_voltClient = client;
    }
    /**
     * Return the unique client id for this invocation of BenchmarkComponent
     * @return
     */
    public final int getClientId() {
        return (m_id);
    }
    /**
     * Returns true if this client thread should submit only single-partition txns.
     * Support for this option must be implemented in the benchmark.
     * @return
     */
    public final boolean isSinglePartitionOnly() {
        boolean ret = (m_id < m_hstoreConf.client.singlepartition_threads);
        if (debug.val && ret)
            LOG.debug(String.format("Client #%03d is marked as single-partiiton only", m_id));
        return (ret);
    }
    /**
     * Return the total number of clients for this benchmark invocation
     * @return
     */
    public final int getNumClients() {
        return (m_numClients);
    }
    /**
     * Returns true if this BenchmarkComponent is not going to make any
     * client connections to an H-Store cluster. This is used for testing
     */
    protected final boolean noClientConnections() {
       return (m_noConnections); 
    }
    /**
     * Return the file path to the catalog that was loaded for this benchmark invocation
     * @return
     */
    public File getCatalogPath() {
        return (m_catalogPath);
    }
    /**
     * Return the project name of this benchmark
     * @return
     */
    public final String getProjectName() {
        return (m_projectName);
    }

    public final int getCurrentTickCounter() {
        return (m_tickCounter);
    }
    
    /**
     * Return the CatalogContext used for this benchmark
     * @return
     */
    public CatalogContext getCatalogContext() {
        // Read back the catalog and populate catalog object
        if (m_catalogContext == null) {
            m_catalogContext = getCatalogContext(m_catalogPath);
        }
        return (m_catalogContext);
    }
    public void setCatalogContext(CatalogContext catalogContext) {
        m_catalogContext = catalogContext;
    }
    public void applyPartitionPlan(File partitionPlanPath) {
        CatalogContext catalogContext = this.getCatalogContext();
        BenchmarkComponent.applyPartitionPlan(catalogContext.database, partitionPlanPath);
    }

    /**
     * Get the HStoreConf handle
     * @return
     */
    public HStoreConf getHStoreConf() {
        return (m_hstoreConf);
    }

    public void setState(final ControlState state, final String reason) {
        m_controlState = state;
        if (m_reason.equals("") == false)
            m_reason += (" " + reason);
        else
            m_reason = reason;
    }
    
    private boolean checkConstraints(String procName, ClientResponse response) {
        boolean isSatisfied = true;
        int orig_position = -1;

        // Check if all the tables in the result set satisfy the constraints.
        for (int i = 0; isSatisfied && i < response.getResults().length; i++) {
            Pair<String, Integer> key = Pair.of(procName, i);
            if (!m_constraints.containsKey(key))
                continue;

            VoltTable table = response.getResults()[i];
            orig_position = table.getActiveRowIndex();
            table.resetRowPosition();

            // Iterate through all rows and check if they satisfy the
            // constraints.
            while (isSatisfied && table.advanceRow()) {
                isSatisfied = Verification.checkRow(m_constraints.get(key), table);
            }

            // Have to reset the position to its original position.
            if (orig_position < 0)
                table.resetRowPosition();
            else
                table.advanceToRow(orig_position);
        }

        if (!isSatisfied)
            LOG.error("Transaction " + procName + " failed check");

        return isSatisfied;
    }

    /**
     * Performs constraint checking on the result set in clientResponse. It does
     * simple sanity checks like if the response code is SUCCESS. If the check
     * transaction flag is set to true by calling setCheckTransaction(), then it
     * will check the result set against constraints.
     *
     * @param procName
     *            The name of the procedure
     * @param clientResponse
     *            The client response
     * @param errorExpected
     *            true if the response is expected to be an error.
     * @return true if it passes all tests, false otherwise
     */
    protected boolean checkTransaction(String procName,
                                       ClientResponse clientResponse,
                                       boolean abortExpected,
                                       boolean errorExpected) {
        final Status status = clientResponse.getStatus();
        if (status != Status.OK) {
            if (errorExpected)
                return true;

            if (abortExpected && status == Status.ABORT_USER)
                return true;

            if (status == Status.ABORT_CONNECTION_LOST) {
                return false;
            }
            if (status == Status.ABORT_REJECT) {
                return false;
            }

            if (clientResponse.getException() != null) {
                clientResponse.getException().printStackTrace();
            }
            if (clientResponse.getStatusString() != null) {
                LOG.warn(clientResponse.getStatusString());
            }
            throw new RuntimeException("Invalid " + procName + " response!\n" + clientResponse);
        }

        if (m_checkGenerator.nextFloat() >= m_checkTransaction)
            return true;

        return checkConstraints(procName, clientResponse);
    }

    /**
     * Sets the given constraint for the table identified by the tableId of
     * procedure 'name'. If there is already a constraint assigned to the table,
     * it is updated to the new one.
     *
     * @param name
     *            The name of the constraint. For transaction check, this should
     *            usually be the procedure name.
     * @param tableId
     *            The index of the table in the result set.
     * @param constraint
     *            The constraint to use.
     */
    protected void addConstraint(String name,
                                 int tableId,
                                 Expression constraint) {
        m_constraints.put(Pair.of(name, tableId), constraint);
    }

    protected void addTableConstraint(String name,
                                      Expression constraint) {
        addConstraint(name, 0, constraint);
        m_tableCheckOrder.add(name);
    }

    /**
     * Removes the constraint on the table identified by tableId of procedure
     * 'name'. Nothing happens if there is no constraint assigned to this table.
     *
     * @param name
     *            The name of the constraint.
     * @param tableId
     *            The index of the table in the result set.
     */
    protected void removeConstraint(String name, int tableId) {
        m_constraints.remove(Pair.of(name, tableId));
    }

    /**
     * Takes a snapshot of all the tables in the database now and check all the
     * rows in each table to see if they satisfy the constraints. The
     * constraints should be added with the table name and table id 0.
     *
     * Since the snapshot files reside on the servers, we have to copy them over
     * to the client in order to check. This might be an overkill, but the
     * alternative is to ask the user to write stored procedure for each table
     * and execute them on all nodes. That's not significantly better, either.
     *
     * This function blocks. Should only be run at the end.
     *
     * @return true if all tables passed the test, false otherwise.
     */
    protected boolean checkTables() {
        return (true);
//        
//        String dir = "/tmp";
//        String nonce = "data_verification";
//        Client client = ClientFactory.createClient(getExpectedOutgoingMessageSize(), null,
//                                                   false, null);
//        // Host ID to IP mappings
//        LinkedHashMap<Integer, String> hostMappings = new LinkedHashMap<Integer, String>();
//        /*
//         *  The key is the table name. the first one in the pair is the hostname,
//         *  the second one is file name
//         */
//        LinkedHashMap<String, Pair<String, String>> snapshotMappings =
//            new LinkedHashMap<String, Pair<String, String>>();
//        boolean isSatisfied = true;
//
//        // Load the native library for loading table from snapshot file
//        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
//
//        try {
//            boolean keepTrying = true;
//            VoltTable[] response = null;
//
//            client.createConnection(m_host, m_username, m_password);
//            // Only initiate the snapshot if it's the first client
//            while (m_id == 0) {
//                // Take a snapshot of the database. This call is blocking.
//                response = client.callProcedure("@SnapshotSave", dir, nonce, 1).getResults();
//                if (response.length != 1 || !response[0].advanceRow()
//                    || !response[0].getString("RESULT").equals("SUCCESS")) {
//                    if (keepTrying
//                        && response[0].getString("ERR_MSG").contains("ALREADY EXISTS")) {
//                        client.callProcedure("@SnapshotDelete",
//                                             new String[] { dir },
//                                             new String[] { nonce });
//                        keepTrying = false;
//                        continue;
//                    }
//
//                    System.err.println("Failed to take snapshot");
//                    return false;
//                }
//
//                break;
//            }
//
//            // Clients other than the one that initiated the snapshot
//            // have to check if the snapshot has completed
//            if (m_id > 0) {
//                int maxTry = 10;
//
//                while (maxTry-- > 0) {
//                    boolean found = false;
//                    response = client.callProcedure("@SnapshotStatus").getResults();
//                    if (response.length != 2) {
//                        System.err.println("Failed to get snapshot status");
//                        return false;
//                    }
//                    while (response[0].advanceRow()) {
//                        if (response[0].getString("NONCE").equals(nonce)) {
//                            found = true;
//                            break;
//                        }
//                    }
//
//                    if (found) {
//                        // This probably means the snapshot is done
//                        if (response[0].getLong("END_TIME") > 0)
//                            break;
//                    }
//
//                    try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException e) {
//                        return false;
//                    }
//                }
//            }
//
//            // Get host ID to hostname mappings
//            response = client.callProcedure("@SystemInformation").getResults();
//            if (response.length != 1) {
//                System.err.println("Failed to get host ID to IP address mapping");
//                return false;
//            }
//            while (response[0].advanceRow()) {
//                if (!response[0].getString("key").equals("hostname"))
//                    continue;
//                hostMappings.put((Integer) response[0].get("node_id", VoltType.INTEGER),
//                                 response[0].getString("value"));
//            }
//
//            // Do a scan to get all the file names and table names
//            response = client.callProcedure("@SnapshotScan", dir).getResults();
//            if (response.length != 3) {
//                System.err.println("Failed to get snapshot filenames");
//                return false;
//            }
//
//            // Only copy the snapshot files we just created
//            while (response[0].advanceRow()) {
//                if (!response[0].getString("NONCE").equals(nonce))
//                    continue;
//
//                String[] tables = response[0].getString("TABLES_REQUIRED").split(",");
//                for (String t : tables)
//                    snapshotMappings.put(t, null);
//                break;
//            }
//
//            while (response[2].advanceRow()) {
//                int id = Integer.parseInt(response[2].getString("HOST_ID"));
//                String tableName = response[2].getString("TABLE");
//
//                if (!snapshotMappings.containsKey(tableName) || !hostMappings.containsKey(id))
//                    continue;
//
//                snapshotMappings.put(tableName, Pair.of(hostMappings.get(id),
//                                                        response[2].getString("NAME")));
//            }
//        } catch (NoConnectionsException e) {
//            e.printStackTrace();
//            return false;
//        } catch (ProcCallException e) {
//            e.printStackTrace();
//            return false;
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//            return false;
//        } catch (IOException e) {
//            e.printStackTrace();
//            return false;
//        }
//
//        // Iterate through all the tables
//        for (String tableName : m_tableCheckOrder) {
//            Pair<String, String> value = snapshotMappings.get(tableName);
//            if (value == null)
//                continue;
//
//            String hostName = value.getFirst();
//            File file = new File(dir, value.getSecond());
//            FileInputStream inputStream = null;
//            TableSaveFile saveFile = null;
//            long rowCount = 0;
//
//            Pair<String, Integer> key = Pair.of(tableName, 0);
//            if (!m_constraints.containsKey(key) || hostName == null)
//                continue;
//
//            System.err.println("Checking table " + tableName);
//
//            // Copy the file over
//            String localhostName = null;
//            try {
//                localhostName = InetAddress.getLocalHost().getHostName();
//            } catch (UnknownHostException e1) {
//                localhostName = "localhost";
//            }
//            if (!hostName.equals("localhost") && !hostName.equals(localhostName)) {
//                if (!SSHTools.copyFromRemote(file, m_username, hostName, file.getPath())) {
//                    System.err.println("Failed to copy the snapshot file " + file.getPath()
//                                       + " from host "
//                                       + hostName);
//                    return false;
//                }
//            }
//
//            if (!file.exists()) {
//                System.err.println("Snapshot file " + file.getPath()
//                                   + " cannot be copied from "
//                                   + hostName
//                                   + " to localhost");
//                return false;
//            }
//
//            try {
//                try {
//                    inputStream = new FileInputStream(file);
//                    saveFile = new TableSaveFile(inputStream.getChannel(), 3, null);
//
//                    // Get chunks from table
//                    while (isSatisfied && saveFile.hasMoreChunks()) {
//                        final BBContainer chunk = saveFile.getNextChunk();
//                        VoltTable table = null;
//
//                        // This probably should not happen
//                        if (chunk == null)
//                            continue;
//
//                        table = PrivateVoltTableFactory.createVoltTableFromBuffer(chunk.b, true);
//                        // Now, check each row
//                        while (isSatisfied && table.advanceRow()) {
//                            isSatisfied = Verification.checkRow(m_constraints.get(key),
//                                                                table);
//                            rowCount++;
//                        }
//                        // Release the memory of the chunk we just examined, be good
//                        chunk.discard();
//                    }
//                } finally {
//                    if (saveFile != null) {
//                        saveFile.close();
//                    }
//                    if (inputStream != null)
//                        inputStream.close();
//                    if (!hostName.equals("localhost") && !hostName.equals(localhostName)
//                        && !file.delete())
//                        System.err.println("Failed to delete snapshot file " + file.getPath());
//                }
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//                return false;
//            } catch (IOException e) {
//                e.printStackTrace();
//                return false;
//            }
//
//            if (isSatisfied) {
//                System.err.println("Table " + tableName
//                                   + " with "
//                                   + rowCount
//                                   + " rows passed check");
//            } else {
//                System.err.println("Table " + tableName + " failed check");
//                break;
//            }
//        }
//
//        // Clean up the snapshot we made
//        try {
//            if (m_id == 0) {
//                client.callProcedure("@SnapshotDelete",
//                                     new String[] { dir },
//                                     new String[] { nonce }).getResults();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ProcCallException e) {
//            e.printStackTrace();
//        }
//
//        System.err.println("Table checking finished "
//                           + (isSatisfied ? "successfully" : "with failures"));
//
//        return isSatisfied;
    }
}
