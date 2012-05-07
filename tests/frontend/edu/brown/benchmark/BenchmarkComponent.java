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

package edu.brown.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONStringer;
import org.voltdb.ClientResponseImpl;
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
import org.voltdb.client.StatsUploaderSettings;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltSampler;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;

/**
 * Base class for clients that will work with the multi-host multi-process
 * benchmark framework that is driven from stdin
 */
public abstract class BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(BenchmarkComponent.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static String CONTROL_MESSAGE_PREFIX = "{HSTORE}";
    
    /**
     * These are the commands that the BenchmarkController will send to each
     * BenchmarkComponent in order to coordinate the benchmark's execution
     */
    public enum Command {
        START,
        POLL,
        CLEAR,
        PAUSE,
        /**
         * Stop this BenchmarkComponent instance
         */
        STOP,
        /**
         * This is the same as STOP except that the BenchmarkComponent will
         * tell the cluster to shutdown first before it exits 
         */
        SHUTDOWN,
        ;
     
        protected static final Map<Integer, Command> idx_lookup = new HashMap<Integer, Command>();
        protected static final Map<String, Command> name_lookup = new HashMap<String, Command>();
        static {
            for (Command vt : EnumSet.allOf(Command.class)) {
                Command.idx_lookup.put(vt.ordinal(), vt);
                Command.name_lookup.put(vt.name().toUpperCase(), vt);
            } // FOR
        }
        
        public static Command get(String name) {
            return (Command.name_lookup.get(name.trim().toUpperCase()));
        }
    }
    
    /**
     * These represent the different states that the BenchmarkComponent's ControlPipe
     * could be in. 
     */
    public static enum ControlState {
        PREPARING,
        READY,
        RUNNING,
        PAUSED,
        ERROR;

        protected static final Map<Integer, ControlState> idx_lookup = new HashMap<Integer, ControlState>();
        protected static final Map<String, ControlState> name_lookup = new HashMap<String, ControlState>();
        static {
            for (ControlState vt : EnumSet.allOf(ControlState.class)) {
                ControlState.idx_lookup.put(vt.ordinal(), vt);
                ControlState.name_lookup.put(vt.name().toUpperCase(), vt);
            } // FOR
        }
        
        public static ControlState get(String name) {
            return (ControlState.name_lookup.get(name.trim().toUpperCase()));
        }
    };

    private static Client globalClient;
    private static Catalog globalCatalog;
    private static PartitionPlan globalPartitionPlan;
    private static boolean globalHasConnections = false;
    
    public static synchronized Client getClient(Catalog catalog, int messageSize, boolean heavyWeight, StatsUploaderSettings statsSettings) {
//        Client newClient = ClientFactory.createClient(
//                    messageSize,
//                    null,
//                    heavyWeight,
//                    statsSettings,
//                    catalog
//            );
//        return (newClient);
        
        if (globalClient == null) {
            globalClient = ClientFactory.createClient(
                    messageSize,
                    null,
                    heavyWeight,
                    statsSettings,
                    catalog
            );
            if (debug.get()) LOG.debug("Created global Client handle");
        }
        return (globalClient);
    }
    
    public static synchronized Catalog getCatalog(File catalogPath) {
        // Read back the catalog and populate catalog object
        if (globalCatalog == null) {
            globalCatalog =  CatalogUtil.loadCatalogFromJar(catalogPath.getAbsolutePath());
        }
        return (globalCatalog);
    }
    
    public static synchronized void applyPartitionPlan(Database catalog_db, String partitionPlanPath) {
        if (globalPartitionPlan == null) {
            if (debug.get()) LOG.debug("Loading PartitionPlan '" + partitionPlanPath + "' and applying it to the catalog");
            globalPartitionPlan = new PartitionPlan();
            try {
                globalPartitionPlan.load(partitionPlanPath, catalog_db);
                globalPartitionPlan.apply(catalog_db);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load PartitionPlan '" + partitionPlanPath + "' and apply it to the catalog", ex);
            }
        }
        return;
    }
    
    /**
     * Client initialized here and made available for use in derived classes
     */
    private Client m_voltClient;

    /**
     * Manage input and output to the framework
     */
    private ControlPipe m_controlPipe;

    /**
     * State of this client
     */
    private volatile ControlState m_controlState = ControlState.PREPARING;

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
    private final int m_txnRate;
    
    private final boolean m_blocking;

    /**
     * Number of transactions to generate for every millisecond of time that
     * passes
     */
    private final double m_txnsPerMillisecond;

    /**
     * Additional parameters (benchmark specific)
     */
    protected final Map<String, String> m_extraParams = new HashMap<String, String>();

    /**
     * Storage for error descriptions
     */
    private String m_reason = "";

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
    private Catalog m_catalog;
    private final String m_projectName;
    
    private final boolean m_exitOnCompletion;
    
    /**
     * Pause Lock
     */
    private final Semaphore m_pauseLock = new Semaphore(1);

    /**
     * Data verification.
     */
    private final float m_checkTransaction;
    private final boolean m_checkTables;
    private final Random m_checkGenerator = new Random();
    private final LinkedHashMap<Pair<String, Integer>, Expression> m_constraints;
    private final List<String> m_tableCheckOrder = new LinkedList<String>();
    protected VoltSampler m_sampler = null;
    
    private final int m_tickInterval;
    private final Thread m_tickThread;
    private int m_tickCounter = 0;
    
    private final boolean m_noUploading;
    private final ReentrantLock m_loaderBlock = new ReentrantLock();
    private final ClientResponse m_dummyResponse = new ClientResponseImpl(-1, -1, -1, Status.OK, HStoreConstants.EMPTY_RESULT, "");
    
    /**
     * Keep track of the number of tuples loaded so that we can generate table statistics
     */
    private final boolean m_tableStats;
    private final File m_tableStatsDir;
    private final Histogram<String> m_tableTuples = new Histogram<String>();
    private final Histogram<String> m_tableBytes = new Histogram<String>();
    private final Map<Table, TableStatistics> m_tableStatsData = new HashMap<Table, TableStatistics>();
    private final TransactionCounter m_txnStats = new TransactionCounter();

    private final Map<String, ProfileMeasurement> computeTime = new HashMap<String, ProfileMeasurement>();
    
    /**
     * 
     */
    private BenchmarkClientFileUploader uploader = null;
    
    /**
     * Configuration
     */
    private final HStoreConf m_hstoreConf;
    private final Histogram<String> m_txnWeights = new Histogram<String>();
    private Integer m_txnWeightsDefault = null;
    

    public void printControlMessage(ControlState state) {
        printControlMessage(state, null);
    }
    
    public void printControlMessage(ControlState state, String message) {
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
    
    /**
     * Implements the simple state machine for the remote controller protocol.
     * Hypothetically, you can extend this and override the answerPoll() and
     * answerStart() methods for other clients.
     */
    protected class ControlPipe implements Runnable {
        final InputStream in;
        
        public ControlPipe(InputStream in) {
            this.in = in;
        }

        public void run() {
            final Thread self = Thread.currentThread();
            self.setName(String.format("client-%02d", m_id));

            Command command = null;
            
            // transition to ready and send ready message
            if (m_controlState == ControlState.PREPARING) {
                printControlMessage(ControlState.READY);
                m_controlState = ControlState.READY;
            } else {
                LOG.error("Not starting prepared!");
                LOG.error(m_controlState + " " + m_reason);
            }
            
            final BufferedReader in = new BufferedReader(new InputStreamReader(this.in));
            while (true) {
                try {
                    command = Command.get(in.readLine());
                    if (debug.get()) 
                        LOG.debug(String.format("Recieved Message: '%s'", command));
                } catch (final IOException e) {
                    // Hm. quit?
                    LOG.fatal("Error on standard input", e);
                    System.exit(-1);
                }
                if (command == null) continue;
                if (debug.get()) LOG.debug("ControlPipe Command = " + command);

                final ControlWorker worker = new ControlWorker();
                final Thread t = new Thread(worker);
                t.setDaemon(true);
                
                // HACK: Convert a SHUTDOWN to a STOP if we're not the first client
                if (command == Command.SHUTDOWN && getClientId() != 0) {
                    command = Command.STOP;
                }
                
                switch (command) {
                    case START: {
                        if (m_controlState != ControlState.READY) {
                            setState(ControlState.ERROR, "START when not READY.");
                            answerWithError();
                            continue;
                        }
                        t.start();
                        if (m_tickThread != null) m_tickThread.start();
                        m_controlState = ControlState.RUNNING;
                        answerOk();
                        break;
                    }
                    case POLL: {
                        if (m_controlState != ControlState.RUNNING) {
                            setState(ControlState.ERROR, "POLL when not RUNNING.");
                            answerWithError();
                            continue;
                        }
                        answerPoll();
                        
                        // Call tick on the client if we're not polling ourselves
                        if (BenchmarkComponent.this.m_tickInterval < 0) {
                            if (debug.get()) LOG.debug("Got poll message! Calling tick()!");
                            invokeTickCallback(m_tickCounter++);
                        }
                        if (debug.get())
                            LOG.debug(String.format("CLIENT QUEUE TIME: %.2fms / %.2fms avg",
                                                    m_voltClient.getQueueTime().getTotalThinkTimeMS(),
                                                    m_voltClient.getQueueTime().getAverageThinkTimeMS()));
                        break;
                    }
                    case CLEAR: {
                        m_txnStats.clear();
                        invokeClearCallback();
                        answerOk();
                        break;
                    }
                    case PAUSE: {
                        assert(m_controlState == ControlState.RUNNING) : "Unexpected " + m_controlState;
                        
                        LOG.info("Pausing client");
                        
                        // Enable the lock and then change our state
                        try {
                            m_pauseLock.acquire();
                        } catch (InterruptedException ex) {
                            LOG.fatal("Unexpected interuption!", ex);
                            System.exit(1);
                        }
                        m_controlState = ControlState.PAUSED;
                        break;
                    }
                    case SHUTDOWN: {
                        if (m_controlState == ControlState.RUNNING || m_controlState == ControlState.PAUSED) {
                            invokeStopCallback();
                            try {
                                m_voltClient.drain();
                                m_voltClient.callProcedure("@Shutdown");
                                m_voltClient.close();
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        System.exit(0);
                        break;
                    }
                    case STOP: {
                        if (m_controlState == ControlState.RUNNING || m_controlState == ControlState.PAUSED) {
                            invokeStopCallback();
                            try {
                                if (m_sampler != null) {
                                    m_sampler.setShouldStop();
                                    m_sampler.join();
                                }
                                m_voltClient.close();
                                if (m_checkTables) {
                                    checkTables();
                                }
                            } catch (InterruptedException e) {
                                System.exit(0);
                            } finally {
                                System.exit(0);
                            }
                        }
                        LOG.fatal("STOP when not RUNNING");
                        System.exit(-1);
                        break;
                    }
                    default: {
                        LOG.fatal("Error on standard input: unknown command " + command);
                        System.exit(-1);
                    }
                } // SWITCH
            }
        }

        public void answerWithError() {
            printControlMessage(m_controlState, m_reason);
        }

        public void answerPoll() {
            JSONStringer stringer = new JSONStringer();
            TransactionCounter copy = m_txnStats; // .copy();
            try {
                stringer.object();
                copy.toJSON(stringer);
                stringer.endObject();
                printControlMessage(m_controlState, stringer.toString());
            } catch (JSONException ex) {
                throw new RuntimeException(ex);
            }
            m_txnStats.basePartitions.clear();
        }

        public void answerOk() {
            printControlMessage(m_controlState, "OK");
        }
    }

    /**
     * Thread that executes the derives classes run loop which invokes stored
     * procedures indefinitely
     */
    private class ControlWorker extends Thread {
        @Override
        public void run() {
            try {
                if (m_txnRate == -1) {
                    if (m_sampler != null) {
                        m_sampler.start();
                    }
                    runLoop();
                } else {
                    if (debug.get()) LOG.debug(String.format("Running rate controlled [m_txnRate=%d, m_txnsPerMillisecond=%f]", m_txnRate, m_txnsPerMillisecond));
                    rateControlledRunLoop();
                }
            } catch (Throwable ex) {
                ex.printStackTrace();
                System.exit(0);
            } finally {
                if (m_exitOnCompletion) System.exit(0);
            }
        }

        /*
         * Time in milliseconds since requests were last sent.
         */
        private long m_lastRequestTime;

        private void rateControlledRunLoop() {
            m_lastRequestTime = System.currentTimeMillis();
            while (true) {
                boolean bp = false;
                try {
                    // If there is back pressure don't send any requests. Update the
                    // last request time so that a large number of requests won't
                    // queue up to be sent when there is no longer any back
                    // pressure.
                    m_voltClient.backpressureBarrier();
                    
                    // Check whether we are currently being paused
                    // We will block until we're allowed to go again
                    if (m_controlState == ControlState.PAUSED) {
                        m_pauseLock.acquire();
                    }
                    assert(m_controlState != ControlState.PAUSED) : "Unexpected " + m_controlState;
                    
                } catch (InterruptedException e1) {
                    throw new RuntimeException();
                }

                final long now = System.currentTimeMillis();

                /*
                 * Generate the correct number of transactions based on how much
                 * time has passed since the last time transactions were sent.
                 */
                final long delta = now - m_lastRequestTime;
                if (delta > 0) {
                    final int transactionsToCreate = (int) (delta * m_txnsPerMillisecond);
                    if (transactionsToCreate < 1) {
                        // Thread.yield();
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                            System.exit(1);
                        }
                        continue;
                    }

                    for (int ii = 0; ii < transactionsToCreate; ii++) {
                        try {
                            bp = !runOnce();
                            if (bp) {
                                m_lastRequestTime = now;
                                break;
                            }
                        }
                        catch (final IOException e) {
                            return;
                        }
                    }
                }
                else {
                    // Thread.yield();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        System.exit(1);
                    }
                }

                m_lastRequestTime = now;
            }
        }
    }

    /**
     * Implemented by derived classes. Loops indefinitely invoking stored
     * procedures. Method never returns and never receives any updates.
     */
    abstract protected void runLoop() throws IOException;
    
    /**
     * Get the display names of the transactions that will be invoked by the
     * dervied class. As a side effect this also retrieves the number of
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
     * @param txn_idx
     */
    protected final void incrementTransactionCounter(ClientResponse cresponse, int txn_idx) {
        // Only include it if it wasn't rejected
        // This is actually handled in the Distributer, but it doesn't hurt to have this here
        Status status = cresponse.getStatus();
        if (status == Status.OK || status == Status.ABORT_USER) {
            m_txnStats.basePartitions.put(cresponse.getBasePartition());
            m_txnStats.transactions.put(m_countDisplayNames[txn_idx]);
        }
    }

    public BenchmarkComponent(final Client client) {
        m_voltClient = client;
        m_exitOnCompletion = false;
        m_password = "";
        m_username = "";
        m_txnRate = -1;
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
        m_tableStats = false;
        m_tableStatsDir = null;
        m_noUploading = false;
        
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
            
        if (HStoreConf.isInitialized() == false) {
            assert(hstore_conf_path != null) : "Missing HStoreConf file";
            File f = new File(hstore_conf_path);
            if (debug.get()) LOG.debug("Initializing HStoreConf from '" + f.getName() + "' along with input parameters");
            HStoreConf.init(f, args);
        } else {
            if (debug.get()) LOG.debug("Initializing HStoreConf only with input parameters");
            HStoreConf.singleton().loadFromArgs(args);
        }
        m_hstoreConf = HStoreConf.singleton();
        if (trace.get()) LOG.trace("HStore Conf\n" + m_hstoreConf.toString(true));
        
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
            
            if (debug.get()) componentParams.put(parts[0], parts[1]);
            
            if (parts[0].equalsIgnoreCase("CATALOG")) {
                catalogPath = new File(parts[1]);
                assert(catalogPath.exists()) : "The catalog file '" + catalogPath.getAbsolutePath() + " does not exist";
                if (debug.get()) componentParams.put(parts[0], catalogPath);
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
                if (debug.get()) componentParams.remove(parts[0]);
                parts[0] = parts[0].substring(HStoreConstants.BENCHMARK_PARAM_PREFIX.length());
                m_extraParams.put(parts[0].toUpperCase(), parts[1]);
            }
        }
        if (trace.get()) {
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            m.put("BenchmarkComponent", componentParams);
            m.put("Extra Client", m_extraParams);
            LOG.debug("Input Parameters:\n" + StringUtil.formatMaps(m));
        }
        
        // Thread.currentThread().setName(String.format("client-%02d", id));
        
        m_catalogPath = catalogPath;
        m_projectName = projectName;
        m_id = id;
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
        

        
        // If we were told to sleep, do that here before we try to load in the catalog
        // This is an attempt to keep us from overloading a single node all at once
        if (startupWait > 0) {
            if (debug.get()) LOG.debug(String.format("Delaying client start-up by %.2f sec", startupWait/1000d));
            try {
                Thread.sleep(startupWait);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Unexpected interruption", ex);
            }
        }
        
        // HACK: This will instantiate m_catalog for us...
        if (m_catalogPath != null) {
            this.getCatalog();
        }
        
        // Parse workload transaction weights
        if (m_hstoreConf.client.weights != null) {
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
                        if (debug.get()) LOG.debug(String.format("Default Transaction Weight: %d", txnWeight));
                    } else {
                        if (debug.get()) LOG.debug(String.format("%s Transaction Weight: %d", txnName, txnWeight));
                        this.m_txnWeights.put(txnName.toUpperCase(), txnWeight);
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
            if (exists) this.applyPartitionPlan(partitionPlanPath);
        }
        
        StatsUploaderSettings statsSettings = null;
        if (statsDatabaseURL != null && statsDatabaseURL.isEmpty() == false) {
            try {
                statsSettings = StatsUploaderSettings.singleton(
                                        statsDatabaseURL,
                                        statsDatabaseUser,
                                        statsDatabasePass,
                                        statsDatabaseJDBC,
                                        projectName,
                                        (isLoader ? "LOADER" : "CLIENT"),
                                        statsPollInterval,
                                        m_catalog);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to initialize StatsUploader", ex);
            }
            if (debug.get())
                LOG.debug("StatsUploaderSettings:\n" + statsSettings);
        }
        Client new_client = BenchmarkComponent.getClient(
                (m_hstoreConf.client.txn_hints ? this.getCatalog() : null),
                getExpectedOutgoingMessageSize(),
                useHeavyweightClient(),
                statsSettings
        );
        if (m_blocking) { //  && isLoader == false) {
            if (debug.get()) 
                LOG.debug(String.format("Using BlockingClient [concurrent=%d]", m_hstoreConf.client.blocking_concurrent));
            m_voltClient = new BlockingClient(new_client, m_hstoreConf.client.blocking_concurrent);
        } else {
            m_voltClient = new_client;
        }
        
        // report any errors that occurred before the client was instantiated
        if (state != ControlState.PREPARING)
            setState(state, reason);

        // scan the inputs again looking for host connections
        if (m_noConnections == false) {
            synchronized (BenchmarkComponent.class) {
                if (globalHasConnections == false) {
                    this.setupConnections();
                    globalHasConnections = true;
                }
            } // SYNCH
        }
        m_checkTransaction = checkTransaction;
        m_checkTables = checkTables;
        m_constraints = new LinkedHashMap<Pair<String, Integer>, Expression>();

        m_countDisplayNames = getTransactionDisplayNames();
        if (m_countDisplayNames != null) {
            for (String txnName : m_countDisplayNames) {
                m_txnStats.transactions.put(txnName, 0);
            } // FOR
        }
        
        // If we need to call tick more frequently that when POLL is called,
        // then we'll want to use a separate thread
        if (m_tickInterval > 0 && isLoader == false) {
            if (debug.get())
                LOG.debug(String.format("Creating local thread that will call BenchmarkComponent.tick() every %.1f seconds", (m_tickInterval / 1000.0)));
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
            if (startImmediately) {
                final ControlWorker worker = clientMain.new ControlWorker();
                worker.start();
                
                // Wait for the worker to finish
                if (debug.get()) LOG.debug(String.format("Started ControlWorker for client #%02d. Waiting until finished...", clientMain.getClientId()));
                worker.join();
                clientMain.invokeStopCallback();
            }
            else {
                // if (debug.get()) LOG.debug(String.format("Deploying ControlWorker for client #%02d. Waiting for control signal...", clientMain.getClientId()));
                // clientMain.start();
            }
        }
        catch (final Throwable e) {
            String name = (clientMain != null ? clientMain.getProjectName()+"." : "") + clientClass.getSimpleName(); 
            LOG.error("Unexpected error while invoking " + name, e);
            System.exit(-1);
        }
        return (clientMain);
    }
    
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
        
        if (trace.get())
            LOG.trace(String.format("%s: Loading %d new rows - TOTAL %d [bytes=%d/%d]",
                                    tableName.toUpperCase(), rowCount, rowTotal, byteCount, byteTotal));
        
        // Load up this dirty mess...
        ClientResponse cr = null;
        if (m_noUploading == false) {
            boolean locked = m_hstoreConf.client.blocking_loader;
            if (locked) m_loaderBlock.lock();
            try {
                cr = m_voltClient.callProcedure("@LoadMultipartitionTable", tableName, vt);
            } catch (Exception e) {
                throw new RuntimeException("Error when trying load data for '" + tableName + "'", e);
            } finally {
                if (locked) m_loaderBlock.unlock();
            } // SYNCH
            if (debug.get()) LOG.debug(String.format("Load %s: txn #%d / %s / %d",
                                                     tableName, cr.getTransactionId(), cr.getStatus(), cr.getClientHandle()));
        } else {
            cr = m_dummyResponse;
        }
        if (cr.getStatus() != Status.OK) {
            LOG.warn(String.format("Failed to load %d rows for '%s': %s", rowCount, tableName, cr.getStatusString()), cr.getException()); 
            return (cr);
        }
        
        m_tableTuples.put(tableName, rowCount);
        m_tableBytes.put(tableName, byteCount);
        
        // Keep track of table stats
        if (m_tableStats && cr.getStatus() == Status.OK) {
            final Catalog catalog = this.getCatalog();
            assert(catalog != null);
            final Database catalog_db = CatalogUtil.getDatabase(catalog);
            final Table catalog_tbl = catalog_db.getTables().getIgnoreCase(tableName);
            assert(catalog_tbl != null) : "Invalid table name '" + tableName + "'";
            
            synchronized (m_tableStatsData) {
                TableStatistics stats = m_tableStatsData.get(catalog_tbl);
                if (stats == null) {
                    stats = new TableStatistics(catalog_tbl);
                    stats.preprocess(catalog_db);
                    m_tableStatsData.put(catalog_tbl, stats);
                }
                vt.resetRowPosition();
                while (vt.advanceRow()) {
                    VoltTableRow row = vt.getRow();
                    stats.process(catalog_db, row);
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
        return (new Histogram<String>(m_tableTuples));
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
        final Catalog catalog = this.getCatalog();
        assert(catalog != null);
        final Database catalog_db = CatalogUtil.getDatabase(catalog);

        // Make sure we call postprocess on all of our friends
        for (TableStatistics tableStats : m_tableStatsData.values()) {
            try {
                tableStats.postprocess(catalog_db);
            } catch (Exception ex) {
                String tableName = tableStats.getCatalogItem(catalog_db).getName();
                throw new RuntimeException("Failed to process TableStatistics for '" + tableName + "'", ex);
            }
        } // FOR
        
        if (trace.get())
            LOG.trace(String.format("Creating WorkloadStatistics for %d tables [totalRows=%d, totalBytes=%d",
                                    m_tableStatsData.size(), m_tableTuples.getSampleCount(), m_tableBytes.getSampleCount()));
        WorkloadStatistics stats = new WorkloadStatistics(catalog_db);
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
    
    private final void invokeStopCallback() {
        // If we were generating stats, then get the final WorkloadStatistics object
        // and write it out to a file for them to use
        if (m_tableStats) {
            WorkloadStatistics stats = this.generateWorkloadStatistics();
            assert(stats != null);
            
            if (m_tableStatsDir.exists() == false) m_tableStatsDir.mkdirs();
            String path = m_tableStatsDir.getAbsolutePath() + "/" + this.getProjectName() + ".stats";
            LOG.info("Writing table statistics data to '" + path + "'");
            try {
                stats.save(path);
            } catch (IOException ex) {
                LOG.error("Failed to save table statistics to '" + path + "'", ex);
                System.exit(1);
            }
        }
        
        this.stopCallback();
    }
    
    private final void invokeClearCallback() {
        this.clearCallback();
    }
    
    /**
     * Internal callback for each POLL tick that we get from the BenchmarkController
     * This will invoke the tick() method that can be implemented benchmark clients 
     * @param counter
     */
    private final void invokeTickCallback(int counter) {
        if (debug.get()) LOG.debug("New Tick Update: " + counter);
        this.tickCallback(counter);
        
        if (debug.get()) {
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
    
    protected synchronized void startComputeTime(String txnName) {
        ProfileMeasurement pm = this.computeTime.get(txnName);
        if (pm == null) {
            pm = new ProfileMeasurement(txnName);
            this.computeTime.put(txnName, pm);
        }
        pm.start();
    }
    
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

    // update the client state and start waiting for a message.
    public void start(InputStream in) {
        m_controlPipe = new ControlPipe(in);
        m_controlPipe.run(); // blocking
    }
    
    public ControlPipe createControlPipe(InputStream in) {
        m_controlPipe = new ControlPipe(in);
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
     * Return the catalog used for this benchmark.
     * @return
     * @throws Exception
     */
    public Catalog getCatalog() {
        // Read back the catalog and populate catalog object
        if (m_catalog == null) {
            m_catalog = getCatalog(m_catalogPath);
        }
        return (m_catalog);
    }
    public void setCatalog(Catalog catalog) {
        m_catalog = catalog;
    }
    public void applyPartitionPlan(String partitionPlanPath) {
        Database catalog_db = CatalogUtil.getDatabase(this.getCatalog());
        BenchmarkComponent.applyPartitionPlan(catalog_db, partitionPlanPath);
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

    private void setupConnections() {
        boolean atLeastOneConnection = false;
        for (Site catalog_site : CatalogUtil.getAllSites(this.getCatalog())) {
            int site_id = catalog_site.getId();
            String host = catalog_site.getHost().getIpaddr();
            int port = catalog_site.getProc_port();
            if (debug.get())
                LOG.debug(String.format("Creating connection to %s at %s:%d",
                                        HStoreThreadManager.formatSiteName(site_id),
                                        host, port));
            try {
                this.createConnection(site_id, host, port);
            } catch (IOException ex) {
                String msg = String.format("Failed to connect to %s on %s:%d",
                                           HStoreThreadManager.formatSiteName(site_id), host, port);
                LOG.error(msg, ex);
                setState(ControlState.ERROR, msg + ": " + ex.getMessage());
                break;
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
        if (debug.get())
            LOG.debug(String.format("Requesting connection to %s %s:%d",
                HStoreThreadManager.formatSiteName(site_id), hostname, port));
        m_voltClient.createConnection(site_id, hostname, port, m_username, m_password);
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

            if (clientResponse.getException() != null) {
                clientResponse.getException().printStackTrace();
            }
            if (clientResponse.getStatusString() != null) {
                LOG.warn(clientResponse.getStatusString());
            }

            System.exit(-1);
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
