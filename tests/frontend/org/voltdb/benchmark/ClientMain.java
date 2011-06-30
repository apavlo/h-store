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
import java.io.IOException;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.Verification.Expression;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.StatsUploaderSettings;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltSampler;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreConf;

/**
 * Base class for clients that will work with the multi-host multi-process
 * benchmark framework that is driven from stdin
 */
public abstract class ClientMain {
    private static final Logger LOG = Logger.getLogger(ClientMain.class);
    
    static {
        // log4j hack!
        LoggerUtil.setupLogging();
    }
    
    public static String CONTROL_PREFIX = "{HSTORE} ";
    
    public enum Command {
        START,
        POLL,
        CLEAR,
        PAUSE,
        SHUTDOWN,
        STOP;
     
        protected static final Map<Integer, Command> idx_lookup = new HashMap<Integer, Command>();
        protected static final Map<String, Command> name_lookup = new HashMap<String, Command>();
        static {
            for (Command vt : EnumSet.allOf(Command.class)) {
                Command.idx_lookup.put(vt.ordinal(), vt);
                Command.name_lookup.put(vt.name().toUpperCase().intern(), vt);
            } // FOR
        }
        
        public static Command get(String name) {
            return (Command.name_lookup.get(name.trim().toUpperCase().intern()));
        }
    }
    
    /** The states important to the remote controller */
    public static enum ControlState {
        PREPARING("PREPARING"),
        READY("READY"),
        RUNNING("RUNNING"),
        PAUSED("PAUSED"),
        ERROR("ERROR");

        ControlState(final String displayname) {
            display = displayname;
        }

        public final String display;
    };

    /**
     * Client initialized here and made available for use in derived classes
     */
    protected final Client m_voltClient;

    /**
     * Manage input and output to the framework
     */
    private final ControlPipe m_controlPipe = new ControlPipe();

    /**
     * State of this client
     */
    private volatile ControlState m_controlState = ControlState.PREPARING;

    /**
     * A host, can be any one. This is only used by data verification
     * at the end of run.
     */
    private String m_host;
    private int m_port;

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
     * Count of transactions invoked by this client. This is updated by derived
     * classes directly
     */
    protected final AtomicLong m_counts[];

    /**
     * Display names for each transaction.
     */
    protected final String m_countDisplayNames[];

    /**
     * Client Id
     */
    protected final int m_id;
    
    /**
     * Total # of Clients
     */
    protected final int m_numClients;
    
    /**
     * Total # of Partitions
     */
    protected final int m_numPartitions;

    /**
     * Path to catalog jar
     */
    protected final File m_catalogPath;
    protected Catalog m_catalog;
    
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
    
    /**
     * Configuration
     */
    protected final HStoreConf m_hstoreConf;
    protected final BenchmarkConfig m_benchmarkConf;
    
    

    public static void printControlMessage(ControlState state) {
        printControlMessage(state, null);
    }
    
    public static void printControlMessage(ControlState state, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s%d,%s", CONTROL_PREFIX, System.currentTimeMillis(), state.display)); // PREFIX TIMESTAMP, STATE
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
    class ControlPipe implements Runnable {

        public void run() {
        	LOG.info("STARTING THE CLIENT");
            final Thread self = Thread.currentThread();
            self.setName(String.format("client-%02d", m_id));
            
            final InputStreamReader reader = new InputStreamReader(System.in);
            final BufferedReader in = new BufferedReader(reader);

            Command command = null;
            
            // transition to ready and send ready message
            if (m_controlState == ControlState.PREPARING) {
                printControlMessage(ControlState.READY);
                m_controlState = ControlState.READY;
            	LOG.info("TRANSITION READY TO START");
            } else {
                LOG.error("Not starting prepared!");
                LOG.error(m_controlState.display + " " + m_reason);
            }

            while (true) {
                final boolean debug = LOG.isDebugEnabled(); 
                
                try {
                    command = Command.get(in.readLine());
                    if (debug) LOG.debug(String.format("Recieved Command: '%s'", command));
                } catch (final IOException e) {
                    // Hm. quit?
                    LOG.fatal("Error on standard input", e);
                    System.exit(-1);
                }
                if (command == null) continue;
                if (debug) LOG.debug("Command = " + command);

                switch (command) {
                    case START: {
                        if (m_controlState != ControlState.READY) {
                            setState(ControlState.ERROR, "START when not READY.");
                            answerWithError();
                            continue;
                        }
                        answerStart();
                        m_controlState = ControlState.RUNNING;
                        break;
                    }
                    case POLL: {
                        if (m_controlState != ControlState.RUNNING) {
                            setState(ControlState.ERROR, "POLL when not RUNNING.");
                            answerWithError();
                            continue;
                        }
                        answerPoll();
                        
                        // Call tick on the client!
                        // if (LOG.isDebugEnabled()) LOG.debug("Got poll message! Calling tick()!");
                        ClientMain.this.tick();
                        break;
                    }
                    case CLEAR: {
                        for (AtomicLong cnt : m_counts) {
                            cnt.set(0);
                        } // FOR
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
                            try {
                                m_voltClient.callProcedure("@Shutdown");
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        System.exit(0);
                        break;
                    }
                    case STOP: {
                        if (m_controlState == ControlState.RUNNING || m_controlState == ControlState.PAUSED) {
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
            final StringBuilder txncounts = new StringBuilder();
            synchronized (m_counts) {
                for (int i = 0; i < m_counts.length; ++i) {
                    if (i > 0) txncounts.append(",");
                    txncounts.append(m_countDisplayNames[i]);
                    txncounts.append(",");
                    txncounts.append(m_counts[i].get());
                }
            }
            printControlMessage(m_controlState, txncounts.toString()); 
        }

        public void answerStart() {
            final ControlWorker worker = new ControlWorker();
            new Thread(worker).start();
        }
    }

    /**
     * Thread that executes the derives classes run loop which invokes stored
     * procedures indefinitely
     */
    private class ControlWorker extends Thread {
        @Override
        public void run() {
            if (m_txnRate == -1) {
                if (m_sampler != null) {
                    m_sampler.start();
                }
                try {
                    runLoop();
                }
                catch (final IOException e) {

                }
            }
            else {
                LOG.debug("Running rate controlled m_txnRate == "
                    + m_txnRate + " m_txnsPerMillisecond == "
                    + m_txnsPerMillisecond);
                System.err.flush();
                rateControlledRunLoop();
            }

            if (m_exitOnCompletion) {
                System.exit(0);
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
     * Is called every time the interval time is reached
     */
    protected void tick() {
        // Default is to do nothing!
//        System.out.println("ClientMain.tick()");
    }
    

    protected boolean useHeavyweightClient() {
        return false;
    }

    /**
     * Implemented by derived classes. Invoke a single procedure without running
     * the network. This allows ClientMain to control the rate at which
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

    /**
     * Get the display names of the transactions that will be invoked by the
     * dervied class. As a side effect this also retrieves the number of
     * transactions that can be invoked.
     *
     * @return
     */
    abstract protected String[] getTransactionDisplayNames();

    public ClientMain(final Client client) {
        m_voltClient = client;
        m_exitOnCompletion = false;
        m_host = "localhost";
        m_password = "";
        m_username = "";
        m_txnRate = -1;
        m_blocking = false;
        m_txnsPerMillisecond = 0;
        m_catalogPath = null;
        m_id = 0;
        m_numClients = 1;
        m_numPartitions = 0;
        m_counts = null;
        m_countDisplayNames = null;
        m_checkTransaction = 0;
        m_checkTables = false;
        m_constraints = new LinkedHashMap<Pair<String, Integer>, Expression>();
        
        // FIXME
        m_hstoreConf = null;
        m_benchmarkConf = null;
        
    }

    abstract protected String getApplicationName();
    abstract protected String getSubApplicationName();

    /**
     * Constructor that initializes the framework portions of the client.
     * Creates a Volt client and connects it to all the hosts provided on the
     * command line with the specified username and password
     *
     * @param args
     */
    public ClientMain(String args[]) {
        /*
         * Input parameters: HOST=host:port (may occur multiple times)
         * USER=username PASSWORD=password
         */

        // default values
        String username = "user";
        String password = "password";
        ControlState state = ControlState.PREPARING; // starting state
        String reason = ""; // and error string
        int transactionRate = -1;
        boolean blocking = false;
        int id = 0;
        int num_clients = 0;
        int num_partitions = 0;
        boolean exitOnCompletion = true;
        float checkTransaction = 0;
        boolean checkTables = false;
//        String statsDatabaseURL = null;
//        int statsPollInterval = 10000;
        File catalogPath = null;

        // HStoreConf Path
        String hstore_conf_path = null;
        
        // Benchmark Conf Path
        String benchmark_conf_path = null;
        
        // scan the inputs once to read everything but host names
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            final String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                state = ControlState.ERROR;
                reason = "Invalid parameter: " + arg;
                break;
            } else if (parts[1].startsWith("${")) {
                continue;
            }
            
            if (parts[0].equalsIgnoreCase("CONF")) {
                hstore_conf_path = parts[1];
            } else if (parts[0].equalsIgnoreCase(BenchmarkController.BENCHMARK_PARAM_PREFIX + "CONF")) {
                benchmark_conf_path = parts[1];
            }
                
            // Strip out benchmark prefix  
            if (parts[0].toLowerCase().startsWith(BenchmarkController.BENCHMARK_PARAM_PREFIX)) {
                parts[0] = parts[0].substring(BenchmarkController.BENCHMARK_PARAM_PREFIX.length());
                args[i] = parts[0] + "=" + parts[1]; // HACK
            }
            
            if (parts[0].equalsIgnoreCase("CATALOG")) {
                catalogPath = new File(parts[1]);
                assert(catalogPath.exists()) : "The catalog file '" + catalogPath.getAbsolutePath() + " does not exist";
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
            else if (parts[0].equalsIgnoreCase("TXNRATE")) {
                transactionRate = Integer.parseInt(parts[1]);
            }
            else if (parts[0].equalsIgnoreCase("BLOCKING")) {
                blocking = Boolean.parseBoolean(parts[1]);
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
//            } else if (parts[0].equalsIgnoreCase("STATSDATABASEURL")) {
//                statsDatabaseURL = parts[1];
//            } else if (parts[0].equalsIgnoreCase("STATSPOLLINTERVAL")) {
//                statsPollInterval = Integer.parseInt(parts[1]);
            } else {
                m_extraParams.put(parts[0], parts[1]);
            }
        }
        
        // Initialize HStoreConf
        if (HStoreConf.isInitialized() == false) {
            assert(hstore_conf_path != null) : "Missing HStoreConf file";
            HStoreConf.init(new File(hstore_conf_path));
        }
        m_hstoreConf = HStoreConf.singleton();
        
        if (benchmark_conf_path != null) {
            m_benchmarkConf = new BenchmarkConfig(new File(benchmark_conf_path));
        } else {
            m_benchmarkConf = null;
        }
        
        // Thread.currentThread().setName(String.format("client-%02d", id));
        
        StatsUploaderSettings statsSettings = null;
//        if (statsDatabaseURL != null) {
//            try {
//                statsSettings =
//                    new
//                        StatsUploaderSettings(
//                            statsDatabaseURL,
//                            getApplicationName(),
//                            getSubApplicationName(),
//                            statsPollInterval);
//            } catch (Exception e) {
//                System.err.println(e.getMessage());
//                //e.printStackTrace();
//                statsSettings = null;
//            }
//        }
        Client new_client =
            ClientFactory.createClient(
                getExpectedOutgoingMessageSize(),
                null,
                useHeavyweightClient(),
                statsSettings);

        m_catalogPath = catalogPath;
        m_id = id;
        m_numClients = num_clients;
        m_numPartitions = num_partitions;
        m_exitOnCompletion = exitOnCompletion;
        m_username = username;
        m_password = password;
        m_txnRate = transactionRate;
        m_txnsPerMillisecond = transactionRate / 1000.0;
        m_blocking = blocking;
        
        if (m_catalogPath != null) {
            try {
                // HACK: This will instantiate m_catalog for us...
                this.getCatalog();
            } catch (Exception ex) {
                LOG.fatal("Failed to load catalog", ex);
                System.exit(1);
            }
        }

        if (m_blocking) {
            LOG.debug("Using BlockingClient!");
            m_voltClient = new BlockingClient(new_client);
        } else {
            m_voltClient = new_client;
        }
        
        // report any errors that occurred before the client was instantiated
        if (state != ControlState.PREPARING)
            setState(state, reason);

        // scan the inputs again looking for host connections
        boolean atLeastOneConnection = false;
        for (final String arg : args) {
            final String[] parts = arg.split("=", 2);
            if (parts.length == 1) {
                continue;
            }
            else if (parts[0].equals("HOST")) {
                final Pair<String, Integer> hostnport = StringUtil.getHostPort(parts[1]);
                m_host = hostnport.getFirst();
                m_port = hostnport.getSecond();
                try {
                    LOG.debug("Creating connection to " + hostnport);
                    createConnection(m_host, m_port);
                    LOG.debug("Created connection.");
                    atLeastOneConnection = true;
                }
                catch (final Exception ex) {
                    setState(ControlState.ERROR, "createConnection to " + arg
                        + " failed: " + ex.getMessage());
                }
            }
        }
        if (!atLeastOneConnection) {
            setState(ControlState.ERROR, "No HOSTS specified on command line.");
            LOG.warn("NO HOSTS WERE PROVIDED!");
        }
        m_checkTransaction = checkTransaction;
        m_checkTables = checkTables;
        m_constraints = new LinkedHashMap<Pair<String, Integer>, Expression>();

        m_countDisplayNames = getTransactionDisplayNames();
        m_counts = new AtomicLong[m_countDisplayNames.length];
        for (int ii = 0; ii < m_counts.length; ii++) {
            m_counts[ii] = new AtomicLong(0);
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
    public static void main(final Class<? extends ClientMain> clientClass,
        final String args[], final boolean startImmediately) {
        try {
            final Constructor<? extends ClientMain> constructor =
                clientClass.getConstructor(new Class<?>[] { new String[0].getClass() });
            final ClientMain clientMain =
                constructor.newInstance(new Object[] { args });
            if (startImmediately) {
                final ControlWorker worker = clientMain.new ControlWorker();
                worker.start();
                // Wait for the worker to finish
                worker.join();
            }
            else {
                clientMain.start();
            }
        }
        catch (final Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // update the client state and start waiting for a message.
    private void start() {
        m_controlPipe.run();
    }

    public final int getClientId() {
        return (m_id);
    }
    
    public final int getNumClients() {
        return (m_numClients);
    }
    
    public File getCatalogPath() {
        return (m_catalogPath);
    }

    /**
     * Return the catalog used for this benchmark.
     * @return
     * @throws Exception
     */
    public Catalog getCatalog() throws Exception {
        // Read back the catalog and populate catalog object
        if (m_catalog == null) {
            m_catalog =  CatalogUtil.loadCatalogFromJar(m_catalogPath.getAbsolutePath());
        }
        return (m_catalog);
    }
    
    public void setCatalog(Catalog catalog) {
    	m_catalog = catalog;
    }

    public void setState(final ControlState state, final String reason) {
        m_controlState = state;
        if (m_reason.equals("") == false)
            m_reason += (" " + reason);
        else
            m_reason = reason;
    }

    private void createConnection(final String hostname, final int port)
        throws UnknownHostException, IOException {
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Requesting connection to %s:%d", hostname, port));
        m_voltClient.createConnection(hostname, port, m_username, m_password);
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
            System.err.println("Transaction " + procName + " failed check");

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
        final byte status = clientResponse.getStatus();
        if (status != ClientResponse.SUCCESS) {
            if (errorExpected)
                return true;

            if (abortExpected && status == ClientResponse.USER_ABORT)
                return true;

            if (status == ClientResponse.CONNECTION_LOST) {
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
