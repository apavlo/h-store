/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.brown.hstore;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.debugstate.InitiatorContext;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.InputHandler;
import org.voltdb.network.NIOReadStream;
import org.voltdb.network.NIOWriteStream;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltNetwork;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.network.WriteStream;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DumpManager;

import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.ThreadUtil;

/**
 * Represents VoltDB's connection to client libraries outside the cluster.
 * This class accepts new connections and manages existing connections through
 * <code>ClientConnection</code> instances.
 *
 */
public class ClientInterface implements DumpManager.Dumpable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(ClientInterface.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    

    // clock time of last call to the initiator's tick()
    static final int POKE_INTERVAL = 1000;
    
    
    // ----------------------------------------------------------------------------
    // QUEUE MONITOR RUNNABLE
    // ----------------------------------------------------------------------------
    
    private final QueueMonitor m_clientQueueMonitor = new QueueMonitor() {
        private final int MAX_QUEABLE = 33554432;

        private int m_queued = 0;

        @Override
        public boolean queue(int queued) {
            synchronized (m_connections) {
                m_queued += queued;
                if (m_queued > MAX_QUEABLE) {
                    if (m_hasGlobalClientBackPressure || m_hasDTXNBackPressure) {
                        m_hasGlobalClientBackPressure = true;
                        //Guaranteed to already have reads disabled
                        return false;
                    }

                    m_hasGlobalClientBackPressure = true;
                    for (final Connection c : m_connections) {
                        c.disableReadSelection();
                    }
                } else {
                    if (!m_hasGlobalClientBackPressure) {
                        return false;
                    }

                    if (m_hasGlobalClientBackPressure && !m_hasDTXNBackPressure) {
                        for (final Connection c : m_connections) {
                            if (!c.writeStream().hadBackPressure()) {
                                /*
                                 * Also synchronize on the individual connection
                                 * so that enabling of read selection happens atomically
                                 * with the checking of client backpressure (client not reading responses)
                                 * in the write stream
                                 * so that this doesn't interleave incorrectly with
                                 * SimpleDTXNInitiator disabling read selection.
                                 */
                                synchronized (c) {
                                    if (!c.writeStream().hadBackPressure()) {
                                        c.enableReadSelection();
                                    }
                                }
                            }
                        }
                    }
                    m_hasGlobalClientBackPressure = false;
                }
            }
            return false;
        }
    };
    
    // ----------------------------------------------------------------------------
    // CLIENT ACCEPTOR RUNNABLE
    // ----------------------------------------------------------------------------
    
    /** A port that accepts client connections */
    public class ClientAcceptor implements Runnable {
        private final int m_port;
        private final ServerSocketChannel m_serverSocket;
        private final VoltNetwork m_network;
        private volatile boolean m_running = true;
        private Thread m_thread = null;

        /**
         * Limit on maximum number of connections. This should be set by inspecting ulimit -n, but
         * that isn't being done.
         */
        private final int MAX_CONNECTIONS = 4000;

        /**
         * Used a cached thread pool to accept new connections.
         */
        private final ExecutorService m_executor = Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicLong m_createdThreadCount = new AtomicLong(0);
            private final ThreadGroup m_group =
                new ThreadGroup(Thread.currentThread().getThreadGroup(), "Client authentication threads");

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(m_group, r, "Client authenticator " + m_createdThreadCount.getAndIncrement(), 131072);
            }
        });

        ClientAcceptor(int port, VoltNetwork network) {
            m_network = network;
            m_port = port;
            ServerSocketChannel socket;
            try {
                socket = ServerSocketChannel.open();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            m_serverSocket = socket;
        }

        public void start() throws IOException {
            if (m_thread != null) {
                throw new IllegalStateException("A thread for this ClientAcceptor is already running");
            }
            if (!m_serverSocket.socket().isBound()) {
                m_serverSocket.socket().bind(new InetSocketAddress(m_port));
            }
            m_running = true;
            m_thread = new Thread( null, this, "Client connection accceptor", 262144);
            m_thread.setDaemon(true);
            m_thread.start();
        }

        public void shutdown() throws InterruptedException {
            //sync prevents interruption while shuttown down executor
            synchronized (this) {
                m_running = false;
                m_thread.interrupt();
            }
            m_thread.join();
        }

        @Override
        public void run() {
            try {
                do {
                    final SocketChannel socket = m_serverSocket.accept();

                    /*
                     * Enforce a limit on the maximum number of connections
                     */
                    if (m_numConnections.get() == MAX_CONNECTIONS) {
                        LOG.warn("Rejected connection from " +
                                socket.socket().getRemoteSocketAddress() +
                                " because the connection limit of " + MAX_CONNECTIONS + " has been reached");
                        /*
                         * Send rejection message with reason code
                         */
                        final ByteBuffer b = ByteBuffer.allocate(1);
                        b.put((byte)1);
                        b.flip();
                        socket.configureBlocking(true);
                        for (int ii = 0; ii < 4 && b.hasRemaining(); ii++) {
                            socket.write(b);
                        }
                        socket.close();
                        continue;
                    }

                    /*
                     * Increment the number of connections even though this one hasn't been authenticated
                     * so that a flood of connection attempts (with many doomed) will not result in
                     * successful authentication of connections that would put us over the limit.
                     */
                    m_numConnections.incrementAndGet();

                    m_executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            if (socket != null) {
                                boolean success = false;
                                try {
                                    final InputHandler handler = authenticate(socket);
                                    if (handler != null) {
                                        socket.configureBlocking(false);
                                        socket.socket().setTcpNoDelay(false);
                                        socket.socket().setKeepAlive(true);

                                        synchronized (m_connections){
                                            Connection c = null;
                                            if (!m_hasDTXNBackPressure) {
                                                c = m_network.registerChannel(socket, handler, SelectionKey.OP_READ);
                                            }
                                            else {
                                                c = m_network.registerChannel(socket, handler, 0);
                                            }
                                            m_connections.add(c);
                                        }
                                        success = true;
                                    }
                                } catch (IOException e) {
                                    try {
                                        socket.close();
                                    } catch (IOException e1) {
                                        //Don't care connection is already lost anyways
                                    }
                                    if (m_running) {
                                        LOG.warn("Exception authenticating and registering user in ClientAcceptor", e);
                                    }
                                } finally {
                                    if (!success) {
                                        m_numConnections.decrementAndGet();
                                    }
                                }
                            }
                        }
                    });
                } while (m_running);
            }  catch (IOException e) {
                if (m_running) {
                    LOG.fatal("Exception in ClientAcceptor. The acceptor has died", e);
                }
            } finally {
                try {
                    m_serverSocket.close();
                } catch (IOException e) {
                    LOG.fatal(null, e);
                }
                //Prevent interruption
                synchronized (this) {
                    Thread.interrupted();
                    m_executor.shutdownNow();
                    try {
                        m_executor.awaitTermination( 1, TimeUnit.DAYS);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        /**
         * Attempt to authenticate the user associated with this socket connection
         * @param socket
         * @return AuthUser a set of user permissions or null if authentication fails
         * @throws IOException
         */
        private InputHandler authenticate(final SocketChannel socket) throws IOException {
            ByteBuffer responseBuffer = ByteBuffer.allocate(6);
            byte version = (byte)0;
            responseBuffer.putInt(2);//message length
            responseBuffer.put(version);//version

            /*
             * The login message is a length preceded name string followed by a length preceded
             * SHA-1 single hash of the password.
             */
            socket.configureBlocking(false);//Doing NIO allows timeouts via Thread.sleep()
            socket.socket().setTcpNoDelay(true);//Greatly speeds up requests hitting the wire
            final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

            //Do non-blocking I/O to retrieve the length preceding value
            for (int ii = 0; ii < 4; ii++) {
                socket.read(lengthBuffer);
                if (!lengthBuffer.hasRemaining()) {
                    break;
                }
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            //Didn't get the value. Client isn't going to get anymore time.
            if (lengthBuffer.hasRemaining()) {
                //Send negative response
                responseBuffer.put((byte)2).flip();
                socket.write(responseBuffer);
                socket.close();
                LOG.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (timeout reading message length).");
                return null;
            }
            lengthBuffer.flip();

            final int messageLength = lengthBuffer.getInt();
            if (messageLength < 0) {
              //Send negative response
                responseBuffer.put((byte)3).flip();
                socket.write(responseBuffer);
                socket.close();
                LOG.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (message length " + messageLength + " is negative).");
                return null;
            }
            if (messageLength > ((1024 * 1024) * 2)) {
                //Send negative response
                  responseBuffer.put((byte)3).flip();
                  socket.write(responseBuffer);
                  socket.close();
                  LOG.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                               "): wire protocol violation (message length " + messageLength + " is too large).");
                  return null;
              }

            final ByteBuffer message = ByteBuffer.allocate(messageLength);
            //Do non-blocking I/O to retrieve the login message
            for (int ii = 0; ii < 4; ii++) {
                socket.read(message);
                if (!message.hasRemaining()) {
                    break;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            //Didn't get the whole message. Client isn't going to get anymore time.
            if (lengthBuffer.hasRemaining()) {
                //Send negative response
                responseBuffer.put((byte)2).flip();
                socket.write(responseBuffer);
                socket.close();
                LOG.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (timeout reading authentication strings).");
                return null;
            }
            message.flip().position(1);//skip version
            FastDeserializer fds = new FastDeserializer(message);
            @SuppressWarnings("unused")
            final String service = fds.readString();
            @SuppressWarnings("unused")
            final String username = fds.readString();
            final byte password[] = new byte[20];
            message.get(password);

            /*
             * Create an input handler.
             */
            InputHandler handler = new ClientInputHandler(socket.socket().getInetAddress().getHostName());
            byte buildString[] = HStore.getVersionString().getBytes("UTF-8");
            responseBuffer = ByteBuffer.allocate(34 + buildString.length);
            responseBuffer.putInt(30 + buildString.length);//message length
            responseBuffer.put((byte)0);//version

            //Send positive response
            responseBuffer.put((byte)0);
            responseBuffer.putInt(hstore_site.getSiteId());
            responseBuffer.putLong(handler.connectionId());
            responseBuffer.putLong(hstore_site.getInstanceId());
            responseBuffer.putInt(0x0); // leaderAddress (part of instanceId)
            responseBuffer.putInt(buildString.length);
            responseBuffer.put(buildString).flip();
            socket.write(responseBuffer);
            
            LOG.info("Established new client connection to " + socket);
            
            return handler;
        }
    }
    
    // ----------------------------------------------------------------------------
    // CLIENT INPUT HANDLER
    // ----------------------------------------------------------------------------

    /** A port that reads client procedure invocations and writes responses */
    public class ClientInputHandler extends VoltProtocolHandler {
        public static final int MAX_READ = 8192 * 4;

        private Connection m_connection;
        private final String m_hostname;

        /**
         *
         * @param user Set of permissions associated with requests coming from this connection
         */
        public ClientInputHandler(String hostname) {
            m_hostname = hostname;
        }

        @Override
        public int getMaxRead() {
            if (m_hasDTXNBackPressure) {
                return 0;
            } else {
                return Math.max( MAX_READ, getNextMessageLength());
            }
        }

        @Override
        public int getExpectedOutgoingMessageSize() {
            return FastSerializer.INITIAL_ALLOCATION;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            hstore_site.queueInvocation(message, this, c);
        }

        @Override
        public void started(final Connection c) {
            m_connection = c;
        }

        @Override
        public void stopping(Connection c) {
            synchronized (m_connections) {
                m_connections.remove(c);
            }
        }

        @Override
        public void stopped(Connection c) {
            m_numConnections.decrementAndGet();
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    LOG.info("Off-backpressure for " + this);
                    /**
                     * Must synchronize to prevent a race between the DTXN backpressure starting
                     * and this attempt to reenable read selection (which should not occur
                     * if there is DTXN backpressure)
                     */
                    synchronized (m_connections) {
                        if (!m_hasDTXNBackPressure) {
                            m_connection.enableReadSelection();
                        }
                    }
                }
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    LOG.info("On-backpressure for " + this);
                    synchronized (m_connection) {
                        m_connection.disableReadSelection();
                    }
                }
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return m_clientQueueMonitor;
        }
    }
    
    // ----------------------------------------------------------------------------
    // INSTANCE MEMBERS
    // ----------------------------------------------------------------------------
    
    private final HStoreSite hstore_site;
    
    private final ClientAcceptor m_acceptor;
    private final ArrayList<Connection> m_connections = new ArrayList<Connection>();

    // Atomically allows the catalog reference to change between access
    private final AtomicReference<CatalogContext> m_catalogContext = new AtomicReference<CatalogContext>(null);

    /**
     * Counter of the number of client connections. Used to enforce a limit on the maximum number of connections
     */
    private final AtomicInteger m_numConnections = new AtomicInteger(0);


    final int m_siteId;
    final String m_dumpId;


    /**
     * This boolean allows the DTXN to communicate to the
     * ClientInputHandler the presence of DTXN backpressure.
     * The m_connections ArrayList is used as the synchronization
     * point to ensure that modifications to read interest ops
     * that are based on the status of this information are atomic.
     * Additionally each connection must be synchronized on before modification
     * because the disabling of read selection for an individual connection
     * due to backpressure (not DTXN backpressure, client backpressure due to a client
     * that refuses to read responses) occurs inside the SimpleDTXNInitiator which
     * doesn't have access to m_connections
     */
    private boolean m_hasDTXNBackPressure = false;

    /**
     * Way too much data tied up sending responses to clients.
     * Wait until they receive data or have been booted.
     */
    private boolean m_hasGlobalClientBackPressure = false;
    
    /**
     * Task to run when a backpressure condition starts
     */
    private final EventObservable<HStoreSite> onBackPressure = new EventObservable<HStoreSite>();

    /**
     * Task to run when a backpressure condition stops
     */
    private final EventObservable<HStoreSite> offBackPressure = new EventObservable<HStoreSite>();

    /**
     * Indicates if backpressure has been seen and reported
     */
    private boolean m_hadBackPressure = false;
    
    // If an initiator handles a full node, it processes approximately 50,000 txns/sec.
    // That's about 50 txns/ms. Try not to keep more than 5 ms of work? Seems like a really
    // small number. On the other hand, backPressure() is just a hint to the ClientInterface.
    // CI will submit ClientPort.MAX_READ * clients / bytesPerStoredProcInvocation txns
    // on average if clients present constant uninterrupted load.
    private final static int MAX_DESIRED_PENDING_BYTES = 20; // 67108864;
    private final static int MAX_DESIRED_PENDING_TXNS = 15000;
    private long m_pendingTxnBytes = 0;
    private int m_pendingTxnCount = 0;

    /**
     * Tick counter used to perform dead client detection every N ticks
     */
    private long m_tickCounter = 0;
    
    // ----------------------------------------------------------------------------
    // PROFILING MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * How much time the site spends with backup pressure blocking disabled
     */
    private final ProfileMeasurement network_backup_off;
    
    /**
     * How much time the site spends with backup pressure blocking enabled
     */
    private final ProfileMeasurement network_backup_on;
    
    // ----------------------------------------------------------------------------
    // BACKPRESSURE OBSERVERS
    // ----------------------------------------------------------------------------

    /**
     * Invoked when DTXN backpressure starts
     */
    private final EventObserver<HStoreSite> onBackPressureObserver = new EventObserver<HStoreSite>() {
        @Override
        public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
            LOG.info("Had back pressure disabling read selection");
            synchronized (m_connections) {
                if (network_backup_off != null) {
                    ProfileMeasurement.swap(network_backup_off, network_backup_on);
                }
                m_hasDTXNBackPressure = true;
                for (final Connection c : m_connections) {
                    c.disableReadSelection();
                }
            }
        }
    };

    /**
     * Invoked when DTXN backpressure stops
     */
    private final EventObserver<HStoreSite> offBackPressureObserver = new EventObserver<HStoreSite>() {
        @Override
        public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
            LOG.info("No more back pressure attempting to enable read selection");
            synchronized (m_connections) {
                if (network_backup_off != null) {
                    ProfileMeasurement.swap(network_backup_on, network_backup_off);
                }
                m_hasDTXNBackPressure = false;
                if (m_hasGlobalClientBackPressure) {
                    return;
                }
                for (final Connection c : m_connections) {
                    if (!c.writeStream().hadBackPressure()) {
                        /*
                         * Also synchronize on the individual connection
                         * so that enabling of read selection happens atomically
                         * with the checking of client backpressure (client not reading responses)
                         * in the write stream
                         * so that this doesn't interleave incorrectly with
                         * SimpleDTXNInitiator disabling read selection.
                         */
                        synchronized (c) {
                            if (!c.writeStream().hadBackPressure()) {
                                c.enableReadSelection();
                            }
                        } // SYNCH
                    }
                } // FOR
            } // SYNCH
        }
    };
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Static factory method to easily create a ClientInterface with the default
     * settings.
     */
    public static ClientInterface create(
            HStoreSite hstore_site,
            VoltNetwork network,
            CatalogContext context,
            int siteId,
            int initiatorId,
            int port,
            SnapshotSchedule schedule) {

        final ClientInterface ci = new ClientInterface(
                hstore_site, port, context, network, siteId, schedule);

        return ci;
    }

    ClientInterface(HStoreSite hstore_site,
                    int port,
                    CatalogContext context,
                    VoltNetwork network,
                    int siteId,
                    SnapshotSchedule schedule) {
        
        this.hstore_site = hstore_site;
        m_catalogContext.set(context);
        m_siteId = siteId;
        m_dumpId = "Initiator." + String.valueOf(siteId);
        DumpManager.register(m_dumpId, this);

        // Backpressure EventObservers
        this.onBackPressure.addObserver(this.onBackPressureObserver);
        this.offBackPressure.addObserver(this.offBackPressureObserver);
        
        m_acceptor = new ClientAcceptor(port, network);
        
        if (hstore_site.getHStoreConf().site.network_profiling) {
            network_backup_off = new ProfileMeasurement("BACKUP-OFF");
            network_backup_on = new ProfileMeasurement("BACKUP-ON");
        } else {
            network_backup_off = null;
            network_backup_on = null;
        }
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public long getPendingTxnBytes() {
        return m_pendingTxnBytes;
    }
    
    public int getPendingTxnCount() {
        return m_pendingTxnCount;
    }
    
    public boolean hasBackPressure() {
        return m_hadBackPressure;
    }
    
    public int getConnectionCount() {
        return m_numConnections.get();
    }
    
    protected ProfileMeasurement getBackPressureOn() {
        return network_backup_on;
    }
    
    protected ProfileMeasurement getBackPressureOff() {
        return network_backup_off;
    }
    
    
    public void increaseBackpressure(int messageSize)
    {
        if (debug.get()) LOG.debug("Increasing Backpressure: " + messageSize);
        
        m_pendingTxnBytes += messageSize;
        m_pendingTxnCount++;
        if (m_pendingTxnBytes > MAX_DESIRED_PENDING_BYTES || m_pendingTxnCount > MAX_DESIRED_PENDING_TXNS) {
            if (!m_hadBackPressure) {
                LOG.trace("DTXN back pressure began");
                m_hadBackPressure = true;
                onBackPressure.notifyObservers(hstore_site);
            }
        }
    }

    public void reduceBackpressure(final int messageSize)
    {
        if (debug.get()) 
            LOG.debug("Reducing Backpressure: " + messageSize);
        
        m_pendingTxnBytes -= messageSize;
        m_pendingTxnCount--;
        if (m_pendingTxnBytes < (MAX_DESIRED_PENDING_BYTES * .8) &&
            m_pendingTxnCount < (MAX_DESIRED_PENDING_TXNS * .8))
        {
            if (m_hadBackPressure)
            {
                LOG.trace("DTXN backpressure ended");
                m_hadBackPressure = false;
                offBackPressure.notifyObservers(hstore_site);
            }
        }
    }

    public void startAcceptingConnections() throws IOException {
        if (this.network_backup_off != null) this.network_backup_off.start(); 
        m_acceptor.start();
    }

//    /**
//     *
//     * @param port
//     * * return True if an error was generated and needs to be returned to the client
//     */
//    private final void handleRead(ByteBuffer buf, ClientInputHandler handler, Connection c) throws IOException {
//        
//        
//        
//        
//        final long now = EstTime.currentTimeMillis();
//        final FastDeserializer fds = new FastDeserializer(buf);
//
//        // Deserialize the client's request and map to a catalog stored procedure
//        final StoredProcedureInvocation task = fds.readObject(StoredProcedureInvocation.class);
//        final Procedure catProc = m_catalogContext.get().procedures.get(task.procName);
//
//        /*
//         * @TODO This ladder stinks. An SPI when deserialized here should be
//         * able to determine if the task is permitted by calling a method that
//         * provides an AuthUser object.
//         */
//        if (task.procName.startsWith("@")) {
//
//            // AdHoc requires unique permission. Then has to plan in a separate thread.
//            if (task.procName.equals("@AdHoc")) {
//                if (!handler.m_user.hasAdhocPermission()) {
//                    final ClientResponseImpl errorResponse =
//                        new ClientResponseImpl(-1, task.clientHandle, -1,
//                                               Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0], "User does not have @AdHoc permission");
//                    authLog.l7dlog(Level.INFO,
//                                   LogKeys.auth_ClientInterface_LackingPermissionForAdhoc.name(),
//                                   new String[] {handler.m_user.m_name}, null);
//                    c.writeStream().enqueue(errorResponse);
//                    return;
//                }
//                task.buildParameterSet();
//                if (task.params.toArray().length != 1) {
//                    final ClientResponseImpl errorResponse =
//                        new ClientResponseImpl(-1, task.clientHandle, -1,
//                                               Hstoreservice.Status.ABORT_UNEXPECTED,
//                                               new VoltTable[0],
//                                               "Adhoc system procedure requires exactly one parameter, the SQL statement to execute.");
//                    c.writeStream().enqueue(errorResponse);
//                    return;
//                }
//                String sql = (String) task.params.toArray()[0];
////                m_asyncCompilerWorkThread.planSQL(
////                                                  sql,
////                                                  task.clientHandle,
////                                                  handler.connectionId(),
////                                                  handler.m_hostname,
////                                                  handler.sequenceId(),
////                                                  c);
//                return;
//            }
//
//            // All other sysprocs require the sysproc permission
//            if (!handler.m_user.hasSystemProcPermission()) {
//                authLog.l7dlog(Level.INFO,
//                               LogKeys.auth_ClientInterface_LackingPermissionForSysproc.name(),
//                               new String[] { handler.m_user.m_name, task.procName },
//                               null);
//                final ClientResponseImpl errorResponse =
//                    new ClientResponseImpl(-1, task.clientHandle, -1,
//                                           Hstoreservice.Status.ABORT_UNEXPECTED,
//                                           new VoltTable[0],
//                                           "User " + handler.m_user.m_name + " does not have sysproc permission");
//                c.writeStream().enqueue(errorResponse);
//                return;
//            }
//
//            // Updating a catalog needs to divert to the catalog processing thread
//            if (task.procName.equals("@UpdateApplicationCatalog")) {
//                task.buildParameterSet();
//                // user only provides catalog URL.
//                if (task.params.size() != 1) {
//                    final ClientResponseImpl errorResponse =
//                        new ClientResponseImpl(-1,task.clientHandle, -1,
//                                               Hstoreservice.Status.ABORT_UNEXPECTED,
//                                               new VoltTable[0],
//                                               "UpdateApplicationCatalog system procedure requires exactly " +
//                                               "one parameter, the URL of the catalog to load");
//                    c.writeStream().enqueue(errorResponse);
//                    return;
//                }
//                String catalogURL = (String) task.params.toArray()[0];
//                m_asyncCompilerWorkThread.prepareCatalogUpdate(catalogURL,
//                                                               task.clientHandle,
//                                                               handler.connectionId(),
//                                                               handler.m_hostname,
//                                                               handler.sequenceId(),
//                                                               c);
//                return;
//            }
//        } else if (!handler.m_user.hasPermission(catProc)) {
//            authLog.l7dlog(Level.INFO,
//                           LogKeys.auth_ClientInterface_LackingPermissionForProcedure.name(),
//                           new String[] { handler.m_user.m_name, task.procName }, null);
//            final ClientResponseImpl errorResponse =
//                new ClientResponseImpl(-1, task.clientHandle, -1,
//                                       Hstoreservice.Status.ABORT_UNEXPECTED,
//                                       new VoltTable[0],
//                                       "User does not have permission to invoke " + catProc.getTypeName());
//            c.writeStream().enqueue(errorResponse);
//            return;
//        }
//
//        if (catProc != null) {
//            int[] involvedPartitions = null;
//            if (catProc.getEverysite() == true) {
//                involvedPartitions = m_allPartitions;
//                task.buildParameterSet();
//            }
//            else if (catProc.getSinglepartition() == false) {
//                involvedPartitions = m_allPartitions;
//                task.buildParameterSet();
//            }
//            else {
//                // break out the Hashinator and calculate the appropriate partition
//                try {
//                    involvedPartitions = new int[] { getPartitionForProcedure(catProc.getPartitionparameter(), task) };
//                }
//                catch (RuntimeException e) {
//                    // unable to hash to a site, return an error
//                    String errorMessage = "Error sending procedure "
//                        + task.procName + " to the correct partition. Make sure parameter values are correct.";
//                    authLog.l7dlog( Level.WARN,
//                            LogKeys.host_ClientInterface_unableToRouteSinglePartitionInvocation.name(),
//                            new Object[] { task.procName }, null);
//                    final ClientResponseImpl errorResponse =
//                        new ClientResponseImpl(-1, task.clientHandle, -1,
//                                             Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0], errorMessage);
//                    c.writeStream().enqueue(errorResponse);
//                }
//            }
//
//            if (involvedPartitions != null) {
//                // initiate the transaction
//                m_initiator.createTransaction(handler.connectionId(), handler.m_hostname, task,
//                                              catProc.getReadonly(),
//                                              catProc.getSinglepartition(),
//                                              catProc.getEverysite(),
//                                              involvedPartitions, involvedPartitions.length,
//                                              c, buf.capacity(),
//                                              now);
//            }
//        }
//        else {
//            // No such procedure: log and tell the client
//            String errorMessage = "Procedure " + task.procName + " was not found";
//            authLog.l7dlog( Level.WARN, LogKeys.auth_ClientInterface_ProcedureNotFound.name(), new Object[] { task.procName }, null);
//            final ClientResponseImpl errorResponse =
//                new ClientResponseImpl(-1, task.clientHandle, -1,
//                        Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0], errorMessage);
//            c.writeStream().enqueue(errorResponse);
//        }
//    }

    


    /**
     * Check for dead connections by providing each connection with the current
     * time so it can calculate the delta between now and the time the oldest message was
     * queued for sending.
     * @param now Current time in milliseconds
     */
    protected final void checkForDeadConnections(final long now) {
        if (++m_tickCounter % 20 != 0) {
            return;
        }
        
        Connection connectionsToCheck[];
        synchronized (m_connections) {
            connectionsToCheck = m_connections.toArray(new Connection[m_connections.size()]);
        }

        final ArrayList<Connection> connectionsToRemove = new ArrayList<Connection>();
        for (final Connection c : connectionsToCheck) {
            final int delta = c.writeStream().calculatePendingWriteDelta(now);
            if (delta > 4000) {
                connectionsToRemove.add(c);
            }
        }

        for (final Connection c : connectionsToRemove) {
            LOG.warn("Closing connection to " + c + " at " + now + " because it refuses to read responses");
            c.unregister();
        }
    }

    @Override
    public boolean isShuttingDown() {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        // TODO Auto-generated method stub
    }
    
    // BUG: this needs some more serious thinking
    // probably should be able to schedule a shutdown event
    // to the dispatcher..  Or write a "stop reading and flush
    // all your read buffers" events .. or something ..
    public void shutdown() {
        if (m_acceptor != null) {
            try {
                m_acceptor.shutdown();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    @Override
    public void goDumpYourself(long timestamp) {
        DumpManager.putDump(m_dumpId, timestamp, true, getDumpContents());
    }

    /**
     * Get the actual file contents for a dump of state reachable by
     * this thread. Can be called unsafely or safely.
     */
    public InitiatorContext getDumpContents() {
        InitiatorContext context = new InitiatorContext();
        context.siteId = m_siteId;

        return context;
    }

    /**
     * A dummy connection to provide to the DTXN. It routes
     * ClientResponses back to the daemon
     *
     */
    private class SnapshotDaemonAdapter implements Connection, WriteStream {

        @Override
        public void disableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NIOReadStream readStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NIOWriteStream writeStream() {
            return null;
        }

        @Override
        public int calculatePendingWriteDelta(long now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(BBContainer c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(FastSerializable f) {
//            initiateSnapshotDaemonWork(
//                    m_snapshotDaemon.processClientResponse((ClientResponseImpl) f));
            return true;
        }

        @Override
        public boolean enqueue(FastSerializable f, int expectedSize) {
//            initiateSnapshotDaemonWork(
//                    m_snapshotDaemon.processClientResponse((ClientResponseImpl) f));
            return true;
        }

        @Override
        public boolean enqueue(DeferredSerialization ds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enqueue(ByteBuffer b) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hadBackPressure() {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public void setBackPressure(boolean enable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getHostname() {
            return "";
        }

        @Override
        public void scheduleRunnable(Runnable r) {
        }

        @Override
        public void unregister() {
        }

    }
}
