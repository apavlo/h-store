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

package org.voltdb;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.compiler.AdHocPlannedStmt;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWorkThread;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.debugstate.InitiatorContext;
import org.voltdb.dtxn.SimpleDtxnInitiator;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.elt.ELTManager;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.Messenger;
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
import org.voltdb.utils.EstTime;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice;

/**
 * Represents VoltDB's connection to client libraries outside the cluster.
 * This class accepts new connections and manages existing connections through
 * <code>ClientConnection</code> instances.
 *
 */
public class ClientInterface implements DumpManager.Dumpable {
    private static final Logger log = Logger.getLogger(ClientInterface.class.getName(), org.voltdb.utils.VoltLoggerFactory.instance());
    private static final Logger authLog = Logger.getLogger("AUTH", org.voltdb.utils.VoltLoggerFactory.instance());
    private static final Logger hostLog = Logger.getLogger("HOST", org.voltdb.utils.VoltLoggerFactory.instance());
    private static final Logger networkLog = Logger.getLogger("NETWORK", org.voltdb.utils.VoltLoggerFactory.instance());
    private final ClientAcceptor m_acceptor;
    private final TransactionInitiator m_initiator;
    private final AsyncCompilerWorkThread m_asyncCompilerWorkThread;
    private final ArrayList<Connection> m_connections = new ArrayList<Connection>();
//    private final SnapshotDaemon m_snapshotDaemon;
    private final SnapshotDaemonAdapter m_snapshotDaemonAdapter = new SnapshotDaemonAdapter();

    // Atomically allows the catalog reference to change between access
    private final AtomicReference<CatalogContext> m_catalogContext = new AtomicReference<CatalogContext>(null);

    /** If this is true, update the catalog */
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    /**
     * Counter of the number of client connections. Used to enforce a limit on the maximum number of connections
     */
    private final AtomicInteger m_numConnections = new AtomicInteger(0);

    // clock time of last call to the initiator's tick()
    static final int POKE_INTERVAL = 1000;
    private long m_lastCompilerThreadPoke = 0;

    private final int m_allPartitions[];
    final int m_siteId;
    final String m_dumpId;

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
                        networkLog.warn("Rejected connection from " +
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
                                        hostLog.warn("Exception authenticating and registering user in ClientAcceptor", e);
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
                    hostLog.fatal("Exception in ClientAcceptor. The acceptor has died", e);
                }
            } finally {
                try {
                    m_serverSocket.close();
                } catch (IOException e) {
                    hostLog.fatal(null, e);
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
        private InputHandler
        authenticate(final SocketChannel socket) throws IOException
        {
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
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
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
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (message length " + messageLength + " is negative).");
                return null;
            }
            if (messageLength > ((1024 * 1024) * 2)) {
                //Send negative response
                  responseBuffer.put((byte)3).flip();
                  socket.write(responseBuffer);
                  socket.close();
                  authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
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
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (timeout reading authentication strings).");
                return null;
            }
            message.flip().position(1);//skip version
            FastDeserializer fds = new FastDeserializer(message);
            final String service = fds.readString();
            final String username = fds.readString();
            final byte password[] = new byte[20];
            message.get(password);

            /*
             * Authenticate the user.
             */
            final AuthSystem.AuthUser user =
                m_catalogContext.get().authSystem.authenticate(username, password);

            if (user == null) {
                //Send negative response
                responseBuffer.put((byte)-1).flip();
                socket.write(responseBuffer);
                socket.close();
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): user " + username + " failed authentication.");
                return null;
            }

            /*
             * Create an input handler.
             */
            InputHandler handler = null;
            if (service.equalsIgnoreCase("database")) {
                handler =
                    new ClientInputHandler(
                            user,
                            socket.socket().getInetAddress().getHostName());
            }
            else {
                // If no processor can handle this service, null is returned.
                String connectorClassName = ELTManager.instance().getConnectorForService(service);
                if (!user.authorizeConnector(connectorClassName)) {
                    //Send negative response
                    responseBuffer.put((byte)-1).flip();
                    socket.write(responseBuffer);
                    socket.close();
                    authLog.warn("Failure to authroize user " + username + " for service " + service + ".");
                    return null;
                }

                handler = ELTManager.instance().createInputHandler(service);
            }

            if (handler != null) {
                byte buildString[] = VoltDB.instance().getBuildString().getBytes("UTF-8");
                responseBuffer = ByteBuffer.allocate(34 + buildString.length);
                responseBuffer.putInt(30 + buildString.length);//message length
                responseBuffer.put((byte)0);//version

                //Send positive response
                responseBuffer.put((byte)0);
                responseBuffer.putInt(VoltDB.instance().getHostMessenger().getHostId());
                responseBuffer.putLong(handler.connectionId());
                responseBuffer.putLong((Long)VoltDB.instance().getInstanceId()[0]);
                responseBuffer.putInt((Integer)VoltDB.instance().getInstanceId()[1]);
                responseBuffer.putInt(buildString.length);
                responseBuffer.put(buildString).flip();
                socket.write(responseBuffer);

            }
            else {
                // Send negative response
                responseBuffer.put((byte)-1).flip();
                socket.write(responseBuffer);
                socket.close();
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): user " + username + " failed authentication.");
                return null;

            }
            return handler;
        }
    }

    /** A port that reads client procedure invocations and writes responses */
    public class ClientInputHandler extends VoltProtocolHandler {
        public static final int MAX_READ = 8192 * 4;

        private Connection m_connection;
        private final String m_hostname;

        /**
         * Set of user permissions associated with this connection. Authentication is performed when
         * the connection is opened and used to selected this permission set.
         */
        private final AuthSystem.AuthUser m_user;

        /**
         *
         * @param user Set of permissions associated with requests coming from this connection
         */
        public ClientInputHandler(AuthSystem.AuthUser user, String hostname) {
            m_user = user;
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
            try {
                handleRead(message, this, c);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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

    /**
     * Invoked when DTXN backpressure starts
     *
     */
    private static class OnDTXNBackPressure implements Runnable {
        private ClientInterface m_ci;

        @Override
        final public void run() {
            log.trace("Had back pressure disabling read selection");
            synchronized (m_ci.m_connections) {
                m_ci.m_hasDTXNBackPressure = true;
                for (final Connection c : m_ci.m_connections) {
                    c.disableReadSelection();
                }
            }
        }
    }

    /**
     * Invoked when DTXN backpressure stops
     *
     */
    private static class OffDTXNBackPressure implements Runnable {
        private ClientInterface m_ci;

        @Override
        final public void run() {
            log.trace("No more back pressure attempting to enable read selection");
            synchronized (m_ci.m_connections) {
                m_ci.m_hasDTXNBackPressure = false;
                if (m_ci.m_hasGlobalClientBackPressure) {
                    return;
                }
                for (final Connection c : m_ci.m_connections) {
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
        }
    }

    /**
     * Static factory method to easily create a ClientInterface with the default
     * settings.
     */
    public static ClientInterface create(
            VoltNetwork network,
            Messenger messenger,
            CatalogContext context,
            int hostCount,
            int siteId,
            int initiatorId,
            int port,
            SnapshotSchedule schedule) {

        int myHostId = -1;

        // create a topology for the initiator
        // XXX-FAILURE this is a horrible way to figure out our host ID
        // XXX-FAILURE also, can iterate through all sites because we might
        // be the dead site.
        for (Site site : context.sites) {
            int aSiteId = Integer.parseInt(site.getTypeName());
            int hostId = Integer.parseInt(site.getHost().getTypeName());

            // if the current site is the local site, remember the host id
            if (aSiteId == siteId)
                myHostId = hostId;
        }

        // create a list of all partitions
        int[] allPartitions = new int[context.numberOfPartitions];
        int index = 0;
        for (Partition partition : CatalogUtil.getAllPartitions(context.cluster))
            allPartitions[index++] = Integer.parseInt(partition.getTypeName());
        assert(index == context.numberOfPartitions);

        // create the dtxn initiator
        /*
         * Construct the runnables so they have access to the list of connections
         */
        final OnDTXNBackPressure onBackPressure = new OnDTXNBackPressure() ;
        final OffDTXNBackPressure offBackPressure = new OffDTXNBackPressure();

        SimpleDtxnInitiator initiator =
            new SimpleDtxnInitiator(
                    context,
                    messenger, myHostId,
                    siteId, initiatorId,
                    onBackPressure, offBackPressure);

        // create the adhoc planner thread
        AsyncCompilerWorkThread plannerThread = new AsyncCompilerWorkThread(context, siteId);
        plannerThread.start();
        final ClientInterface ci = new ClientInterface(
                port, context, network, siteId, initiator,
                plannerThread, allPartitions, schedule);
        onBackPressure.m_ci = ci;
        offBackPressure.m_ci = ci;

        return ci;
    }

    ClientInterface(int port, CatalogContext context, VoltNetwork network, int siteId,
                    TransactionInitiator initiator, AsyncCompilerWorkThread plannerThread,
                    int[] allPartitions, SnapshotSchedule schedule)
    {
        m_catalogContext.set(context);
        m_initiator = initiator;
        m_asyncCompilerWorkThread = plannerThread;

        // pre-allocate single partition array
        m_allPartitions = allPartitions;

        m_siteId = siteId;
        m_dumpId = "Initiator." + String.valueOf(siteId);
        DumpManager.register(m_dumpId, this);

        m_acceptor = new ClientAcceptor(port, network);
//        m_snapshotDaemon = new SnapshotDaemon(schedule);
        // if this ClientInterface's site ID is the lowest non-execution site ID
        // in the cluster, make our SnapshotDaemon responsible for snapshots
        if (siteId ==
            m_catalogContext.get().siteTracker.getLowestLiveNonExecSiteId())
        {
//            m_snapshotDaemon.makeActive();
        }
    }

    /**
     * Set the flag that tells this client interface to update its
     * catalog when it's threadsafe.
     */
    public void notifyOfCatalogUpdate() {
        m_shouldUpdateCatalog.set(true);
        m_asyncCompilerWorkThread.notifyOfCatalogUpdate();
    }

    /**
     *
     * @param port
     * * return True if an error was generated and needs to be returned to the client
     */
    private final void handleRead(ByteBuffer buf, ClientInputHandler handler, Connection c) throws IOException {
        final long now = EstTime.currentTimeMillis();
        final FastDeserializer fds = new FastDeserializer(buf);

        // Deserialize the client's request and map to a catalog stored procedure
        final StoredProcedureInvocation task = fds.readObject(StoredProcedureInvocation.class);
        final Procedure catProc = m_catalogContext.get().procedures.get(task.procName);

        /*
         * @TODO This ladder stinks. An SPI when deserialized here should be
         * able to determine if the task is permitted by calling a method that
         * provides an AuthUser object.
         */
        if (task.procName.startsWith("@")) {

            // AdHoc requires unique permission. Then has to plan in a separate thread.
            if (task.procName.equals("@AdHoc")) {
                if (!handler.m_user.hasAdhocPermission()) {
                    final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(-1, task.clientHandle, -1,
                                               Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0], "User does not have @AdHoc permission");
                    authLog.l7dlog(Level.INFO,
                                   LogKeys.auth_ClientInterface_LackingPermissionForAdhoc.name(),
                                   new String[] {handler.m_user.m_name}, null);
                    c.writeStream().enqueue(errorResponse);
                    return;
                }
                task.buildParameterSet();
                if (task.params.toArray().length != 1) {
                    final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(-1, task.clientHandle, -1,
                                               Hstoreservice.Status.ABORT_UNEXPECTED,
                                               new VoltTable[0],
                                               "Adhoc system procedure requires exactly one parameter, the SQL statement to execute.");
                    c.writeStream().enqueue(errorResponse);
                    return;
                }
                String sql = (String) task.params.toArray()[0];
//                m_asyncCompilerWorkThread.planSQL(
//                                                  sql,
//                                                  task.clientHandle,
//                                                  handler.connectionId(),
//                                                  handler.m_hostname,
//                                                  handler.sequenceId(),
//                                                  c);
                return;
            }

            // All other sysprocs require the sysproc permission
            if (!handler.m_user.hasSystemProcPermission()) {
                authLog.l7dlog(Level.INFO,
                               LogKeys.auth_ClientInterface_LackingPermissionForSysproc.name(),
                               new String[] { handler.m_user.m_name, task.procName },
                               null);
                final ClientResponseImpl errorResponse =
                    new ClientResponseImpl(-1, task.clientHandle, -1,
                                           Hstoreservice.Status.ABORT_UNEXPECTED,
                                           new VoltTable[0],
                                           "User " + handler.m_user.m_name + " does not have sysproc permission");
                c.writeStream().enqueue(errorResponse);
                return;
            }

            // Updating a catalog needs to divert to the catalog processing thread
            if (task.procName.equals("@UpdateApplicationCatalog")) {
                task.buildParameterSet();
                // user only provides catalog URL.
                if (task.params.size() != 1) {
                    final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(-1,task.clientHandle, -1,
                                               Hstoreservice.Status.ABORT_UNEXPECTED,
                                               new VoltTable[0],
                                               "UpdateApplicationCatalog system procedure requires exactly " +
                                               "one parameter, the URL of the catalog to load");
                    c.writeStream().enqueue(errorResponse);
                    return;
                }
                String catalogURL = (String) task.params.toArray()[0];
                m_asyncCompilerWorkThread.prepareCatalogUpdate(catalogURL,
                                                               task.clientHandle,
                                                               handler.connectionId(),
                                                               handler.m_hostname,
                                                               handler.sequenceId(),
                                                               c);
                return;
            }
        } else if (!handler.m_user.hasPermission(catProc)) {
            authLog.l7dlog(Level.INFO,
                           LogKeys.auth_ClientInterface_LackingPermissionForProcedure.name(),
                           new String[] { handler.m_user.m_name, task.procName }, null);
            final ClientResponseImpl errorResponse =
                new ClientResponseImpl(-1, task.clientHandle, -1,
                                       Hstoreservice.Status.ABORT_UNEXPECTED,
                                       new VoltTable[0],
                                       "User does not have permission to invoke " + catProc.getTypeName());
            c.writeStream().enqueue(errorResponse);
            return;
        }

        if (catProc != null) {
            int[] involvedPartitions = null;
            if (catProc.getEverysite() == true) {
                involvedPartitions = m_allPartitions;
                task.buildParameterSet();
            }
            else if (catProc.getSinglepartition() == false) {
                involvedPartitions = m_allPartitions;
                task.buildParameterSet();
            }
            else {
                // break out the Hashinator and calculate the appropriate partition
                try {
                    involvedPartitions = new int[] { getPartitionForProcedure(catProc.getPartitionparameter(), task) };
                }
                catch (RuntimeException e) {
                    // unable to hash to a site, return an error
                    String errorMessage = "Error sending procedure "
                        + task.procName + " to the correct partition. Make sure parameter values are correct.";
                    authLog.l7dlog( Level.WARN,
                            LogKeys.host_ClientInterface_unableToRouteSinglePartitionInvocation.name(),
                            new Object[] { task.procName }, null);
                    final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(-1, task.clientHandle, -1,
                                             Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0], errorMessage);
                    c.writeStream().enqueue(errorResponse);
                }
            }

            if (involvedPartitions != null) {
                // initiate the transaction
                m_initiator.createTransaction(handler.connectionId(), handler.m_hostname, task,
                                              catProc.getReadonly(),
                                              catProc.getSinglepartition(),
                                              catProc.getEverysite(),
                                              involvedPartitions, involvedPartitions.length,
                                              c, buf.capacity(),
                                              now);
            }
        }
        else {
            // No such procedure: log and tell the client
            String errorMessage = "Procedure " + task.procName + " was not found";
            authLog.l7dlog( Level.WARN, LogKeys.auth_ClientInterface_ProcedureNotFound.name(), new Object[] { task.procName }, null);
            final ClientResponseImpl errorResponse =
                new ClientResponseImpl(-1, task.clientHandle, -1,
                        Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0], errorMessage);
            c.writeStream().enqueue(errorResponse);
        }
    }

    private final void checkForFinishedCompilerWork() {
        if (m_asyncCompilerWorkThread == null) return;

        AsyncCompilerResult result = null;

        while ((result = m_asyncCompilerWorkThread.getPlannedStmt()) != null) {
            if (result.errorMsg == null) {
                if (result instanceof AdHocPlannedStmt) {
                    AdHocPlannedStmt plannedStmt = (AdHocPlannedStmt) result;
                    // create the execution site task
                    StoredProcedureInvocation task = new StoredProcedureInvocation();
                    task.procName = "@AdHoc";
                    task.params = new ParameterSet(
                            plannedStmt.aggregatorFragment, plannedStmt.collectorFragment,
                            plannedStmt.sql, plannedStmt.isReplicatedTableDML ? 1 : 0
                    );
                    task.clientHandle = plannedStmt.clientHandle;

                    // initiate the transaction
                    m_initiator.createTransaction(plannedStmt.connectionId, plannedStmt.hostname,
                                                  task, false, false, false, m_allPartitions,
                                                  m_allPartitions.length, plannedStmt.clientData, 0, 0);
                }
                else if (result instanceof CatalogChangeResult) {
                    CatalogChangeResult changeResult = (CatalogChangeResult) result;
                    // create the execution site task
                    StoredProcedureInvocation task = new StoredProcedureInvocation();
                    task.procName = "@UpdateApplicationCatalog";
                    task.params = new ParameterSet(
                            changeResult.encodedDiffCommands, changeResult.catalogURL,
                            changeResult.expectedCatalogVersion
                    );
                    task.clientHandle = changeResult.clientHandle;

                    // initiate the transaction. These hard-coded values from catalog
                    // procedure are horrible, horrible, horrible.
                    m_initiator.createTransaction(changeResult.connectionId, changeResult.hostname,
                                                  task, false, true, true, m_allPartitions,
                                                  m_allPartitions.length, changeResult.clientData, 0, 0);
                }
                else {
                    throw new RuntimeException(
                            "Should not be able to get here (ClientInterface.checkForFinishedCompilerWork())");
                }
            }
            else {
                ClientResponseImpl errorResponse =
                    new ClientResponseImpl(-1, result.clientHandle, -1,
                            Hstoreservice.Status.ABORT_UNEXPECTED, new VoltTable[0],
                            result.errorMsg);
                final Connection c = (Connection) result.clientData;
                c.writeStream().enqueue(errorResponse);
            }
        }
    }

    /**
     * Tick counter used to perform dead client detection every N ticks
     */
    private long m_tickCounter = 0;

    public final void processPeriodicWork() {
        final long time = m_initiator.tick();
        m_tickCounter++;
        if (m_tickCounter % 20 == 0) {
            checkForDeadConnections(time);
        }
        //System.out.printf("Sending tick after %d ms pause.\n", delta);
        //System.out.flush();

        // this code ensures that things put in the out of process
        // planner make it out
        long delta = time - m_lastCompilerThreadPoke;
        if (delta > POKE_INTERVAL) {
            m_lastCompilerThreadPoke = time;
            m_asyncCompilerWorkThread.verifyEverthingIsKosher();
        }

        // check for catalog updates
        if (m_shouldUpdateCatalog.compareAndSet(true, false)) {
            m_catalogContext.set(VoltDB.instance().getCatalogContext());
            m_asyncCompilerWorkThread.notifyShouldUpdateCatalog();
            // When the catalog updates, check to see if we're now
            // responsible for snapshots since a node failure might have
            // caused this
            if (m_siteId ==
                m_catalogContext.get().siteTracker.getLowestLiveNonExecSiteId())
            {
//                m_snapshotDaemon.makeActive();
            }
        }

        // poll planner queue
        checkForFinishedCompilerWork();

        // snapshot work
//        initiateSnapshotDaemonWork(m_snapshotDaemon.processPeriodicWork(time));

        return;
    }

    /**
     * Check for dead connections by providing each connection with the current
     * time so it can calculate the delta between now and the time the oldest message was
     * queued for sending.
     * @param now Current time in milliseconds
     */
    private final void checkForDeadConnections(final long now) {
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
            networkLog.warn("Closing connection to " + c + " at " + now + " because it refuses to read responses");
            c.unregister();
        }
    }

    // BUG: this needs some more serious thinking
    // probably should be able to schedule a shutdown event
    // to the dispatcher..  Or write a "stop reading and flush
    // all your read buffers" events .. or something ..
    protected void shutdown() throws InterruptedException {
        if (m_acceptor != null) {
            m_acceptor.shutdown();
        }

        if (m_asyncCompilerWorkThread != null) {
            m_asyncCompilerWorkThread.shutdown();
            m_asyncCompilerWorkThread.join();
        }
    }

    public void startAcceptingConnections() throws IOException {
        m_acceptor.start();
    }

    /**
     * Identify the partition for an execution site task.
     * @return The partition best set up to execute the procedure.
     */
    int getPartitionForProcedure(int partitionIndex, StoredProcedureInvocation task) {
        return TheHashinator.hashToPartition(task.getParameterAtIndex(partitionIndex));
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

        if (m_initiator instanceof SimpleDtxnInitiator)
            ((SimpleDtxnInitiator) m_initiator).getDumpContents(context);

        return context;
    }

    private void initiateSnapshotDaemonWork(Pair<String, Object[]> invocation) {
        if (invocation != null) {
            // get procedure from the catalog
           final Procedure catProc = m_catalogContext.get().procedures.get(invocation.getFirst());
           if (catProc == null) {
               throw new RuntimeException("SnapshotDaemon attempted to invoke " + invocation.getFirst() +
                       " which is not a known procedure");
           }

           if (!catProc.getSystemproc()) {
               throw new RuntimeException("SnapshotDaemon attempted to invoke " + invocation.getFirst() +
                       " which is not a system procedure");
           }
           StoredProcedureInvocation spi = new StoredProcedureInvocation();
           spi.procName = invocation.getFirst();
           spi.params = new ParameterSet();
           spi.params.setParameters(invocation.getSecond());
            // initiate the transaction
           m_initiator.createTransaction(-1, "SnapshotDaemon", spi, catProc.getReadonly(),
                                         catProc.getSinglepartition(), catProc.getEverysite(),
                                         m_allPartitions, m_allPartitions.length, m_snapshotDaemonAdapter, 0, 0);
       }
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
