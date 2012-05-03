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

package org.voltdb.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltNetwork;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Pair;

import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 *   De/multiplexes transactions across a cluster
 *
 *   It is safe to synchronized on an individual connection and then the distributer, but it is always unsafe
 *   to synchronized on the distributer and then an individual connection.
 */
class Distributer {
    private static final Logger LOG = Logger.getLogger(Distributer.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // collection of connections to the cluster
    private final ArrayList<NodeConnection> m_connections = new ArrayList<NodeConnection>();
    
    /** SiteId -> NodeConnection */
    private final Map<Integer, Collection<NodeConnection>> m_connectionSiteXref = new HashMap<Integer, Collection<NodeConnection>>();

    private final ArrayList<ClientStatusListener> m_listeners = new ArrayList<ClientStatusListener>();

    //Selector and connection handling, does all work in blocking selection thread
    private final VoltNetwork m_network;

    // Temporary until a distribution/affinity algorithm is written
    private int m_nextConnection = 0;

    private final int m_expectedOutgoingMessageSize;

    private final DBBPool m_pool;

    private final boolean m_useMultipleThreads;
    
    private final int m_backpressureWait;

    private final String m_hostname;

    /**
     * Server's instances id. Unique for the cluster
     */
    private Object m_clusterInstanceId[];

    private final ClientStatsLoader m_statsLoader;
    private String m_buildString;

    private static class ProcedureStats {
        private final String m_name;

        private long m_invocationsCompleted = 0;
        private long m_lastInvocationsCompleted = 0;
        private long m_invocationAborts = 0;
        private long m_lastInvocationAborts = 0;
        private long m_invocationErrors = 0;
        private long m_lastInvocationErrors = 0;
        private long m_restartCounter = 0;

        // cumulative latency measured by client, used to calculate avg. lat.
        private long m_roundTripTime = 0;
        private long m_lastRoundTripTime = 0;

        private int m_maxRoundTripTime = Integer.MIN_VALUE;
        private int m_lastMaxRoundTripTime = Integer.MIN_VALUE;
        private int m_minRoundTripTime = Integer.MAX_VALUE;
        private int m_lastMinRoundTripTime = Integer.MAX_VALUE;

        // cumulative latency measured by the cluster, used to calculate avg lat.
        private long m_clusterRoundTripTime = 0;
        private long m_lastClusterRoundTripTime = 0;

        // 10ms buckets. Last bucket is all transactions > 190ms.
        static int m_numberOfBuckets = 20;
        private long m_clusterRoundTripTimeBuckets[] = new long[m_numberOfBuckets];
        private long m_roundTripTimeBuckets[] = new long[m_numberOfBuckets];

        private int m_maxClusterRoundTripTime = Integer.MIN_VALUE;
        private int m_lastMaxClusterRoundTripTime = Integer.MIN_VALUE;
        private int m_minClusterRoundTripTime = Integer.MAX_VALUE;
        private int m_lastMinClusterRoundTripTime = Integer.MAX_VALUE;

        public ProcedureStats(String name) {
            m_name = name;
        }

        public void update(int roundTripTime, int clusterRoundTripTime, boolean abort, boolean error, int restartCounter) {
            m_maxRoundTripTime = Math.max(roundTripTime, m_maxRoundTripTime);
            m_lastMaxRoundTripTime = Math.max( roundTripTime, m_lastMaxRoundTripTime);
            m_minRoundTripTime = Math.min( roundTripTime, m_minRoundTripTime);
            m_lastMinRoundTripTime = Math.max( roundTripTime, m_lastMinRoundTripTime);

            m_maxClusterRoundTripTime = Math.max( clusterRoundTripTime, m_maxClusterRoundTripTime);
            m_lastMaxClusterRoundTripTime = Math.max( clusterRoundTripTime, m_lastMaxClusterRoundTripTime);
            m_minClusterRoundTripTime = Math.min( clusterRoundTripTime, m_minClusterRoundTripTime);
            m_lastMinClusterRoundTripTime = Math.min( clusterRoundTripTime, m_lastMinClusterRoundTripTime);

            m_invocationsCompleted++;
            if (abort) {
                m_invocationAborts++;
            }
            if (error) {
                m_invocationErrors++;
            }
            m_roundTripTime += roundTripTime;
            m_clusterRoundTripTime += clusterRoundTripTime;
            m_restartCounter += restartCounter;

            // calculate the latency buckets to increment and increment.
            int rttBucket = (int)(Math.floor(roundTripTime / 10));
            if (rttBucket >= m_roundTripTimeBuckets.length) {
                rttBucket = m_roundTripTimeBuckets.length - 1;
            }
            m_roundTripTimeBuckets[rttBucket] += 1;

            int rttClusterBucket = (int)(Math.floor(clusterRoundTripTime / 10));
            if (rttClusterBucket >= m_clusterRoundTripTimeBuckets.length) {
                rttClusterBucket = m_clusterRoundTripTimeBuckets.length - 1;
            }
            m_clusterRoundTripTimeBuckets[rttClusterBucket] += 1;

        }
    }
    
    class CallbackValues {
        final long time;
        final ProcedureCallback callback;
        final String name;
        
        public CallbackValues(long time, ProcedureCallback callback, String name) {
            this.time = time;
            this.callback = callback;
            this.name = name;
        }
    }

    class NodeConnection extends VoltProtocolHandler implements org.voltdb.network.QueueMonitor {
        private final HashMap<Long, CallbackValues> m_callbacks;
        private final HashMap<String, ProcedureStats> m_stats
            = new HashMap<String, ProcedureStats>();
        private final int m_hostId;
        private final long m_connectionId;
        private Connection m_connection;
        private String m_hostname;
        private int m_port;
        private boolean m_isConnected = true;
        private final AtomicBoolean m_hasBackPressure = new AtomicBoolean(false);
        private long m_hasBackPressureTimestamp = -1;
        private int m_lastServerTimestamp = Integer.MIN_VALUE;

        private long m_invocationsThrottled = 0;
        private long m_lastInvocationsThrottled = 0;
        private long m_invocationsCompleted = 0;
        private long m_lastInvocationsCompleted = 0;
        private long m_invocationAborts = 0;
        private long m_lastInvocationAborts = 0;
        private long m_invocationErrors = 0;
        private long m_lastInvocationErrors = 0;

        public NodeConnection(long ids[]) {
            m_callbacks = new HashMap<Long, CallbackValues>();
            m_hostId = (int)ids[0];
            m_connectionId = ids[1];
        }
        
        @Override
        public String toString() {
            return (String.format("NodeConnection[id=%d, host=%s, port=%d]", m_hostId, m_hostname, m_port));
        }

        public void createWork(long now, long handle, String name, BBContainer c, ProcedureCallback callback) {
            synchronized (this) {
                if (!m_isConnected) {
                    final ClientResponse r = new ClientResponseImpl(-1, -1, -1, Hstoreservice.Status.ABORT_CONNECTION_LOST,
                            new VoltTable[0], "Connection to database host (" + m_hostname +
                            ") was lost before a response was received");
                    callback.clientCallback(r);
                    c.discard();
                    return;
                }
                m_callbacks.put(handle, new CallbackValues(now, callback, name));
            }
            m_connection.writeStream().enqueue(c);
        }

        public void createWork(long now, long handle, String name, FastSerializable f, ProcedureCallback callback) {
            synchronized (this) {
                if (!m_isConnected) {
                    final ClientResponse r = new ClientResponseImpl(-1, -1, -1, Hstoreservice.Status.ABORT_CONNECTION_LOST,
                            new VoltTable[0], "Connection to database host (" + m_hostname +
                            ") was lost before a response was received");
                    callback.clientCallback(r);
                    return;
                }
                m_callbacks.put(handle, new CallbackValues(now, callback, name));
            }
            m_connection.writeStream().enqueue(f);
        }

        private void updateStats(
                String name,
                int roundTrip,
                int clusterRoundTrip,
                boolean abort,
                boolean error,
                int restartCounter) {
            ProcedureStats stats = m_stats.get(name);
            if (stats == null) {
                stats = new ProcedureStats(name);
                m_stats.put( name, stats);
            }
            stats.update(roundTrip, clusterRoundTrip, abort, error, restartCounter);
        }

        @Override
        public void handleMessage(ByteBuffer buf, Connection c) {
            ClientResponseImpl response = null;
            FastDeserializer fds = new FastDeserializer(buf);
            try {
                response = fds.readObject(ClientResponseImpl.class);
            } catch (IOException e) {
                LOG.error("Invalid ClientResponse object returned by " + this, e);
            }
            ProcedureCallback cb = null;
            long callTime = 0;
            int delta = 0;
            
            if (response == null) {
                LOG.warn("Got back null ClientResponse. Ignoring...");
                return;
            }
            
            final long clientHandle = response.getClientHandle();
            final boolean should_throttle = response.getThrottleFlag();
            final Hstoreservice.Status status = response.getStatus();
            final int timestamp = response.getRequestCounter();
            final int restart_counter = response.getRestartCounter();
            
            boolean abort = false;
            boolean error = false;
            
            CallbackValues stuff = null;
            long now = System.currentTimeMillis();
            synchronized (this) {
                stuff = m_callbacks.remove(clientHandle);
            
                if (stuff != null) {
                    callTime = stuff.time;
                    delta = (int)(now - callTime);
                    cb = stuff.callback;
                    m_invocationsCompleted++;
                    
                    if (debug.get()) {
                        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
                        m0.put("Txn #", response.getTransactionId());
                        m0.put("Status", response.getStatus());
                        m0.put("ClientHandle", clientHandle);
                        m0.put("ThrottleFlag", should_throttle);
                        m0.put("Timestamp", timestamp);
                        m0.put("RestartCounter", restart_counter);
                        
                        Map<String, Object> m1 = new ListOrderedMap<String, Object>();
                        m1.put("Connection", this);
                        m1.put("Back Pressure", m_hasBackPressure.get());
                        m1.put("BackPressure Timestamp", m_hasBackPressureTimestamp);
                        m1.put("Last Server Timestamp", m_lastServerTimestamp);
                        m1.put("Completed Invocations", m_invocationsCompleted);
                        m1.put("Error Invocations", m_invocationErrors);
                        m1.put("Abort Invocations", m_invocationAborts);
                        m1.put("Throttled Invocations", m_invocationsThrottled);
                        LOG.debug("ClientResponse Information:\n" + StringUtil.formatMaps(m0, m1));
                    }
                    
                    // BackPressure (Throttle)
                    // If this response's timestamp is greater than the last one that we processed, then we'll allow it to modify whether
                    // we are throttled or not. This ensures that we don't get stuck because the messages came back out of order
                    if (timestamp > m_lastServerTimestamp) {
                        m_lastServerTimestamp = timestamp;
                        
                        if (should_throttle == false && m_hasBackPressure.compareAndSet(true, should_throttle)) {
                            if (debug.get()) LOG.debug(String.format("Disabling throttling mode [counter=%d]", m_invocationsThrottled));
                            m_connection.writeStream().setBackPressure(false);
                            
                        } else if (should_throttle == true && m_hasBackPressure.compareAndSet(false, true)) {
                            m_invocationsThrottled++;
                            if (debug.get()) LOG.debug(String.format("Enabling throttling mode [counter=%d]", m_invocationsThrottled));
                            m_connection.writeStream().setBackPressure(true);
                            m_hasBackPressureTimestamp = now;
                        }
                    }
                } else {
                    LOG.warn("Failed to get callback for client handle #" + clientHandle + " from " + this);
                }
            } // SYNCH

            if (stuff != null) {
                if (status == Hstoreservice.Status.ABORT_USER || status == Hstoreservice.Status.ABORT_GRACEFUL) {
                    m_invocationAborts++;
                    abort = true;
                } else if (status != Hstoreservice.Status.OK) {
                    m_invocationErrors++;
                    error = true;
                }
                updateStats(stuff.name, delta, response.getClusterRoundtrip(), abort, error, restart_counter);
            }

            if (cb != null) {
                // We always need to call this so that we unblock the blocking client
                // if (status != Hstoreservice.Status.ABORT_THROTTLED && status != Hstoreservice.Status.ABORT_REJECT) {
                    response.setClientRoundtrip(delta);
                    cb.clientCallback(response);
                //}
            } else if (m_isConnected) {
                // TODO: what's the right error path here?
//                LOG.warn("No callback available for clientHandle " + clientHandle);
//                assert(false);
            }
        }

        /**
         * A number specify the expected size of the majority of outgoing messages.
         * Used to determine the tipping point between where a heap byte buffer vs. direct byte buffer will be
         * used. Also effects the usage of gathering writes.
         */
        @Override
        public int getExpectedOutgoingMessageSize() {
            return m_expectedOutgoingMessageSize;
        }

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        public boolean hadBackPressure(long now) {
            if (trace.get()) 
                LOG.trace(String.format("Checking whether %s has backup pressure: %s",
                                        m_connection, m_hasBackPressure));
            if (m_hasBackPressure.get()) {
                if (now - m_hasBackPressureTimestamp > m_backpressureWait) {
                    if (trace.get()) 
                        LOG.trace(String.format("Disabling backpresure at %s because client has waited for %d ms",
                                                this, (now - m_hasBackPressureTimestamp)));
//                    assert(m_hasBackPressureTimestamp >= 0);
                    m_hasBackPressure.set(false);
                    m_hasBackPressureTimestamp = -1;
                    return (false);
                }
                return (true);
            }
            return (false); 
        }

        @Override
        public void stopping(Connection c) {
            super.stopping(c);
            synchronized (this) {
                //Prevent queueing of new work to this connection
                synchronized (Distributer.this) {
                    m_connections.remove(this);
                    //Notify listeners that a connection has been lost
                    for (ClientStatusListener s : m_listeners) {
                        s.connectionLost(m_hostname, m_connections.size());
                    }
                }
                m_isConnected = false;

                //Invoke callbacks for all queued invocations with a failure response
                final ClientResponse r =
                    new ClientResponseImpl(-1, -1, -1, Hstoreservice.Status.ABORT_CONNECTION_LOST,
                        new VoltTable[0], "Connection to database host (" + m_hostname +
                        ") was lost before a response was received");
                for (final CallbackValues cbv : m_callbacks.values()) {
                    cbv.callback.clientCallback(r);
                }
            }
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    /*
                     * Synchronization on Distributer.this is critical to ensure that queue
                     * does not report backpressure AFTER the write stream reports that backpressure
                     * has ended thus resulting in a lost wakeup.
                     */
                    synchronized (Distributer.this) {
                        for (final ClientStatusListener csl : m_listeners) {
                            csl.backpressure(false);
                        }
                    }
                }
            };
        }

        @Override
        public Runnable onBackPressure() {
            return null;
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return this;
        }

        /**
         * Get counters for invocations completed, aborted, errors. In that order.
         */
        public synchronized long[] getCounters() {
            return new long[] { m_invocationsCompleted, m_invocationAborts, m_invocationErrors };
        }

        /**
         * Get counters for invocations completed, aborted, errors. In that order.
         * Count returns count since this method was last invoked
         */
        public synchronized long[] getCountersInterval() {
            final long invocationsCompletedThisTime = m_invocationsCompleted - m_lastInvocationsCompleted;
            m_lastInvocationsCompleted = m_invocationsCompleted;

            final long invocationsAbortsThisTime = m_invocationAborts - m_lastInvocationAborts;
            m_lastInvocationAborts = m_invocationAborts;

            final long invocationErrorsThisTime = m_invocationErrors - m_lastInvocationErrors;
            m_lastInvocationErrors = m_invocationErrors;

            final long invocationsThrottledThisTime = m_invocationsThrottled - m_lastInvocationsThrottled;
            m_lastInvocationsThrottled = m_invocationsThrottled;
            
            return new long[] {
                    invocationsCompletedThisTime,
                    invocationsAbortsThisTime,
                    invocationErrorsThisTime,
                    invocationsThrottledThisTime
            };
        }

        private int m_queuedBytes = 0;
        private final int m_maxQueuedBytes = 2097152;

        @Override
        public boolean queue(int bytes) {
            m_queuedBytes += bytes;
            if (m_queuedBytes > m_maxQueuedBytes) {
                return true;
            }
            return false;
        }
    }

    void drain() throws NoConnectionsException {
        boolean more;
        do {
            more = false;
            synchronized (this) {
                for (NodeConnection cxn : m_connections) {
                    synchronized(cxn.m_callbacks) {
                        more = more || cxn.m_callbacks.size() > 0;
                    }
                }
            }
            Thread.yield();
        } while(more);

        synchronized (this) {
            for (NodeConnection cxn : m_connections ) {
                assert(cxn.m_callbacks.size() == 0);
            }
        }
    }

    Distributer() {
        this( 128, null, false, null);
    }
    
    Distributer(
            int expectedOutgoingMessageSize,
            int arenaSizes[],
            boolean useMultipleThreads,
            StatsUploaderSettings statsSettings) {
        this(expectedOutgoingMessageSize, arenaSizes, useMultipleThreads, statsSettings, 100);
    }

    Distributer(
            int expectedOutgoingMessageSize,
            int arenaSizes[],
            boolean useMultipleThreads,
            StatsUploaderSettings statsSettings,
            int backpressureWait) {
        if (statsSettings != null) {
            m_statsLoader = new ClientStatsLoader(statsSettings, this);
        } else {
            m_statsLoader = null;
        }
        m_useMultipleThreads = useMultipleThreads;
        m_backpressureWait = backpressureWait;
        m_network = new VoltNetwork( useMultipleThreads, true, 3);
        m_expectedOutgoingMessageSize = expectedOutgoingMessageSize;
        m_network.start();
        m_pool = new DBBPool(false, arenaSizes, false);
        String hostname = "";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hostname = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
        }
        m_hostname = hostname;
        
        if (debug.get())
            LOG.debug(String.format("Created new Distributer for %s [multiThread=%s, backpressureWait=%d]",
                                    m_hostname, m_useMultipleThreads, m_backpressureWait));

//        new Thread() {
//            @Override
//            public void run() {
//                long lastBytesRead = 0;
//                long lastBytesWritten = 0;
//                long lastRuntime = System.currentTimeMillis();
//                try {
//                    while (true) {
//                        Thread.sleep(10000);
//                        final long now = System.currentTimeMillis();
//                        org.voltdb.utils.Pair<Long, Long> counters = m_network.getCounters();
//                        final long read = counters.getFirst();
//                        final long written = counters.getSecond();
//                        final long readDelta = read - lastBytesRead;
//                        final long writeDelta = written - lastBytesWritten;
//                        final long timeDelta = now - lastRuntime;
//                        lastRuntime = now;
//                        final double seconds = timeDelta / 1000.0;
//                        final double megabytesRead = readDelta / (double)(1024 * 1024);
//                        final double megabytesWritten = writeDelta / (double)(1024 * 1024);
//                        final double readRate = megabytesRead / seconds;
//                        final double writeRate = megabytesWritten / seconds;
//                        lastBytesRead = read;
//                        lastBytesWritten = written;
//                        System.err.printf("Read rate %.2f Write rate %.2f\n", readRate, writeRate);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }.start();
    }

//    void createConnection(String host, String program, String password) throws UnknownHostException, IOException {
//        LOG.info(String.format("Creating a new connection [host=%s, program=%s]", host, program));
//        
//        // HACK: If they stick the port # at the end of the host name, we'll extract
//        // it out because we're generally nice people
//        int port = Client.VOLTDB_SERVER_PORT;
//        if (host.contains(":")) {
//            String split[] = host.split(":");
//            host = split[0];
//            port = Integer.valueOf(split[1]);
//        }
//        createConnection(host, program, password, port);
//    }

    public synchronized void createConnection(Integer site_id, String host, int port, String program, String password) throws UnknownHostException, IOException {
        if (debug.get()) {
            LOG.debug(String.format("Creating new connection [site=%s, host=%s, port=%d]",
                                    HStoreThreadManager.formatSiteName(site_id), host, port));
            LOG.debug("Trying for an authenticated connection...");
        }
        Object connectionStuff[] = null;
        try {
            connectionStuff =
            ConnectionUtil.getAuthenticatedConnection(host, program, password, port);
        } catch (Exception ex) {
            LOG.error("Failed to get connection to " + host + ":" + port, (debug.get() ? ex : null));
            throw new IOException(ex);
        }
        if (debug.get()) 
            LOG.debug("We now have an authenticated connection. Let's grab the socket...");
        final SocketChannel aChannel = (SocketChannel)connectionStuff[0];
        final long numbers[] = (long[])connectionStuff[1];
        if (m_clusterInstanceId == null) {
            long timestamp = numbers[2];
            int addr = (int)numbers[3];
            m_clusterInstanceId = new Object[] { timestamp, addr };
            if (m_statsLoader != null) {
                if (debug.get()) LOG.debug("statsLoader = " + m_statsLoader);
                try {
                    m_statsLoader.start( timestamp, addr);
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        } else {
//            if (!(((Long)m_clusterInstanceId[0]).longValue() == numbers[2]) ||
//                !(((Integer)m_clusterInstanceId[1]).longValue() == numbers[3])) {
//                aChannel.close();
//                throw new IOException(
//                        "Cluster instance id mismatch. Current is " + m_clusterInstanceId[0] + "," + m_clusterInstanceId[1]
//                        + " and server's was " + numbers[2] + "," + numbers[3]);
//            }
        }
        m_buildString = (String)connectionStuff[2];
        NodeConnection cxn = new NodeConnection(numbers);
        m_connections.add(cxn);
        if (site_id != null) {
            if (debug.get())
                LOG.debug(String.format("Created connection for Site %s: %s", HStoreThreadManager.formatSiteName(site_id), cxn));
            synchronized (m_connectionSiteXref) {
                Collection<NodeConnection> nc = m_connectionSiteXref.get(site_id);
                if (nc == null) {
                    nc = new HashSet<NodeConnection>();
                    m_connectionSiteXref.put(site_id, nc);
                }
                nc.add(cxn);    
            } // SYNCH
        }
        
        Connection c = m_network.registerChannel(aChannel, cxn);
        cxn.m_hostname = c.getHostname();
        cxn.m_port = port;
        cxn.m_connection = c;
        if (debug.get()) 
            LOG.debug("From what I can tell, we have a connection: " + cxn);
    }

//    private HashMap<String, Long> reportedSizes = new HashMap<String, Long>();

    /**
     * Queue invocation on first node connection without backpressure. If there is none with without backpressure
     * then return false and don't queue the invocation
     * @param invocation
     * @param cb
     * @param expectedSerializedSize
     * @param ignoreBackPressure If true the invocation will be queued even if there is backpressure
     * @return True if the message was queued and false if the message was not queued due to backpressure
     * @throws NoConnectionsException
     */
    boolean queue(
            StoredProcedureInvocation invocation,
            ProcedureCallback cb,
            int expectedSerializedSize,
            final boolean ignoreBackpressure)
        throws NoConnectionsException {
        return this.queue(invocation, cb, expectedSerializedSize, ignoreBackpressure, null);
    }
    
    boolean queue(
            StoredProcedureInvocation invocation,
            ProcedureCallback cb,
            int expectedSerializedSize,
            final boolean ignoreBackpressure,
            final Integer site_id)
        throws NoConnectionsException {
        NodeConnection cxn = null;
        boolean backpressure = true;
        int queuedInvocations = 0;
        long now = System.currentTimeMillis();
        
        final int totalConnections = m_connections.size();

        if (totalConnections == 0) {
            throw new NoConnectionsException("No connections.");
        }
        if (site_id != null && m_connectionSiteXref.containsKey(site_id)) {
             cxn = CollectionUtil.random(m_connectionSiteXref.get(site_id));
//            cxn = CollectionUtil.first(m_connectionSiteXref.get(site_id));
            if (cxn == null) {
                LOG.warn("No direct connection to " + HStoreThreadManager.formatSiteName(site_id));
            } else backpressure = false; // XXX
//            else if (!cxn.hadBackPressure(now) || ignoreBackpressure) {
//                backpressure = false;
//            }
//            else {
//                cxn = null;
//            }
        }
        
        /*
         * Synchronization is necessary to ensure that m_connections is not modified
         * as well as to ensure that backpressure is reported correctly
         */
        if (cxn == null) {
            synchronized (this) {
                for (int i=0; i < totalConnections; ++i) {
                    int idx = Math.abs(++m_nextConnection % totalConnections);
                    cxn = m_connections.get(idx);
                    if (trace.get())
                        LOG.trace("m_nextConnection = " + idx + " / " + totalConnections + " [" + cxn + "]");
                    queuedInvocations += cxn.m_callbacks.size();
                    if (cxn.hadBackPressure(now) == false || ignoreBackpressure) {
                        // serialize and queue the invocation
                        backpressure = false;
                        break;
                    }
                } // FOR
            } // SYNCH
        } 
        if (backpressure) {
            cxn = null;
            for (ClientStatusListener s : m_listeners) {
                s.backpressure(true);
            }
        }
        
        if (debug.get()) 
            LOG.debug(String.format("Queuing new %s Request [clientHandle=%d, siteId=%s]",
                                    invocation.getProcName(), invocation.getClientHandle(), site_id));

        /*
         * Do the heavy weight serialization outside the synchronized block.
         * createWork synchronizes on an individual connection which allows for more concurrency
         */
        if (cxn != null) {
            if (m_useMultipleThreads) {
                cxn.createWork(now, invocation.getClientHandle(), invocation.getProcName(), invocation, cb);
            } else {
                final FastSerializer fs = new FastSerializer(m_pool, expectedSerializedSize);
                BBContainer c = null;
                try {
                    c = fs.writeObjectForMessaging(invocation);
                } catch (IOException e) {
                    fs.getBBContainer().discard();
                    throw new RuntimeException(e);
                }
                cxn.createWork(now, invocation.getClientHandle(), invocation.getProcName(), c, cb);
            }
//            final String invocationName = invocation.getProcName();
//            if (reportedSizes.containsKey(invocationName)) {
//                if (reportedSizes.get(invocationName) < c.b.remaining()) {
//                    System.err.println("Queued invocation for " + invocationName + " is " + c.b.remaining() + " which is greater then last value of " + reportedSizes.get(invocationName));
//                    reportedSizes.put(invocationName, (long)c.b.remaining());
//                }
//            } else {
//                reportedSizes.put(invocationName, (long)c.b.remaining());
//                System.err.println("Queued invocation for " + invocationName + " is " + c.b.remaining());
//            }


        }

        return !backpressure;
    }

    /**
     * Shutdown the VoltNetwork allowing the Ports to close and free resources
     * like memory pools
     * @throws InterruptedException
     */
    final void shutdown() throws InterruptedException {
        if (m_statsLoader != null) {
            m_statsLoader.stop();
        }
        m_network.shutdown();
        synchronized (this) {
            m_pool.clear();
        }
    }

    synchronized void addClientStatusListener(ClientStatusListener listener) {
        if (!m_listeners.contains(listener)) {
            m_listeners.add(listener);
        }
    }

    synchronized boolean removeClientStatusListener(ClientStatusListener listener) {
        return m_listeners.remove(listener);
    }

    private final ColumnInfo connectionStatsColumns[] = new ColumnInfo[] {
            new ColumnInfo( "TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo( "HOSTNAME", VoltType.STRING),
            new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOST_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOSTNAME", VoltType.STRING),
            new ColumnInfo( "SERVER_CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_COMPLETED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_ABORTED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_FAILED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_THROTTLED", VoltType.BIGINT),
            new ColumnInfo( "BYTES_READ", VoltType.BIGINT),
            new ColumnInfo( "MESSAGES_READ", VoltType.BIGINT),
            new ColumnInfo( "BYTES_WRITTEN", VoltType.BIGINT),
            new ColumnInfo( "MESSAGES_WRITTEN", VoltType.BIGINT)
    };

    private final ColumnInfo procedureStatsColumns[] = new ColumnInfo[] {
            new ColumnInfo( "TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo( "HOSTNAME", VoltType.STRING),
            new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOST_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOSTNAME", VoltType.STRING),
            new ColumnInfo( "SERVER_CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "PROCEDURE_NAME", VoltType.STRING),
            new ColumnInfo( "ROUNDTRIPTIME_AVG", VoltType.INTEGER),
            new ColumnInfo( "ROUNDTRIPTIME_MIN", VoltType.INTEGER),
            new ColumnInfo( "ROUNDTRIPTIME_MAX", VoltType.INTEGER),
            new ColumnInfo( "CLUSTER_ROUNDTRIPTIME_AVG", VoltType.INTEGER),
            new ColumnInfo( "CLUSTER_ROUNDTRIPTIME_MIN", VoltType.INTEGER),
            new ColumnInfo( "CLUSTER_ROUNDTRIPTIME_MAX", VoltType.INTEGER),
            new ColumnInfo( "INVOCATIONS_COMPLETED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_ABORTED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_FAILED", VoltType.BIGINT),
            new ColumnInfo( "TIMES_RESTARTED", VoltType.BIGINT)
    };

    VoltTable getProcedureStats(final boolean interval) {
        final Long now = System.currentTimeMillis();
        final VoltTable retval = new VoltTable(procedureStatsColumns);

        long totalInvocations = 0;
        long totalAbortedInvocations = 0;
        long totalFailedInvocations = 0;
        long totalRoundTripTime = 0;
        int totalRoundTripMax = Integer.MIN_VALUE;
        int totalRoundTripMin = Integer.MAX_VALUE;
        long totalClusterRoundTripTime = 0;
        int totalClusterRoundTripMax = Integer.MIN_VALUE;
        int totalClusterRoundTripMin = Integer.MAX_VALUE;
        long totalRestarts = 0;
        synchronized (m_connections) {
            for (NodeConnection cxn : m_connections) {
                synchronized (cxn) {
                    for (ProcedureStats stats : cxn.m_stats.values()) {
                        long invocationsCompleted = stats.m_invocationsCompleted;
                        long invocationAborts = stats.m_invocationAborts;
                        long invocationErrors = stats.m_invocationErrors;
                        long roundTripTime = stats.m_roundTripTime;
                        int maxRoundTripTime = stats.m_maxRoundTripTime;
                        int minRoundTripTime = stats.m_minRoundTripTime;
                        long clusterRoundTripTime = stats.m_clusterRoundTripTime;
                        int clusterMinRoundTripTime = stats.m_minClusterRoundTripTime;
                        int clusterMaxRoundTripTime = stats.m_maxClusterRoundTripTime;
                        long restartCounter = stats.m_restartCounter;

                        if (interval) {
                            invocationsCompleted = stats.m_invocationsCompleted - stats.m_lastInvocationsCompleted;
                            if (invocationsCompleted == 0) {
                                //No invocations since last interval
                                continue;
                            }
                            stats.m_lastInvocationsCompleted = stats.m_invocationsCompleted;

                            invocationAborts = stats.m_invocationAborts - stats.m_lastInvocationAborts;
                            stats.m_lastInvocationAborts = stats.m_invocationAborts;

                            invocationErrors = stats.m_invocationErrors - stats.m_lastInvocationErrors;
                            stats.m_lastInvocationErrors = stats.m_invocationErrors;

                            roundTripTime = stats.m_roundTripTime - stats.m_lastRoundTripTime;
                            stats.m_lastRoundTripTime = stats.m_roundTripTime;

                            maxRoundTripTime = stats.m_lastMaxRoundTripTime;
                            minRoundTripTime = stats.m_lastMinRoundTripTime;

                            stats.m_lastMaxRoundTripTime = Integer.MIN_VALUE;
                            stats.m_lastMinRoundTripTime = Integer.MAX_VALUE;

                            clusterRoundTripTime = stats.m_clusterRoundTripTime - stats.m_lastClusterRoundTripTime;
                            stats.m_lastClusterRoundTripTime = stats.m_clusterRoundTripTime;

                            clusterMaxRoundTripTime = stats.m_lastMaxClusterRoundTripTime;
                            clusterMinRoundTripTime = stats.m_lastMinClusterRoundTripTime;

                            stats.m_lastMaxClusterRoundTripTime = Integer.MIN_VALUE;
                            stats.m_lastMinClusterRoundTripTime = Integer.MAX_VALUE;
                        }
                        totalInvocations += invocationsCompleted;
                        totalAbortedInvocations += invocationAborts;
                        totalFailedInvocations += invocationErrors;
                        totalRoundTripTime += roundTripTime;
                        totalRoundTripMax = Math.max(maxRoundTripTime, totalRoundTripMax);
                        totalRoundTripMin = Math.min(minRoundTripTime, totalRoundTripMin);
                        totalClusterRoundTripTime += clusterRoundTripTime;
                        totalClusterRoundTripMax = Math.max(clusterMaxRoundTripTime, totalClusterRoundTripMax);
                        totalClusterRoundTripMin = Math.min(clusterMinRoundTripTime, totalClusterRoundTripMin);
                        totalRestarts += restartCounter;
                        retval.addRow(
                                now,
                                m_hostname,
                                cxn.connectionId(),
                                cxn.m_hostId,
                                cxn.m_hostname,
                                cxn.m_connectionId,
                                stats.m_name,
                                (int)(roundTripTime / invocationsCompleted),
                                minRoundTripTime,
                                maxRoundTripTime,
                                (int)(clusterRoundTripTime / invocationsCompleted),
                                clusterMinRoundTripTime,
                                clusterMaxRoundTripTime,
                                invocationsCompleted,
                                invocationAborts,
                                invocationErrors,
                                restartCounter
                                );
                    }
                }
            }
        }
        return retval;
    }

    VoltTable getConnectionStats(final boolean interval) {
        final Long now = System.currentTimeMillis();
        final VoltTable retval = new VoltTable(connectionStatsColumns);
        final Map<Long, Pair<String,long[]>> networkStats =
                        m_network.getIOStats(interval);
        long totalInvocations = 0;
        long totalAbortedInvocations = 0;
        long totalFailedInvocations = 0;
        long totalThrottledInvocations = 0;
        synchronized (m_connections) {
            for (NodeConnection cxn : m_connections) {
                synchronized (cxn) {
                    long counters[];
                    if (interval) {
                        counters = cxn.getCountersInterval();
                    } else {
                        counters = cxn.getCounters();
                    }
                    totalInvocations += counters[0];
                    totalAbortedInvocations += counters[1];
                    totalFailedInvocations += counters[2];
                    totalThrottledInvocations += counters[3];
                    
                    final long networkCounters[] = networkStats.get(cxn.connectionId()).getSecond();
                    final String hostname = networkStats.get(cxn.connectionId()).getFirst();
                    long bytesRead = 0;
                    long messagesRead = 0;
                    long bytesWritten = 0;
                    long messagesWritten = 0;
                    if (networkCounters != null) {
                        bytesRead = networkCounters[0];
                        messagesRead = networkCounters[1];
                        bytesWritten = networkCounters[2];
                        messagesWritten = networkCounters[3];
                    }

                    retval.addRow(
                            now,
                            m_hostname,
                            cxn.connectionId(),
                            cxn.m_hostId,
                            hostname,
                            cxn.m_connectionId,
                            counters[0],
                            counters[1],
                            counters[2],
                            counters[3],
                            bytesRead,
                            messagesRead,
                            bytesWritten,
                            messagesWritten);
                }
            }
        }

        final long globalIOStats[] = networkStats.get(-1L).getSecond();
        retval.addRow(
                now,
                m_hostname,
                -1,
                -1,
                "GLOBAL",
                -1,
                totalInvocations,
                totalAbortedInvocations,
                totalFailedInvocations,
                totalThrottledInvocations,
                globalIOStats[0],
                globalIOStats[1],
                globalIOStats[2],
                globalIOStats[3]);
        return retval;
    }

    public Object[] getInstanceId() {
        return m_clusterInstanceId;
    }

    public String getBuildString() {
        return m_buildString;
    }
    public int getConnectionCount() {
        return m_connections.size();
    }
    
}
