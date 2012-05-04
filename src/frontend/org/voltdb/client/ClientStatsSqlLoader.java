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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import edu.brown.catalog.CatalogUtil;

/**
 * Polls a Distributer instance for IO and procedure invocation information and ELTs the results
 * to a database via JDBC.
 *
 */
public class ClientStatsSqlLoader implements ClientStatsLoader {
    private static final Logger LOG = Logger.getLogger(ClientStatsSqlLoader.class);
    
    private final StatsUploaderSettings m_settings;
    private final Connection m_conn;
    private final String m_applicationName;
    private final String m_subApplicationName;
    private final int m_pollInterval;
    private final Distributer m_distributer;
    private int m_instanceId = -1;
    private final Thread m_loadThread = new Thread(new Loader(), "Client stats loader");

    private static final String tablePrefix = ""; // "ma_";

    private static final String instancesTable = tablePrefix + "clientInstances";
    private static final String connectionStatsTable = tablePrefix + "clientConnectionStats";
    private static final String procedureStatsTable = tablePrefix + "clientProcedureStats";

    private static final String createInstanceStatement = "insert into " + instancesTable +
            " ( clusterStartTime, clusterLeaderAddress, applicationName, subApplicationName, " +
            " numHosts, numSites, numPartitions) " +
            "values ( ?, ?, ?, ?, ?, ?, ? );";

    private static final String insertConnectionStatsStatement = "insert into " + connectionStatsTable +
            " ( instanceId, tsEvent, hostname, connectionId, serverHostId, serverHostname, " +
            " serverConnectionId, numInvocations, numAborts, numFailures, numThrottled, numBytesRead, " +
            " numMessagesRead, numBytesWritten, numMessagesWritten) " +
            "values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? );";

    private static final String insertProcedureStatsStatement = "insert into " + procedureStatsTable +
            " ( instanceId, tsEvent, hostname, connectionId, serverHostId, serverHostname, " +
            " serverConnectionId, procedureName, roundtripAvg, roundtripMin, roundtripMax, " +
            " clusterRoundtripAvg, clusterRoundtripMin, clusterRoundtripMax, " +
            " numInvocations, numAborts, numFailures, numRestarts) " +
            "values ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? );";

    private PreparedStatement insertConnectionStatsStmt;
    private PreparedStatement insertProcedureStatsStmt;

    public ClientStatsSqlLoader(
            StatsUploaderSettings settings,
            Distributer distributer) {
        if (LOG.isDebugEnabled())
            LOG.debug("Creating new connection to stats database [" + settings.databaseURL + "]");
        try {
            if (settings.databaseJDBC != null && settings.databaseJDBC.isEmpty() == false) {
                Class.forName(settings.databaseJDBC);
            }
            
            m_conn = DriverManager.getConnection(settings.databaseURL,
                                               settings.databaseUser,
                                               settings.databasePass);
            m_conn.setAutoCommit(false);
            m_conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        catch (Exception e) {
            String msg = "Failed to connect to SQL reporting server with message:\n    ";
            msg += e.getMessage();
            throw new RuntimeException(msg);
        }
        
        m_settings = settings;
        m_applicationName = settings.applicationName;
        m_subApplicationName = settings.subApplicationName;
        m_pollInterval = settings.pollInterval;
        m_distributer = distributer;
    }

    public void start(long startTime, int leaderAddress) throws SQLException {
        Timestamp timestamp = new Timestamp(startTime);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Cluster Start Time: %s [%d]", timestamp, startTime));
        
        PreparedStatement instanceStmt =
            m_conn.prepareStatement(
                    createInstanceStatement,
                    PreparedStatement.RETURN_GENERATED_KEYS);
        insertConnectionStatsStmt = m_conn.prepareStatement(insertConnectionStatsStatement);
        insertProcedureStatsStmt = m_conn.prepareStatement(insertProcedureStatsStatement);
        instanceStmt.setTimestamp( 1, timestamp);
        instanceStmt.setInt( 2, leaderAddress);
        instanceStmt.setString( 3, m_applicationName);
        if (m_subApplicationName != null) {
            instanceStmt.setString( 4, m_subApplicationName);
        } else {
            instanceStmt.setNull( 4, Types.VARCHAR);
        }
        instanceStmt.setInt(5, CatalogUtil.getNumberOfHosts(m_settings.getCatalog()));
        instanceStmt.setInt(6, CatalogUtil.getNumberOfSites(m_settings.getCatalog()));
        instanceStmt.setInt(7, CatalogUtil.getNumberOfPartitions(m_settings.getCatalog()));
        instanceStmt.execute();
        ResultSet results = instanceStmt.getGeneratedKeys();
        while (results.next()) {
            m_instanceId = results.getInt( 1 );
        }
        results.close();
        instanceStmt.close();
        if (m_instanceId < 0) {
            throw new SQLException("Unable to generate an instance id to identify this client");
        }
        insertConnectionStatsStmt.setInt( 1, m_instanceId);
        insertProcedureStatsStmt.setInt( 1, m_instanceId);
        m_conn.commit();
        m_loadThread.setDaemon(true);
        m_loadThread.start();
        if (LOG.isDebugEnabled())
            LOG.debug("ClientStatsLoader has been started");
    }

    public synchronized void stop() throws InterruptedException {
        m_shouldStop = true;
        notifyAll();
        while (!m_stopped) {
            wait();
        }
    }

    private boolean m_shouldStop = false;
    private boolean m_stopped = false;

    private class Loader implements Runnable {
        @Override
        public void run() {
            long sleepLess = 0;
            synchronized (ClientStatsSqlLoader.this) {
                try {
                    while (true) {
                        if (m_shouldStop) {
                            break;
                        }
                        try {
                            if (m_pollInterval - sleepLess > 0) {
                                ClientStatsSqlLoader.this.wait(m_pollInterval
                                        - sleepLess);
                            }
                        } catch (InterruptedException e) {
                            return;
                        }

                        final long startTime = System.currentTimeMillis();
                        final VoltTable ioStats = m_distributer
                                .getConnectionStats(true);
                        final VoltTable procedureStats = m_distributer
                                .getProcedureStats(true);

                        try {
                            while (ioStats.advanceRow()) {
                                int index = 1;
                                insertConnectionStatsStmt.setInt(index++,
                                        m_instanceId);
                                insertConnectionStatsStmt.setTimestamp(index++,
                                        new Timestamp(ioStats.getLong("TIMESTAMP")));
                                insertConnectionStatsStmt.setString(index++,
                                        ioStats.getString("HOSTNAME"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("CONNECTION_ID"));
                                insertConnectionStatsStmt
                                        .setInt(index++, (int) ioStats
                                                .getLong("SERVER_HOST_ID"));
                                insertConnectionStatsStmt.setString(index++,
                                        ioStats.getString("SERVER_HOSTNAME"));
                                insertConnectionStatsStmt
                                        .setLong(
                                                index++,
                                                ioStats
                                                        .getLong("SERVER_CONNECTION_ID"));
                                insertConnectionStatsStmt
                                        .setLong(
                                                index++,
                                                ioStats
                                                        .getLong("INVOCATIONS_COMPLETED"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("INVOCATIONS_ABORTED"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("INVOCATIONS_FAILED"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("INVOCATIONS_THROTTLED"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("BYTES_READ"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("MESSAGES_READ"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("BYTES_WRITTEN"));
                                insertConnectionStatsStmt.setLong(index++,
                                        ioStats.getLong("MESSAGES_WRITTEN"));
                                insertConnectionStatsStmt.addBatch();
                            }
                            insertConnectionStatsStmt.executeBatch();
                        } catch (SQLException e) {
                            if (e.getCause() instanceof InterruptedException) {
                                return;
                            }
                            e.printStackTrace();
                        }

                        try {
                            while (procedureStats.advanceRow()) {
                                int index = 1;
                                insertProcedureStatsStmt.setInt(index++,
                                        m_instanceId);
                                insertProcedureStatsStmt.setTimestamp(index++,
                                        new Timestamp(procedureStats.getLong("TIMESTAMP")));
                                insertProcedureStatsStmt.setString(index++,
                                        procedureStats.getString("HOSTNAME"));
                                insertProcedureStatsStmt
                                        .setLong(index++, procedureStats
                                                .getLong("CONNECTION_ID"));
                                insertProcedureStatsStmt.setInt(index++,
                                        (int) procedureStats
                                                .getLong("SERVER_HOST_ID"));
                                insertProcedureStatsStmt.setString(index++,
                                        procedureStats
                                                .getString("SERVER_HOSTNAME"));
                                insertProcedureStatsStmt
                                        .setLong(
                                                index++,
                                                procedureStats
                                                        .getLong("SERVER_CONNECTION_ID"));
                                insertProcedureStatsStmt.setString(index++,
                                        procedureStats
                                                .getString("PROCEDURE_NAME"));
                                insertProcedureStatsStmt.setInt(index++,
                                        (int) procedureStats
                                                .getLong("ROUNDTRIPTIME_AVG"));
                                insertProcedureStatsStmt.setInt(index++,
                                        (int) procedureStats
                                                .getLong("ROUNDTRIPTIME_MIN"));
                                insertProcedureStatsStmt.setInt(index++,
                                        (int) procedureStats
                                                .getLong("ROUNDTRIPTIME_MAX"));
                                insertProcedureStatsStmt
                                        .setInt(
                                                index++,
                                                (int) procedureStats
                                                        .getLong("CLUSTER_ROUNDTRIPTIME_AVG"));
                                insertProcedureStatsStmt
                                        .setInt(
                                                index++,
                                                (int) procedureStats
                                                        .getLong("CLUSTER_ROUNDTRIPTIME_MIN"));
                                insertProcedureStatsStmt
                                        .setInt(
                                                index++,
                                                (int) procedureStats
                                                        .getLong("CLUSTER_ROUNDTRIPTIME_MAX"));
                                insertProcedureStatsStmt
                                        .setLong(
                                                index++,
                                                procedureStats
                                                        .getLong("INVOCATIONS_COMPLETED"));
                                insertProcedureStatsStmt
                                        .setLong(index++, procedureStats
                                                .getLong("INVOCATIONS_ABORTED"));
                                insertProcedureStatsStmt.setLong(index++,
                                        procedureStats
                                                .getLong("INVOCATIONS_FAILED"));
                                insertProcedureStatsStmt.setLong(index++,
                                        procedureStats
                                                .getLong("TIMES_RESTARTED"));
                                insertProcedureStatsStmt.addBatch();
                            }
                            insertProcedureStatsStmt.executeBatch();
                        } catch (SQLException e) {
                            if (e.getCause() instanceof InterruptedException) {
                                return;
                            }
                            e.printStackTrace();
                        }
                        try {
                            m_conn.commit();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        final long endTime = System.currentTimeMillis();
                        sleepLess = endTime - startTime;
                    }
                } finally {
                    m_stopped = true;
                    ClientStatsSqlLoader.this.notifyAll();
                }
            }
        }
    }
}
