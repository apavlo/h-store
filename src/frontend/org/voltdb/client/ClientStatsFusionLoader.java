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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import au.com.bytecode.opencsv.CSVReader;

import com.google.gdata.client.ClientLoginAccountType;
import com.google.gdata.client.GoogleService;
import com.google.gdata.client.Service.GDataRequest;
import com.google.gdata.client.Service.GDataRequest.RequestType;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ContentType;
import com.google.gdata.util.ServiceException;

import edu.brown.catalog.CatalogUtil;

/**
 * Polls a Distributer instance for IO and procedure invocation information and ELTs the results
 * to a database via JDBC.
 *
 */
public class ClientStatsFusionLoader implements ClientStatsLoader {
    private static final Logger LOG = Logger.getLogger(ClientStatsFusionLoader.class);
    
    private final StatsUploaderSettings m_settings;
    private final String m_applicationName;
    private final String m_subApplicationName;
    private final int m_pollInterval;
    private final Distributer m_distributer;
    private int m_instanceId = -1;
    private final Thread m_loadThread = new Thread(new Loader(), "Client stats loader");

    private static final String instancesTable = "clientInstances";
    private static final String connectionStatsTable = "clientConnectionStats";
    private static final String procedureStatsTable = "clientProcedureStats";

    private static String instancesTableId;
    private static String connectionStatsTableId;
    private static String procedureStatsTableId;
    
    private static final String createInstanceStatement = "INSERT INTO %s " +
            "(instanceId, clusterStartTime, clusterLeaderAddress, applicationName, subApplicationName, " +
            "numHosts, numSites, numPartitions) VALUES " +
            "('%d', '%s', '%d', '%s', '%s', '%d', '%d', '%d');";

    private static final String createInstancesTable = "CREATE TABLE " + instancesTable +
            " (instanceId:NUMBER, clusterStartTime:DATETIME, clusterLeaderAddress:NUMBER, " +
            "applicationName:STRING, subApplicationName:STRING, numHosts:NUMBER, " +
            "numSites:NUMBER, numPartitions:NUMBER);";
    
    private static final String insertConnectionStatsStatement = "INSERT INTO %s " +
            "(instanceId, tsEvent, hostname, connectionId, serverHostId, serverHostname, " +
            "serverConnectionId, numInvocations, numAborts, numFailures, numThrottled, numBytesRead, " +
            "numMessagesRead, numBytesWritten, numMessagesWritten) VALUES" +
            "('%d', '%s', '%s', '%d', '%d', '%s', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d');";

    private static final String createConnectionStatsTable = "CREATE TABLE " + connectionStatsTable +
            " (instanceId:NUMBER, tsEvent:DATETIME, hostname:STRING, connectionId:NUMBER, " +
            "serverHostId:NUMBER, serverHostname:STRING, serverConnectionId:NUMBER, " +
            "numInvocations:NUMBER, numAborts:NUMBER, numFailures:NUMBER, numThrottled:NUMBER, " +
            "numBytesRead:NUMBER, numMessagesRead:NUMBER, numBytesWritten:NUMBER, " +
            "numMessagesWritten:NUMBER);";
    
    private static final String insertProcedureStatsStatement = "INSERT INTO %s" +
            "(instanceId, tsEvent, hostname, connectionId, serverHostId, serverHostname, " +
            "serverConnectionId, procedureName, roundtripAvg, roundtripMin, roundtripMax, " +
            "clusterRoundtripAvg, clusterRoundtripMin, clusterRoundtripMax, " +
            "numInvocations, numAborts, numFailures, numRestarts) VALUES " +
            "('%d', '%s', '%s', '%d', '%d', '%s', '%d', '%s', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d', '%d');";

    private static final String createProcedureStatsTable = "CREATE TABLE " + procedureStatsTable +
            " (instanceId:NUMBER, tsEvent:DATETIME, hostname:STRING, connectionId:NUMBER, " +
            "serverHostId:NUMBER, serverHostname:STRING, serverConnectionId:NUMBER, " +
            "procedureName:STRING, roundtripAvg:NUMBER, roundtripMin:NUMBER, " +
            "roundtripMax:NUMBER, clusterRoundtripAvg:NUMBER, clusterRoundtripMin:NUMBER, " +
            "clusterRoundtripMax:NUMBER, numInvocations:NUMBER, numAborts:NUMBER, " +
            "numFailures:NUMBER, numRestarts:NUMBER);";
    
    /**
     * Google Fusion Tables API URL. 
     * All requests to the Google Fusion Tables service begin with this URL.
     */
    private static final String SERVICE_URL = "https://www.google.com/fusiontables/api/query";
    
    /**
     * Service to handle requests to Google Fusion Tables.
     */
    private GoogleService service;
    
    /**
     * Returns the results of running a Fusion Tables SQL query.
     *
     * @param  query the SQL query to send to Fusion Tables
     * @param  isUsingEncId includes the encrypted table ID in the result if {@code true}, otherwise
     *         includes the numeric table ID
     * @return the results from the Fusion Tables SQL query
     * @throws IOException when there is an error writing to or reading from GData service
     * @throws ServiceException when the request to the Fusion Tables service fails
     * @see    com.google.gdata.util.ServiceException
     */
    public QueryResults runQuery(String query) throws IOException, ServiceException {

      String lowercaseQuery = query.toLowerCase();
      String encodedQuery = URLEncoder.encode(query, "UTF-8");

      GDataRequest request;
      // If the query is a select, describe, or show query, run a GET request.
      if (lowercaseQuery.startsWith("select") ||
          lowercaseQuery.startsWith("describe") ||
          lowercaseQuery.startsWith("show")) {
        URL url = new URL(SERVICE_URL + "?sql=" + encodedQuery + "&encid=true");
        request = service.getRequestFactory().getRequest(RequestType.QUERY, url,
            ContentType.TEXT_PLAIN);
      } else {
        // Otherwise, run a POST request.
        URL url = new URL(SERVICE_URL + "?encid=true");
        request = service.getRequestFactory().getRequest(RequestType.INSERT, url,
            new ContentType("application/x-www-form-urlencoded"));
        OutputStreamWriter writer = new OutputStreamWriter(request.getRequestStream());
        writer.append("sql=" + encodedQuery);
        writer.flush();
      }

      request.execute();

      return getResults(request);
    }
    
    /**
     * Returns the Fusion Tables CSV response as a {@code QueryResults} object.
     *
     * @return an object containing a list of column names and a list of row values from the
     *         Fusion Tables response
     */
    private QueryResults getResults(GDataRequest request)
        throws IOException {
      InputStreamReader inputStreamReader = new InputStreamReader(request.getResponseStream());
      BufferedReader bufferedStreamReader = new BufferedReader(inputStreamReader);
      CSVReader reader = new CSVReader(bufferedStreamReader);
      // The first line is the column names, and the remaining lines are the rows.
      List<String[]> csvLines = reader.readAll();
      List<String> columns = Arrays.asList(csvLines.get(0));
      List<String[]> rows = csvLines.subList(1, csvLines.size());
      QueryResults results = new QueryResults(columns, rows);
      return results;
    }
    
    /**
     * Result of a Fusion Table query.
     */
    private static class QueryResults {
      final List<String> columnNames;
      final List<String[]> rows;

      public QueryResults(List<String> columnNames, List<String[]> rows) {
        this.columnNames = columnNames;
        this.rows = rows;
      }

     /**
      * Prints the query results.
      *
      * @param the results from the query
      */
     public void print() {
       String sep = "";
       for (int i = 0; i < columnNames.size(); i++) {
         System.out.print(sep + columnNames.get(i));
         sep = ", ";
       }
       System.out.println();

       for (int i = 0; i < rows.size(); i++) {
         String[] rowValues = rows.get(i);
         sep = "";
         for (int j = 0; j < rowValues.length; j++) {
           System.out.print(sep + rowValues[j]);
           sep = ", ";
         }
         System.out.println();
       }
     }
    }
    
    public static void main(String[] args) throws Exception {
        /*GoogleService service = new GoogleService("fusiontables", "h-store.ClientStatsFusionLoader");
        service.setUserCredentials("hstore.dev@gmail.com", "hstore!321", ClientLoginAccountType.GOOGLE);
        
        boolean useEncId = true;

        System.out.println("--- Create a table ---");
        QueryResults results = ClientStatsFusionLoader.run("CREATE TABLE demo (name:STRING, date:DATETIME)", useEncId, service);
        results.print();
        String tableId = (results.rows.get(0))[0];

        System.out.println("--- Insert data into the table ---");
        results = ClientStatsFusionLoader.run("INSERT INTO " + tableId + " (name, date) VALUES ('bob', '1/1/2012')",
            useEncId, service);
        results.print();

        System.out.println("--- Insert more data into the table ---");
        results = ClientStatsFusionLoader.run("INSERT INTO " + tableId + " (name, date) VALUES ('george', '1/4/2012')",
            useEncId, service);
        results.print();

        System.out.println("--- Select data from the table ---");
        results = ClientStatsFusionLoader.run("SELECT * FROM " + tableId + " WHERE date > '1/3/2012'", useEncId, service);
        results.print();

        System.out.println("--- Drop the table ---");
        results = ClientStatsFusionLoader.run("DROP TABLE " + tableId, useEncId, service);
        results.print();*/
        
        /*// start the connection with Fusion Tables
        GoogleService service = new GoogleService("fusiontables", "h-store.ClientStatsFusionLoader");
        service.setUserCredentials("hstore.dev@gmail.com", "hstore!321", ClientLoginAccountType.GOOGLE);
        
        // create the test table if it does not already exist
        // find info on tables so we know which one is the test table
        QueryResults results = ClientStatsFusionLoader.run("SHOW TABLES", true, service);
        results.print();
        
        // find the table ID of the test table
        String tableId = null;
        for (String[] row : results.rows) {
            if (row[1].equals("TestTable")) {
                tableId = row[0];
            }
        }
        if (tableId == null) {
            System.out.println("Could not find the test table so creating it");
            results = ClientStatsFusionLoader.run("CREATE TABLE TestTable (int:NUMBER, time:DATETIME)", true, service);
            results.print();
        }
        
        // find info on tables so we know which one is the test table
        results = ClientStatsFusionLoader.run("SHOW TABLES", true, service);
        results.print();
        
        // find the table ID of the test table
        tableId = null;
        for (String[] row : results.rows) {
            if (row[1].equals("TestTable")) {
                tableId = row[0];
            }
        }
        if (tableId == null) {
            throw new RuntimeException("Could not find the test table!");
        }
        
        // find the next instance id (auto increment DNE)
        int instanceId = -1;
        results = ClientStatsFusionLoader.run(String.format("SELECT * FROM %s", tableId), true, service);
        results.print();
        for (String[] row : results.rows) {
            int currentId = Integer.parseInt(row[0]);
            if (currentId > instanceId) {
                instanceId = currentId;
            }
        }
        ++instanceId;
        System.out.println(instanceId);
        
        // insert a time stamp into the test table
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        System.out.println(timestamp);
        String stmt = String.format("insert into %s (int, time) values ('%d', '%s');", tableId, instanceId, timestamp);
        results = ClientStatsFusionLoader.run(stmt, true, service);
        results.print();*/
    }
    
    public ClientStatsFusionLoader(
            StatsUploaderSettings settings,
            Distributer distributer) {
        LOG.debug("database user: " + settings.databaseUser);
        LOG.debug("database pass: " + settings.databasePass);
        
        m_settings = settings;
        m_applicationName = settings.applicationName;
        m_subApplicationName = settings.subApplicationName;
        m_pollInterval = settings.pollInterval;
        m_distributer = distributer;
        
        service = new GoogleService("fusiontables", "h-store.ClientStatsFusionLoader");
        try {
            service.setUserCredentials(settings.databaseUser, settings.databasePass, ClientLoginAccountType.GOOGLE);
        } catch (Exception e) {
            LOG.debug("Could not connect to Fusion Tables site");
            String msg = "Failed to connect to Google FusionTables reporting server with message:\n";
            msg += e.getMessage();
            throw new RuntimeException(msg);
        }
        
        QueryResults results = null;
        try {
            results = runQuery("SHOW TABLES;");
        } catch (Exception e) {
            String msg = "Failed to query Google FusionTables server for tables with message:\n";
            msg += e.getMessage();
            throw new RuntimeException(msg);
        }
        LOG.debug("Queried for tables");
        
        for (String[] row : results.rows) {
            if (row[1].equals(instancesTable)) {
                instancesTableId = row[0];
            }
            else if (row[1].equals(connectionStatsTable)) {
                connectionStatsTableId = row[0];
            }
            else if (row[1].equals(procedureStatsTable)) {
                procedureStatsTableId = row[0];
            }
        }
        LOG.debug("Found H-Store tables");
        
        try {
            if (instancesTableId == null) {
                LOG.debug("Could not find the instances table so creating it as " + instancesTable);
                results = runQuery(createInstancesTable);
                results.print();
            }
            if (connectionStatsTableId == null) {
                LOG.debug("Could not find the connection stats table so creating it as " + connectionStatsTable);
                results = runQuery(createConnectionStatsTable);
                results.print();
            }
            if (procedureStatsTableId == null) {
                LOG.debug("Could not find the procedure stats table so creating it as " + procedureStatsTable);
                results = runQuery(createProcedureStatsTable);
                results.print();
            }
        } catch (Exception e) {
            String msg = "Failed to create new Google FusionTable with message:\n";
            msg += e.getMessage();
            throw new RuntimeException(msg);
        }
        LOG.debug("Connection established to Fusion Tables site!");
    }
    
    public void start(long startTime, int leaderAddress) throws IOException, ServiceException {
        Timestamp timestamp = new Timestamp(startTime);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Cluster Start Time: %s [%d]", timestamp, startTime));
        
        QueryResults results = runQuery(String.format("SELECT instanceId FROM %s;", instancesTableId));
        for (String[] row : results.rows) {
            int currentId = Integer.parseInt(row[0]);
            if (currentId > m_instanceId) {
                m_instanceId = currentId;
            }
        }
        ++m_instanceId;
        
        runQuery(String.format(createInstanceStatement,
                instancesTableId,
                m_instanceId,
                timestamp,
                leaderAddress,
                m_applicationName,
                m_subApplicationName == null ? "null" : m_subApplicationName,
                CatalogUtil.getNumberOfHosts(m_settings.getCatalog()),
                CatalogUtil.getNumberOfSites(m_settings.getCatalog()),
                CatalogUtil.getNumberOfPartitions(m_settings.getCatalog())));
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
            synchronized (ClientStatsFusionLoader.this) {
                try {
                    while (true) {
                        if (m_shouldStop) {
                            break;
                        }
                        try {
                            if (m_pollInterval - sleepLess > 0) {
                                ClientStatsFusionLoader.this.wait(m_pollInterval
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

                        /**
                         * NOTE FROM FUSION TABLES SITE: You can list up to 500 INSERTs, separated by semicolons, as long as the total size of the
                         * data does not exceed 1 MB and the total number of table cells being added does not exceed 10,000 cells. The calculation may
                         * not be obvious. For example, if you have 100 columns in your table and your INSERT statement includes only 10 columns, the
                         * number of cells being added is still 100.
                         */
                        try {
                            int count = 0;
                            StringBuilder query = new StringBuilder();
                            while (ioStats.advanceRow()) {
                                query.append(String.format(insertConnectionStatsStatement,
                                        connectionStatsTableId,
                                        m_instanceId,
                                        new Timestamp(ioStats.getLong("TIMESTAMP")),
                                        ioStats.getString("HOSTNAME"),
                                        ioStats.getLong("CONNECTION_ID"),
                                        ioStats.getLong("SERVER_HOST_ID"),
                                        ioStats.getString("SERVER_HOSTNAME"),
                                        ioStats.getLong("SERVER_CONNECTION_ID"),
                                        ioStats.getLong("INVOCATIONS_COMPLETED"),
                                        ioStats.getLong("INVOCATIONS_ABORTED"),
                                        ioStats.getLong("INVOCATIONS_FAILED"),
                                        ioStats.getLong("INVOCATIONS_THROTTLED"),
                                        ioStats.getLong("BYTES_READ"),
                                        ioStats.getLong("MESSAGES_READ"),
                                        ioStats.getLong("BYTES_WRITTEN"),
                                        ioStats.getLong("MESSAGES_WRITTEN")));
                                ++count;
                                if (count % 500 == 0) {
                                    runQuery(query.toString());
                                    query = new StringBuilder(query.capacity());
                                }
                            }
                            if (count % 500 > 0) {
                                runQuery(query.toString());
                            }
                        } catch (Exception e) {
                            if (e.getCause() instanceof InterruptedException) {
                                return;
                            }
                            e.printStackTrace();
                        }

                        try {
                            int count = 0;
                            StringBuilder query = new StringBuilder();
                            while (procedureStats.advanceRow()) {
                                query.append(String.format(insertProcedureStatsStatement,
                                        procedureStatsTableId,
                                        m_instanceId,
                                        new Timestamp(procedureStats.getLong("TIMESTAMP")),
                                        procedureStats.getString("HOSTNAME"),
                                        procedureStats.getLong("CONNECTION_ID"),
                                        procedureStats.getLong("SERVER_HOST_ID"),
                                        procedureStats.getString("SERVER_HOSTNAME"),
                                        procedureStats.getLong("SERVER_CONNECTION_ID"),
                                        procedureStats.getString("PROCEDURE_NAME"),
                                        (int) procedureStats.getLong("ROUNDTRIPTIME_AVG"),
                                        (int) procedureStats.getLong("ROUNDTRIPTIME_MIN"),
                                        (int) procedureStats.getLong("ROUNDTRIPTIME_MAX"),
                                        (int) procedureStats.getLong("CLUSTER_ROUNDTRIPTIME_AVG"),
                                        (int) procedureStats.getLong("CLUSTER_ROUNDTRIPTIME_MIN"),
                                        (int) procedureStats.getLong("CLUSTER_ROUNDTRIPTIME_MAX"),
                                        procedureStats.getLong("INVOCATIONS_COMPLETED"),
                                        procedureStats.getLong("INVOCATIONS_ABORTED"),
                                        procedureStats.getLong("INVOCATIONS_FAILED"),
                                        procedureStats.getLong("TIMES_RESTARTED")));
                                ++count;
                                if (count % 500 == 0) {
                                    runQuery(query.toString());
                                    query = new StringBuilder(query.capacity());
                                }
                            }
                            if (count % 500 > 0) {
                                runQuery(query.toString());
                            }
                        } catch (Exception e) {
                            if (e.getCause() instanceof InterruptedException) {
                                return;
                            }
                            e.printStackTrace();
                        }
                        final long endTime = System.currentTimeMillis();
                        sleepLess = endTime - startTime;
                    }
                } finally {
                    m_stopped = true;
                    ClientStatsFusionLoader.this.notifyAll();
                }
            }
        }
    }
}
