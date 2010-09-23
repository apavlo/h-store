/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
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

import com.vertica.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.HashMap;
import java.sql.*;

import org.voltdb.utils.BBInputStream;
import org.voltdb.utils.DBBPool;

public class VerticaLoader extends Thread implements ELTManager.Loader {

    /** Number of connections to create per host. */
    private final static int kHostMultiplexingFactor = 1;

    /** All load work is offered through this queue.  Using
        VoltDB mailbox requires a Site for the loader and a
        blocking mailbox. Otherwise, impl matches the mailbox
        pattern. */
    private LinkedBlockingQueue<ELTManager.LoaderMessage> m_worklist;

    /** The set of available, unused connections */
    private final LinkedBlockingQueue<Connection> m_connectionList;
    
    /** Cached copy statements hashed by table id */
    private final HashMap<Integer, String> m_copyStatements;

    /** Pool of worker threads for performing loader work. */
    private ExecutorService m_threadPool;

    /** Load the driver and inititalize loader's data structures */
    public VerticaLoader() {
        try {
            Class.forName("com.vertica.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Failed to find vertica driver.");
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        m_worklist = new LinkedBlockingQueue<ELTManager.LoaderMessage>();
        m_connectionList = new LinkedBlockingQueue<Connection>();
        m_copyStatements = new HashMap<Integer, String>();
    }       

    /** Return connection to the connection set for reuse. This should
        result in roughly round-robin connection use. */
    public void returnConnection(Connection conn) {
        boolean done = m_connectionList.offer(conn);
        assert(done);
    }

    /** Remove a connection from the connection set */
    public Connection getConnection() {
        Connection conn = null;
        try {
            conn = m_connectionList.take();
        } 
        catch (java.lang.InterruptedException e) {
            throw new IllegalStateException("VerticaLoader connection pool timed-out");
        }
        return conn;
    }

    /** Get the copy statement for a tableid */
    public String getStatement(int tid) {
        return m_copyStatements.get(tid);
    }

    /** Obtain a reference to the worklist queue for this loader */
    public LinkedBlockingQueue<ELTManager.LoaderMessage> getQueue() { 
        return m_worklist;
    }

    /** Add a host from the catalog by creating a connection for this host */
    public void addHost(String host, String port, String database,
                       String username, String password) {
        Connection conn = null;
        String connect = "jdbc:vertica://" + host + ":" + port + "/" + database;
        System.out.println("Connecting to " + connect + " as " + username + "," + password);

        try {
            for (int i=0; i < kHostMultiplexingFactor; ++i)
            {
                conn = DriverManager.getConnection(connect, username, password);
                m_connectionList.add(conn);
            }
        }
        catch (Throwable e) {
            System.out.println(connect + " failed: " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    /** Add a table from the catalog to the loader */
    public void addTable(String database, String table, int tid) {
        System.out.println("Adding EL table " + table + " " + tid);
        String stmt = "COPY " + table +
          " FROM STDIN DELIMITER '|' NULL '' RECORD TERMINATOR '\n'";
        m_copyStatements.put(tid, stmt);
    }

    /** Finalize setup and get ready to start accepting load data 
        Create the threadpool before creating the runloop thread
        to allow the ELTManager to catch and process pool setup
        errors. (Otherwise, they happen in the spawned thread).
     */
    public void readyForData() {
        try {
            m_threadPool = Executors.newFixedThreadPool(m_connectionList.size());
        } catch (Throwable e) {
            // don't expose a worklist if there is no backing thread pool.
            // but the expected error path on setup failure is for VoltDB to
            // shutdown.
            m_worklist = null;
            throw new IllegalStateException("VerticaLoader thread pool setup failed.");
        }
        start();
    }

    /** Execute the runloop. If the thread pool can not accept the upload request,
     *  be careful to always return message data to the allocation pool. If the
     *  worklist queue is interrupted or a stop message is discovered, shutdown
     *  the pool.
     */
    public void run() {     
        System.out.println("Running VerticaLoader");
        ELTManager.LoaderMessage msg;
        try {
            for(;;) {
                msg = m_worklist.take();
                if (msg.isStopMessage()) {
                    break;
                }
                try {
                    m_threadPool.execute(new UploadTask(this, msg.m_data, msg.m_tableId));
                } catch (RejectedExecutionException e) {
                    System.out.println("VerticaLoader unable to load data.");
                    msg.m_data.discard();
                } catch (NullPointerException e) {
                    System.out.println("VerticaLoader unable to create UploadTask");
                    msg.m_data.discard();
                } catch (Throwable e) {
                    System.out.println("VerticaLoader: " + e.getMessage());
                    msg.m_data.discard();
                }
            }
        } catch (Throwable e) {
            System.out.println("Shutting down the VerticaLoader pool.");
            if (m_threadPool != null) {
                m_threadPool.shutdown();
            }
        }
    }

    /** An UploadTask transfers one block of data to the Vertica database. */
    static class UploadTask implements Runnable {
        private final VerticaLoader m_owner;
        private final DBBPool.DBBContainer m_data;
        private final int m_tableId;
        
        public UploadTask(VerticaLoader owner, DBBPool.DBBContainer data, int tableId) {
            m_owner = owner;
            m_data = data;
            m_tableId = tableId;
        }

        public void run() {
            Statement stmt = null;
            String stmtText = m_owner.getStatement(m_tableId);
            Connection conn = m_owner.getConnection();
            try {
                try {
                    stmt = conn.createStatement();
                } 
                catch (SQLException e) {
                    e.printStackTrace();
                    m_data.discard();
                }

                try {
                    if (stmt != null) {
                        //System.out.println("Starting copy for " + m_tableId);
                        //System.out.flush();
                        BBInputStream stream = new BBInputStream();
                        stream.offer(m_data);
                        stream.EOF();
                        /*final boolean result =*/ ((PGStatement)stmt).executeCopyIn(stmtText, stream);
                        // ((PGStatement)stmt).startCopyIn(stmtText, stream);
                        // ((PGStatement)stmt).finishCopyIn();
                        //System.out.println("Finished copy for : " + m_tableId + " " + result);
                        //System.out.flush();
                    }
                } 
                catch (SQLException e) {
                    e.printStackTrace();
                    // stream owns data. Do not discard it!
                }
            } 
            finally {
                m_owner.returnConnection(conn);
            }
        }
    }
}
