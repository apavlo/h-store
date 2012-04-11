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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.brown.utils.StringUtil;

/**
 * Encapsulates configuration settings for client statistics loading. This is not production
 * ready and the schema is subject to change and there are no tools provided to initialize a database
 * with the schema nor is the schema documented.
 *
 */
public class StatsUploaderSettings {
    private static final Logger LOG = Logger.getLogger(StatsUploaderSettings.class);
    
    final String databaseURL;
    final String databaseUser;
    final String databasePass;
    final String databaseJDBC;
    final String applicationName;
    final String subApplicationName;
    final int pollInterval;
    final Connection conn;

    /**
     *
     * Constructor that stores settings and verifies that a connection to the specified
     * databaseURL can be created
     * @param databaseURL URL of the database to connect to. Must include login credentials in URL.
     * For example "jdbc:mysql://[host][,failoverhost...][:port]/[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]..."
     * @param applicationName Identifies this application in the statistics tables
     * @param subApplicationName More specific aspect of this application (Client, Loader etc.)
     * @param pollInterval Interval in milliseconds that stats should be polled and uploaded
     * @throws SQLException Thrown if a connection to the database can't be created or if the appropriate JDBC
     * driver can't be found.
     */
    private StatsUploaderSettings(
            String databaseURL,
            String databaseUser,
            String databasePass,
            String databaseJDBC,
            String applicationName,
            String subApplicationName,
            int pollInterval) {
        this.databaseURL = databaseURL;
        this.databaseUser = databaseUser;
        this.databasePass = databasePass;
        this.databaseJDBC = databaseJDBC;
        this.applicationName = applicationName;
        this.subApplicationName = subApplicationName;
        this.pollInterval = pollInterval;

        if (applicationName == null || applicationName.isEmpty()) {
            throw new IllegalArgumentException("Application name is null or empty");
        }

        if (pollInterval < 1000) {
            throw new IllegalArgumentException("Polling more then once per second is excessive");
        }

        if ((databaseURL == null) || (databaseURL.isEmpty())) {
            String msg = "Not connecting to SQL reporting server as connection URL is null or missing.";
            throw new RuntimeException(msg);
        }

        if (LOG.isDebugEnabled())
            LOG.debug("Creating new connection to stats database [" + databaseURL + "]");
        try {
            if (this.databaseJDBC != null && this.databaseJDBC.isEmpty() == false) {
                Class.forName(this.databaseJDBC);
            }
            
            conn = DriverManager.getConnection(this.databaseURL,
                                               this.databaseUser,
                                               this.databasePass);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        catch (Exception e) {
            String msg = "Failed to connect to SQL reporting server with message:\n    ";
            msg += e.getMessage();
            throw new RuntimeException(msg);
        }
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("URL", this.databaseURL);
        m.put("User", this.databaseUser);
        m.put("Pass", this.databasePass);
        m.put("JDBC", this.databaseJDBC);
        m.put("Application", this.applicationName);
        m.put("Sub-Application", this.subApplicationName);
        m.put("Poll Interval", this.pollInterval);
        return (StringUtil.formatMaps(m));
    }
    
    // -----------------------------------------------------------
    
    private static final Map<Integer, StatsUploaderSettings> cache = new HashMap<Integer, StatsUploaderSettings>();
    
    public static StatsUploaderSettings singleton(
            String databaseURL,
            String databaseUser,
            String databasePass,
            String databaseJDBC,
            String applicationName,
            String subApplicationName,
            int pollInterval) {
        
        Object params[] = {
            databaseURL,
            databaseUser,
            databasePass,
            databaseJDBC,
            applicationName,
            subApplicationName,
            pollInterval
        };
        int hash = Arrays.hashCode(params);
        StatsUploaderSettings singleton = null;
        synchronized (StatsUploaderSettings.class) {
            singleton = cache.get(hash);
            if (singleton == null) {
                singleton = new StatsUploaderSettings(
                    databaseURL,
                    databaseUser,
                    databasePass,
                    databaseJDBC,
                    applicationName,
                    subApplicationName,
                    pollInterval);
                cache.put(hash, singleton);
            }
        } // SYNCH
        
        return (singleton);
        
        
    }
}