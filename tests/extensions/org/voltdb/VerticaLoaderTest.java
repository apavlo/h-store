/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
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

package org.voltdb;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.vertica.PGStatement;

import junit.framework.TestCase;

public class VerticaLoaderTest extends TestCase {

    final static int verticaPort = 5433;
    final static String dbName = "database";
    final static String username = "dbadmin";
    final static String password = "";
    final static String host = "demo2";
    final static String jdbcConnectionString = "jdbc:vertica://" + host + ":" + 
        String.valueOf(verticaPort) + "/" + dbName;
    final static String tableName = "VOLT_TEST_TABLE";
    
    final static String tableDDL = "create table \"" + tableName + "\" (foo int8, bar int8);";
    final static String projDDL = "create projection \"public." + tableName + "_super\" (\n" +
        "foo ENCODING NONE, bar ENCODING NONE) AS select foo, bar from public." + 
        tableName + " order by foo;";
    
    // data to be loaded
    final static String loaderData = "50|100!60|110";
    
    // JDBC class-vars
    Connection vdbConn;
    Statement stmt;
    DatabaseMetaData dbmd;
    
    /**
     * Create an <code>InputStream</code> from a Java string so it can
     * be used where <code>InputStream</code>s are needed, like in XML parsers.
     *
     */
    public class StringInputStream extends InputStream {
        StringReader sr;
        
        /**
         * Create the stream from a string.
         * @param value The string value to turn into an InputStream.
         */
        public StringInputStream(String value) { sr = new StringReader(value); }
        
        /**
         * Read a single byte from the stream (string).
         */
        public int read() throws IOException { return sr.read(); }
    }
    
    public void testLoader() {
        try {
            // try to load the vertica JDBC driver
            Class.forName("com.vertica.Driver");
            // try to connect to vertica db
            vdbConn = DriverManager.getConnection(jdbcConnectionString, username, password);
            // create extra JDBC junk
            stmt = vdbConn.createStatement();
            dbmd = vdbConn.getMetaData();
            
            // kill all the other users on this host... this is a hack for now
            stmt.execute("select close_all_sessions();");
            
            // check if a table exists
            ResultSet tables = dbmd.getTables(null, null, tableName, null);
            // if tables.next() works, then the table exists
            if (tables.next()) {
                stmt.executeUpdate("drop table \"" + tableName + "\" cascade;");
            }
            
            // create the table and the trivial/temp projection
            stmt.execute(tableDDL);
            stmt.execute(projDDL);
            
            // count how many values are in the table
            String sql = "select count(*) as \"num\" from \"" + tableName + "\";";
            ResultSet results = stmt.executeQuery(sql);
            assertEquals(results.next(), true);
            // make sure the table is empty
            assertEquals(results.getInt("num"), 0);
            
            // create an inputstream of data
            StringInputStream stream = new StringInputStream(loaderData);
            
            // execute the copy
            sql = "COPY " + tableName + " FROM STDIN DELIMITER '|' NULL '' RECORD TERMINATOR '!'";
            /*final boolean result =*/ ((PGStatement)stmt).executeCopyIn(sql, stream);
            
            //assertEquals(result, true);
            
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(true, false);
        }
    }
    
}
