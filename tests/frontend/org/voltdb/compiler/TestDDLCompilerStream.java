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

package org.voltdb.compiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlannerTestAideDeCamp;
import org.voltdb.planner.TestPlansGroupBy;
import org.voltdb.plannodes.AbstractPlanNode;

import junit.framework.*;

public class TestDDLCompilerStream extends TestCase {
	
	private PlannerTestAideDeCamp aide;

    public void testSimpleDDLCompiler() throws HSQLParseException {
    	/**
        String ddl1 =
            "CREATE STREAM \"ticker\" ( " +
            "\"t_id\" integer default '0' NOT NULL, " +
            "\"t_name\" varchar(10) default NULL," +
            "\"t_stamp\" int NOT NULL, " +
            "PRIMARY KEY  (\"t_stamp\") " +
            ");";
            */
    	
    	String ddl1 =
                "CREATE TABLE TABLEC (" +
                "A_ID     BIGINT NOT NULL," +
                "A_VALUE  VARCHAR(64)," +
                "PRIMARY KEY (A_ID)" +
                ");"; 
    	String ddl2 =
                "CREATE TABLE TABLEA (" +
                "A_ID     BIGINT NOT NULL," +
                "A_VALUE  VARCHAR(64)," +
                "PRIMARY KEY (A_ID)" +
                ");";

        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(ddl1);
        hsql.runDDLCommand(ddl2);

        String xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

        hsql.close();
    }
    

    /**
     * Note, this should succeed as HSQL doesn't have a hard limit
     * on the number of columns. The test in TestVoltCompiler will
     * fail on 1025 columns.
     * @throws HSQLParseException
     *//**
    public void testTooManyColumnTable() throws IOException, HSQLParseException {
        String schemaPath = "";
        URL url = TestVoltCompiler.class.getResource("toowidetable-ddl.sql");
        schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        FileReader fr = new FileReader(new File(schemaPath));
        BufferedReader br = new BufferedReader(fr);
        String ddl1 = "";
        String line;
        while ((line = br.readLine()) != null) {
            ddl1 += line + "\n";
        }

        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        hsql.runDDLCommand(ddl1);

        String xml = hsql.getXMLFromCatalog();
        System.out.println(xml);
        assertTrue(xml != null);

        hsql.close();
    }*/
}
