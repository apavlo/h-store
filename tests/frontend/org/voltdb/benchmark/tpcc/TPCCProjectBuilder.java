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

package org.voltdb.benchmark.tpcc;

import java.io.File;
import java.io.IOException;

import org.voltdb.benchmark.tpcc.procedures.*;
import org.voltdb.catalog.Catalog;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.BenchmarkComponent;

/**
 * A subclass of VoltProjectBuilder that already knows about all of the
 * procedures, schema and partitioning info for TPC-C. It also contains
 * some helper code for other tests that use part of TPC-C.
 *
 */
public class TPCCProjectBuilder extends AbstractProjectBuilder {
    
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_clientClass = TPCCClient.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = TPCCLoader.class;

    /**
     * All procedures needed for TPC-C tests + benchmark
     */
    public static Class<?> PROCEDURES[] = new Class<?>[] {
        delivery.class,
        neworder.class,
        ostatByCustomerId.class,
        ostatByCustomerName.class,
        paymentByCustomerName.class,
        paymentByCustomerId.class,
        slev.class,
        
        // Utility Procedures
        noop.class,
        SelectAll.class,
        ResetWarehouse.class,
        LoadWarehouse.class,
        LoadWarehouseReplicated.class,
        GetTableCounts.class,
        MRquery1.class,
        MRqueryJoinAgg.class,
    };
    
    // Transaction Frequencies
    {
        addTransactionFrequency(delivery.class, 4);
        addTransactionFrequency(neworder.class, 45);
        addTransactionFrequency(ostatByCustomerId.class, 2);
        addTransactionFrequency(ostatByCustomerName.class, 3);
        addTransactionFrequency(paymentByCustomerId.class, 26);
        addTransactionFrequency(paymentByCustomerName.class, 17);
        addTransactionFrequency(slev.class, 4);
    }
    
    // Vertical Partitions
    {
//        addVerticalPartitionInfo("CUSTOMER", "C_W_ID", "C_D_ID", "C_ID", "C_FIRST", "C_LAST");
    }

    public static String partitioning[][] = new String[][] {
        {"WAREHOUSE", "W_ID"},
        {"DISTRICT", "D_W_ID"},
        {"CUSTOMER", "C_W_ID"},
        {"HISTORY", "H_W_ID"},
        {"STOCK", "S_W_ID"},
        {"ORDERS", "O_W_ID"},
        {"NEW_ORDER", "NO_W_ID"},
        {"ORDER_LINE", "OL_W_ID"},
        
//        {"ITEM", "I_ID"},
    };

    public TPCCProjectBuilder() {
        super("tpcc", TPCCProjectBuilder.class, PROCEDURES, partitioning);
        
        // MapReduce OLAP Experimental Queries
        addStmtProcedure("OLAPQuery1",
                         "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), " +
                         "       AVG(ol_quantity), AVG(ol_amount), COUNT(*)" +
                         "  FROM ORDER_LINE " +
                         " GROUP BY ol_number order by ol_number");
        addStmtProcedure("OLAPQuery6",
                         "SELECT ol_number, SUM(ol_amount) " +
                         "  FROM ORDER_LINE " +
                         " WHERE ol_quantity between 1 and 100000 " +
                         " GROUP BY ol_number order by ol_number");
        addStmtProcedure("OLAPQuery19",
                         "SELECT ol_number, SUM(ol_amount) " +
                         "  FROM ORDER_LINE " +
                         "WHERE (ol_o_id >= 20 and ol_o_id <= 100) " +
                         "   OR (ol_o_id >= 105 and ol_o_id <= 200)" +
                         "   OR (ol_o_id >= 210 and ol_o_id <= 290)" +
                         "GROUP BY ol_number order by ol_number");
        addStmtProcedure("OLAPJoinAgg",
                         "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), " +
                         "       SUM(i_price), AVG(ol_quantity), AVG(ol_amount), " +
                         "       AVG(i_price), COUNT(*) " +
                         "  FROM ORDER_LINE, ITEM " +
                         " WHERE order_line.ol_i_id = item.i_id " +
                         " GROUP BY ol_number " +
                        "  ORDER BY ol_number");
        
        // Helpers
        addStmtProcedure("GetWarehouse", "SELECT * FROM WAREHOUSE WHERE W_ID = ?");
    }

    /**
     * Get a pointer to a compiled catalog for TPCC with all the procedures.
     */
    public Catalog createTPCCSchemaCatalog() throws IOException {
        return (this.createTPCCSchemaCatalog(false));
    }
    
    public Catalog createTPCCSchemaCatalog(boolean fkeys) throws IOException {
        // compile a catalog
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + this.getJarName(true);

        addSchema(this.getDDLURL(fkeys));
        addDefaultPartitioning();
        addDefaultProcedures();
        //this.addProcedures(org.voltdb.benchmark.tpcc.procedures.InsertHistory.class);

        boolean status = compile(catalogJar);
        assert(status);

        // read in the catalog
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(catalogJar, null);
        assert(serializedCatalog != null);

        // create the catalog (that will be passed to the ClientInterface
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        return catalog;
    }

    /**
     * Get a pointer to a compiled catalog for TPCC with all the procedures.
     * This can be run without worrying about setting up anything else in this class.
     */
    public static Catalog getTPCCSchemaCatalog(boolean fkeys) throws IOException {
        return (new TPCCProjectBuilder().createTPCCSchemaCatalog(fkeys));
    }
    
    static public Catalog getTPCCSchemaCatalog() throws IOException {
        return (new TPCCProjectBuilder().createTPCCSchemaCatalog(false));
    }
}
