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
import java.net.URL;

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
    public static final Class<? extends BenchmarkComponent> m_loaderClass = MultiLoader.class;

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
        
        //MRquery1.class,
        //MRquery19.class,
        //MRquery6.class
        
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
    }
    
    /**
     * Add the TPC-C procedures to the VoltProjectBuilder base class.
     */
    public void addDefaultProcedures() {
        addProcedures(PROCEDURES);
        
        addStmtProcedure("Query1", "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount),AVG(ol_quantity),AVG(ol_amount),COUNT(*) FROM order_line GROUP BY ol_number order by ol_number");
        addStmtProcedure("Query19","select ol_number, sum(ol_amount) from order_line " +
        		                   "where ( ol_o_id >= 20 and ol_o_id <= 100  )" +
        		                   " or (ol_o_id >= 105 and ol_o_id <= 200 )" +
        		                   " or (ol_o_id >= 210 and ol_o_id <= 290 )" +
        		                   "GROUP BY ol_number order by ol_number"
        		                   );
        addStmtProcedure("Query6","select  ol_number, sum(ol_amount) " +
                                    "from    order_line " +
                                    "where  ol_quantity between 1 and 100000 " +
                                    "group by ol_number order by ol_number"
                                    );
//        addStmtProcedure("Query6","se");
//        addStmtProcedure("InsertCustomer", "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "CUSTOMER.C_W_ID: 2");
//        addStmtProcedure("InsertWarehouse", "INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", "WAREHOUSE.W_ID: 0");
//        addStmtProcedure("InsertStock", "INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "STOCK.S_W_ID: 1");
//        addStmtProcedure("InsertOrders", "INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "ORDERS.O_W_ID: 2");
//        addStmtProcedure("InsertOrderLine", "INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "ORDER_LINE.OL_W_ID: 2");
//        addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");
//        addStmtProcedure("InsertItem", "INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);");
//        addStmtProcedure("InsertHistory", "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "HISTORY.H_W_ID: 4");
//        addStmtProcedure("InsertDistrict", "INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "DISTRICT.D_W_ID: 1");
//        addStmtProcedure("InsertCustomerName", "INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);");
    }

    /**
     * Add the TPC-C partitioning to the VoltProjectBuilder base class.
     */
    public void addDefaultPartitioning() {
        for (String pair[] : partitioning) {
            addPartitionInfo(pair[0], pair[1]);
        }
    }

    /**
     * Add the TPC-C schema to the VoltProjectBuilder base class.
     */
    public void addDefaultSchema() {
        addSchema(this.getDDLURL(true));
    }

    public void addDefaultELT() {
        addELT("org.voltdb.elt.connectors.VerticaConnector", true, null, null);

        /* Fixed after the loader completes. */
        // addELTTable("WAREHOUSE", false);
        // addELTTable("DISTRICT", false);
        // addELTTable("ITEM", false);
        // addELTTable("CUSTOMER", false);
        // addELTTable("CUSTOMER_NAME", false);
        // addELTTable("STOCK", false);

        /* Modified by actual benchmark: approx 6.58 ins/del per txn. */
        // addELTTable("HISTORY", false);     // 1 insert per payment (43%)
        // addELTTable("ORDERS", false);      // 1 insert per new order (45%)
        // addELTTable("NEW_ORDER", false);   // 1 insert per new order; 10 deletes per delivery (4%)
        addELTTable("ORDER_LINE", false);     // 10 inserts per new order
    }

    @Override
    public void addAllDefaults() {
        addDefaultProcedures();
        addDefaultPartitioning();
        addDefaultSchema();
        // addDefaultELT();
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
