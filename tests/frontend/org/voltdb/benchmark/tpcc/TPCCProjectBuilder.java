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

import org.voltdb.benchmark.tpcc.procedures.GetTableCounts;
import org.voltdb.benchmark.tpcc.procedures.LoadWarehouse;
import org.voltdb.benchmark.tpcc.procedures.LoadWarehouseReplicated;
import org.voltdb.benchmark.tpcc.procedures.ResetWarehouse;
import org.voltdb.benchmark.tpcc.procedures.SelectAll;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerIdC;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerIdW;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerNameC;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerNameW;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Catalog;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

import edu.brown.benchmark.AbstractProjectBuilder;

/**
 * A subclass of VoltProjectBuilder that already knows about all of the
 * procedures, schema and partitioning info for TPC-C. It also contains
 * some helper code for other tests that use part of TPC-C.
 *
 */
public class TPCCProjectBuilder extends AbstractProjectBuilder {

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
        SelectAll.class,
        ResetWarehouse.class,
        LoadWarehouse.class,
        LoadWarehouseReplicated.class,
        GetTableCounts.class,

        // We shouldn't be calling these, but they're here for now...
        paymentByCustomerIdC.class,
        paymentByCustomerNameC.class,
        paymentByCustomerIdW.class,
        paymentByCustomerNameW.class,
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

    public static final URL ddlURL = TPCCProjectBuilder.class.getResource("tpcc-ddl.sql");
    public static final URL ddlFkeysURL = TPCCProjectBuilder.class.getResource("tpcc-ddl-fkeys.sql");

    public TPCCProjectBuilder() {
        super("tpcc", TPCCProjectBuilder.class, PROCEDURES, partitioning, null, true);
    }
    
    /**
     * Add the TPC-C procedures to the VoltProjectBuilder base class.
     */
    public void addDefaultProcedures() {
        addProcedures(PROCEDURES);
        addStmtProcedure("InsertCustomer", "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "CUSTOMER.C_W_ID: 2");
        addStmtProcedure("InsertWarehouse", "INSERT INTO WAREHOUSE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", "WAREHOUSE.W_ID: 0");
        addStmtProcedure("InsertStock", "INSERT INTO STOCK VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "STOCK.S_W_ID: 1");
        addStmtProcedure("InsertOrders", "INSERT INTO ORDERS VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "ORDERS.O_W_ID: 2");
        addStmtProcedure("InsertOrderLine", "INSERT INTO ORDER_LINE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "ORDER_LINE.OL_W_ID: 2");
        addStmtProcedure("InsertNewOrder", "INSERT INTO NEW_ORDER VALUES (?, ?, ?);", "NEW_ORDER.NO_W_ID: 2");
        addStmtProcedure("InsertItem", "INSERT INTO ITEM VALUES (?, ?, ?, ?, ?);");
        addStmtProcedure("InsertHistory", "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?);", "HISTORY.H_W_ID: 4");
        addStmtProcedure("InsertDistrict", "INSERT INTO DISTRICT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", "DISTRICT.D_W_ID: 1");
        addStmtProcedure("InsertCustomerName", "INSERT INTO CUSTOMER_NAME VALUES (?, ?, ?, ?, ?);");
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
        addSchema(ddlFkeysURL);
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
        String catalogJar = testDir + File.separator + "tpcc-jni.jar";

        addSchema(fkeys ? ddlFkeysURL : ddlURL);
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
