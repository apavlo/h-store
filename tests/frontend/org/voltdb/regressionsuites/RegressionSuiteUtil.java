package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCLoader;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.LoadMultipartitionTable;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.tm1.TM1Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;

public abstract class RegressionSuiteUtil {

    /**
     * Populate the given table with a bunch of random tuples
     * @param client
     * @param catalog_tbl
     * @param rand
     * @param num_tuples
     * @throws IOException
     * @throws ProcCallException
     */
    public static void loadRandomData(Client client, Table catalog_tbl, Random rand, int num_tuples) throws IOException, ProcCallException {
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
        VoltType types[] = new VoltType[num_cols];
        int maxSizes[] = new int[num_cols];
        for (Column catalog_col : catalog_tbl.getColumns()) {
            int idx = catalog_col.getIndex();
            types[idx] = VoltType.get(catalog_col.getType());
            if (types[idx] == VoltType.STRING) {
                maxSizes[idx] = catalog_col.getSize();
            }
        } // FOR
        
        for (int i = 0; i < num_tuples; i++) {
            Object row[] = new Object[num_cols];
            for (int col = 0; col < num_cols; col++) {
                row[col] = VoltTypeUtil.getRandomValue(types[col], rand);
                if (types[col] == VoltType.STRING) {
                    if (row[col].toString().length() >= maxSizes[col]) {
                        row[col] = row[col].toString().substring(0, maxSizes[col]-1);
                    }
                }
            } // FOR (col)
            vt.addRow(row);
        } // FOR (row)
        // System.err.printf("Loading %d rows for %s\n%s\n\n", vt.getRowCount(), catalog_tbl, vt.toString());
        String procName = VoltSystemProcedure.procCallName(LoadMultipartitionTable.class);
        ClientResponse cr = client.callProcedure(procName, catalog_tbl.getName(), vt);
        TestCase.assertEquals(Status.OK, cr.getStatus());
    }

    public static final void initializeTPCCDatabase(final Catalog catalog, final Client client) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.WAREHOUSE_PER_PARTITION=true",
            "BENCHMARK.NUM_LOADTHREADS=1",
        };
        TPCCLoader loader = new TPCCLoader(args) {
            {
                this.setCatalog(catalog);
                this.setClientHandle(client);
            }
            @Override
            public Catalog getCatalog() {
                return (catalog);
            }
        };
        loader.load();
    }

    public static final void initializeTM1Database(final Catalog catalog, final Client client) throws Exception {
        String args[] = { "NOCONNECTIONS=true", };
        TM1Loader loader = new TM1Loader(args) {
            {
                this.setCatalog(catalog);
                this.setClientHandle(client);
            }
            @Override
            public Catalog getCatalog() {
                return (catalog);
            }
        };
        loader.load();
    }
    
}
