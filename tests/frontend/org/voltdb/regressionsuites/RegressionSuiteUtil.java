package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.voltdb.CatalogContext;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCLoader;
import org.voltdb.benchmark.tpcc.TPCCSimulation;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.AdHoc;
import org.voltdb.sysprocs.LoadMultipartitionTable;
import org.voltdb.sysprocs.SetConfiguration;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.tm1.TM1Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.rand.DefaultRandomGenerator;

public abstract class RegressionSuiteUtil {

    static final double SCALEFACTOR = 0.0001;
    private static final DefaultRandomGenerator rng = new DefaultRandomGenerator(0);
    
    public static ClientResponse setHStoreConf(Client client, String paramName, Object paramValue) throws Exception {
        assert(HStoreConf.isConfParameter(paramName)) :
            "Invalid HStoreConf parameter '" + paramName + "'";
        String procName = VoltSystemProcedure.procCallName(SetConfiguration.class);
        String confParams[] = {paramName};
        String confValues[] = {paramValue.toString()};
        ClientResponse cresponse = client.callProcedure(procName, confParams, confValues);
        assert(cresponse.getStatus() == Status.OK) : cresponse.toString();
        return (cresponse);
    }
    
    public static ClientResponse getStats(Client client, SysProcSelector statsType) throws Exception {
        String procName = VoltSystemProcedure.procCallName(Statistics.class);
        Object params[] = { statsType.name(), 0 };
        ClientResponse cresponse = client.callProcedure(procName, params);
        assert(cresponse.getStatus() == Status.OK) : cresponse.toString();
        return (cresponse);
    }
    
    public static ClientResponse sql(Client client, String sql) throws IOException, ProcCallException {
        String procName = VoltSystemProcedure.procCallName(AdHoc.class);
        ClientResponse cresponse = client.callProcedure(procName, sql);
        assert(cresponse.getStatus() == Status.OK) : cresponse.toString();
        return (cresponse);
    }
    
    public static ClientResponse load(Client client, Table tbl, VoltTable data) throws IOException, ProcCallException {
        String procName = VoltSystemProcedure.procCallName(LoadMultipartitionTable.class);
        ClientResponse cresponse = client.callProcedure(procName, tbl.getName(), data);
        assert(cresponse.getStatus() == Status.OK) : cresponse.toString();
        return (cresponse);
    }
    
    public static long getRowCount(Client client, Table tbl) throws Exception {
        ClientResponse cresponse = getStats(client, SysProcSelector.TABLE);
        VoltTable result = cresponse.getResults()[0];
        
        long count = 0;
        boolean found = false;
        while (result.advanceRow()) {
            if (tbl.getName().equalsIgnoreCase(result.getString("TABLE_NAME"))) {
                found = true;
                count += result.getLong("TUPLE_COUNT");
                if (tbl.getIsreplicated()) break;
            }
        } // WHILE
        if (found == false) {
            throw new IllegalArgumentException("Invalid table '" + tbl + "'");
        }
        return (count);
    }
    
    public static Map<String, Map<Integer, Long>> getRowCountPerPartition(Client client) throws Exception {
        ClientResponse statsResponse = RegressionSuiteUtil.getStats(client, SysProcSelector.TABLE);
        assert(Status.OK == statsResponse.getStatus());
        VoltTable statsResult = statsResponse.getResults()[0];
        Map<String, Map<Integer, Long>> rowCounts = new HashMap<String, Map<Integer,Long>>();
        while (statsResult.advanceRow()) {
            String tableName = statsResult.getString("TABLE_NAME");
            int partition = (int)statsResult.getLong("PARTITION_ID");
            long tupleCount = statsResult.getLong("TUPLE_COUNT");
            Map<Integer, Long> cnts = rowCounts.get(tableName);
            if (cnts == null) {
                cnts = new HashMap<Integer, Long>();
                rowCounts.put(tableName, cnts);
            }
            cnts.put(partition, tupleCount);
        } // WHILE
        return (rowCounts);
    }
    
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
        load(client, catalog_tbl, vt);
    }

    public static final void initializeTPCCDatabase(final CatalogContext catalogContext, final Client client) throws Exception {
        initializeTPCCDatabase(catalogContext, client, false);
    }
    
    public static final void initializeTPCCDatabase(final CatalogContext catalogContext, final Client client, boolean scaleItems) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.WAREHOUSE_PER_PARTITION=true",
            "BENCHMARK.NUM_LOADTHREADS=1",
            "BENCHMARK.SCALE_ITEMS="+scaleItems,
        };
        TPCCLoader loader = new TPCCLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        loader.load();
    }

    public static final void initializeTM1Database(final CatalogContext catalogContext, final Client client) throws Exception {
        String args[] = { "NOCONNECTIONS=true", };
        TM1Loader loader = new TM1Loader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        loader.load();
    }
    
    protected static Object[] generateNewOrder(int num_warehouses, boolean dtxn, int w_id, int d_id) throws Exception {
        short supply_w_id;
        if (dtxn) {
            int start = TPCCConstants.STARTING_WAREHOUSE;
            int stop = TPCCConstants.STARTING_WAREHOUSE + num_warehouses;
            supply_w_id = TPCCSimulation.generatePairedWarehouse(w_id, start, stop);
            assert(supply_w_id != w_id);
        } else {
            supply_w_id = (short)w_id;
        }
        
        // ORDER_LINE ITEMS
        int num_items = rng.number(TPCCConstants.MIN_OL_CNT, TPCCConstants.MAX_OL_CNT);
        int item_ids[] = new int[num_items];
        short supwares[] = new short[num_items];
        int quantities[] = new int[num_items];
        for (int i = 0; i < num_items; i++) { 
            item_ids[i] = rng.nextInt(100);
            supwares[i] = (i == 1 && dtxn ? supply_w_id : (short)w_id);
            quantities[i] = 1;
        } // FOR
        
        Object params[] = {
            (short)w_id,        // W_ID
            (byte)d_id,         // D_ID
            1,                  // C_ID
            new TimestampType(),// TIMESTAMP
            item_ids,           // ITEM_IDS
            supwares,           // SUPPLY W_IDS
            quantities          // QUANTITIES
        };
        return (params);
    }

    protected static Object[] generateNewOrder(int num_warehouses, boolean dtxn, short w_id) throws Exception {
        int d_id = rng.number(1, TPCCConstants.DISTRICTS_PER_WAREHOUSE);
        return generateNewOrder(num_warehouses, dtxn, w_id, d_id);
    }
    
}
