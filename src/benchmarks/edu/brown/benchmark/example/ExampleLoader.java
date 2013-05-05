package edu.brown.benchmark.example;

import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.catalog.CatalogUtil;

public class ExampleLoader extends BenchmarkComponent {

    public static void main(String args[]) throws Exception {
        BenchmarkComponent.main(ExampleLoader.class, args, true);
    }

    public ExampleLoader(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }

    @Override
    public void runLoop() {
        CatalogContext catalog = this.getCatalogContext();
        for (Table catalog_tbl : catalog.database.getTables()) {
            // Create an empty VoltTable handle and then populate it in batches
            // to be sent to the DBMS
            VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
            try {
                // Makes a blocking call to @LoadMultipartitionTable sysproc in
                // order to load the contents of the VoltTable into the cluster
                this.loadVoltTable(catalog_tbl.getName(), table);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load data for " + catalog_tbl, e);
            }
        } // FOR
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // IGNORE: Only needed for Client
        return new String[] {};
    }
}
