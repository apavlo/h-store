/**
 * 
 */
package edu.brown.api;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.sysprocs.EvictTuples;

import edu.brown.api.BenchmarkController.BenchmarkInterest;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.AntiCacheManager;

/**
 * Special thread to execute the @EvictTuples sysproc and update
 * the BenchmarkInterests accordingly.
 * @author pavlo
 */
public class PeriodicEvictionThread implements Runnable, ProcedureCallback {
    private final Database database;
    private final Client client;
    
    private final String procName = "@" + EvictTuples.class.getSimpleName();
    private final String tableNames[];
    private final long evictionSize[];
    private final BenchmarkInterest results[];
    private final AtomicInteger callbacks = new AtomicInteger();
    
    public PeriodicEvictionThread(Database database, Client client, BenchmarkInterest...results) {
        this.database = database;
        this.client = client;
        this.results = results;
        
        Collection<Table> evictables = CatalogUtil.getEvictableTables(this.database);
        this.tableNames = new String[evictables.size()];
        this.evictionSize = new long[this.tableNames.length];
        int i = 0;
        for (Table catalog_tbl : evictables) {
            this.tableNames[i] = catalog_tbl.getName();
            this.evictionSize[i] = AntiCacheManager.DEFAULT_EVICTED_BLOCK_SIZE;
            i++;
        } // FOR
    }
    
    @Override
    public void run() {
        int num_partitions = CatalogUtil.getNumberOfPartitions(database);
        this.callbacks.set(num_partitions);
        
        // Let all our BenchmarkInterests know that we are doing an eviction now
        for (BenchmarkInterest b : this.results) {
            b.markEvictionStart();
        } // FOR
        for (int p = 0; p < num_partitions; p++) {
            try {
                this.client.callProcedure(this,
                                          this.procName,
                                          p, this.tableNames, this.evictionSize);
            } catch (Exception ex) {
                String msg = "Failed to invoke " + this.procName + " for partition #" + p;
                throw new RuntimeException(msg, ex);
            }
        } // FOR
    }

    @Override
    public void clientCallback(ClientResponse clientResponse) {
        if (this.callbacks.decrementAndGet() == 0) {
            for (BenchmarkInterest b : this.results) {
                b.markEvictionStop();
            } // FOR
        }
        
    }

}
