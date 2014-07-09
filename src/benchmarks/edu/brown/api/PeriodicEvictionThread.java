/**
 * 
 */
package edu.brown.api;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.sysprocs.EvictTuples;

/**
 * Special thread to execute the @EvictTuples sysproc and update
 * the BenchmarkInterests accordingly.
 * @author pavlo
 */
public class PeriodicEvictionThread implements Runnable, ProcedureCallback {
    private static final Logger LOG = Logger.getLogger(PeriodicEvictionThread.class);
    
    private final CatalogContext catalogContext;
    private final Client client;
    
    private final String procName = "@" + EvictTuples.class.getSimpleName();
    private final String tableNames[];
    private final long evictionSize[];
    private final Collection<BenchmarkInterest> printers;
    private final AtomicInteger callbacks = new AtomicInteger();
    
    public PeriodicEvictionThread(CatalogContext catalogContext, Client client, long blockSize, Collection<BenchmarkInterest> printers) {
        this.catalogContext = catalogContext;
        this.client = client;
        this.printers = printers;
        
        Collection<Table> evictables = this.catalogContext.getEvictableTables();
        this.tableNames = new String[evictables.size()];
        this.evictionSize = new long[this.tableNames.length];
        int i = 0;
        for (Table catalog_tbl : evictables) {
            this.tableNames[i] = catalog_tbl.getName();
            this.evictionSize[i] = blockSize;
            i++;
        } // FOR
    }
    
    @Override
    public void run() {
        if (this.callbacks.get() != 0) return;
        this.callbacks.set(catalogContext.numberOfPartitions);
        
        LOG.info("Invoking " + this.procName + " on " + catalogContext.numberOfPartitions + " partitions");
        
        // Let all our BenchmarkInterests know that we are doing an eviction now
        for (BenchmarkInterest b : this.printers) {
            b.markEvictionStart();
        } // FOR
        for (int p = 0; p < catalogContext.numberOfPartitions; p++) {
            try {
                this.client.callProcedure(this,
                                          this.procName,
                                          p, this.tableNames, null, this.evictionSize);
            } catch (Exception ex) {
                String msg = "Failed to invoke " + this.procName + " for partition #" + p;
                throw new RuntimeException(msg, ex);
            }
        } // FOR
    }

    @Override
    public void clientCallback(ClientResponse clientResponse) {
        if (LOG.isDebugEnabled())
            LOG.debug(clientResponse); 
        if (this.callbacks.decrementAndGet() == 0) {
            for (BenchmarkInterest b : this.printers) {
                b.markEvictionStop();
            } // FOR
        }
        
    }

}
