package edu.brown.hstore;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.jni.ExecutionEngine;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.AbstractProcessingThread;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.FileUtil;

/**
 * A high-level manager for the anti-cache feature
 * Most of the work is done down in the EE, so this is just an abstraction 
 * layer for now
 * @author pavlo
 */
public class AntiCacheManager extends AbstractProcessingThread<AntiCacheManager.QueueEntry> {
    private static final Logger LOG = Logger.getLogger(AntiCacheManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final Database catalog_db;
    
    protected class QueueEntry {
        final LocalTransaction ts;
        final int partition;
        final Table catalog_tbl;
        final short block_ids[];
        
        public QueueEntry(LocalTransaction ts, int partition, Table catalog_tbl, short block_ids[]) {
            this.ts = ts;
            this.partition = partition;
            this.catalog_tbl = catalog_tbl;
            this.block_ids = block_ids;
        }
    }
    
    protected AntiCacheManager(HStoreSite hstore_site) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_ANTICACHE,
              new LinkedBlockingQueue<QueueEntry>(),
              false);
        this.catalog_db = hstore_site.getDatabase();
    }
    
    @Override
    protected void processingCallback(QueueEntry next) {
        // We need to get the EE handle for the partition that this txn
        // needs to have read in some blocks from disk
        PartitionExecutor executor = hstore_site.getPartitionExecutor(next.partition);
        ExecutionEngine ee = executor.getExecutionEngine();
        
        // We can now tell it to read in the blocks that we want
        // TODO: We need to make sure that this is safe to do when there 
        // TODO: We may want to create a HStoreConf option that allows to dispatch this
        //       request asynchronously per partition. For now we're just going to
        //       block the AntiCacheManager until each of the requests are finished
        
        
    }
    
    @Override
    protected void removeCallback(QueueEntry next) {
        this.hstore_site.transactionReject(next.ts, Status.ABORT_GRACEFUL);
    }

    
    /**
     * Queue a transaction that needs to wait until the evicted blocks at the target Table 
     * are read back in at the given partition. This is a non-blocking call.
     * The AntiCacheManager will figure out when it's ready to get these blocks back in
     * <B>Note:</B> The given LocalTransaction handle must have been already started. 
     * @param ts
     * @param partition
     * @param catalog_tbl
     * @param block_ids
     */
    public boolean queue(LocalTransaction ts, int partition, Table catalog_tbl, short block_ids[]) {
        QueueEntry e = new QueueEntry(ts, partition, catalog_tbl, block_ids);
        return (this.queue.offer(e));
    }
    
    // ----------------------------------------------------------------------------
    // STATIC HELPER METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns the directory where the EE should store the anti-cache
     * database for this PartitionExecutor
     * @return
     */
    public static File getDatabaseDir(PartitionExecutor executor) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        Database catalog_db = CatalogUtil.getDatabase(executor.getCatalogSite());
        
        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.anticache_dir + 
                                            File.separatorChar +
                                            catalog_db.getProject());
        synchronized (AntiCacheManager.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNCH
        
        // Then each partition will have a separate directory inside of the base one
        String partitionName = HStoreThreadManager.formatPartitionName(executor.getSiteId(),
                                                                       executor.getPartitionId());

        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);
        FileUtil.makeDirIfNotExists(dbDirPath);
        
        // TODO: What do we do if the directory already exists?
        //       There should be an HStoreConf that says we should delete it first
        
        return (dbDirPath);
    }
    
}
