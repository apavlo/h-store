package edu.brown.hstore;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.jni.ExecutionEngine;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.internal.TableStatsRequestMessage;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.AbstractProcessingThread;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.MathUtil;

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

    // ----------------------------------------------------------------------------
    // INSTANCE MEMBERS
    // ----------------------------------------------------------------------------

    private final long availableMemory;
    private final double memoryThreshold;
    private final Collection<Table> evictableTables;
    
    /**
     * 
     */
    private final TableStatsRequestMessage statsMessage;
    
    /**
     * The amount of memory used at each partition
     * PartitionOffset -> Kilobytes
     */
    private final long partitionSizes[];
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    protected AntiCacheManager(HStoreSite hstore_site) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_ANTICACHE,
              new LinkedBlockingQueue<QueueEntry>(),
              false);
        
        // XXX: Do we want to use Runtime.getRuntime().totalMemory() instead?
        this.availableMemory = hstore_conf.site.memory;
        this.memoryThreshold = hstore_conf.site.anticache_threshold;
        this.evictableTables = CatalogUtil.getEvictableTables(hstore_site.getDatabase()); 
                
        this.partitionSizes = new long[hstore_site.getLocalPartitionIds().size()];
        Arrays.fill(this.partitionSizes, 0);
        
        this.statsMessage = new TableStatsRequestMessage(CatalogUtil.getDataTables(hstore_site.getDatabase()));
        this.statsMessage.getObservable().addObserver(new EventObserver<VoltTable>() {
            @Override
            public void update(EventObservable<VoltTable> o, VoltTable vt) {
                AntiCacheManager.this.updatePartitionStats(vt);
            }
        });
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION PROCESSING
    // ----------------------------------------------------------------------------
    
    @Override
    protected void processingCallback(QueueEntry next) {
        // We need to get the EE handle for the partition that this txn
        // needs to have read in some blocks from disk
        PartitionExecutor executor = hstore_site.getPartitionExecutor(next.partition);
        ExecutionEngine ee = executor.getExecutionEngine();
        
        // We can now tell it to read in the blocks that this txn needs
        // Note that we are doing this without checking whether another txn is already
        // running. That's because reading in unevicted tuples is a two-stage process.
        // First we read the blocks from disk in a standalone buffer. Then once we 
        // know that all of the tuples that we need are there, we will requeue the txn, 
        // which knows that it needs to tell the EE to merge in the results from this buffer
        // before it executes anything.
        // 
        // TODO: We may want to create a HStoreConf option that allows to dispatch this
        //       request asynchronously per partition. For now we're just going to
        //       block the AntiCacheManager until each of the requests are finished
        try {
            ee.antiCacheReadBlocks(next.catalog_tbl, next.block_ids);
        } catch (SerializableException ex) {
            
        }
        
        // Now go ahead and requeue our transaction
        next.ts.setAntiCacheMergeTable(next.catalog_tbl);
        this.hstore_site.transactionStart(next.ts, next.ts.getBasePartition());
        
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
        
        // TODO: We should check whether there are any other txns that are also blocked waiting
        // for these blocks. This will ensure that we don't try to read in blocks twice.
        
        return (this.queue.offer(e));
    }
    
    // ----------------------------------------------------------------------------
    // EVICTION INITIATION
    // ----------------------------------------------------------------------------
    
    protected void checkEviction() {
        double usage = (MathUtil.sum(this.partitionSizes)/1024d) / (double)this.availableMemory;
        if (usage >= this.memoryThreshold) {
            // TODO: Invoke our special sysproc that will tell the EE to evict some blocks
            //       to save us space.
            // TODO: Need to figure out what the optimal solution is for picking the block sizes
        }
    }
    
    // ----------------------------------------------------------------------------
    // MEMORY MANAGEMENT METHODS
    // ----------------------------------------------------------------------------
    
    protected void getPartitionSize(int partition) {
        
        // Queue up a utility work operation at the PartitionExecutor so
        // that we can get the total size of the partition
        hstore_site.getPartitionExecutor(partition).queueUtilityWork(this.statsMessage);
        
    }
    
    protected void updatePartitionStats(VoltTable vt) {
        int partition = (int)vt.getLong("PARTITION_ID");
        long totalSizeKb = 0;
        vt.resetRowPosition();
        int memory_idx = -1;
        while (vt.advanceRow()) {
            if (memory_idx == -1) {
                vt.getColumnIndex("TUPLE_DATA_MEMORY");
            }
            totalSizeKb += vt.getLong(memory_idx);
        } // WHILE
        
        // TODO: If the total size is greater than some threshold, then
        //       we need to initiate the eviction process
        int offset = hstore_site.getLocalPartitionOffset(partition);
        if (debug.get())
            LOG.debug(String.format("Partition #%d Size - New:%dkb / Old:%dkb",
                                    partition, totalSizeKb, this.partitionSizes[offset])); 
        this.partitionSizes[offset] = totalSizeKb;
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
