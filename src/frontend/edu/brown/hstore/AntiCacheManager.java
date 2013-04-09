package edu.brown.hstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.EvictedTupleAccessException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.sysprocs.EvictTuples;
import org.voltdb.utils.VoltTableUtil;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.internal.UtilityWorkMessage.TableStatsRequestMessage;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.util.AbstractProcessingRunnable;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.FileUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringUtil;

/**
 * A high-level manager for the anti-cache feature Most of the work is done down in the EE,
 * so this is just an abstraction layer for now
 * @author pavlo
 * @author jdebrabant
 */
public class AntiCacheManager extends AbstractProcessingRunnable<AntiCacheManager.QueueEntry> {
    private static final Logger LOG = Logger.getLogger(AntiCacheManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static long DEFAULT_MAX_MEMORY_SIZE_MB = 500;
    public static final int MAX_BLOCKS_TO_EVICT_EACH_EVICTION = 1000;

    public static final int TOTAL_BLOCKS_TO_EVICT = 200000;    
    public static final int BLOCK_SIZE = 262144; // 256 KB

    // ----------------------------------------------------------------------------
    // INTERNAL QUEUE ENTRY
    // ----------------------------------------------------------------------------

    protected class QueueEntry {
        final LocalTransaction ts;
        final Table catalog_tbl;
        final int partition;
        final short block_ids[];
        final int tuple_offsets[]; 

        public QueueEntry(LocalTransaction ts, int partition, Table catalog_tbl, short block_ids[], int tuple_offsets[]) {
            this.ts = ts;
            this.partition = partition;
            this.catalog_tbl = catalog_tbl;
            this.block_ids = block_ids;
            this.tuple_offsets = tuple_offsets;
        }
        @Override
        public String toString() {
            return String.format("%s{%s / Table:%s / Partition:%d / BlockIds:%s}",
                                 this.getClass().getSimpleName(), this.ts,
                                 this.catalog_tbl.getName(), this.partition,
                                 Arrays.toString(this.block_ids));
        }
    }

    // ----------------------------------------------------------------------------
    // INSTANCE MEMBERS
    // ----------------------------------------------------------------------------

    private final long availableMemory;
    private final double evictionMemoryThreshold;
    private final Collection<Table> evictableTables;
    private boolean evicting = false;
    private final AntiCacheManagerProfiler profilers[];
    private boolean firstEviction = true;
    
    private long totalDataSize = 0; 

    /**
     * 
     */
    private final TableStatsRequestMessage statsMessage;

    /**
     * The amount of memory used at each local partition
     */
    private final long partitionSizes[];
    
    private final long partitionEvictions[];
    
    private final long partitionFetches[]; 

    /**
     * Thread that is periodically executed to check whether the amount of memory used by this HStoreSite is over the
     * threshold
     */
    private final ExceptionHandlingRunnable memoryMonitor = new ExceptionHandlingRunnable() {
        @Override
        public void runImpl() {
            try {
                // update all the partition sizes
                for (int partition : hstore_site.getLocalPartitionIds().values()) {
                    getPartitionSize(partition);
                } // FOR

                // check to see if we should start eviction
                if (hstore_conf.site.anticache_enable && checkEviction()) {
                    executeEviction();
                }

            } catch (Throwable ex) {
                ex.printStackTrace();
            }
        }
    };

    /**
     * Local RpcCallback that will notify us when one of our eviction sysprocs is finished
     */
    private final RpcCallback<ClientResponseImpl> evictionCallback = new RpcCallback<ClientResponseImpl>() {
        @Override
        public void run(ClientResponseImpl parameter) {
            int partition = parameter.getBasePartition();
            if (hstore_conf.site.anticache_profiling) profilers[partition].eviction_time.stopIfStarted();
            
            LOG.info(String.format("Eviction Response for Partition %02d:\n%s",
                     partition, VoltTableUtil.format(parameter.getResults())));

            LOG.info(String.format("Execution Time: %.1f sec\n", parameter.getClusterRoundtrip() / 1000d));

            // XXX: This doesn't make sense, since there will be a callback invoked for 
            //      every partition whenever we do an eviction.
            evicting = false;
        }
    };

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    protected AntiCacheManager(HStoreSite hstore_site) {
        super(hstore_site,
              HStoreConstants.THREAD_NAME_ANTICACHE,
              new LinkedBlockingQueue<QueueEntry>(),
              false);

        // XXX: Do we want to use Runtime.getRuntime().maxMemory() instead?
        // XXX: We could also use Runtime.getRuntime().totalMemory() instead of getting table stats
        // this.availableMemory = Runtime.getRuntime().maxMemory();
        this.availableMemory = hstore_conf.site.memory * 1024l * 1024l;
        if (debug.val)
            LOG.debug("AVAILABLE MEMORY: " + StringUtil.formatSize(this.availableMemory));

        CatalogContext catalogContext = hstore_site.getCatalogContext();
        this.evictionMemoryThreshold = hstore_conf.site.anticache_threshold;
        //this.evictionMemoryThreshold = DEFAULT_MAX_MEMORY_SIZE_MB;
        this.evictableTables = catalogContext.getEvictableTables();

        int num_partitions = hstore_site.getCatalogContext().numberOfPartitions;
        this.partitionSizes = new long[num_partitions];
        this.partitionEvictions = new long[num_partitions];
        this.partitionFetches = new long[num_partitions];
        Arrays.fill(this.partitionSizes, 0);
        Arrays.fill(this.partitionEvictions, 0);
        Arrays.fill(this.partitionFetches, 0);

        this.profilers = new AntiCacheManagerProfiler[num_partitions];
        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            this.profilers[partition] = new AntiCacheManagerProfiler();
        } // FOR
        
        this.statsMessage = new TableStatsRequestMessage(catalogContext.getDataTables());
        this.statsMessage.getObservable().addObserver(new EventObserver<VoltTable>() {
            @Override
            public void update(EventObservable<VoltTable> o, VoltTable vt) {
                AntiCacheManager.this.updatePartitionStats(vt);
            }
        });
    }

    public Collection<Table> getEvictableTables() {
        return (this.evictableTables);
    }

    public Runnable getMemoryMonitorThread() {
        return this.memoryMonitor;
    }

    // ----------------------------------------------------------------------------
    // TRANSACTION PROCESSING
    // ----------------------------------------------------------------------------

    @Override
    protected void processingCallback(QueueEntry next) {
        assert(next.ts.isInitialized()) :
            String.format("Unexpected uninitialized transaction handle: %s", next);
        assert(next.partition == next.ts.getBasePartition()) :
            String.format("The base partition for %s is %d but we want to fetch a block for partition %d: %s",
                          next.ts, next.ts.getBasePartition(), next.partition, next);
        LOG.debug("Processing " + next);
        
        // We need to get the EE handle for the partition that this txn
        // needs to have read in some blocks from disk
        PartitionExecutor executor = hstore_site.getPartitionExecutor(next.partition);
        ExecutionEngine ee = executor.getExecutionEngine();

        boolean merge_needed = true; 

        // We can now tell it to read in the blocks that this txn needs
        // Note that we are doing this without checking whether another txn is already
        // running. That's because reading in unevicted tuples is a two-stage process.
        // First we read the blocks from disk in a standalone buffer. Then once we
        // know that all of the tuples that we need are there, we will requeue the txn,
        // which knows that it needs to tell the EE to merge in the results from this buffer
        // before it executes anything.
        //
        // TODO: We may want to create a HStoreConf option that allows to dispatch this
        // request asynchronously per partition. For now we're just going to
        // block the AntiCacheManager until each of the requests are finished
        if (hstore_conf.site.anticache_profiling) 
            this.profilers[next.partition].retrieval_time.start();
        try {
            LOG.debug(String.format("Asking EE to read in evicted blocks from table %s on partition %d: %s",
                     next.catalog_tbl.getName(), next.partition, Arrays.toString(next.block_ids)));

            ee.antiCacheReadBlocks(next.catalog_tbl, next.block_ids, next.tuple_offsets);

            LOG.debug(String.format("Finished reading blocks from partition %d",
                     next.partition));
        } catch (SerializableException ex) {
            LOG.info("Caught unexpected SerializableException while reading anti-cache block.", ex);

            merge_needed = false; 
        } finally {
            if (hstore_conf.site.anticache_profiling) 
                this.profilers[next.partition].retrieval_time.stopIfStarted();
        }
        
        // HACK HACK HACK HACK HACK HACK
        // We need to get a new txnId for ourselves, since the one that we
        // were given before is now probably too far in the past
        this.hstore_site.getTransactionInitializer().resetTransactionId(next.ts, next.partition);
        // Now go ahead and requeue our transaction

//        if(merge_needed)
            next.ts.setAntiCacheMergeTable(next.catalog_tbl);

        this.hstore_site.transactionInit(next.ts);
//        this.hstore_site.transactionReject(next.ts, Status.ABORT_GRACEFUL);
    }

    @Override
    protected void removeCallback(QueueEntry next) {
        this.hstore_site.transactionReject(next.ts, Status.ABORT_GRACEFUL);
    }

    /**
     * Queue a transaction that needs to wait until the evicted blocks at the target Table are read back in at the given
     * partition. This is a non-blocking call. The AntiCacheManager will figure out when it's ready to get these blocks
     * back in <B>Note:</B> The given LocalTransaction handle must not have been already started.
     * 
     * @param ts
     *            - A new LocalTransaction handle created from an aborted transaction
     * @param partition
     *            - The partitionId that we need to read evicted tuples from
     * @param catalog_tbl
     *            - The table catalog
     * @param block_ids
     *            - The list of blockIds that need to be read in for the table
     */
    public boolean queue(LocalTransaction ts, int partition, Table catalog_tbl, short block_ids[], int tuple_offsets[]) {
//        if (hstore_conf.site.anticache_profiling) {
            assert(ts.getPendingError() != null) :
                String.format("Missing original %s for %s", EvictedTupleAccessException.class.getSimpleName(), ts);
            assert(ts.getPendingError() instanceof EvictedTupleAccessException) :
                String.format("Unexpected error for %s: %s", ts, ts.getPendingError().getClass().getSimpleName());
            this.profilers[partition].restarted_txns++;
            this.profilers[partition].addEvictedAccess(ts, (EvictedTupleAccessException)ts.getPendingError());
//        }

        QueueEntry e = new QueueEntry(ts, partition, catalog_tbl, block_ids, tuple_offsets);

        // TODO: We should check whether there are any other txns that are also blocked waiting
        // for these blocks. This will ensure that we don't try to read in blocks twice.

        //LOG.info("Queueing a transaction for partition " + partition);
        return (this.queue.offer(e));
    }

    // ----------------------------------------------------------------------------
    // EVICTION INITIATION
    // ----------------------------------------------------------------------------

    /**
     * Check whether the amount of memory used by this HStoreSite is above the eviction threshold.
     */
    protected boolean checkEviction() {

        long total_size_kb = MathUtil.sum(this.partitionSizes);
        long total_evicted_blocks = MathUtil.sum(this.partitionEvictions);
        long total_fetched_blocks = MathUtil.sum(this.partitionFetches);

        this.totalDataSize = (int)(total_size_kb / 1024);

        LOG.info("Current Memory Usage: " + this.totalDataSize + " MB");
        LOG.info("Blocks Currently Evicted: " + total_evicted_blocks);
        LOG.info("Total Blocks Fetched: " + total_fetched_blocks);

        if(firstEviction)
        {
            // only start eviction once we've passed this threshold
            if(this.totalDataSize < DEFAULT_MAX_MEMORY_SIZE_MB)
            {
                return false;
            }
            firstEviction = false;
        }

        return(total_evicted_blocks < TOTAL_BLOCKS_TO_EVICT);
    }

    protected long blocksToEvict()
    {
        long blocks_currently_evicted = MathUtil.sum(this.partitionEvictions);
        long blocks_to_evict = 0;

        // I think this happens because some blocks are unevicted twice
        if(blocks_currently_evicted < 0 || TOTAL_BLOCKS_TO_EVICT - blocks_currently_evicted > MAX_BLOCKS_TO_EVICT_EACH_EVICTION) 
            return MAX_BLOCKS_TO_EVICT_EACH_EVICTION;

        return(TOTAL_BLOCKS_TO_EVICT - blocks_currently_evicted);
    }

    protected void executeEviction() {
        
        // Invoke our special sysproc that will tell the EE to evict some blocks
        // to save us space.
        this.evicting = true;

        String tableNames[] = new String[this.evictableTables.size()];
        long evictBytes[] = new long[this.evictableTables.size()];
        int evictBlocks[] = new int[this.evictableTables.size()]; 
        int i = 0;

        long blocks_to_evict = blocksToEvict(); 
        if(blocks_to_evict <= 0)
            return;

        LOG.info("Evicting " + blocks_to_evict + " blocks."); 

        // initialize params
        for (Table catalog_tbl : this.evictableTables) {
            tableNames[i] = catalog_tbl.getName();
            //evictBytes[i] = hstore_conf.site.anticache_block_size;
            evictBytes[i] = BLOCK_SIZE; 

            if(blocks_to_evict < hstore_site.getLocalPartitionIds().size()) {  // make sure we evict at least 1 block from each partition
                evictBlocks[i] = 1;
            }
            else {
                evictBlocks[i] = (int)blocks_to_evict / hstore_site.getLocalPartitionIds().size();
            }
            i++;
        } // FOR

        Object params[] = new Object[] { HStoreConstants.NULL_PARTITION_ID, tableNames, evictBytes, evictBlocks};
        String procName = VoltSystemProcedure.procCallName(EvictTuples.class);

        StoredProcedureInvocation invocation = new StoredProcedureInvocation(1, procName, params);
        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            if (hstore_conf.site.anticache_profiling)
            this.profilers[partition].eviction_time.start();

            invocation.getParams().toArray()[0] = partition;
            ByteBuffer b = null;
            try {
                b = ByteBuffer.wrap(FastSerializer.serialize(invocation));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            this.hstore_site.invocationProcess(b, this.evictionCallback);
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
        long totalSizeKb = 0;
        int partition = -1;
        int memory_idx = -1;
        long num_blocks_evicted = 0;
        long num_blocks_fetched = 0;
        vt.resetRowPosition();
        while (vt.advanceRow()) {
            if (memory_idx == -1) {
                partition = (int) vt.getLong("PARTITION_ID");
                memory_idx = vt.getColumnIndex("TUPLE_DATA_MEMORY");
            }
            assert(memory_idx >= 0);
            num_blocks_evicted += vt.getLong("BLOCKS_EVICTED");
            num_blocks_fetched += vt.getLong("BLOCKS_READ");
            totalSizeKb += vt.getLong(memory_idx);
        } // WHILE

        // TODO: If the total size is greater than some threshold, then
        // we need to initiate the eviction process
        if (debug.val)
            LOG.debug(String.format("Partition #%d Size - New:%dkb / Old:%dkb",
                      partition, totalSizeKb, this.partitionSizes[partition]));
        this.partitionEvictions[partition] = num_blocks_evicted;
        this.partitionFetches[partition] = num_blocks_fetched;
        this.partitionSizes[partition] = totalSizeKb;
    }

    // ----------------------------------------------------------------------------
    // STATIC HELPER METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns the directory where the EE should store the anti-cache database
     * for this PartitionExecutor
     * @return
     */
    public static File getDatabaseDir(PartitionExecutor executor) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        Database catalog_db = CatalogUtil.getDatabase(executor.getPartition());

        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.anticache_dir +
                                            File.separatorChar +
                                            catalog_db.getProject());
        synchronized (AntiCacheManager.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNC

        // Then each partition will have a separate directory inside of the base one
        String partitionName = HStoreThreadManager.formatPartitionName(executor.getSiteId(),
                                                                       executor.getPartitionId());
        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);
        if (hstore_conf.site.anticache_reset) {
            LOG.warn(String.format("Deleting anti-cache directory '%s'", dbDirPath));
            FileUtil.deleteDirectory(dbDirPath);
        }
        FileUtil.makeDirIfNotExists(dbDirPath);

        return (dbDirPath);
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    public class Debug implements DebugContext {
        public AntiCacheManagerProfiler getProfiler(int partition) {
            return (profilers[partition]);
        }
        public double getevictionMemoryThreshold() {
            return (evictionMemoryThreshold);
        }
        public boolean isEvicting() {
            return (evicting);
        }
    }
    
    private AntiCacheManager.Debug cachedDebugContext;
    public AntiCacheManager.Debug getDebugContext() {
        if (cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            cachedDebugContext = new AntiCacheManager.Debug();
        }
        return cachedDebugContext;
    }

}
