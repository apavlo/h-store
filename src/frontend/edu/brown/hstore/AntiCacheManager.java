package edu.brown.hstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

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
import org.voltdb.types.AntiCacheEvictionPolicyType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.UnevictDataResponse;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.internal.UtilityWorkMessage.TableStatsRequestMessage;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.hstore.util.AbstractProcessingRunnable;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.AntiCacheManagerProfiler;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ExceptionHandlingRunnable;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

/**
 * A high-level manager for the anti-cache feature Most of the work is done down in the EE,
 * so this is just an abstraction layer for now
 * @author pavlo
 * @author jdebrabant
 */
public class AntiCacheManager extends AbstractProcessingRunnable<AntiCacheManager.QueueEntry> {
    private static final Logger LOG = Logger.getLogger(AntiCacheManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    private static final ReentrantLock lock = new ReentrantLock();

    public static ReentrantLock getLock() {
        return lock;
    }

    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

//    public static long DEFAULT_MAX_MEMORY_SIZE_MB = 500;
//    public static final long TOTAL_BYTES_TO_EVICT = 2400 * 1024 * 1024;
//    
//    public static final int MAX_BLOCKS_TO_EVICT_EACH_EVICTION = 2000;
//    public static final long TOTAL_BLOCKS_TO_EVICT = 1000;
//    public static final long BLOCK_SIZE = 262144; // 256 KB


    // ----------------------------------------------------------------------------
    // INTERNAL QUEUE ENTRY
    // ----------------------------------------------------------------------------

    protected class QueueEntry {
        final AbstractTransaction ts;
        final Table catalog_tbl;
        final int partition;
        final int block_ids[];
        final int tuple_offsets[]; 

        public QueueEntry(AbstractTransaction ts, int partition, Table catalog_tbl, int block_ids[], int tuple_offsets[]) {
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

    private final String[] evictableTables;
    protected int pendingEvictions = 0;
    /*
     *  Can't use a simple count because sometimes stats requests get lost and we must reissue them.
     *  Thus, we need to keep track of whether at least one stats request came back on a per-partition basis.
     */
    protected boolean pendingStatsUpdates[];

    private final AntiCacheManagerProfiler profilers[];
    private final AntiCacheEvictionPolicyType evictionDistributionPolicy;
    
    private final double UNEVICTION_RATIO_EMA_ALPHA = .1;
    private final double UNEVICTION_RATIO_CLUSTER_THRESHOLD = .1;
    private final double ACCESS_RATE_CLUSTER_THRESHOLD = .1;

    /**
     * 
     */
    private final TableStatsRequestMessage statsMessage;

    /**
     * The amount of memory used at each local partition
     */
    private final PartitionStats[] partitionStats;

    /**
     * Thread that is periodically executed to check whether the amount of memory used by this HStoreSite is over the
     * threshold
     */
    private final ExceptionHandlingRunnable memoryMonitor = new ExceptionHandlingRunnable() {
        @Override
        public void runImpl() {
            synchronized(AntiCacheManager.this) {
                try {
                    // update all the partition sizes
                	if (debug.val)
                	    LOG.warn("In mem monitor");
                    for (int partition : hstore_site.getLocalPartitionIds().values()) {
                    	if (debug.val)
                    	    LOG.warn("Updating partition stats");
                        getPartitionSize(partition);
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        }
    };

    private final ExceptionHandlingRunnable evictionExecutor = new ExceptionHandlingRunnable() {
        @Override
        public void runImpl() {
            //LOG.warn("We ran!!");
            synchronized(AntiCacheManager.this) {
                try {
                    //LOG.warn("We got the lock@!@!");
                    // check to see if we should start eviction
                    if (debug.val)
                        LOG.warn("Checking and evicting");
                    if (hstore_conf.site.anticache_enable && checkEviction()) {
                        executeEviction();
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
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

            synchronized(AntiCacheManager.this) {
                pendingEvictions--;
            };
        }
    };

    // Anticache levels
    private static int numDBs = 1;

    public static int getNumDBs() {
        return numDBs;
    }

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
        
        evictableTables = new String[catalogContext.getEvictableTables().size()];
        int i = 0;
        for (Table table : catalogContext.getEvictableTables()) {
        	if(!table.getBatchevicted()){
                evictableTables[i] = table.getName();
                i++;        		
        	}
        }

        AntiCacheEvictionPolicyType policy = AntiCacheEvictionPolicyType.get(hstore_conf.site.anticache_eviction_distribution);
        if (policy == null) {
            LOG.warn(String.format("Bad value for site.anticache_eviction_distribution: %s. Using default of 'even'",
                    hstore_conf.site.anticache_eviction_distribution));
            policy = AntiCacheEvictionPolicyType.EVEN;
        }
        this.evictionDistributionPolicy = policy;

        int num_partitions = hstore_site.getCatalogContext().numberOfPartitions;

        this.partitionStats = new PartitionStats[num_partitions];
        for(i = 0; i < num_partitions; i++) {
            this.partitionStats[i] = new PartitionStats();  
        }
        this.pendingStatsUpdates = new boolean[num_partitions];
        Arrays.fill(pendingStatsUpdates, false);

        this.profilers = new AntiCacheManagerProfiler[num_partitions];
        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            this.profilers[partition] = new AntiCacheManagerProfiler();
        } // FOR

        this.statsMessage = new TableStatsRequestMessage(catalogContext.getDataTables());
        this.statsMessage.getObservable().addObserver(new EventObserver<VoltTable>() {
            @Override
            public void update(EventObservable<VoltTable> o, VoltTable vt) {
            	if (debug.val)
            	    LOG.debug("updating partition stats in observer");
                AntiCacheManager.this.updatePartitionStats(vt);
            }
        });
        
        if (hstore_conf.site.anticache_enable_multilevel) {
            String config = hstore_conf.site.anticache_levels;
            String delims = "[;]";
            String[] levels = config.split(delims);

            numDBs = levels.length;
        }
    }

    public Collection<Table> getEvictableTables() {
        return hstore_site.getCatalogContext().getEvictableTables();
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
        if (next.partition != next.ts.getBasePartition()) { // distributed txn
            LOG.warn(String.format("The base partition for %s is %d but we want to fetch a block for partition %d: %s",
                     next.ts, next.ts.getBasePartition(), next.partition, next));
            // if we are the remote site then we should go ahead and continue processing
            // if no then we should simply requeue the entry? 
            
        }
        if (debug.val)
            LOG.debug("Processing " + next);

        // We need to get the EE handle for the partition that this txn
        // needs to have read in some blocks from disk
        PartitionExecutor executor = hstore_site.getPartitionExecutor(next.partition);
        ExecutionEngine ee = executor.getExecutionEngine();

        // boolean merge_needed = true; 

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
        lock.lock();
        try {
            if (debug.val)
                LOG.debug(String.format("Asking EE to read in evicted blocks from table %s on partition %d: %s",
                          next.catalog_tbl.getName(), next.partition, Arrays.toString(next.block_ids)));

            //LOG.warn(Arrays.toString(next.block_ids) + "\n" + Arrays.toString(next.tuple_offsets));
            ee.antiCacheReadBlocks(next.catalog_tbl, next.block_ids, next.tuple_offsets);

            if (debug.val)
                LOG.debug(String.format("Finished reading blocks from partition %d",
                          next.partition));
        } catch (SerializableException ex) {
            LOG.info("Caught unexpected SerializableException while reading anti-cache block.", ex);

            // merge_needed = false; 
        } finally {
            lock.unlock();
            if (hstore_conf.site.anticache_profiling) 
                this.profilers[next.partition].retrieval_time.stopIfStarted();
        }

        if (debug.val) LOG.debug("anticache block removal done");
        // Long oldTxnId = next.ts.getTransactionId();
        // Now go ahead and requeue our transaction

        //        if(merge_needed)
        next.ts.setAntiCacheMergeTable(next.catalog_tbl);

        if (next.ts instanceof LocalTransaction){
            // HACK HACK HACK HACK HACK HACK
            // We need to get a new txnId for ourselves, since the one that we
            // were given before is now probably too far in the past
        	if(next.partition != next.ts.getBasePartition()){
                lock.lock();
        		ee.antiCacheMergeBlocks(next.catalog_tbl);
                lock.unlock();
        	}
            this.hstore_site.getTransactionInitializer().resetTransactionId(next.ts, next.partition);

            if (debug.val) LOG.debug("restartin on local");
        	this.hstore_site.transactionInit(next.ts);	
        } else {
            lock.lock();
        	ee.antiCacheMergeBlocks(next.catalog_tbl);
            lock.unlock();
        	RemoteTransaction ts = (RemoteTransaction) next.ts; 
        	RpcCallback<UnevictDataResponse> callback = ts.getUnevictCallback();
        	UnevictDataResponse.Builder builder = UnevictDataResponse.newBuilder()
        		.setSenderSite(this.hstore_site.getSiteId())
        		.setTransactionId(ts.getNewTransactionId())
        		.setPartitionId(next.partition)
        		.setStatus(Status.OK);
        	callback.run(builder.build());        	
        	
        }
    }

    @Override
    protected void removeCallback(QueueEntry next) {
    	LocalTransaction ts = (LocalTransaction) next.ts;
        this.hstore_site.transactionReject(ts, Status.ABORT_GRACEFUL);
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
    public boolean queue(AbstractTransaction txn, int partition, Table catalog_tbl, int block_ids[], int tuple_offsets[]) {
    	if (debug.val)
    	    LOG.debug(String.format("\nBase partition: %d \nPartition that needs to unevict data: %d",
    	              txn.getBasePartition(), partition));
    	
    	// HACK
	    if (hstore_conf.site.anticache_block_merge) {
            //LOG.warn("here!!");
    	    Set<Integer> allBlockIds = new HashSet<Integer>();
    	    for (int block : block_ids) {
    	        allBlockIds.add(block);
    	    }
    	    block_ids = new int[allBlockIds.size()];
    	    int i = 0;
    	    for (int block : allBlockIds) {
    	        block_ids[i++] = block;
    	    }
        }
    	
    	if (txn instanceof LocalTransaction) {
    		LocalTransaction ts = (LocalTransaction)txn;
    		// Different partition generated the exception
	    	if (ts.getBasePartition() != partition  && !hstore_site.isLocalPartition(partition)){ 
	    		int site_id = hstore_site.getCatalogContext().getSiteIdForPartitionId(partition);
	    		hstore_site.getCoordinator().sendUnevictDataMessage(site_id, ts, partition, catalog_tbl, block_ids, tuple_offsets);
	    		return true;
	    		// should we enqueue the transaction on our side?
	    		// if yes then we need to prevent the queue item from being picked up 
	    		// and prevent it from bombing the partition error
	    		// if no then simply return?
	    		
	    		// how to take care of LRU?
	    		
	    	}
	    	
	    	if (hstore_conf.site.anticache_profiling) {
		        assert(ts.getPendingError() != null) :
		            String.format("Missing original %s for %s", EvictedTupleAccessException.class.getSimpleName(), ts);
		        assert(ts.getPendingError() instanceof EvictedTupleAccessException) :
		            String.format("Unexpected error for %s: %s", ts, ts.getPendingError().getClass().getSimpleName());
		        this.profilers[partition].restarted_txns++;
		        this.profilers[partition].addEvictedAccess(ts, (EvictedTupleAccessException)ts.getPendingError());
	    		LOG.debug("Restarting transaction " + String.format("%s",ts) + ", " + ts.getRestartCounter() + " total restarts."); 
	    		LOG.debug("Total Restarted Txns: " + this.profilers[partition].restarted_txns); 
	    	}
    	}

    	if (debug.val)
    	    LOG.debug(String.format("AntiCacheManager queuing up an item for uneviction at site %d",
    	              hstore_site.getSiteId()));
        QueueEntry e = new QueueEntry(txn, partition, catalog_tbl, block_ids, tuple_offsets);

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
        long totalSizeKb = 0;
        long totalBlocksEvicted = 0;
        long totalBlocksFetched = 0;
        long totalEvictableSizeKb = 0;
        long totalIndexKb = 0;

        /**
         * TODO: What commented in the loop below will make the eviction manager ignore index memory while calculating eviction threshold
         *       In some cases, we may do want exclude index memory. Then uncomment them!
         */
        for (PartitionStats stats : this.partitionStats) {
            totalSizeKb += stats.sizeKb - stats.indexes;
            totalIndexKb += stats.indexes;
            totalBlocksEvicted += stats.blocksEvicted;
            totalBlocksFetched += stats.blocksFetched;
            for (Stats tstats : stats.getTableStats()) {
                totalEvictableSizeKb += tstats.sizeKb - tstats.indexes;
            }
        }

        long totalDataSize = (int)(totalSizeKb / 1024);
        long totalEvictedMB = ((totalBlocksEvicted * hstore_conf.site.anticache_block_size) / 1024 / 1024); 
        long totalActiveDataSize = totalDataSize - totalEvictedMB; 

        LOG.info("Current Memory Usage: " + totalDataSize + " / " +
                hstore_conf.site.anticache_threshold_mb + " MB");
        LOG.info("Current Active Memory Usage: " + totalActiveDataSize + " / " +
                hstore_conf.site.anticache_threshold_mb + " MB");
        LOG.info("Index memory: " + totalIndexKb);
        LOG.info("Blocks Currently Evicted: " + totalBlocksEvicted);
        LOG.info("Total Blocks Fetched: " + totalBlocksFetched);
        LOG.info("Total Evictable Kb: " + totalEvictableSizeKb);
        LOG.info("Partitions Evicting: " + this.pendingEvictions);

        /*
         *  Evict if we
         *  - have at least one evictable block (TODO maybe raise this limit) 
         *  - are not currently evicting
         *  - are past usage threshold
         *  - haven't overevicted (wtf does this actually mean, why's there a limit)
         */
        return  totalEvictableSizeKb >= (hstore_conf.site.anticache_block_size / 1024) &&
                this.pendingEvictions == 0 &&
                totalDataSize > hstore_conf.site.anticache_threshold_mb &&
//                totalEvictedMB < (totalDataSize * hstore_conf.site.anticache_threshold) &&
                totalBlocksEvicted < hstore_conf.site.anticache_max_evicted_blocks;
    }

    protected long blocksToEvict() {
        long totalBlocksEvicted = 0;
        for (PartitionStats stats : this.partitionStats) {
            totalBlocksEvicted += stats.blocksEvicted;
        }

        int max_blocks_per_eviction = hstore_conf.site.anticache_blocks_per_eviction;
        int max_evicted_blocks = hstore_conf.site.anticache_max_evicted_blocks;

        // I think this happens because some blocks are unevicted twice
        if(totalBlocksEvicted < 0 ||
                max_evicted_blocks - totalBlocksEvicted > max_blocks_per_eviction) 
            return max_blocks_per_eviction;

        return max_evicted_blocks - totalBlocksEvicted;
    }

    protected void executeEviction() {
        // Invoke our special sysproc that will tell the EE to evict some blocks
        // to save us space.

        long blocksToEvict = blocksToEvict(); 
        if(blocksToEvict <= 0)
            return;

        LOG.info("Evicting " + blocksToEvict + " blocks."); 

        Map<Integer, Map<String, Integer>> distribution = getEvictionDistribution(blocksToEvict);
        
        // Save current stats so we can get deltas at next check.
        for (PartitionStats stats : partitionStats) {
            stats.setEvicted();
        }

        String procName = VoltSystemProcedure.procCallName(EvictTuples.class);

        for (int partition : hstore_site.getLocalPartitionIds().values()) {
            // XXX what if this pdist is empty, probably just go to next
            Map<String, Integer> pdist = distribution.get(partition);
            String tableNames[] = new String[pdist.size()];
            long evictBlockSizes[] = new long[pdist.size()];
            int evictBlocks[] = new int[pdist.size()];
            int i = 0;
            CatalogContext catalogContext = hstore_site.getCatalogContext();
            String children[] = new String[pdist.size()];
            for (String table : pdist.keySet()) {
                tableNames[i] = table;
                Table catalogTable = catalogContext.getTableByName(table);
                if(hstore_conf.site.anticache_batching == true){
                    children = CatalogUtil.getChildTables(catalogContext.database, catalogTable);
                    System.out.println(children);                	
                }
                evictBlockSizes[i] = hstore_conf.site.anticache_block_size;
                evictBlocks[i] = pdist.get(table); 
                i++;
            }
            
            
            Object params[] = new Object[] { partition, tableNames, children, evictBlockSizes, evictBlocks};

            StoredProcedureInvocation invocation = new StoredProcedureInvocation(1, procName, params);

            if (hstore_conf.site.anticache_profiling)
                this.profilers[partition].eviction_time.start();

            ByteBuffer b = null;
            try {
                b = ByteBuffer.wrap(FastSerializer.serialize(invocation));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            this.pendingEvictions++;
            this.hstore_site.invocationProcess(b, this.evictionCallback);
        } 
    }

    protected Map<Integer, Map<String, Integer>> getEvictionDistribution(long blocksToEvict) {
        Map<Integer, Map<String, Integer>> distribution = new HashMap<Integer, Map<String, Integer>>();
        for (int partition : hstore_site.getLocalPartitionIds()) {
            distribution.put(partition, new HashMap<String, Integer>());
        }
        switch (evictionDistributionPolicy) {
            case EVEN:
                fillEvenEvictionDistribution(distribution, blocksToEvict);
                break;
            case PROPORTIONAL:
                fillProportionalEvictionDistribution(distribution, blocksToEvict);
                break;
            case UNEVICTION_RATIO:
                fillUnevictionRatioEvictionDistribution(distribution, blocksToEvict);
                break;
            case ACCESS_RATE:
                fillAccessRateEvictionDistribution(distribution, blocksToEvict);
                break;
            default:
                assert(false):
                    String.format("Unsupported eviction distribution policy %s\n",
                            evictionDistributionPolicy);
                fillEvenEvictionDistribution(distribution, blocksToEvict);
        } // SWITCH

        String msg = "Eviction distribution:\n";
        for (int partition : distribution.keySet()) {
            msg += String.format("PARTITION %d\n", partition);
            for (String table : distribution.get(partition).keySet()) {
                msg += String.format("%s: %d\t", table, distribution.get(partition).get(table));
            }
            msg += "\n";

        }
        LOG.warn(msg);
        return distribution;
    }

    protected void fillEvenEvictionDistribution(Map<Integer, Map<String, Integer>> distribution, long blocksToEvict) {
        // blocks to evict / (#tables * #partitions)
        int blocks = (int) blocksToEvict / (evictableTables.length * hstore_site.getLocalPartitionIds().size());

        for (Map<String, Integer> tableBlocks : distribution.values()) {
            for (String table : evictableTables) {
                tableBlocks.put(table, blocks);
            }
        }
    }

    protected void fillProportionalEvictionDistribution(Map<Integer, Map<String, Integer>> distribution, long blocksToEvict) {
        float totalEvictableKb = 0;
        for (PartitionStats stats : this.partitionStats) {
            for (Stats tstats : stats.getTableStats()) {
                totalEvictableKb += tstats.sizeKb;
            }
        }

        for (int partition : distribution.keySet()) {
            Map<String, Integer> tdist = distribution.get(partition);
            for (String table : evictableTables) {
                long tableSize = partitionStats[partition].get(table).sizeKb;

                int blocks = (int) Math.floor((tableSize / totalEvictableKb) * blocksToEvict);
                if (blocks > 0) {
                    tdist.put(table, blocks);
                }
            }
        }
    }

    private interface Metric {
        public double getMetric(int partition, String table);
    }
    
    private void fillUnevictionRatioEvictionDistribution(Map<Integer, Map<String, Integer>> distribution,
            long blocksToEvict) {
        for (PartitionStats stats : partitionStats) {
            for (Stats tstats : stats.getTableStats()) {
                double blocksFetchedDelta = tstats.blocksFetched - tstats.evictionBlocksFetched;
                double blocksWrittenDelta = tstats.blocksWritten - tstats.evictionBlocksWritten;
                double newUnevictionRatio = blocksWrittenDelta == 0 ? 0 : blocksFetchedDelta / blocksWrittenDelta;
                tstats.unevictionRatio = (UNEVICTION_RATIO_EMA_ALPHA * newUnevictionRatio) +
                        ((1.0 - UNEVICTION_RATIO_EMA_ALPHA) * tstats.unevictionRatio);
            }
        }
        
        fillMetricEvictionDistribution(distribution, blocksToEvict, UNEVICTION_RATIO_CLUSTER_THRESHOLD, new Metric() {
            public double getMetric(int partition, String table) {
                return partitionStats[partition].get(table).unevictionRatio;
            }
        });
        
        for (int partition : distribution.keySet()) {
            for (String table : evictableTables) {
                if (!distribution.get(partition).containsKey(table)) {
                    distribution.get(partition).put(table, 1);
                }
            }
        }
    }

    private void fillAccessRateEvictionDistribution(Map<Integer, Map<String, Integer>> distribution,
            long blocksToEvict) {
        double total = 0;
        for (PartitionStats stats : partitionStats) {
            for (Stats tstats : stats.getTableStats()) {
                total += tstats.accesses - tstats.evictionAccesses;
            }
        }
        
        final double final_total = total;

        fillMetricEvictionDistribution(distribution, blocksToEvict, ACCESS_RATE_CLUSTER_THRESHOLD, new Metric() {
            public double getMetric(int partition, String table) {
                Stats tstats = partitionStats[partition].get(table);
                return (tstats.accesses - tstats.evictionAccesses) / final_total;
            }
        });
    }
    
    protected void fillMetricEvictionDistribution(Map<Integer, Map<String, Integer>> distribution, 
            long blocksToEvict, double clusterThreshold, final Metric metric) {
        Comparator<Pair<Integer, String>> comparator = new Comparator<Pair<Integer, String>> () {
            public int compare(Pair<Integer, String> t1, Pair<Integer, String> t2) {
                double r1 = metric.getMetric(t1.getFirst(), t1.getSecond());
                double r2 = metric.getMetric(t2.getFirst(), t2.getSecond());
                return r1 > r2 ? 1 : (r2 > r1 ? -1 : 0);
            }
        };

        ArrayList<Pair<Integer, String>> allChunks = new ArrayList<Pair<Integer, String>>();
        for (int partition : distribution.keySet()) {
            for (String table : evictableTables) {
                allChunks.add(new Pair<Integer, String>(partition, table));
            }
        }

        Collections.sort(allChunks, comparator);
        for (Pair<Integer, String> chunk : allChunks) {
            int partition = chunk.getFirst();
            String table = chunk.getSecond();
            Stats tstats = partitionStats[partition].get(table);
            if (debug.val)
                LOG.warn(String.format("%d %s Ratio %f Evicted %d Read %d Written %d Accesses %d",
                         partition, table, metric.getMetric(partition, table),
                         tstats.blocksEvicted, tstats.blocksFetched, tstats.blocksWritten, tstats.accesses));
        }

        
        long blocksLeft = blocksToEvict;
        while (!allChunks.isEmpty() && blocksLeft > 0) {
            Iterator<Pair<Integer, String>> iter = allChunks.iterator();
            Pair<Integer, String> firstChunk = iter.next();
            double chunkMetric = metric.getMetric(firstChunk.getFirst(), firstChunk.getSecond());
            if (debug.val)
                LOG.warn(String.format("Current metric: %f", chunkMetric));
            
            ArrayList<Pair<Integer, String>> chunks = new ArrayList<Pair<Integer, String>>();
            chunks.add(firstChunk);
            iter.remove();
            
            while (iter.hasNext()) {
                Pair<Integer, String> nextChunk = iter.next();
                double nextChunkMetric = metric.getMetric(nextChunk.getFirst(), nextChunk.getSecond());
                if (chunkMetric == nextChunkMetric ||
                        (firstChunk.getSecond().equals(nextChunk.getSecond()) &&
                        nextChunkMetric < chunkMetric + clusterThreshold)) {
                    chunks.add(nextChunk);
                    iter.remove();
                }
                
                if (nextChunkMetric >= chunkMetric + clusterThreshold) {
                    break;
                }
            }
            
            if (debug.val)
                LOG.warn(String.format("Distributing to %d table(s)", chunks.size()));
            long totalSize = 0;
            for (Pair<Integer, String> chunk : chunks) {
                totalSize += partitionStats[chunk.getFirst()].get(chunk.getSecond()).sizeKb;
            }
            
            long evictableBlocks = totalSize / (hstore_conf.site.anticache_block_size / 1024);
            if (debug.val) {
                LOG.warn(String.format("Total evictable blocks %d", evictableBlocks));
                LOG.warn(String.format("Blocks left %d", blocksLeft));
            }
            
            long currentBlocksToEvict = Math.min(evictableBlocks, blocksLeft);
            for (Pair<Integer, String> chunk : chunks) {
                double size = partitionStats[chunk.getFirst()].get(chunk.getSecond()).sizeKb;
                if (debug.val)
                    LOG.warn(String.format("Proportion: %f", size / totalSize));
                int blocks = (int) Math.ceil((size / totalSize) * currentBlocksToEvict);
                distribution.get(chunk.getFirst()).put(chunk.getSecond(), blocks);
            }

            if (currentBlocksToEvict == blocksLeft) {
                break;
            } else {
                blocksLeft -= currentBlocksToEvict;
            }
        }
    }
    // ----------------------------------------------------------------------------
    // MEMORY MANAGEMENT METHODS
    // ----------------------------------------------------------------------------

    protected void getPartitionSize(int partition) {
        // Queue up a utility work operation at the PartitionExecutor so
        // that we can get the total size of the partition
        hstore_site.getPartitionExecutor(partition).queueUtilityWork(this.statsMessage);
	    LOG.debug(String.format("setting partition %d to true", partition));
        pendingStatsUpdates[partition] = true;
    }

    private class PartitionStats extends Stats {
        public PartitionStats() {
            this.tables = new HashMap<String, Stats>();
            for (String table : evictableTables) {
                this.tables.put(table, new Stats());
            }
        }

        public void update(String table, long sizeKb, long blocksEvicted,
                long blocksFetched, long blocksWritten, long accesses, long indexes){
            this.sizeKb += sizeKb;
            this.blocksEvicted += blocksEvicted;
            this.blocksFetched += blocksFetched;
            this.blocksWritten += blocksWritten;
            this.accesses += accesses;
            this.indexes += indexes;
            if (this.tables.containsKey(table)) {
                Stats tableStats = this.tables.get(table);
                tableStats.sizeKb = sizeKb;
                tableStats.blocksEvicted = blocksEvicted;
                tableStats.blocksFetched = blocksFetched;
                tableStats.blocksWritten = blocksWritten;
                tableStats.accesses = accesses;
                tableStats.indexes = indexes;
            }
        }
        
        public Stats get(String table) {
            return tables.get(table);
        }
        
        public Collection<Stats> getTableStats() {
            return tables.values();
        }
        
        public void setEvicted() {
            evictionSizeKb = sizeKb;
            evictionBlocksEvicted = blocksEvicted;
            evictionBlocksFetched = blocksFetched;
            evictionBlocksWritten = blocksWritten;
            evictionAccesses = accesses;
            for(Stats tstats : tables.values()) {
                tstats.evictionSizeKb = tstats.sizeKb;
                tstats.evictionBlocksEvicted = tstats.blocksEvicted;
                tstats.evictionBlocksFetched = tstats.blocksFetched;
                tstats.evictionBlocksWritten = tstats.blocksWritten;
                tstats.evictionAccesses = tstats.accesses;
            }
        }
        
        public void reset() {
            super.reset();
            for (Stats tstats : tables.values()) {
                tstats.reset();
            }
        }

        private HashMap<String, Stats> tables;
    }

    private class Stats {
        public long sizeKb = 0;
        public long blocksEvicted = 0;
        public long blocksFetched = 0;
        public long blocksWritten = 0;
        public long accesses = 0;
        public long evictionSizeKb = 0;
        public long evictionBlocksEvicted = 0;
        public long evictionBlocksFetched = 0;
        public long evictionBlocksWritten = 0;
        public long evictionAccesses = 0;
        public double unevictionRatio = 0;
        public long indexes = 0;
        
        public void reset() {
            sizeKb = 0;
            blocksEvicted = 0;
            blocksFetched = 0;
            blocksWritten = 0;
            accesses = 0;
            indexes = 0;
        }
    }

    protected void updatePartitionStats(VoltTable vt) {

//        VoltTable[] vts = new VoltTable[1];
//        vts[0] = vt;
//        LOG.warn("Table stats:");
//        LOG.warn(VoltTableUtil.format(vts));

         synchronized(this) {
            PartitionStats stats;
            vt.resetRowPosition();
            vt.advanceRow();
            int partition = (int) vt.getLong("PARTITION_ID");
            stats = this.partitionStats[partition];
            // long oldSizeKb = stats.sizeKb;
            stats.reset();

            //int tupleMem = 0;
            //int stringMem = 0;
            //int indexMem = 0;

            do {
                String table = vt.getString("TABLE_NAME");
                long sizeKb = vt.getLong("TUPLE_DATA_MEMORY") + vt.getLong("STRING_DATA_MEMORY") + vt.getLong("INDEX_MEMORY");
                long indexes = vt.getLong("INDEX_MEMORY");
                //tupleMem += vt.getLong("TUPLE_DATA_MEMORY");
                //stringMem += vt.getLong("STRING_DATA_MEMORY");
                //indexMem += vt.getLong("INDEX_MEMORY");
                long blocksEvicted = vt.getLong("ANTICACHE_BLOCKS_EVICTED");
                long blocksFetched = vt.getLong("ANTICACHE_BLOCKS_READ");
                long blocksWritten = vt.getLong("ANTICACHE_BLOCKS_WRITTEN");
                long accesses = vt.getLong("TUPLE_ACCESSES");
                stats.update(table, sizeKb, blocksEvicted, blocksFetched, blocksWritten, accesses, indexes);
            } while(vt.advanceRow());

            //LOG.info(String.format("Tuple Mem: %d; String Mem: %d\n", tupleMem, stringMem));
            //LOG.info(String.format("Index Mem: %d\n", indexMem));

//            LOG.warn(String.format("Partition #%d Size - New:%dkb / Old:%dkb",
//                    partition, stats.sizeKb, oldSizeKb));

            pendingStatsUpdates[partition] = false;
            boolean allBack = true;
            for (int i = 0; i < pendingStatsUpdates.length; i++) {
                if(pendingStatsUpdates[i]) {
                    allBack = false;
                    //for (int j = 0; j < pendingStatsUpdates.length; j++) 
                      //  LOG.info(String.format("%d:%b", j, pendingStatsUpdates[j]));
                }
            }


            // All partitions have reported back, schedule an eviction check
            if (allBack) {
                //LOG.info("All back!!");
                hstore_site.getThreadManager().scheduleWork(evictionExecutor);
            }
         }
    }

    // ----------------------------------------------------------------------------
    // STATIC HELPER METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns the directory where the EE should store the anti-cache database
     * for this PartitionExecutor
     * @return
     */
    public static File getDatabaseDir(PartitionExecutor executor, int dbnum) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        Database catalog_db = CatalogUtil.getDatabase(executor.getPartition());
        // initial DB initialization
        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.anticache_dir +
                    File.separatorChar +
                    catalog_db.getProject());
       
        if (hstore_conf.site.anticache_enable_multilevel) {
            String config = hstore_conf.site.anticache_multilevel_dirs;
            String delims = "[;]";
            String[] dirs = config.split(delims);
            base_dir = FileUtil.realpath(dirs[dbnum] +
                    File.separatorChar +
                    catalog_db.getProject());
        } 
            
        synchronized (AntiCacheManager.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNC

        // Then each partition will have a separate directory inside of the base one
        String partitionName = HStoreThreadManager.formatPartitionName(executor.getSiteId(),
                executor.getPartitionId());
        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);
        if (hstore_conf.site.anticache_reset) {
            //LOG.warn(String.format("Deleting anti-cache directory '%s'", dbDirPath));
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
        public boolean isEvicting() {
            return (pendingEvictions != 0);
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
