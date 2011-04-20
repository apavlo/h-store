package edu.brown.benchmark.auctionmark;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.types.TimestampType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;

public class AuctionMarkClientBenchmarkProfile extends AuctionMarkBenchmarkProfile {

    private transient final int _clientId;

    // inclusive offsets
    private long _partitionStartOffset;
    private long _partitionEndOffset;

    private transient final AtomicLong _nextUserId = new AtomicLong();
    private transient final AtomicLong _nextItemId = new AtomicLong();

    /**
     * Whether to use the zipf distribution
     */
    public static final boolean zipf = true;

    private RandomDistribution.Zipf zipf_distribution;

    /**
     * Current Timestamp
     */
    public transient TimestampType current_time = new TimestampType();

    /**
     * The last time that we called CHECK_WINNING_BIDS on this client
     */
    public transient TimestampType lastCheckWinningBidTime = null;

    /**
     * Constructor
     * 
     * @param profile
     * @param clientId
     * @param partitionRange
     */
    public AuctionMarkClientBenchmarkProfile(AuctionMarkBenchmarkProfile profile, int clientId, long partitionRange, Catalog catalog_db, AbstractRandomGenerator rng) {
        super();

        Cluster cluster = CatalogUtil.getCluster(catalog_db);
        zipf_distribution = new RandomDistribution.Zipf(rng, 0, cluster.getNum_partitions(), 1.001d);
        zipf_distribution.enableHistory();

        if (clientId < 0) {
            throw new IllegalArgumentException("ClientId must be more than 0 : " + clientId);
        }

        if (null == profile) {
            throw new IllegalArgumentException("AuctionMarkBenchmarkProfile cannot be null");
        }

        if (partitionRange < 1 || partitionRange > AuctionMarkConstants.MAXIMUM_CLIENT_IDS) {
            throw new IllegalArgumentException("partitionRange is not in range 1 to " + AuctionMarkConstants.MAXIMUM_CLIENT_IDS + " : " + partitionRange);
        }

        this._clientId = clientId;
        this._partitionStartOffset = clientId * partitionRange;
        this._partitionEndOffset = ((clientId + 1) * partitionRange) - 1;

        this.initItemMap(profile.user_available_items, this.user_available_items, this.user_available_items_histogram);
        this.initItemMap(profile.user_wait_for_purchase_items, this.user_wait_for_purchase_items, this.user_wait_for_purchase_items_histogram);

        this.initItemMap(profile.user_complete_items, this.user_complete_items, this.user_complete_items_histogram);

        this.initItemBidMap(profile.item_bid_map);
        this.initItemBuyerMap(profile.item_buyer_map);

        this.initUserIds(profile.user_ids);

        this.gag_gav_map = profile.gag_gav_map;
        this.gag_gav_histogram = profile.gag_gav_histogram;

        this.item_category_histogram = profile.item_category_histogram;

        this._nextItemId.set(profile.getTableSize(AuctionMarkConstants.TABLENAME_ITEM));
    }

    public long getNextItemId() {
        return this._nextItemId.getAndIncrement();
    }

    private void initItemMap(Map<Long, List<Long>> sourceMap, Map<Long, List<Long>> targetMap, Histogram targetHistogram) {
        long count = 0;
        for (Map.Entry<Long, List<Long>> entry : sourceMap.entrySet()) {
            if (this.isOffsetInRange(entry.getKey())) {
                targetHistogram.put(entry.getKey(), entry.getValue().size());
                count += entry.getValue().size();
                targetMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void initItemBidMap(Map<Long, Long> sourceItemBidMap) {
        for (Map.Entry<Long, List<Long>> entry : this.user_wait_for_purchase_items.entrySet()) {
            for (Long itemId : entry.getValue()) {
                this.item_bid_map.put(itemId, sourceItemBidMap.get(itemId));
            }
        }
    }

    private void initItemBuyerMap(Map<Long, Long> sourceItemBuyerMap) {
        for (Map.Entry<Long, List<Long>> entry : this.user_wait_for_purchase_items.entrySet()) {
            for (Long itemId : entry.getValue()) {
                this.item_buyer_map.put(itemId, sourceItemBuyerMap.get(itemId));
            }
        }
    }

    private void initUserIds(List<Long> sourceUserIds) {
        long maxUserId = -1;
        synchronized (this.user_ids) {
            for (Long userId : sourceUserIds) {
                if (this.isOffsetInRange(userId)) {
                    this.user_ids.add(userId);
                    if (userId > maxUserId) {
                        maxUserId = userId;
                    }
                }
            }
            this._nextUserId.set(maxUserId + 1);
        }
    }

    /**
     * Gets next available user ID within the client.
     * 
     * @return long client ID
     */
    public long getNextUserId() {
        return (this._nextUserId.getAndIncrement());
    }

    private boolean isOffsetInRange(long offset) {
        return (offset >= this._partitionStartOffset && offset <= this._partitionEndOffset);
    }

    public int getClientId() {
        return this._clientId;
    }

    public synchronized TimestampType updateAndGetCurrentTime() {
        this.current_time = new TimestampType();
        return (this.current_time);
    }

    public TimestampType getCurrentTime() {
        return (this.current_time);
    }

    public TimestampType getLastCheckWinningBidTime() {
        return (this.lastCheckWinningBidTime);
    }

    public synchronized TimestampType updateAndGetLastCheckWinningBidTime() {
        this.lastCheckWinningBidTime = new TimestampType();
        return (this.lastCheckWinningBidTime);
    }

    public RandomDistribution.Zipf getBuyerAffinityPartitionDistribution() {
        return zipf_distribution;
    }
}
