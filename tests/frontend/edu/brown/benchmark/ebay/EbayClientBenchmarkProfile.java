package edu.brown.benchmark.ebay;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.types.TimestampType;

import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.statistics.Histogram;

public class EbayClientBenchmarkProfile extends EbayBenchmarkProfile {

	private int _clientId;
	private long _partitionRange;

	// inclusive offsets
	private long _partitionStartOffset;
	private long _partitionEndOffset;

	private long _nextUserId;
	private long _nextItemId;

	private boolean logdi = false;
	
    /**
     * Current Timestamp
     */
    public transient TimestampType current_time;
    
    /**
     * The last time that we called CHECK_WINNING_BIDS on this client
     */
    public transient TimestampType lastCheckWinningBidTime = null;
    
	
	public EbayClientBenchmarkProfile(EbayBenchmarkProfile profile, int clientId, long partitionRange){
		super();
		
		if(clientId < 0) {
			throw new IllegalArgumentException("ClientId must be more than 0 : " + clientId);
		}
		
		if(null == profile){
			throw new IllegalArgumentException("EbayBenchmarkProfile cannot be null");	
		}
		
		if(partitionRange < 1 || partitionRange > EbayConstants.MAXIMUM_CLIENT_IDS){
			throw new IllegalArgumentException("partitionRange is not in range 1 to " + 
					EbayConstants.MAXIMUM_CLIENT_IDS + " : " + partitionRange);
		}
		
		_clientId = clientId;
		_partitionStartOffset = clientId * partitionRange;
		_partitionEndOffset = ((clientId + 1) * partitionRange) - 1;
		
		initItemMap(profile.user_available_items, this.user_available_items, this.user_available_items_histogram);
		initItemMap(profile.user_wait_for_purchase_items, this.user_wait_for_purchase_items, this.user_wait_for_purchase_items_histogram);
		
		initItemMap(profile.user_complete_items, this.user_complete_items, this.user_complete_items_histogram);
		
		
		initItemBidMap(profile.item_bid_map);
		initItemBuyerMap(profile.item_buyer_map);
		
		initUserIds(profile.user_ids);
		
		gag_gav_map = profile.gag_gav_map;
		gag_gav_histogram = profile.gag_gav_histogram;
		
		item_category_histogram = profile.item_category_histogram;
		
		_nextItemId = profile.getTableSize(EbayConstants.TABLENAME_ITEM);
	}
	
	public long getNextItemId(){
		return _nextItemId++;
	}
	
	private void initItemMap(Map<Long, List<Long>> sourceMap, Map<Long, List<Long>> targetMap, Histogram targetHistogram){
		long count=0;
		for(Map.Entry<Long, List<Long>> entry: sourceMap.entrySet()){
			if(isOffsetInRange(entry.getKey())){
				for(Long itemId: entry.getValue()){
					targetHistogram.put(entry.getKey());
				}
				count+=entry.getValue().size();
				targetMap.put(entry.getKey(), entry.getValue());
				
			}
		}
	}
	
	private void initItemBidMap(Map<Long, Long> sourceItemBidMap){
		for(Map.Entry<Long, List<Long>> entry: user_wait_for_purchase_items.entrySet()){
			for(Long itemId: entry.getValue()){
				item_bid_map.put(itemId, sourceItemBidMap.get(itemId));
			}
		}
	}
	
	private void initItemBuyerMap(Map<Long, Long> sourceItemBuyerMap){
		for(Map.Entry<Long, List<Long>> entry: user_wait_for_purchase_items.entrySet()){
			for(Long itemId: entry.getValue()){
				item_buyer_map.put(itemId, sourceItemBuyerMap.get(itemId));
			}
		}
	}
	
	private void initUserIds(List<Long> sourceUserIds){
		long maxUserId = -1;
		for(Long userId: sourceUserIds){
			if(isOffsetInRange(userId)){
				user_ids.add(userId);
				if(userId > maxUserId){
					maxUserId = userId;
				}
			}
		}
		_nextUserId = maxUserId + 1;
	}
	
	/**
	 * Gets next available user ID within the client.
	 * 
	 * @return long client ID
	 */
	public synchronized long getNextUserId(){
    	return _nextUserId ++;
    }
	
    /**
     * Gets a random user ID within the client.
     * 
     * @param rng
     * @return
     */
    public synchronized long getRandomUserId(AbstractRandomGenerator rng){
    	Gaussian randomUserIndex = (new Gaussian(rng, 0, user_ids.size() - 1));
    	return user_ids.get(randomUserIndex.nextInt());
    }
	
	private boolean isOffsetInRange(long offset){
		return (offset >= _partitionStartOffset && offset <= _partitionEndOffset);
	}
	
	public int getClientId() {
		return _clientId;
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
    
}
