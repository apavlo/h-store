package edu.brown.benchmark.auctionmark.util;

import java.util.Iterator;

import org.voltdb.utils.NotImplementedException;

import edu.brown.statistics.Histogram;

public class UserIdGenerator implements Iterator<UserId> {

    private final int numClients;
    private final Integer clientId;
    private final Histogram<Integer> users_per_item_count;
    private final int min_size;
    private final int max_size;
    private Integer currentSize = null;
    private int currentOffset;
    private UserId next = null;
    private long total_ctr = 0l;
    
    /**
     * Construct a new generator based on the given histogram.
     * If clientId is not null, then this generator will only return UserIds that are mapped
     * to that clientId based on the UserId's offset
     * @param users_per_item_count
     * @param numClients
     * @param clientId
     */
    public UserIdGenerator(Histogram<Integer> users_per_item_count, int numClients,  Integer clientId) {
        if (numClients <= 0)
            throw new IllegalArgumentException("numClients must be more than 0 : " + numClients);
        if (clientId != null && clientId < 0)
            throw new IllegalArgumentException("clientId must be more than or equal to 0 : " + clientId);

        this.numClients = numClients;
        this.clientId = clientId;
        this.users_per_item_count = users_per_item_count;
        this.min_size = users_per_item_count.getMinValue();
        this.max_size = users_per_item_count.getMaxValue();
        
        this.setCurrentSize(this.min_size);
    }

    public UserIdGenerator(Histogram<Integer> users_per_item_count, int numClients) {
        this(users_per_item_count, numClients, null);
    }
    
    public void setCurrentSize(int size) {
        this.currentSize = size;
        this.currentOffset = this.users_per_item_count.get(this.currentSize).intValue();
    }
    
    private UserId getNext() {
        // Find the next id for this size level
        Integer found = null;
        while (this.currentSize <= this.max_size) {
            while (this.currentOffset > 0) {
                int nextCtr = this.currentOffset--;
                this.total_ctr++;
                
                // If we weren't given a clientId, then we'll generate UserIds
                // for all users in a given size level
                if (this.clientId == null) {
                    found = nextCtr;
                    break;
                }
                // Otherwise we have to spin through and find one for our client
                else if (this.total_ctr % this.numClients == this.clientId) {
                    found = nextCtr;
                    break;
                }
            } // WHILE
            if (found != null) break;
            this.currentSize++;
            this.currentOffset = (int)this.users_per_item_count.get(this.currentSize, 0l);
        } // WHILE
        if (found == null) return (null);
        
        return (new UserId(this.currentSize, found));
    }
    
    @Override
    public synchronized boolean hasNext() {
        if (this.next == null) {
            synchronized (this) {
                if (this.next == null) this.next = this.getNext();
            }
        }
        return (this.next != null);
    }

    @Override
    public synchronized  UserId next() {
        if (this.next == null) {
            this.next = this.getNext();
        }
        UserId ret = this.next;
        this.next = null;
        return (ret);
    }

    @Override
    public void remove() {
        throw new NotImplementedException("Can't call remove");
    }
}
