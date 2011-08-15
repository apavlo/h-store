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
    private Integer currentIndex = null;
    private Long currentCtr = null;
    private UserId next = null;
    
    public UserIdGenerator(int numClients, Histogram<Integer> users_per_item_count) {
        this(numClients, null, users_per_item_count);
    }

    public UserIdGenerator(int numClients, Integer clientId,  Histogram<Integer> users_per_item_count) {
        if (numClients <= 0)
            throw new IllegalArgumentException("numClients must be more than 0 : " + numClients);
        if (clientId != null && clientId < 0)
            throw new IllegalArgumentException("clientId must be more than or equal to 0 : " + clientId);

        this.numClients = numClients;
        this.clientId = clientId;
        this.users_per_item_count = users_per_item_count;
        this.min_size = users_per_item_count.getMinValue();
        this.max_size = users_per_item_count.getMaxValue();
        
        this.currentIndex = this.min_size;
        this.currentCtr = this.users_per_item_count.get(this.currentIndex);
    }

    private UserId getNext() {
        // Find the next id for this size level
        Long found = null;
        while (this.currentIndex < this.max_size) {
            while (this.currentCtr > 0) {
                long nextCtr = this.currentCtr--;
                // If we weren't given a clientId, then we'll generate UserIds
                // for all users in a given size level
                if (this.clientId == null) {
                    found = nextCtr;
                    break;
                }
                // Otherwise we have to spin through and find one for our client
                else if (nextCtr % this.numClients == this.clientId) {
                    found = nextCtr;
                    break;
                }
            } // WHILE
            if (found != null) break;
            this.currentIndex++;
            this.currentCtr = this.users_per_item_count.get(this.currentIndex);
        } // WHILE
        if (found == null) return (null);
        
        return (new UserId(this.currentIndex, found));
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
