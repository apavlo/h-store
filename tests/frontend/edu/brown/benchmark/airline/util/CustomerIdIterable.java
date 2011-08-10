package edu.brown.benchmark.airline.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections15.set.ListOrderedSet;

public class CustomerIdIterable implements Iterable<CustomerId> {
    private final Map<Long, Long> airport_max_customer_id;
    private final ListOrderedSet<Long> airport_ids = new ListOrderedSet<Long>();
    private Long last_airport_id = null;
    private Long last_id = null;
    private Long last_max_id = null;
    
    public CustomerIdIterable(Map<Long, Long> airport_max_customer_id, long...airport_ids) {
        this.airport_max_customer_id = airport_max_customer_id;
        for (long id : airport_ids) {
            this.airport_ids.add(id);
        } // FOR
    }
    
    public CustomerIdIterable(Map<Long, Long> airport_max_customer_id) {
        this.airport_max_customer_id = airport_max_customer_id;
        this.airport_ids.addAll(airport_max_customer_id.keySet());
    }
    
    public CustomerIdIterable(Map<Long, Long> airport_max_customer_id, Collection<Long> airport_ids) {
        this.airport_max_customer_id = airport_max_customer_id;
        this.airport_ids.addAll(airport_ids);
    }
    
    @Override
    public Iterator<CustomerId> iterator() {
        return new Iterator<CustomerId>() {
            @Override
            public boolean hasNext() {
                return (!CustomerIdIterable.this.airport_ids.isEmpty() || (last_id != null && last_id < last_max_id));
            }
            @Override
            public CustomerId next() {
                if (last_airport_id == null) {
                    last_airport_id = airport_ids.remove(0);
                    last_id = 0l;
                    last_max_id = airport_max_customer_id.get(last_airport_id);
                }
                CustomerId next_id = new CustomerId(last_id, last_airport_id);
                if (++last_id == last_max_id) last_airport_id = null;
                return next_id;
            }
            @Override
            public void remove() {
                // Not implemented
            }
        };
    }
} // END CLASS
