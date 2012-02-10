package edu.brown.statistics;

import org.voltdb.VoltType;

/**
 * Fixed-size histogram that only stores integers
 * @author pavlo
 */
public class FastIntHistogram extends Histogram<Integer> {
    
    private final int histogram[];
    private int value_count = 0;
    
    public FastIntHistogram(int size) {
        this.histogram = new int[size];
        this.clear();
    }
    
    public int[] fastValues() {
        return this.histogram;
    }

    @Override
    public Long get(Integer value) {
        return ((long)this.histogram[value.intValue()]);
    }
    
    public int fastGet(int value) {
        return (this.histogram[value]);
    }
    
    @Override
    public long get(Integer value, long value_if_null) {
        int idx = value.intValue();
        if (this.histogram[idx] == -1) {
            return (value_if_null);
        } else {
            return (this.histogram[idx]);
        }
    }
    
    @Override
    public synchronized void put(Integer value) {
        int idx = value.intValue();
        if (this.histogram[idx] == -1) {
            this.histogram[idx] = 1;
            this.value_count++;
        } else {
            this.histogram[idx]++;
        }
    }
    
    @Override
    public synchronized void put(Integer value, long i) {
        int idx = value.intValue();
        if (this.histogram[idx] == -1) {
            this.histogram[idx] = (int)i;
            this.value_count++;
        } else {
            this.histogram[idx] += i;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FastIntHistogram) {
            FastIntHistogram other = (FastIntHistogram)obj;
            if (this.histogram.length != other.histogram.length) return (false);
            for (int i = 0; i < this.histogram.length; i++) {
                if (this.histogram[i] != other.histogram[i]) return (false);
            } // FOR
            return (true);
        }
        return (false);
    }
    
    @Override
    public VoltType getEstimatedType() {
        return VoltType.INTEGER;
    }
    
    @Override
    public int getValueCount() {
        return this.value_count;
    }

    @Override
    public boolean contains(Integer value) {
        return (this.histogram[value.intValue()] != -1);
    }

    @Override
    public synchronized void clear() {
        for (int i = 0; i < this.histogram.length; i++) {
            this.histogram[i] = 0;
        } // FOR
    }

    @Override
    public synchronized void clearValues() {
        for (int i = 0; i < this.histogram.length; i++) {
            this.histogram[i] = -1;
        } // FOR
        this.value_count = 0;
    }
    
}
