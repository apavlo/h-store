package edu.brown.statistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.voltdb.VoltType;

/**
 * Fixed-size histogram that only stores integers
 * 
 * @author pavlo
 */
public class FastIntHistogram extends Histogram<Integer> {

    private final long histogram[];
    private int value_count = 0;

    public FastIntHistogram(int size) {
        this.histogram = new long[size];
        this.clearValues();
    }

    public long[] fastValues() {
        return this.histogram;
    }

    @Override
    public Long get(Integer value) {
        return (this.histogram[value.intValue()]);
    }

    public long fastGet(int value) {
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
        this.num_samples++;
    }

    @Override
    public synchronized void put(Integer value, long i) {
        int idx = value.intValue();
        if (this.histogram[idx] == -1) {
            this.histogram[idx] = i;
            this.value_count++;
        } else {
            this.histogram[idx] += i;
        }
        this.num_samples += i;
    }

    @Override
    public void putAll(Collection<Integer> values) {
        for (Integer v : values)
            this.put(v);
    }

    @Override
    public synchronized void putAll(Collection<Integer> values, long count) {
        for (Integer v : values)
            this.put(v, count);
    }

    @Override
    public synchronized void putHistogram(Histogram<Integer> other) {
        if (other instanceof FastIntHistogram) {
            FastIntHistogram fast = (FastIntHistogram) other;
            // FIXME
        } else {
            for (Integer v : other.values()) {
                this.put(v, other.get(v));
            }
        }
    }

    @Override
    public synchronized void remove(Integer value) {
        this.remove(value, 1);
    }

    @Override
    public synchronized void remove(Integer value, long count) {
        int idx = value.intValue();
        // if (histogram[idx] != -1) {
        // histogram[idx] = Math.max(-1, histogram[idx] - count);

        // TODO Auto-generated method stub
        super.remove(value, count);
    }

    @Override
    public synchronized void removeAll(Integer value) {
        // TODO Auto-generated method stub
        super.removeAll(value);
    }

    @Override
    public synchronized void removeValues(Collection<Integer> values) {
        // TODO Auto-generated method stub
        super.removeValues(values);
    }

    @Override
    public synchronized void removeValues(Collection<Integer> values, long delta) {
        // TODO Auto-generated method stub
        super.removeValues(values, delta);
    }

    @Override
    public synchronized void removeHistogram(Histogram<Integer> other) {
        // TODO Auto-generated method stub
        super.removeHistogram(other);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FastIntHistogram) {
            FastIntHistogram other = (FastIntHistogram) obj;
            if (this.histogram.length != other.histogram.length)
                return (false);
            if (this.value_count != other.value_count || this.num_samples != other.num_samples)
                return (false);
            for (int i = 0; i < this.histogram.length; i++) {
                if (this.histogram[i] != other.histogram[i])
                    return (false);
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
    public Collection<Integer> values() {
        List<Integer> values = new ArrayList<Integer>();
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1)
                values.add(i);
        }
        return (values);
    }

    @Override
    public int getValueCount() {
        return this.value_count;
    }

    @Override
    public boolean isEmpty() {
        return (this.value_count == 0);
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
        this.num_samples = 0;
    }

    @Override
    public synchronized void clearValues() {
        for (int i = 0; i < this.histogram.length; i++) {
            this.histogram[i] = -1;
        } // FOR
        this.value_count = 0;
        this.num_samples = 0;
    }

    @Override
    public int getSampleCount() {
        return (this.num_samples);
    }

    @Override
    public Integer getMinValue() {
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1)
                return (i);
        } // FOR
        return (null);
    }

    @Override
    public Integer getMaxValue() {
        for (int i = this.histogram.length - 1; i >= 0; i--) {
            if (this.histogram[i] != -1)
                return (i);
        } // FOR
        return (null);
    }

    @Override
    public long getMinCount() {
        long min_cnt = Integer.MAX_VALUE;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1 && this.histogram[i] < min_cnt) {
                min_cnt = this.histogram[i];
            }
        } // FOR
        return (min_cnt);
    }

    @Override
    public Collection<Integer> getMinCountValues() {
        List<Integer> min_values = new ArrayList<Integer>();
        long min_cnt = Integer.MAX_VALUE;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1) {
                if (this.histogram[i] == min_cnt) {
                    min_values.add(i);
                } else if (this.histogram[i] < min_cnt) {
                    min_values.clear();
                    min_values.add(i);
                    min_cnt = this.histogram[i];
                }
            }
        } // FOR
        return (min_values);
    }

    @Override
    public long getMaxCount() {
        long max_cnt = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1 && this.histogram[i] > max_cnt) {
                max_cnt = this.histogram[i];
            }
        } // FOR
        return (max_cnt);
    }

    @Override
    public Collection<Integer> getMaxCountValues() {
        List<Integer> max_values = new ArrayList<Integer>();
        long max_cnt = 0;
        for (int i = 0; i < this.histogram.length; i++) {
            if (this.histogram[i] != -1) {
                if (this.histogram[i] == max_cnt) {
                    max_values.add(i);
                } else if (this.histogram[i] > max_cnt) {
                    max_values.clear();
                    max_values.add(i);
                    max_cnt = this.histogram[i];
                }
            }
        } // FOR
        return (max_values);
    }

}
