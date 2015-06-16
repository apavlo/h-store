/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package edu.brown.rand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;

/**
 * A class that generates random numbers that follow some distribution.
 * <p>
 * Copied from <a
 * href="https://issues.apache.org/jira/browse/HADOOP-3315">hadoop-3315
 * tfile</a>. Remove after tfile is committed and use the tfile version of this
 * class instead.
 * </p>
 */
public class RandomDistribution {
    /**
     * Interface for discrete (integer) random distributions.
     */
    public static abstract class DiscreteRNG extends Random {
        private static final long serialVersionUID = 1L;
        protected final long min;
        protected final long max;
        protected final Random random;
        protected final double mean;
        protected final long range_size;
        private ObjectHistogram<Long> history;

        public DiscreteRNG(Random random, long min, long max) {
            if (min >= max)
                throw new IllegalArgumentException("Invalid range [" + min + " >= " + max + "]");
            this.random = random;
            this.min = min;
            this.max = max;
            this.range_size = (max - min) + 1;
            this.mean = this.range_size / 2.0;
        }

        protected abstract long nextLongImpl();

        /**
         * Enable keeping track of the values that the RNG generates
         */
        public void enableHistory() {
            assert (this.history == null) : "Trying to enable history tracking more than once";
            this.history = new ObjectHistogram<Long>();
        }

        public boolean isHistoryEnabled() {
            return (this.history != null);
        }

        /**
         * Return the histogram of the values that have been generated
         * 
         * @return
         */
        public ObjectHistogram<Long> getHistory() {
            assert (this.history != null) : "Trying to get value history but tracking wasn't enabled";
            return (this.history);
        }

        /**
         * Return the count for the number of values that have been generated
         * Only works if history tracking is enabled
         * 
         * @return
         */
        public long getSampleCount() {
            return (this.history.getSampleCount());
        }

        public long getRange() {
            return this.range_size;
        }

        public double getMean() {
            return this.mean;
        }

        public long getMin() {
            return this.min;
        }

        public long getMax() {
            return this.max;
        }

        public Random getRandom() {
            return (this.random);
        }

        public Set<Integer> getRandomIntSet(int cnt) {
            assert (cnt < this.range_size);
            Set<Integer> ret = new HashSet<Integer>();
            do {
                ret.add(this.nextInt());
            } while (ret.size() < cnt);
            return (ret);
        }

        public Set<Integer> getRandomLongSet(int cnt) {
            assert (cnt < this.range_size);
            Set<Integer> ret = new HashSet<Integer>();
            do {
                ret.add(this.nextInt());
            } while (ret.size() < cnt);
            return (ret);
        }

        public double calculateMean(int num_samples) {
            long total = 0l;
            for (int i = 0; i < num_samples; i++) {
                total += this.nextLong();
            } // FOR
            return (total / (double) num_samples);
        }

        /**
         * Get the next random number as an int
         * 
         * @return the next random number.
         */
        @Override
        public final int nextInt() {
            long val = (int) this.nextLongImpl();
            if (this.history != null)
                this.history.put(val);
            return ((int) val);
        }

        /**
         * Get the next random number as a long
         * 
         * @return the next random number.
         */
        @Override
        public final long nextLong() {
            long val = this.nextLongImpl();
            if (this.history != null)
                this.history.put(val);
            return (val);
        }

        @Override
        public String toString() {
            return String.format("%s[min=%d, max=%d]", this.getClass().getSimpleName(), this.min, this.max);
        }

        public static long nextLong(Random rng, long n) {
            // error checking and 2^x checking removed for simplicity.
            long bits, val;
            do {
                bits = (rng.nextLong() << 1) >>> 1;
                val = bits % n;
            } while (bits - val + (n - 1) < 0L);
            return val;
        }
    }

    /**
     * P(i)=1/(max-min)
     */
    public static class Flat extends DiscreteRNG {
        private static final long serialVersionUID = 1L;

        /**
         * Generate random integers from min (inclusive) to max (exclusive)
         * following even distribution.
         * 
         * @param random
         *            The basic random number generator.
         * @param min
         *            Minimum integer
         * @param max
         *            maximum integer (exclusive).
         */
        public Flat(Random random, long min, long max) {
            super(random, min, max);
        }

        /**
         * @see DiscreteRNG#nextInt()
         */
        @Override
        protected long nextLongImpl() {
            // error checking and 2^x checking removed for simplicity.
            long bits, val;
            do {
                bits = (random.nextLong() << 1) >>> 1;
                val = bits % (this.range_size - 1);
            } while (bits - val + (this.range_size - 1) < 0L);
            val += this.min;
            assert (val >= min);
            assert (val < max);
            return val;
        }
    }

    /**
     * P(i)=1/(max-min)
     */
    public static class FlatHistogram<T> extends DiscreteRNG {
        private static final long serialVersionUID = 1L;
        private final Flat inner;
        private final Histogram<T> histogram;
        private final SortedMap<Long, T> value_rle = new TreeMap<Long, T>();
        private Histogram<T> history;

        /**
         * Generate a run-length of the values of the histogram
         */
        public FlatHistogram(Random random, Histogram<T> histogram) {
            super(random, 0, (int) histogram.getSampleCount());
            this.histogram = histogram;
            this.inner = new Flat(random, 0, (int) histogram.getSampleCount());

            long total = 0;
            for (T k : this.histogram.values()) {
                long v = this.histogram.get(k);
                total += v;
                this.value_rle.put(total, k);
            } // FOR
        }

        @Override
        public void enableHistory() {
            this.history = new ObjectHistogram<T>();
        }

        @Override
        public boolean isHistoryEnabled() {
            return (this.history != null);
        }

        public Histogram<T> getHistogramHistory() {
            if (this.history != null) {
                return (this.history);
            }
            return (null);
        }

        public T nextValue() {
            int idx = this.inner.nextInt();
            Long total = this.value_rle.tailMap((long) idx).firstKey();
            T val = this.value_rle.get(total);
            if (this.history != null)
                this.history.put(val);
            return (val);
            // assert(false) : "Went beyond our expected total '" + idx + "'";
            // return (null);
        }

        /**
         * @see DiscreteRNG#nextLong()
         */
        @Override
        protected long nextLongImpl() {
            Object val = this.nextValue();
            if (val instanceof Integer) {
                return ((Integer) val);
            }
            return ((Long) val);
        }
    }

    /**
     * Gaussian Distribution
     */
    public static class Gaussian extends DiscreteRNG {
        private static final long serialVersionUID = 1L;

        public Gaussian(Random random, long min, long max) {
            super(random, min, max);
        }

        @Override
        protected long nextLongImpl() {
            int value = -1;
            while (value < 0 || value >= this.range_size) {
                double gaussian = (this.random.nextGaussian() + 2.0) / 4.0;
                value = (int) Math.round(gaussian * this.range_size);
            }
            return (value + this.min);
        }
    }

	public static class HotWarmCold extends DiscreteRNG {
		
		int hot_data_access_skew; 
		int warm_data_access_skew; 
		int hot_data_size; 
		int warm_data_size; 

		// the max of the hot/warm/cold ranges, where hot_data_max < warm_data_max < max
		int min; 
		int max; 
		int hot_data_max; 	// integers in the range 0 < x < hot_data_max will represent the "hot" numbers getting hot_data_access_skew% of the accesses 
		int warm_data_max;  // integers in the range hot_data_max < x < warm_data_max will represent the "warm" numbers
		
		public HotWarmCold(Random r, int _min, int _max, int _hot_data_access_skew, int _hot_data_size, int _warm_data_access_skew, int _warm_data_size) {
			super(r, _min, _max); 
			
			assert(_hot_data_access_skew + _warm_data_access_skew <= 100) : "Workload skew cannot be more than 100%."; 

			hot_data_access_skew = _hot_data_access_skew; 
			warm_data_access_skew = _warm_data_access_skew; 
			hot_data_size = _hot_data_size; 
			warm_data_size = _warm_data_size;

			min = _min; 
			max = _max;  

			hot_data_max = (int)(max * (hot_data_size / (double)100)) + min;
			warm_data_max = (int)(max * (warm_data_size / (double)100)) + hot_data_max; 
		}
		
		@Override
        protected long nextLongImpl()
		{
			int key = 0; 
			int access_skew_rand = random.nextInt(100); 

			if(access_skew_rand < hot_data_access_skew)  // generate a number in the "hot" data range, 0 < x < hot_data_max
			{
				key = random.nextInt(hot_data_max) + min;
			}
			else if(access_skew_rand < hot_data_access_skew + warm_data_access_skew) // generate a key in the "warm" data range, hot_data_max < x < warm_data_max
			{
				key = random.nextInt(warm_data_max - hot_data_max + 1) + hot_data_max; 
			}
			else  // generate a number in the "cold" data range, warm_data_max < x < max
			{
				key = random.nextInt(max - warm_data_max + 1) + warm_data_max; 
			}

			return key; 
		}
	}

    /**
     * Zipf distribution. The ratio of the probabilities of integer i and j is
     * defined as follows: P(i)/P(j)=((j-min+1)/(i-min+1))^sigma.
     */
    public static class Zipf extends DiscreteRNG {
        private static final long serialVersionUID = 1L;
        private static final double DEFAULT_EPSILON = 0.001;
        private final ArrayList<Long> k;
        private final ArrayList<Double> v;

        /**
         * Constructor
         * 
         * @param r
         *            The random number generator.
         * @param min
         *            minimum integer (inclusvie)
         * @param max
         *            maximum integer (exclusive)
         * @param sigma
         *            parameter sigma. (sigma > 1.0)
         */
        public Zipf(Random r, long min, long max, double sigma) {
            this(r, min, max, sigma, DEFAULT_EPSILON);
        }

        /**
         * Constructor.
         * 
         * @param r
         *            The random number generator.
         * @param min
         *            minimum integer (inclusvie)
         * @param max
         *            maximum integer (exclusive)
         * @param sigma
         *            parameter sigma. (sigma > 1.0)
         * @param epsilon
         *            Allowable error percentage (0 < epsilon < 1.0).
         */
        public Zipf(Random r, long min, long max, double sigma, double epsilon) {
            super(r, min, max);
            if ((max <= min) || (sigma <= 1) || (epsilon <= 0) || (epsilon >= 0.5)) {
                throw new IllegalArgumentException("Invalid arguments [min=" + min + ", max=" + max + ", sigma=" + sigma + ", epsilon=" + epsilon + "]");
            }
            k = new ArrayList<Long>();
            v = new ArrayList<Double>();

            double sum = 0;
            long last = -1;
            for (long i = min; i < max; ++i) {
                sum += Math.exp(-sigma * Math.log(i - min + 1));
                if ((last == -1) || i * (1 - epsilon) > last) {
                    k.add(i);
                    v.add(sum);
                    last = i;
                }
            } // FOR

            if (last != max - 1) {
                k.add(max - 1);
                v.add(sum);
            }

            v.set(v.size() - 1, 1.0);

            for (int i = v.size() - 2; i >= 0; --i) {
                v.set(i, v.get(i) / sum);
            }
        }

        /**
         * @see DiscreteRNG#nextInt()
         */
        @Override
        protected long nextLongImpl() {
            double d = random.nextDouble();
            int idx = Collections.binarySearch(v, d);

            if (idx > 0) {
                ++idx;
            } else {
                idx = -(idx + 1);
            }

            if (idx >= v.size()) {
                idx = v.size() - 1;
            }

            if (idx == 0) {
                return k.get(0);
            }

            long ceiling = k.get(idx);
            long lower = k.get(idx - 1);

            return ceiling - DiscreteRNG.nextLong(random, ceiling - lower);
        }
    }

    /**
     * Binomial distribution. P(k)=select(n, k)*p^k*(1-p)^(n-k) (k = 0, 1, ...,
     * n) P(k)=select(max-min-1, k-min)*p^(k-min)*(1-p)^(k-min)*(1-p)^(max-k-1)
     */
    public static final class Binomial extends DiscreteRNG {
        private static final long serialVersionUID = 1L;
        private final double[] v;
        private final long n;

        private static double select(long n, long k) {
            double ret = 1.0;
            for (long i = k + 1; i <= n; ++i) {
                ret *= (double) i / (i - k);
            }
            return ret;
        }

        private static double power(double p, long k) {
            return Math.exp(k * Math.log(p));
        }

        /**
         * Generate random integers from min (inclusive) to max (exclusive)
         * following Binomial distribution.
         * 
         * @param random
         *            The basic random number generator.
         * @param min
         *            Minimum integer
         * @param max
         *            maximum integer (exclusive).
         * @param p
         *            parameter.
         */
        public Binomial(Random random, long min, long max, double p) {
            super(random, min, max);
            this.n = max - min - 1;
            if (n > 0) {
                v = new double[(int) n + 1];
                double sum = 0.0;
                for (int i = 0; i <= n; ++i) {
                    sum += select(n, i) * power(p, i) * power(1 - p, n - i);
                    v[i] = sum;
                }
                for (int i = 0; i <= n; ++i) {
                    v[i] /= sum;
                }
            } else {
                v = null;
            }
        }

        /**
         * @see DiscreteRNG#nextInt()
         */
        @Override
        protected long nextLongImpl() {
            if (v == null) {
                return min;
            }
            double d = random.nextDouble();
            int idx = Arrays.binarySearch(v, d);
            if (idx > 0) {
                ++idx;
            } else {
                idx = -(idx + 1);
            }

            if (idx >= v.length) {
                idx = v.length - 1;
            }
            return idx + min;
        }
    } 
    /**
     *       
     * Power Law distribution.
     *       
     * k = 1 + alpha
     * x = [(max^k - min^k) * y - min^k]^(1/k)
     * where y ~ uniform(0, 1)
     */

    public static class PowerLaw extends DiscreteRNG {
        private static final long serialVersionUID = 1L;
        private final double alpha_;

        /**
         * Constructor.
         * 
         * @param r
         *            The random number generator.
         * @param min
         *            minimum integer (inclusvie)
         * @param max
         *            maximum integer (exclusive)
         * @param alpha
         *            parameter alpha. (alpha < 0)
         */
        public PowerLaw(Random r, long min_x, long max_x, double alpha) {
            super(r, min_x, max_x);
            if ((max <= min) || (alpha >= 0)) {
                throw new IllegalArgumentException("Invalid arguments [min=" + min + ", max=" + max + ", alpha=" + alpha + "]");
            }
            alpha_ = alpha;
        }

        /**
         * @see DiscreteRNG#nextInt()
         */
        @Override
        protected long nextLongImpl() {
            double y = random.nextDouble();
            long x = 1;
            double k = 1 + alpha_;

            x = Math.round(Math.pow((Math.pow(max, k) - Math.pow(min, k)) * y + Math.pow(min, k), 1/k));
            return x;
        }
    }
}
