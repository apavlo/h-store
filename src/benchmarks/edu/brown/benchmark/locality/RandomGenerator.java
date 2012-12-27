/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Written by:                                                            *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.locality;

import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONStringer;
import org.voltdb.utils.Pair;

import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.FileUtil;

/**
 * @author pavlo
 */
public class RandomGenerator extends AbstractRandomGenerator implements JSONString {
    public enum AffinityRecordMembers {
        MINIMUM, MAXIMUM, MAP, SIGMA,
    }

    /**
     * Table Name -> Affinity Record
     */
    protected final SortedMap<Pair<String, String>, AffinityRecord> affinity_map = new TreeMap<Pair<String, String>, AffinityRecord>();
    protected final transient SortedMap<String, Set<Pair<String, String>>> table_pair_xref = new TreeMap<String, Set<Pair<String, String>>>();

    /**
     * 
     */
    protected class AffinityRecord {
        private final SortedMap<Integer, Integer> map = new TreeMap<Integer, Integer>();
        private final transient SortedMap<Integer, RandomDistribution.Zipf> distributions = new TreeMap<Integer, RandomDistribution.Zipf>();
        private double zipf_sigma = 1.01d;
        private int minimum;
        private int maximum;
        private transient int range;
        private final transient SortedMap<Integer, ObjectHistogram> histograms = new TreeMap<Integer, ObjectHistogram>();

        public AffinityRecord() {
            // For serialization...
        }

        public AffinityRecord(int minimum, int maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
            this.range = this.maximum - this.minimum;
        }

        public double getZipfSigma() {
            return (this.zipf_sigma);
        }

        public void setZipfSigma(double zipf_sigma) {
            // We have to regenerate all the random number generators
            if (this.zipf_sigma != zipf_sigma) {
                this.zipf_sigma = zipf_sigma;
                for (int source : this.map.keySet()) {
                    int target = this.map.get(source);
                    this.distributions.put(source, new RandomDistribution.Zipf(RandomGenerator.this, target, target + this.range, this.zipf_sigma));
                } // FOR
            }
        }

        public int getMinimum() {
            return (this.minimum);
        }

        public int getMaximum() {
            return (this.maximum);
        }

        public Set<Integer> getSources() {
            return (this.map.keySet());
        }

        public int getTarget(int source) {
            return (this.map.get(source));
        }

        public RandomDistribution.Zipf getDistribution(int source) {
            return (this.distributions.get(source));
        }

        public ObjectHistogram getHistogram(int source) {
            return (this.histograms.get(source));
        }

        public void add(int source, int target) {
            this.map.put(source, target);
            this.distributions.put(source, new RandomDistribution.Zipf(RandomGenerator.this, target, target + this.range, this.zipf_sigma));
            this.histograms.put(source, new ObjectHistogram());
        }

        public void toJSONString(JSONStringer stringer) throws JSONException {
            stringer.key(AffinityRecordMembers.SIGMA.name()).value(this.zipf_sigma);
            stringer.key(AffinityRecordMembers.MINIMUM.name()).value(this.minimum);
            stringer.key(AffinityRecordMembers.MAXIMUM.name()).value(this.maximum);

            stringer.key(AffinityRecordMembers.MAP.name()).object();
            for (Integer source_id : this.map.keySet()) {
                stringer.key(source_id.toString()).value(this.map.get(source_id));
            } // FOR
            stringer.endObject();
        }

        public void fromJSONObject(JSONObject object) throws JSONException {
            this.zipf_sigma = object.getDouble(AffinityRecordMembers.SIGMA.name());
            this.minimum = object.getInt(AffinityRecordMembers.MINIMUM.name());
            this.maximum = object.getInt(AffinityRecordMembers.MAXIMUM.name());
            this.range = this.maximum - this.minimum;

            JSONObject jsonMap = object.getJSONObject(AffinityRecordMembers.MAP.name());
            Iterator<String> i = jsonMap.keys();
            while (i.hasNext()) {
                String key = i.next();
                Integer source = Integer.parseInt(key);
                Integer target = jsonMap.getInt(key);
                assert (source != null);
                assert (target != null);
                this.add(source, target);
            } // WHILE
        }

        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            for (int source : this.map.keySet()) {
                int target = this.map.get(source);
                buffer.append(source).append(" => ").append(target).append("\n");
                buffer.append(this.histograms.get(source)).append("\n-------------\n");
            } // FOR
            return (buffer.toString());
        }
    } // END CLASS

    /**
     * Seeds the random number generator using the default Random() constructor.
     */
    public RandomGenerator() {
        super(0);
    }

    /**
     * Seeds the random number generator with seed.
     * 
     * @param seed
     */
    public RandomGenerator(Integer seed) {
        super(seed);
    }

    /**
     * Set the Zipfian distribution sigma factor
     * 
     * @param zipf_sigma
     */
    public void setZipfSigma(String table, double zipf_sigma) {
        assert (this.affinity_map.containsKey(table));
        this.affinity_map.get(table).setZipfSigma(zipf_sigma);
    }

    private void addAffinityRecord(Pair<String, String> pair, AffinityRecord record) {
        this.affinity_map.put(pair, record);
        if (!this.table_pair_xref.containsKey(pair.getFirst())) {
            this.table_pair_xref.put(pair.getFirst(), new HashSet<Pair<String, String>>());
        }
        this.table_pair_xref.get(pair.getFirst()).add(pair);
    }

    /**
     * Sets the affinity preference of a source value to a target
     * 
     * @param source_id
     * @param target_id
     */
    public void addAffinity(String source_table, int source_id, String target_table, int target_id, int minimum, int maximum) {
        assert (minimum <= target_id) : "Min check failed (" + minimum + " <= " + target_id + ")";
        assert (maximum >= target_id);
        Pair<String, String> pair = Pair.of(source_table, target_table);
        AffinityRecord record = this.affinity_map.get(pair);
        if (record == null) {
            record = new AffinityRecord(minimum, maximum);
        } else {
            assert (this.affinity_map.get(pair).getMinimum() == minimum);
            assert (this.affinity_map.get(pair).getMaximum() == maximum);
        }
        record.add(source_id, target_id);
        this.addAffinityRecord(pair, record);
    }

    /**
     * Return all the tables that we have a record for
     * 
     * @return
     */
    public Set<String> getTables() {
        return (this.table_pair_xref.keySet());
    }

    public Set<Pair<String, String>> getTables(String source_table) {
        return (this.table_pair_xref.get(source_table));
    }

    public Integer getTarget(String source_table, int source_id, String target_table) {
        Pair<String, String> pair = Pair.of(source_table, target_table);
        AffinityRecord record = this.affinity_map.get(pair);
        return (record != null ? record.getTarget(source_id) : null);
    }

    /**
     * @param record
     * @param source_id
     * @param exclude_base
     * @return
     */
    private int nextInt(AffinityRecord record, int source_id, boolean exclude_base) {
        RandomDistribution.Zipf zipf = record.getDistribution(source_id);
        assert (zipf != null);
        int value = zipf.nextInt();

        int max = record.getMaximum();
        if (exclude_base) {
            long range_start = record.getTarget((int) zipf.getMin());
            if (value >= range_start)
                value++;
            if (value > max) {
                value = (value % max) + 1;
                if (value == max)
                    value--;
            }
        } else if (value > max) {
            value = (value % max) + 1;
        }
        record.getHistogram(source_id).put(value);

        return (value);
    }

    @Override
    public int numberAffinity(int minimum, int maximum, int base, String source_table, String target_table) {
        Pair<String, String> pair = Pair.of(source_table, target_table);
        AffinityRecord record = this.affinity_map.get(pair);
        int value;

        /*
         * System.out.println("min=" + minimum); System.out.println("max=" +
         * maximum); System.out.println("base=" + base);
         * System.out.println("table=" + table); System.out.println("record=" +
         * (record != null)); System.exit(1);
         */

        // Use an AffinityRecord to generate next number
        if (record != null && record.getDistribution(base) != null) {
            value = this.nextInt(record, base, false);
            // Otherwise, just use the default random number generator
        } else {
            value = super.number(minimum, maximum);
        }
        return (value);
    }

    /**
     * Generate an affinity-skewed random number that excludes the number given
     */
    @Override
    public int numberExcluding(int minimum, int maximum, int excluding, String source_table, String target_table) {
        Pair<String, String> pair = Pair.of(source_table, target_table);
        AffinityRecord record = this.affinity_map.get(pair);
        int value;

        // Use an AffinityRecord to generate next number
        if (record != null && record.getDistribution(excluding) != null) {
            value = this.nextInt(record, excluding, true);

            // Otherwise, just use the default random number generator
        } else {
            value = super.numberExcluding(minimum, maximum, excluding);
        }
        return (value);
    }

    /**
     * 
     */
    @Override
    public void loadProfile(String input_path) throws Exception {
        String contents = FileUtil.readFile(input_path);
        if (contents.isEmpty()) {
            throw new Exception("The partition plan file '" + input_path + "' is empty");
        }
        JSONObject jsonObject = new JSONObject(contents);
        System.out.println(jsonObject.toString(2));
        this.fromJSONObject(jsonObject);
    }

    /**
     * @param output_path
     * @throws Exception
     */
    @Override
    public void saveProfile(String output_path) throws Exception {
        String json = this.toJSONString();
        JSONObject jsonObject = new JSONObject(json);

        FileOutputStream out = new FileOutputStream(output_path);
        out.write(jsonObject.toString(2).getBytes());
        out.close();
    }

    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            this.toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        for (String source_table : this.getTables()) {
            stringer.key(source_table).object();
            for (Pair<String, String> pair : this.getTables(source_table)) {
                stringer.key(pair.getSecond()).object();
                this.affinity_map.get(pair).toJSONString(stringer);
                stringer.endObject();
            } // FOR
            stringer.endObject();
        } // FOR
    }

    public void fromJSONObject(JSONObject object) throws JSONException {
        Iterator<String> i = object.keys();
        while (i.hasNext()) {
            String source_table = i.next();
            JSONObject innerObject = object.getJSONObject(source_table);
            Iterator<String> j = innerObject.keys();
            while (j.hasNext()) {
                String target_table = j.next();
                Pair<String, String> pair = Pair.of(source_table, target_table);
                AffinityRecord record = new AffinityRecord();
                record.fromJSONObject(innerObject.getJSONObject(target_table));
                this.addAffinityRecord(pair, record);
            } // WHILE
        } // WHILE
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(this.getClass().getSimpleName()).append("\n");
        for (String source_table : this.getTables()) {
            for (Pair<String, String> pair : this.getTables(source_table)) {
                buffer.append("TABLES: ").append(pair).append("\n");
                buffer.append(this.affinity_map.get(pair));
            } // FOR
        } // FOR
        return (buffer.toString());
    }

    /**
     * Construct profile for TPC-C warehouses.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int minimum = 1;
        int num_warehouses = Integer.parseInt(args[0]);
        String output_path = args[1];
        RandomGenerator rand = new RandomGenerator();

        /**
         * int half = (int)Math.ceil(num_warehouses / 2.0d); String table_name =
         * Constants.TABLENAME_ORDERLINE; for (int ctr = minimum; ctr < half;
         * ctr++) { int source = ctr; int target = ctr + half;
         * rand.addAffinity(table_name, source, target, minimum,
         * num_warehouses); rand.addAffinity(table_name, target, source,
         * minimum, num_warehouses); } // FOR if ((half - minimum) % 2 == 1) {
         * rand.addAffinity(table_name, half, half, minimum, num_warehouses); }
         */
        System.out.println(rand.toJSONString());
        rand.saveProfile(output_path);
        System.out.println("Saved RandomGenerator profile to '" + output_path + "'");
    }
}