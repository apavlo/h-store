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

import java.util.Random;

import junit.framework.TestCase;

import org.json.JSONObject;
import org.voltdb.utils.Pair;

import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;

public class TestRandomGenerator extends TestCase {
    static RandomGenerator generator;
    static final int num_warehouses = 10;
    static final String table_name = "WAREHOUSE";
    static final int seed = 1;

    public void setUp() {
        if (generator == null) {
            generator = new RandomGenerator(seed);
            Random rand = new Random();
            for (int w_id = 1; w_id <= num_warehouses; w_id++) {
                int other_id = w_id;
                while (other_id == w_id)
                    other_id = rand.nextInt(num_warehouses) + 1;
                generator.addAffinity(table_name, w_id, table_name, other_id, 1, num_warehouses);
            } // FOR
        }
    }

    /**
     * testAddAffinity
     */
    public void testAddAffinity() {
        int source = 1;
        int target = 5;
        String table = "DISTRICT";
        generator.addAffinity(table, source, table, target, 1, num_warehouses);

        assertTrue(generator.getTables().contains(table));
        assertNotNull(generator.getTables(table));
        assertEquals(1, generator.getTables(table).size());

        for (Pair<String, String> pair : generator.getTables(table)) {
            assertNotNull(generator.affinity_map.get(pair));
            assertNotNull(generator.affinity_map.get(pair).getDistribution(source));
            assertEquals(target, generator.affinity_map.get(pair).getTarget(source));
            assertEquals(1, generator.affinity_map.get(pair).getMinimum());
            assertEquals(num_warehouses, generator.affinity_map.get(pair).getMaximum());
        } // FOR
    }

    /**
     * testNumberAffinity
     */
    public void testNumberAffinity() {
        int source = 1;
        int count = 10000;
        
        Histogram<Integer> histogram = new ObjectHistogram<Integer>();
        while (count-- > 0) {
            Integer value = generator.numberAffinity(1, num_warehouses, source, table_name, table_name);
            assertNotNull(value);
            assertTrue(value >= 1);
            assertTrue(value <= num_warehouses);
            histogram.put(value);
        } // WHILE

        // Make sure our target has the most entries in the histogram
        Integer expected = generator.getTarget(table_name, source, table_name);
        assertNotNull(expected);
        assertEquals(expected, CollectionUtil.first(histogram.getMaxCountValues()));
    }

    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = generator.toJSONString();
        assertNotNull(json);
        assertTrue(json.indexOf(table_name) != -1);
        for (RandomGenerator.AffinityRecordMembers element : RandomGenerator.AffinityRecordMembers.values()) {
            // System.out.println(element); System.out.flush();
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
        // System.out.println(json);
    }

    /**
     * testFromJSONString
     */
    public void testFromJSONString() throws Exception {
        String json = generator.toJSONString();
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        RandomGenerator copy = new RandomGenerator(seed);
        // System.out.println(jsonObject.toString(2));
        copy.fromJSONObject(jsonObject);

        for (String source_table : generator.getTables()) {
            assertTrue(copy.getTables().contains(source_table));

            for (Pair<String, String> pair : generator.getTables(source_table)) {
                RandomGenerator.AffinityRecord orig_record = generator.affinity_map.get(pair);
                RandomGenerator.AffinityRecord copy_record = copy.affinity_map.get(pair);

                assertEquals(orig_record.getMinimum(), copy_record.getMinimum());
                assertEquals(orig_record.getMaximum(), copy_record.getMaximum());

                for (Integer source : orig_record.getSources()) {
                    assertTrue(copy_record.getSources().contains(source));
                    assertEquals(orig_record.getTarget(source), copy_record.getTarget(source));
                    assertNotNull(copy_record.getDistribution(source));
                } // FOR
            } // FOR
        } // FOR
    }

}
