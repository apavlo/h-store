/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *                                                                         *
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

package edu.brown.benchmark.bikerstream;

import java.util.Random;

import edu.brown.rand.RandomDistribution.Zipf;

/* primarily a placeholder for now */

public abstract class BikerStreamUtil {

    public static final Random rand = new Random();
    public static Zipf zipf = null;
    public static final double zipf_sigma = 1.001d;

    /**
     * Return the number of contestants to use for the given scale factor
     * @param scaleFactor
     */
    /*
    public static int getScaledNumContestants(double scaleFactor) {
        int min_contestants = 1;
        int max_contestants = VoterWinSStoreConstants.CONTESTANT_NAMES_CSV.split(",").length;
        
        int num_contestants = (int)Math.round(VoterWinSStoreConstants.NUM_CONTESTANTS * scaleFactor);
        if (num_contestants < min_contestants) num_contestants = min_contestants;
        if (num_contestants > max_contestants) num_contestants = max_contestants;
        
        return (num_contestants);
    }*/
   /* 
    public static int isActive() {
        return number(1, 100) < number(86, 100) ? 1 : 0;
    } */

    // modified from tpcc.RandomGenerator
    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator
    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = number(minimum_length, maximum_length).intValue();
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static Long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    public static String padWithZero(Long n) {
        String meat = n.toString();
        char[] zeros = new char[15 - meat.length()];
        for (int i = 0; i < zeros.length; i++)
            zeros[i] = '0';
        return (new String(zeros) + meat);
    }

    /**
     * Returns sub array of arr, with length in range [min_len, max_len]. Each
     * element in arr appears at most once in sub array.
     */
    public static int[] subArr(int arr[], int min_len, int max_len) {
        assert min_len <= max_len && min_len >= 0;
        int sub_len = number(min_len, max_len).intValue();
        int arr_len = arr.length;

        assert sub_len <= arr_len;

        int sub[] = new int[sub_len];
        for (int i = 0; i < sub_len; i++) {
            int j = number(0, arr_len - 1).intValue();
            sub[i] = arr[j];
            // arr[j] put to tail
            int tmp = arr[j];
            arr[j] = arr[arr_len - 1];
            arr[arr_len - 1] = tmp;

            arr_len--;
        } // FOR

        return sub;
    }

    /**
     * Compute the distance in miles between two GPS coordinate points (lat1, lon1)
     * and (lat2, lon2) using Haversine formula.
     *
     * @param lat1 Latitude of the first point in degree
     * @param lon1 Longitude of the first point in degree
     * @param lat2 Latitude of the second point in degree
     * @param lon2 Longitude of the second point in degree
     * @return  The distance between two GPS coordinates in miles
     */
    public static double geoDistance(double lat1, double lon1, double lat2, double lon2) {
        final double EARTH_RADIUS = 3958.761;

        final double lat1Rad = Math.toRadians(lat1);
        final double lon1Rad = Math.toRadians(lon1);
        final double lat2Rad = Math.toRadians(lat2);
        final double lon2Rad = Math.toRadians(lon2);
        final double latDiff = Math.abs(lat2Rad - lat1Rad);
        final double lonDiff = Math.abs(lon2Rad - lon1Rad);

        double m =  haversine(latDiff) + (Math.cos(lat1Rad) * Math.cos(lat2Rad) * haversine(lonDiff));
        return EARTH_RADIUS * 2 * Math.atan2(Math.sqrt(m), Math.sqrt(1-m));
    }

    private static double haversine(double angle) {
        return Math.sin(angle / 2) * Math.sin(angle / 2);
    }
}
