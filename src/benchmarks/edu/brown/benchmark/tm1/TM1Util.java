package edu.brown.benchmark.tm1;

import java.util.Random;

import edu.brown.rand.RandomDistribution.Zipf;

public abstract class TM1Util {

    public static final Random rand = new Random();
    public static Zipf zipf = null;
    public static final double zipf_sigma = 1.001d;

    public static int isActive() {
        return number(1, 100) < number(86, 100) ? 1 : 0;
    }

    public static long getSubscriberId(long subscriberSize) {
        // We have to initalize the zipfian random distribution the first time
        // we are called
        // if (zipf == null) {
        // zipf = new Zipf(rand, 1, subscriberSize, zipf_sigma);
        // }
        // return (zipf.nextLong());
        return (TM1Util.number(1, subscriberSize));
    }

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
        int length = (int)number(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    public static String padWithZero(long n) {
        return String.format("%015d", n);
    }

    /**
     * Returns sub array of arr, with length in range [min_len, max_len]. Each
     * element in arr appears at most once in sub array.
     */
    public static int[] subArr(int arr[], int min_len, int max_len) {
        assert min_len <= max_len && min_len >= 0;
        int sub_len = (int)number(min_len, max_len);
        int arr_len = arr.length;

        assert sub_len <= arr_len;

        int sub[] = new int[sub_len];
        for (int i = 0; i < sub_len; i++) {
            int j = (int)number(0, arr_len - 1);
            sub[i] = arr[j];
            // arr[j] put to tail
            int tmp = arr[j];
            arr[j] = arr[arr_len - 1];
            arr[arr_len - 1] = tmp;

            arr_len--;
        } // FOR

        return sub;
    }

}
