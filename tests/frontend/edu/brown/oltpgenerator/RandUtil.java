package edu.brown.oltpgenerator;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class RandUtil {

    private static final Random seed = new Random();

    /**
     * 
     * @param m
     * @param n
     * @return An Unsigned numeric value with m total digits, of which n digits
     *         are to the right(after) the decimal point.
     */
    public static double randDouble(int m, int n) {
        assert (m >= n);
        int left = m - n;
        return Double.parseDouble(randNString(left, left) + "." + randNString(n, n));
    }

    public static String randChars(int len) {
        return randAString(len, len);
    }

    public static Object oneOf(Object[] objects) {
        return objects[(int) (Math.random() * objects.length)];
    }

    public static Date randDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, 365 - seed.nextInt(730));
        return calendar.getTime();
    }

    public static Date randDateTime() {
        return randDate();
    }

    // modified from tpcc.RandomGenerator
    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String randAString(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator
    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String randNString(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    private static String randomString(int minimum_length, int maximum_length,
            char base, int numCharacters) {
        int length = randLong(minimum_length, maximum_length).intValue();
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + randLong(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    public static Long randLong(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(seed.nextLong()) % (maximum - minimum + 1)
                + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }
    
    public static int randInt(int minimum, int maximum) {
        assert minimum <= maximum;
        int range_size = maximum - minimum + 1;
        int value = seed.nextInt(range_size);
        value += minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }
}

