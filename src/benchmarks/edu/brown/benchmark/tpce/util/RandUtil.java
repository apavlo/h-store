/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
 *                                                                         *
 *  Modifications by:                                                      *
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
/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
 *                                                                         *
 *  Modifications by:                                                      *
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
package edu.brown.benchmark.tpce.util;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;

/**
 * @author zhe
 */
public class RandUtil {

    private static final Random rand = new Random();

    private static final Integer signs[] = { 1, -1 };

    private static final Integer bools[] = { 0, 1 };

    public static Object randNum(String numType) {
        if (numType.equals("IDENT_T"))
            return randNum(11);
        if (numType.equals("VALUE_T"))
            return randSNum(10, 2);
        if (numType.equals("BALANCE_T"))
            return randSNum(12, 2);
        if (numType.equals("FIN_AGG_T"))
            return randSNum(15, 2);
        if (numType.equals("S_COUNT_T"))
            return randNum(12);
        if (numType.equals("S_PRICE_T"))
            return randNum(8, 2);
        if (numType.equals("S_QTY_T"))
            return randSNum(6);
        if (numType.equals("TRADE_T"))
            return randNum(15);
        if (numType.equals("BOOLEAN"))
            return oneOf(bools);
        throw new RuntimeException("Invalid number type: " + numType);
    }

    public static long randNum(int m) {
        return (long) randNum(m, 0);
    }

    /**
     * @param m
     * @param n
     * @return An Unsigned numeric value with m total digits, of which n digits
     *         are to the right(after) the decimal point.
     */
    public static double randNum(int m, int n) {
        assert (m >= n);
        int left = m - n;
        return Double.parseDouble(nstring(left, left) + "." + nstring(n, n));
    }

    private static double randSNum(int m, int n) {
        return (Integer) oneOf(signs) * randNum(m, n);
    }

    private static long randSNum(int m) {
        return (long) randSNum(m, 0);
    }

    public static String randChars(int len) {
        return astring(len, len);
    }

    public static Object oneOf(Object[] objects) {
        return objects[(int) (Math.random() * objects.length)];
    }

    /**
     * @return a random Date in range [min_date, max_date]
     */
    public static Date randDate(Date min_date, Date max_date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(max_date);
        int maxDate = cal.get(Calendar.DATE);
        cal.setTime(min_date);
        int minDate = cal.get(Calendar.DATE);

        int diff = maxDate - minDate;

        cal.add(Calendar.DATE, (int) randNum(0, diff));
        return cal.getTime();
    }

    // modified from tpcc.RandomGenerator
    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    private static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator
    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    private static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    private static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
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
}
