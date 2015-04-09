/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *								   
 *                                                                         *
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

package edu.brown.benchmark.ycsb;

import java.util.Random;

public abstract class YCSBUtil {

    public static final Random rand = new Random();

    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = (int)(minimum_length == maximum_length ?
                                minimum_length :
                                number(minimum_length, maximum_length));
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
        long value = Math.abs(rand.nextInt()) % (maximum - minimum + 1) + minimum;
        value = Math.abs(value); // "fixing" a strange out of range error
        assert(minimum <= value && value <= maximum) :
            String.format("Unexpected invalid random number '%d' [min=%d / max=%d]",
                          value, minimum, maximum);
        return value;
    }
}
