/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
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

public class EGenRandom {
    private long rnd;
    private native long newEGenRandom(long seed);
    
    private native long rndNthElement(long rnd, long baseSeed, long count);
    private native long getSeed(long rnd);
    private native void setSeed(long rnd, long seed);
    
    private native int intRange(long rnd, int min, int max);
    private native long int64Range(long rnd, long min, long max);
    
    private native int intRangeExclude(long rnd, int low, int high, int exclude);
    private native long int64RangeExclude(long rnd, long low, long high, long exclude);
    
    private native int rndNthIntRange(long rnd, long seed, long count, int min, int max);
    private native long rndNthInt64Range(long rnd, long seed, long count, long min, long max);
    
    private native double doubleRange(long rnd, double min, double max);
    private native double doubleIncrRange(long rnd, double min, double max, double incr); // incr -- precision
    private native double doubleNegExp(long rnd, double mean); // negative exponential distribution
    
    private native long rndNU(long rnd, long p, long q, int a, int s);
    private native String rndAlphaNumFormatted(long rnd, String format);
    
    
    static {
        System.loadLibrary("egen_random");
    }
    
    public EGenRandom() {
        this(System.currentTimeMillis());
    }
    public EGenRandom(long seed) {
        this.rnd = newEGenRandom(seed);
    }
    
    public long rndNthElement(long baseSeed, long count) {
        return rndNthElement(rnd, baseSeed, count);
    }
    public long getSeed() {
        return getSeed(rnd);
    }
    
    public void setSeed(long seed) {
        setSeed(rnd, seed);
    }

    public int intRange(int min, int max) {
        return intRange(rnd, min, max);
    }
    
    public long int64Range(long min, long max) {
        return int64Range(rnd, min, max);
    }

    public int intRangeExclude(int low, int high, int exclude) {
        return intRangeExclude(rnd, low, high, exclude);
    }
    
    public long int64RangeExclude(long low, long high, long exclude) {
        return int64RangeExclude(rnd, low, high, exclude);
    }

    public int rndNthIntRange(long seed, long count, int min, int max) {
        return rndNthIntRange(rnd, seed, count, min, max);
    }
    
    public long rndNthInt64Range(long seed, long count, long min, long max) {
        return rndNthInt64Range(rnd, seed, count, min, max);
    }

    public double doubleRange(double min, double max) {
        return doubleRange(rnd, min, max);
    }
    
    // incr -- precision
    public double doubleIncrRange(double min, double max, double incr) {
        return doubleIncrRange(rnd, min, max, incr);
    }
    
    // negative exponential distribution
    public double doubleNegExp(double mean) {
        return doubleNegExp(rnd, mean);
    }

   /* 
    *  Returns a non-uniform random 64-bit integer in range of [P .. Q].
    *  
    *  NURnd is used to create a skewed data access pattern.  The function is
    *  similar to NURand in TPC-C.  (The two functions are identical when C=0
    *  and s=0.)
    *
    *  The parameter A must be of the form 2^k - 1, so that Rnd[0..A] will
    *  produce a k-bit field with all bits having 50/50 probability of being 0
    *  or 1.
    *
    *  With a k-bit A value, the weights range from 3^k down to 1 with the
    *  number of equal probability values given by C(k,i) = k! /(i!(k-i)!) for
    *  0 <= i <= k.  So a bigger A value from a larger k has much more skew.
    *
    *  Left shifting of Rnd[0..A] by "s" bits gets a larger interval without
    *  getting huge amounts of skew.  For example, when applied to elapsed time
    *  in milliseconds, s=10 effectively ignores the milliseconds, while s=16
    *  effectively ignores seconds and milliseconds, giving a granularity of
    *  just over 1 minute (65.536 seconds).  A smaller A value can then give
    *  the desired amount of skew at effectively one-minute resolution.
    */
    public long rndNU(long p, long q, int a, int s) {
        return rndNU(rnd, p, q, a, s);
    }
    
    //Returns random alphanumeric string obeying a specific format.
    //For the format: n - given character must be numeric
    //                a - given character must be alphabetical
    //Example: "nnnaannnnaannn"
    public String rndAlphaNumFormatted(String format) {
        return rndAlphaNumFormatted(rnd, format);
    }
}
