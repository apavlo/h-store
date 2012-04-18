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
    /*
     * These are different seed bases for pseudo-random EGen generator.
     */
    
    // Default seed used for all tables.
    public static final long RNG_SEED_TABLE_DEFAULT = 37039940;

    // This value is added to the AD_ID when seeding the RNG for
    // generating a threshold into the TownDivisionZipCode list.
    public static final long RNG_SEED_BASE_TOWN_DIV_ZIP = 26778071;

    // This is the base seed used when generating C_TIER.
    public static final long RNG_SEED_BASE_C_TIER = 16225173;

    // Base seeds used for generating C_AREA_1, C_AREA_2, C_AREA_3
    public static final long RNG_SEED_BASE_C_AREA_1 = 97905013;
    public static final long RNG_SEED_BASE_C_AREA_2 = 68856487;
    public static final long RNG_SEED_BASE_C_AREA_3 = 67142295;

    // Base seed used when generating names.
    public static final long RNG_SEED_BASE_FIRST_NAME = 95066470;
    public static final long RNG_SEED_BASE_MIDDLE_INITIAL = 71434514;
    public static final long RNG_SEED_BASE_LAST_NAME = 35846049;

    // Base seed used when generating gender.
    public static final long RNG_SEED_BASE_GENDER = 9568922;

    // Base seed used when generating tax ID
    public static final long RNG_SEED_BASE_TAX_ID = 8731255;

    // Base seed used when generating the number of accounts for a customer
    //public static final long RNG_SEED_BASE_NUMBER_OF_ACCOUNTS = 37486207;

    // Base seed used when generating the number of permissions on an account
    public static final long RNG_SEED_BASE_NUMBER_OF_ACCOUNT_PERMISSIONS = 27794203;

    // Base seeds used when generating CIDs for additional account permissions
    public static final long RNG_SEED_BASE_CID_FOR_PERMISSION1 = 76103629;
    public static final long RNG_SEED_BASE_CID_FOR_PERMISSION2 = 103275149;

    // Base seed used when generating acount tax status
    public static final long RNG_SEED_BASE_ACCOUNT_TAX_STATUS = 34376701;

    // Base seed for determining account broker id
    public static final long RNG_SEED_BASE_BROKER_ID = 75607774;

    // Base seed used when generating tax rate row
    public static final long RNG_SEED_BASE_TAX_RATE_ROW = 92740731;

    // Base seed used when generating the number of holdings for an account
    public static final long RNG_SEED_BASE_NUMBER_OF_SECURITIES = 23361736;

    // Base seed used when generating the starting security ID for the
    // set of securities associated with a particular account.
    public static final long RNG_SEED_BASE_STARTING_SECURITY_ID = 12020070;

    // Base seed used when generating a company's SP Rate
    public static final long RNG_SEED_BASE_SP_RATE = 56593330;

    // Base seed for initial trade generation class
    public static final long RNG_SEED_TRADE_GEN = 32900134;

    // Base seed for the MEESecurity class
    public static final long RNG_SEED_BASE_MEE_SECURITY = 75791232;

    // Base seed for non-uniform customer selection
    public static final long RNG_SEED_CUSTOMER_SELECTION = 9270899;

    // Base seed for MEE Ticker Tape
    public static final long RNG_SEED_BASE_MEE_TICKER_TAPE = 42065035;

    // Base seed for MEE Trading Floor
    public static final long RNG_SEED_BASE_MEE_TRADING_FLOOR = 25730774;

    // Base seed for TxnMixGenerator
    public static final long RNG_SEED_BASE_TXN_MIX_GENERATOR = 87944308;

    // Base seed for TxnInputGenerator
    public static final long RNG_SEED_BASE_TXN_INPUT_GENERATOR = 80534927;
    
    
    private long rnd;
    private native long newEGenRandom(long seed);
    
    private native long rndNthElement(long rnd, long baseSeed, long count);
    private native long getSeed(long rnd);
    private native void setSeed(long rnd, long seed);
    private native void setSeedNth(long rnd, long seed, long count);
    
    private native int intRange(long rnd, int min, int max);
    private native long int64Range(long rnd, long min, long max);
    
    private native int intRangeExclude(long rnd, int low, int high, int exclude);
    private native long int64RangeExclude(long rnd, long low, long high, long exclude);
    
    private native int rndNthIntRange(long rnd, long seed, long count, int min, int max);
    private native long rndNthInt64Range(long rnd, long seed, long count, long min, long max);
    
    private native double doubleRange(long rnd, double min, double max);
    private native double doubleIncrRange(long rnd, double min, double max, double incr); // incr -- precision
    
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

    public void setSeedNth(long seed, long count) {
        setSeedNth(rnd, seed, count);        
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
    
    public boolean rndPercent(int percent) {
        return intRange(1, 100) <= percent;
    }
    
    public int rndPercentage() {
        return intRange(1, 100);
    }
}
