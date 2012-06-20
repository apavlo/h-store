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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This class emulates the original EGen Random class.
 * Note that the original class uses unsigned 64-bit
 * arithmetics, which is non-existent in Java.
 * 
 * However, since Java uses the two's complement representation
 * it should work for all operations, except comparisons, which
 * should be treated differently.
 * 
 * Note, that the generator always output signed 64-bit numbers,
 * so this is definitely okay to represent them as long in Java.
 * 
 * @author akalinin
 *
 */
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
    
    // For alpha-numeric strings generation
    private static final String UPPER_CASE_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String NUMERALS = "0123456789";
    
    // for generating doubles
    private static final double RECIPROCAL_2_POWER_64 = 5.421010862427522E-20;
    
    /*
     * We use a linear congruential generator.
     * So here are the parameters.
     */
    private static final long A_MULT = 6364136223846793005L;
    private static final long C_INC  = 1;
    
    // the seed
    long seed;
    
    public EGenRandom() {
        this(System.currentTimeMillis());
    }
    
    public EGenRandom(long seed) {
        //this.rnd = newEGenRandom(seed);
        this.seed = seed;
    }
    
    public long getSeed() {
        return seed;
    }
    
    public void setSeed(long seed) {
        this.seed = seed;
    }

    // generates next 64-bit number
    private long int64Rand() {
        seed = seed * A_MULT + C_INC;
        
        return seed;
    }
    
    private long rndNthElement(long baseSeed, long count) {
        //return rndNthElement(rnd, baseSeed, count);
        // nothing to do
        if(count == 0) {
            return seed;
        }

       /*
        * Recursively compute X(n) = A * X(n-1) + C
        *
        * explicitly:
        *   X(n) = A^n * X(0) + { A^(n-1) + A^(n-2) + ... A + 1 } * C
        *
        * we write this as:
        *   X(n) = aPow(n) * X(0) + dSum(n) * C
        *
        * we use the following relations:
        *   aPow(n) = A^(n % 2) * aPow(n / 2) * aPow(n / 2)
        *   dSum(n) = (n % 2) * aPow(n / 2) * aPow(n / 2) + (aPow(n / 2) + 1) * dSum(n / 2)
        */

        // first get the highest non-zero bit
        int nBit;
        for (nBit = 0; (count >>> nBit) != 1; nBit++);

        long aPow = A_MULT;
        long dSum = 1;
        
        // go 1 bit at the time
        while (--nBit >= 0) {
          dSum *= (aPow + 1);
          aPow = aPow * aPow;
          if (((count >>> nBit) % 2) == 1) { // odd value
            dSum += aPow;
            aPow *= A_MULT;
          }
        }
        
        return baseSeed * aPow + dSum * C_INC;
    }
    
    /**
     * Returns a positive double value from the long value.
     *   
     * Must return a positive double!
     * 
     * However, v might be a negative number. We translate it to a unsigned string representation
     * and then to double via BigDecimal.
     * 
     * NOTE: It might be the case that some simpler conversion is enough, but all signed-only Java arithmetics gives
     * me a headache and this, at least, works as it prescribed and the same as with the original EGen.
     * 
     * @param b The value to convert
     * 
     * @return The resulting double value
     * 
     */
    private double longToDouble(long v) {
        if (v >= 0) {
            return (double)v;
        }
        else {
            return new BigDecimal(new BigInteger(Long.toBinaryString(v), 2)).doubleValue();
        }
    }
    
    /**
     * Returns a positive double value in the LCG sequence.
     *   
     * @param seed The seed
     * @param count The number in the sequence to return
     * 
     * @return Double value from the sequence
     * 
     */
    private double rndNthDouble(long seed, long count) {
        double rnd = longToDouble(rndNthElement(seed, count));
        return rnd * RECIPROCAL_2_POWER_64;
    }
    
    /**
     * Returns a random value in the range [0 .. 0.99999999999999999994578989137572]
     * 
     * @return The generated value
     * 
     */
    public double rndDouble() {
        double rnd = longToDouble(int64Rand());
        return rnd * RECIPROCAL_2_POWER_64;
    }
    
    public void setSeedNth(long seed, long count) {
        setSeed(rndNthElement(seed, count));
    }
    
    public int intRange(int min, int max) {
        if (min == max) {
            return min;
        }
        
        max++; // overflow?
        if (max <= min) {
            return max;
        }
        
        return min + (int)(rndDouble() * (double)(max - min));
    }
    
    public long int64Range(long min, long max) {
        if (min == max) {
            return min;
        }
        
        max++; // overflow?
        if (max <= min) {
            return max;
        }
        
        return min + (long)(rndDouble() * (double)(max - min));
    }

    public int intRangeExclude(int low, int high, int exclude) {
        int tmp;

        tmp = intRange(low, high-1);
        if (tmp >= exclude)
            tmp += 1;

        return tmp;
    }
    
    public long int64RangeExclude(long low, long high, long exclude) {
        long tmp;

        tmp = int64Range(low, high-1);
        if (tmp >= exclude)
            tmp += 1;

        return tmp;
    }

    public int rndNthIntRange(long seed, long count, int min, int max) {
        if (min == max) {
            return min;
        }
        
        max++;
        if (max <= min) {
            return max;
        }

        return min + (int)(rndNthDouble(seed, count) * (double)(max - min));
    }
    
    public long rndNthInt64Range(long seed, long count, long min, long max) {
        if (min == max) {
            return min;
        }
        
        max++;
        if (max <= min) {
            return max;
        }

        return min + (long)(rndNthDouble(seed, count) * (double)(max - min));
    }

    public double doubleRange(double min, double max) {
        return min + rndDouble() * (max - min);
    }
    
    /**
     *  @param incr Precision
     */
    public double doubleIncrRange(double min, double max, double incr) {
        long width = (long)((max - min) / incr);  // need [0..width], so no +1
        return min + ((double)int64Range(0, width) * incr);
    }
    
   /** 
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
        return (((int64Range(p, q) | (int64Range(0, a) << s)) % (q - p + 1)) + p);
        
    }
    
    /**
     * Returns random alphanumeric string obeying a specific format.
     * 
     * For the format: n - given character must be numeric
     *                 a - given character must be alphabetical
     *                 
     * All other symbols are just copied as is.                 
     *                 
     * @param format A format string as described above
     * @return A string corresponding to the format
     * 
     */
    public String rndAlphaNumFormatted(String format) {
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < format.length(); i++) {
            char c = format.charAt(i);
            switch (c) {
                case 'a':
                    sb.append(UPPER_CASE_LETTERS.charAt(intRange(0, UPPER_CASE_LETTERS.length() - 1)));
                    break;
                    
                case 'n':
                    sb.append(NUMERALS.charAt(intRange(0, NUMERALS.length() - 1)));
                    break;
                    
                default:
                    sb.append(c);
                    break;
            }
        }
        
        return sb.toString();
    }
    
    public boolean rndPercent(int percent) {
        return intRange(1, 100) <= percent;
    }
    
    public int rndPercentage() {
        return intRange(1, 100);
    }
}
