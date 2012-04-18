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

package edu.brown.benchmark.tpce.generators;

import java.util.Date;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.EGenMoney;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * This class is used to simulate monetary security exchanges
 * both for the loader and for the driver *
 */
public class MEESecurity {
    /*
     *   Period of security price change (in seconds)
     *   (e.g., when the price will repeat)
     */
    private final static int secPricePeriod = 900;  // 15 minutes

    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_MEE_SECURITY);
    
    private final EGenMoney rangeLow = new EGenMoney(TPCEConstants.minSecPrice);
    private final EGenMoney rangeHigh = new EGenMoney(TPCEConstants.maxSecPrice);
    private final EGenMoney range = new EGenMoney(TPCEConstants.maxSecPrice - TPCEConstants.minSecPrice);
    private final int period = secPricePeriod;      // time to get to the same price (in seconds)

    
    private int tradingTimeSoFar;
    private Date baseTime, currTime;
    private double meanInTheMoneySubmissionDelay;
    
    /**
     * Initializes (resets) the generator.
     * 
     * @param tradingTimeSoFar time for picking up where we last left off on the price curve
     * @param baseTime
     * @param currentTime
     * @param meanInTheMoneySubmissionDelay Mean delay between Pending and Submission times 
     *                                      for an immediately triggered (in-the-money) limit order.
     *                                      The actual delay is randomly calculated in the range [0.5 * Mean .. 1.5 * Mean]
     */
    public void init(int tradingTimeSoFar, Date baseTime, Date currentTime, double meanInTheMoneySubmissionDelay) {
        this.tradingTimeSoFar = tradingTimeSoFar;
        this.baseTime = baseTime;
        this.currTime = currentTime;
        this.meanInTheMoneySubmissionDelay = meanInTheMoneySubmissionDelay;
        rnd.setSeed(EGenRandom.RNG_SEED_BASE_MEE_SECURITY);
    }
    
   /**
    * Calculate the "unique" starting offset
    * in the price curve based on the security ID (0-based)
    * 0 corresponds to rangeLow price,
    * period/2 corresponds to rangeHigh price,
    * period corresponds again to rangeLow price
    *
    * @param secId unique security index to generate a unique starting price
    * @return time from which to calculate initial price
    *  
    */
    private double initialTime(long secId) {
        return ((((tradingTimeSoFar * 1000) + (secId * 556237 + 253791)) % (secPricePeriod * 1000)) / 1000.0); // 1000 here is for millisecs
    }
    
    public EGenMoney calculatePrice(long secId, double time) {
        double periodTime = (time + initialTime(secId)) / period;
        double timeWithinPeriod = (periodTime - (int)periodTime) * period;

        double pricePosition; // 0..1 corresponding to rangeLow..rangeHigh
        if (timeWithinPeriod < period / 2) {
            pricePosition = timeWithinPeriod / (period / 2);
        }
        else {
            pricePosition = (period - timeWithinPeriod) / (period / 2);
        }

        EGenMoney priceCents = new EGenMoney(range);
        priceCents.multiplyByDouble(pricePosition);
        priceCents.add(rangeLow);

        return priceCents;
    }
}
