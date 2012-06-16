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

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.TradeGenerator.TradeType;
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
    private final static int SEC_PRICE_PERIOD = 900;  // 15 minutes
    
    // Mean delay between Submission and Completion times
    private final static double MEAN_COMPLETION_TIME_DELAY = 1.0;
    
    // Delay added to the clipped MEE Completion delay
    // to simulate SUT-to-MEE and MEE-to-SUT processing delays.
    private static final double COMPLETION_SUT_DELAY = 1.0;

    private final EGenRandom rnd;
    
    private final EGenMoney rangeLow;
    private final EGenMoney rangeHigh;
    private final EGenMoney range;
    private final int period;      // time to get to the same price (in seconds)
    
    private int tradingTimeSoFar;
    private Date baseTime, currTime;
    private double meanInTheMoneySubmissionDelay;
    
    public MEESecurity() {
        rnd = new EGenRandom(EGenRandom.RNG_SEED_BASE_MEE_SECURITY);
        
        rangeLow = new EGenMoney(TPCEConstants.minSecPrice);
        rangeHigh = new EGenMoney(TPCEConstants.maxSecPrice);
        range = new EGenMoney(TPCEConstants.maxSecPrice - TPCEConstants.minSecPrice);
        period = SEC_PRICE_PERIOD;      // time to get to the same price (in seconds)
    }
    
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
        return ((((tradingTimeSoFar * 1000) + (secId * 556237 + 253791)) % (SEC_PRICE_PERIOD * 1000)) / 1000.0); // 1000 here is for millisecs
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
    
   /**
    * Calculate triggering time for limit orders.
    *
    * @param  secIndex    Unique security index to generate a unique starting price
    * @param  pendingTime Pending time of the order, in seconds from time 0
    * @param  limitPrice  Limit price of the order
    * @param  tradeType   Order trade type
    * 
    * @return The expected submission time
    */
    public double getSubmissionTime(long secIndex, double pendingTime, EGenMoney limitPrice, TradeType tradeType) {
        EGenMoney priceAtPendingTime = calculatePrice(secIndex, pendingTime);
        double submissionTimeFromPending;  // Submission - Pending time difference

        /*
         *  Check if the order can be fulfilled immediately
         *  i.e., if the current price is less than the buy price
         *  or the current price is more than the sell price.
         */
        if (((tradeType == TradeType.eLimitBuy || tradeType == TradeType.eStopLoss) && priceAtPendingTime.lessThanOrEqual(limitPrice)) ||
            ((tradeType == TradeType.eLimitSell) && limitPrice.lessThanOrEqual(priceAtPendingTime))) {
            // Trigger the order immediately.
            submissionTimeFromPending = rnd.doubleRange(0.5 * meanInTheMoneySubmissionDelay, 1.5 * meanInTheMoneySubmissionDelay);
        }
        else {
            int directionAtPendingTime;
            if ((int)(pendingTime + initialTime(secIndex)) % period < period / 2) {
                //  In the first half of the period => price is going up
                directionAtPendingTime = 1;
            }
            else {
                //  In the second half of the period => price is going down
                directionAtPendingTime = -1;
            }

            submissionTimeFromPending = calculateTime(priceAtPendingTime, limitPrice, directionAtPendingTime);
        }

        return pendingTime + submissionTimeFromPending;
    }
    
   /**
    * Calculate time required to move between certain prices
    * with certain initial direction of price change.
    *
    * @param startPrice     Price at the start of the time interval
    * @param endPrice       Price at the end of the time interval
    * @param startDirection Direction (up or down) on the price curve at the start of the time interval
    *
    * @return Seconds required to move from the start price to the end price
    */
    private double calculateTime(EGenMoney startPrice, EGenMoney endPrice, int startDirection) {
        int halfPeriod = period / 2;

        // Distance on the price curve from StartPrice to EndPrice (in dollars)
        EGenMoney distance;

        // Amount of time (in seconds) needed to move $1 on the price curve.
        // In half a period the price moves over the entire price range.
        double speed = halfPeriod / range.getDollars();

        if (startPrice.lessThan(endPrice)) {
            if (startDirection > 0) {
                distance = EGenMoney.subMoney(endPrice, startPrice);
            }
            else {
                distance = EGenMoney.addMoney(EGenMoney.subMoney(startPrice, rangeLow), EGenMoney.subMoney(endPrice, rangeLow));
            }
        }
        else {
            if (startDirection > 0) {
                distance = EGenMoney.addMoney(EGenMoney.subMoney(rangeHigh, startPrice), EGenMoney.subMoney(rangeHigh, endPrice));
            }
            else {
                distance = EGenMoney.subMoney(startPrice, endPrice);
            }
        }

        return distance.getDollars() * speed;
    }
    
   /**
    *  Negative exponential distribution.
    *
    *  PARAMETERS:
    *           IN  fMean  - mean value of the distribution
    *
    *  RETURNS:
    *           random value according to the negative
    *           exponential distribution with the given mean.
    */
    private double negExp(double mean) {
        return (-1.0 * Math.log(rnd.rndDouble())) * mean;
    }
    
    
   /**
    * Return the expected completion time and the completion price.
    * Completion time is between 0 and 5 seconds
    * with 1 sec mean.
    *
    * Used to calculate completion time for
    * both limit (first must get submission time)
    * and market orders.
    *
    * Equivalent of MEE function sequence
    * 'receive trade' then 'complete the trade request'.
    *
    * @param secIndex Unique security index to generate a unique starting price
    * @param submissionTime Time when the order was submitted, in seconds from time 0
    *
    * @return The approximated completion time for the trade and the price
    */
    public Object[] getCompletionTimeAndPrice(long secIndex, double submissionTime) {
        double completionDelay = negExp(MEAN_COMPLETION_TIME_DELAY);

        // Clip at 5 seconds to prevent rare, but really long delays
        if (completionDelay > 5.0) {
            completionDelay = 5.0;
        }

        Object[] res = new Object[2];
        res[0] = submissionTime + completionDelay + COMPLETION_SUT_DELAY;
        res[1] = calculatePrice(secIndex, submissionTime + completionDelay);

        return res;
    }
    
    public EGenMoney getMinPrice(){
    	return rangeLow;
    }
    
    public EGenMoney getMaxPrice(){
    	return rangeHigh;
    }
    
    public EGenMoney getCurrentPrice(long secId){
    	GregorianCalendar currGreTime = new GregorianCalendar();
    	GregorianCalendar baseGreTime = new GregorianCalendar();
    	currGreTime.setTime(currTime);
    	baseGreTime.setTime(baseTime);
    	return calculatePrice(secId, (currGreTime.getTimeInMillis() - baseGreTime.getTimeInMillis()) / 1000);
    }
}
