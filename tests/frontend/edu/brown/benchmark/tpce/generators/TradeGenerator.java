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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.PriorityQueue;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.NotImplementedException;

import weka.gui.beans.Startable;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpce.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenMoney;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * Note that this class generates all the growing tables. Since they depend on the same
 * process of generating trades, it would be hard to separate them and, moreover,
 * very inefficient.
 * 
 * next() and hasNext() method work as expected, but the caller should check the table for
 * which next() just gave the next tuple. Tuples from different tables can be intermixed. So,
 * hasNext() should be read as "we have another tuple for one of the growing tables".
 * 
 * @author akalinin
 */
public class TradeGenerator implements Iterator<Object[]> {
    public enum TradeType {
        eMarketBuy(0),
        eMarketSell(1),
        eStopLoss(2),
        eLimitSell(3),
        eLimitBuy(4);
        
        private final int id;
        
        private TradeType(int id) {
            this.id = id;
        }
        
        public int getValue() {
            return id;
        }
    }
    
    private class TradeInfo {
        public long tradeId;
        
        // customer that executes the trade
        public long customer; 
        public TierId customerTier;
        
        public long accId; // account for the trade
        public long secFileIndex; // symbol record from the security file
        public int  secAccIndex; // index of the symbol from the account basket
        
        public TradeType tradeType;
        public StatusTypeId tradeStatus;
        
        public EGenMoney bidPrice;
        public EGenMoney tradePrice;
        public int tradeQty;
        
        // All times are in seconds from the start time
        public double completionTime;
        public double submissionTime;
        public double pendingTime; // only for limit buys/sells
        
        public boolean isLIFO;
    }
    
    private class HoldingInfo {
        public long tradeId;
        public int tradeQty;
        EGenMoney tradePrice;
        Date buyDTS; // absolute time (i.e., EGenDate here)
        long symbolIndex;   // stock symbol index in the input flat file - stored for performance
    }
    
    private class AddTradeInfo {
        public EGenMoney buyValue;
        public EGenMoney sellValue;
    }
    
    // this list holds pregenerated holding history rows
    private List<Object[]> holdingHistory = new ArrayList<Object[]>();
    
    private final static double MEAN_IN_THE_MONEY_SUBMISSION_DELAY = 1.0;
    
    // Percentage of trades modifying holdings in Last-In-First-Out order.
    private static final int PERCENT_TRADE_IS_LIFO = 35;
    
   /*
    * Maximum number of HOLDING_HISTORY rows that can be output
    * for one completed trade.
    * 
    * Determined by the maximum number of holdings that a trade
    * can modify. The maximum number would be a trade with
    * the biggest possible quantity modifying holdings each having
    * the smallest possible quantity.
    */
    private static final int MAX_HOLDING_HISTORY_ROWS_PER_TRADE = 800 / 100;
    
    private class TradeInfoComparator implements Comparator<TradeInfo> {
        @Override
        public int compare(TradeInfo t1, TradeInfo t2) {
            if (t1.completionTime < t2.completionTime) {
                return 1;
            }
            else if (t1.completionTime > t2.completionTime) {
                return -1;
            }
            else {
                return 0;
            }
        }
    }
    
    private final long totalTrades;
    private final int tradesPerWorkDay;
    private final double meanBetweenTrades;
    
    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TRADE_GEN);
    private final CustomerSelection custSelection;
    private final HoldingsAndTrades holdsGenerator;
    private final MEESecurity meeSecurity;
    
    private long currCompletedTrades, currInitiatedTrades;
    private TradeInfo newTrade;
    private final AddTradeInfo addTrade = new AddTradeInfo(); // generated once for every trade row
    
    //private List<List<HoldingInfo>> customerHoldings = new ArrayList<List<HoldingInfo>>();
    private final List<HoldingInfo>[][] customerHoldings;
    private ListIterator<HoldingInfo> holdingIterator;
    
    private int currAccountForHolding, currSecurityForHolding;
    
    private final long startFromAccount; // starting account number for customers
    
    private double currentSimulatedTime;
    private Date startTime;
    private long currentTradeId;
    
    private final TradeInfoComparator tradeComparator = new TradeInfoComparator();
    private PriorityQueue<TradeInfo> currentTrades = new PriorityQueue<TradeGenerator.TradeInfo>(11, tradeComparator);
    
    private final Database catalogDb;
    
    /**
     * @param catalog_tbl
     * @param generator
     */
    public TradeGenerator(TPCEGenerator generator, Database catalogDb) {
        int scalingFactor = generator.getScalingFactor();
        int hoursOfInitialTrades = generator.getInitTradeDays() * 8; // 8 hours per work day
        
        totalTrades = hoursOfInitialTrades * 3600 * TPCEConstants.DEFAULT_LOAD_UNIT / scalingFactor; // 8 hours per work day
        tradesPerWorkDay = 8 * 3600 * TPCEConstants.DEFAULT_LOAD_UNIT / scalingFactor *
                HoldingsAndTrades.ABORT_TRADE / 100; // 8 hours per work day, 3600 seconds per hour
        meanBetweenTrades = 100.0 / HoldingsAndTrades.ABORT_TRADE * (double)scalingFactor / TPCEConstants.DEFAULT_LOAD_UNIT;
        
        customerHoldings = (List<HoldingInfo>[][])new ArrayList[TPCEConstants.DEFAULT_LOAD_UNIT * CustomerAccountsGenerator.MAX_ACCOUNTS_PER_CUST]
                [HoldingsAndTrades.MAX_SECURITIES_PER_ACCOUNT];
        
        currentTradeId = hoursOfInitialTrades * 3600 * (generator.getStartCustomer() - TPCEConstants.DEFAULT_START_CUSTOMER_ID) /
                scalingFactor * HoldingsAndTrades.ABORT_TRADE / 100 + TPCEConstants.IDENT_SHIFT;
        
        custSelection = new CustomerSelection(rnd, 0, 0, 100, generator.getStartCustomer(), TPCEConstants.DEFAULT_LOAD_UNIT);
        holdsGenerator = new HoldingsAndTrades(generator);
        
        meeSecurity = new MEESecurity();
        meeSecurity.init(0, null, null, MEAN_IN_THE_MONEY_SUBMISSION_DELAY);
        
        startFromAccount = CustomerAccountsGenerator.getStartingAccId(generator.getStartCustomer() + TPCEConstants.IDENT_SHIFT);
        
        this.catalogDb = catalogDb;
        
        startTime = EGenDate.getDateFromTime(TPCEConstants.initialTradePopulationBaseYear,
                TPCEConstants.initialTradePopulationBaseMonth, 
                TPCEConstants.initialTradePopulationBaseDay,
                TPCEConstants.initialTradePopulationBaseHour,
                TPCEConstants.initialTradePopulationBaseMinute,
                TPCEConstants.initialTradePopulationBaseSecond,
                TPCEConstants.initialTradePopulationBaseFraction);
    }
    
    private double generateDelay() {
        return rnd.doubleIncrRange(0.0, meanBetweenTrades, 0.001);
    }
    
    private void generateNewTrade() {
        newTrade = new TradeInfo();
        
        newTrade.tradeId = ++currentTradeId;
        newTrade.customer = custSelection.genRandomCustomer(newTrade.customerTier);
        
        long[] accIdAndSecs = holdsGenerator.generateRandomAccSecurity(newTrade.customer, newTrade.customerTier);
        newTrade.accId = accIdAndSecs[0];
        newTrade.secAccIndex = (int)accIdAndSecs[1];
        newTrade.secFileIndex = accIdAndSecs[2];
        
        newTrade.tradeType = generateTradeType();
        newTrade.tradeStatus = StatusTypeId.E_COMPLETED; // always "completed" for the loading phase
        
        newTrade.bidPrice = new EGenMoney(rnd.doubleIncrRange(TPCEConstants.minSecPrice, TPCEConstants.maxSecPrice, 0.01));
        newTrade.tradeQty = HoldingsAndTrades.TRADE_QTY_SIZES[rnd.intRange(0, HoldingsAndTrades.TRADE_QTY_SIZES.length - 1)];
        
        if (newTrade.tradeType == TradeType.eMarketBuy || newTrade.tradeType == TradeType.eMarketSell) {
            newTrade.submissionTime = currentSimulatedTime;
            newTrade.bidPrice = meeSecurity.calculatePrice(newTrade.secAccIndex, currentSimulatedTime); // correcting the price
        }
        else { // limit order
            newTrade.pendingTime = currentSimulatedTime;
            newTrade.submissionTime = meeSecurity.getSubmissionTime(newTrade.secAccIndex, newTrade.pendingTime,
                    newTrade.bidPrice, newTrade.tradeType);
        }
        
        /*
         * Move orders that would submit after market close (5pm)
         * to the beginning of the next day.
         *
         * Submission time here is kept from the beginning of the day, even though
         * it is later output to the database starting from 9am. So time 0h corresponds
         * to 9am, time 8hours corresponds to 5pm.
         * 
         * -- The comment is from EGen
         */
        if (((int)(newTrade.submissionTime / 3600) % 24 == 8) &&   // 24 hour day, 8 hours work days; >=5pm
            ((newTrade.submissionTime / 3600) - (int)(newTrade.submissionTime / 3600) > 0)) {  // fractional seconds exist, e.g. not 5:00pm
            newTrade.submissionTime += 16 * 3600;   // add 16 hours to move to 9am next day
        }
        
        // get a completion time and a trade price
        newTrade.completionTime = meeSecurity.getCompletionTime(newTrade.secAccIndex, newTrade.submissionTime, newTrade.tradePrice);
        
        // Make sure the trade has the right price based on the type of trade.
        if ((newTrade.tradeType == TradeType.eLimitBuy && newTrade.bidPrice.lessThan(newTrade.tradePrice)) ||
            (newTrade.tradeType == TradeType.eLimitSell && newTrade.tradePrice.lessThan(newTrade.bidPrice))) {
            newTrade.tradePrice = newTrade.bidPrice;
        }

        // determine if the trade is a LIFO one
        if (rnd.rndPercent(PERCENT_TRADE_IS_LIFO)) {
            newTrade.isLIFO = true;
        }
        else {
            newTrade.isLIFO = false;
        }

        currInitiatedTrades++;
    }
    
    private TradeType generateTradeType() {
        TradeType res = TradeType.eLimitBuy; // to suppress the warning
        
        /* 
         * Generate Trade Type
         * NOTE: The order of these "if" tests is significant!
         */
        int loadTradeTypePct = rnd.rndPercentage();
        if (loadTradeTypePct <= HoldingsAndTrades.MARKET_BUY_LOAD_THRESHOLD) {         //  1% - 30%
            res = TradeType.eMarketBuy;
        }
        else if(loadTradeTypePct <= HoldingsAndTrades.MARKET_SELL_LOAD_THRESHOLD) {   // 31% - 60%
            res = TradeType.eMarketSell;
        }
        else if(loadTradeTypePct <= HoldingsAndTrades.LIMIT_BUY_LOAD_THRESHOLD) {     // 61% - 80%
            res = TradeType.eLimitBuy;
        }
        else if (loadTradeTypePct <= HoldingsAndTrades.LIMIT_SELL_LOAD_THRESHOLD) {    // 81% - 90%
            res = TradeType.eLimitSell;
        }
        else if(loadTradeTypePct <= HoldingsAndTrades.STOP_LOSS_LOAD_THRESHOLD) {     // 91% - 100%
            res = TradeType.eStopLoss;
        }
        else {
            assert false;  // this should never happen
        }

        return res;
    }
    
    private void generateCompleteTrade() {
        
    }
    
    private void updateHoldings() {
        addTrade.buyValue = new EGenMoney(0);
        addTrade.sellValue = new EGenMoney(0);
        
        int neededQty = newTrade.tradeQty;
        
        // new series of holding history rows 
        holdingHistory.clear();
        
        List<HoldingInfo> holdingList = getHoldingListForCurrentTrade();
        int holdingIndex = positionAtHoldingList(holdingList, newTrade.isLIFO);
        
        if (newTrade.tradeType == TradeType.eMarketBuy || newTrade.tradeType == TradeType.eLimitBuy) {
            // Buy trade: liquidate negative (short) holdings
            while (!holdingList.isEmpty() && holdingList.get(holdingIndex).tradeQty < 0 && neededQty > 0) {
                HoldingInfo holding = holdingList.get(holdingIndex);
                int holdQty = holding.tradeQty;
                
                holding.tradeQty += neededQty;                
                if (holding.tradeQty > 0) {
                    holding.tradeQty = 0; // holding fully closed
                    
                    addTrade.sellValue.add(EGenMoney.mulMoneyByInt(holding.tradePrice, -holdQty));
                    addTrade.buyValue.add(EGenMoney.mulMoneyByInt(newTrade.tradePrice, -holdQty));
                }
                else {
                    addTrade.sellValue.add(EGenMoney.mulMoneyByInt(holding.tradePrice, neededQty));
                    addTrade.buyValue.add(EGenMoney.mulMoneyByInt(newTrade.tradePrice, neededQty));
                }
                
                generateHoldingHistoryRow(holding.tradeId, newTrade.tradeId, holdQty, holding.tradeQty);
                
                if (holding.tradeQty == 0) {
                    holdingList.remove(holdingIndex);
                }
                
                holdingIndex = positionAtHoldingList(holdingList, newTrade.isLIFO);
                neededQty += holdQty;
            }
            
            if (neededQty > 0) {
                // Still shares left after closing positions => create a new holding
                HoldingInfo newHolding = new HoldingInfo();
                newHolding.tradeId = newTrade.tradeId;
                newHolding.tradeQty = neededQty;
                newHolding.tradePrice = newTrade.tradePrice;
                newHolding.buyDTS = getCurrentTradeCompletionTime();
                newHolding.symbolIndex = newTrade.secAccIndex;
                
                holdingList.add(newHolding);
                generateHoldingHistoryRow(newTrade.tradeId, newTrade.tradeId, 0, neededQty);
            }
        }
        else {
            // Sell trade: liquidate positive (long) holdings
            while (!holdingList.isEmpty() && holdingList.get(holdingIndex).tradeQty > 0 && neededQty > 0) {
                HoldingInfo holding = holdingList.get(holdingIndex);
                int holdQty = holding.tradeQty;
                
                holding.tradeQty -= neededQty;                
                if (holding.tradeQty < 0) {
                    holding.tradeQty = 0; // holding fully closed
                    
                    addTrade.sellValue.add(EGenMoney.mulMoneyByInt(newTrade.tradePrice, holdQty));
                    addTrade.buyValue.add(EGenMoney.mulMoneyByInt(holding.tradePrice, holdQty));
                }
                else {
                    addTrade.sellValue.add(EGenMoney.mulMoneyByInt(newTrade.tradePrice, neededQty));
                    addTrade.buyValue.add(EGenMoney.mulMoneyByInt(holding.tradePrice, neededQty));
                }
                
                generateHoldingHistoryRow(holding.tradeId, newTrade.tradeId, holdQty, holding.tradeQty);
                
                if (holding.tradeQty == 0) {
                    holdingList.remove(holdingIndex);
                }
                
                holdingIndex = positionAtHoldingList(holdingList, newTrade.isLIFO);
                neededQty -= holdQty;
            }
            
            if (neededQty > 0) {
                // Still shares left after closing positions => create a new holding
                HoldingInfo newHolding = new HoldingInfo();
                newHolding.tradeId = newTrade.tradeId;
                newHolding.tradeQty = -neededQty;
                newHolding.tradePrice = newTrade.tradePrice;
                newHolding.buyDTS = getCurrentTradeCompletionTime();
                newHolding.symbolIndex = newTrade.secAccIndex;
                
                holdingList.add(newHolding);
                generateHoldingHistoryRow(newTrade.tradeId, newTrade.tradeId, 0, -neededQty);
            }
        }
    }
    
    /*
    *   Helper function to get the list of holdings
    *   to modify after the last completed trade
    *
    *   RETURNS:
    *           reference to the list of holdings
    */
    private List<HoldingInfo> getHoldingListForCurrentTrade() {
        return customerHoldings[(int)(newTrade.accId - startFromAccount)][newTrade.secAccIndex - 1];
    }
    
    private int positionAtHoldingList(List<HoldingInfo> holdingList, boolean isLifo) {
        if (holdingList.isEmpty()) {
            return holdingList.size(); // iterator positioned after the last element
        }
        else if (isLifo) {
            return holdingList.size() - 1; // position before the last element
        }
        else {
            return 0;
        }
    }
    
    private void findNextHolding() {
        
    }
    
    private void findNextHoldingList() {
        
    }
    
    private void generateHoldingHistoryRow(long holdingTradeId, long tradeTradeId, int beforeQty, int afterQty) {
        if (holdingHistory.size() < MAX_HOLDING_HISTORY_ROWS_PER_TRADE) {
            int columnNum = getColumnNum(TPCEConstants.TABLENAME_HOLDING_HISTORY);
            Object[] tuple = new Object[columnNum];
            
            tuple[0] = holdingTradeId; // hh_h_t_id
            tuple[1] = tradeTradeId; // hh_t_id
            tuple[2] = beforeQty; // hh_before_qty
            tuple[3] = afterQty; // hh_after_qty
            
            holdingHistory.add(tuple);
        }
    }
    
    private boolean generateNextTrade() {
        boolean moreTrades;
        
        if (currCompletedTrades < totalTrades) {
            /*
             * While the earliest completion time is before the current
             * simulated ('Trade Order') time, keep creating new
             * incomplete trades putting them on the queue and
             * incrementing the current simulated time.
             */
            while ((currCompletedTrades + currentTrades.size() < totalTrades) &&
                   (currentTrades.isEmpty() || currentSimulatedTime < currentTrades.peek().completionTime)) {
                
                currentSimulatedTime = (currInitiatedTrades / tradesPerWorkDay) * 3600 * 24 + // seconds per day
                        (currInitiatedTrades % tradesPerWorkDay) * meanBetweenTrades + generateDelay();
                
                generateNewTrade();
                
                // ignore aborted trades
                if (HoldingsAndTrades.isAbortedTrade(currInitiatedTrades)) {
                    continue;
                }
                
                currentTrades.add(newTrade);
            }
            
            newTrade = currentTrades.remove();
            
            updateHoldings();
            
            generateCompleteTrade();
            
            moreTrades = currCompletedTrades < totalTrades;
        }
        else {
            moreTrades = false;
        }
        
        if (!moreTrades) {
            holdingIterator = customerHoldings[currAccountForHolding][currSecurityForHolding].listIterator();
            
            findNextHolding();
            findNextHoldingList();
            
            assert currentTrades.size() == 0;
        }
        
        return moreTrades;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public Object[] next() {
        // TODO Auto-generated method stub
        return null;
    }

    public void remove() {
        throw new NotImplementedException("Remove not implemented");
    }
    
    private int getColumnNum(String tableName) {
        return catalogDb.getTables().get(tableName).getColumns().size();
    }
    
    private Date getCurrentTradeCompletionTime() {
        int daysFromStart  = (int)(newTrade.completionTime / (3600 * 24));
        int msecsFromStart = (int)((newTrade.completionTime - daysFromStart * 3600 * 24) * 1000);
        
        return EGenDate.addDaysMsecs(startTime, daysFromStart, msecsFromStart, true); // add days and msec and adjust for weekend
    }
    
}
