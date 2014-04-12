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

package edu.brown.benchmark.tpceb.generators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.voltdb.catalog.Database;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.NotImplementedException;

import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpceb.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpceb.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpceb.util.EGenDate;
import edu.brown.benchmark.tpceb.util.EGenMoney;
import edu.brown.benchmark.tpceb.util.EGenRandom;

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
        eMarketBuy,
        eMarketSell,
        eStopLoss,
        eLimitSell,
        eLimitBuy;
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
        public EGenMoney tax;

        public long brokerId;

        EGenMoney charge;
        EGenMoney commission;
        EGenMoney settlement;

       
        
        boolean isCash;
    }
    
    /*
     * Since we load a lot of different table via this generator,
     * this enum describes different phases of generating tuples.
     * 
     * This is basically the current table we are generating a tuple for.
     */
    private enum State {
        stNone,
        stTrade,
        stTradeHistory,
        stSettle,
        stCash,
        stHoldHistory,
        stBroker,
        stHoldSummary,
        stHolding
    }
    
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
    
    // Number of RNG calls for one simulated trade
    private static final int RNG_SKIP_ONE_TRADE = 11;    // average count for v3.5: 6.5
    
    // percent of cash transactions
    private static final int PERCENT_BUYS_ON_MARGIN = 16;
    
    // number of accounts per load unit
    private static final int LOAD_UNIT_ACCOUNT_COUNT = TPCEConstants.DEFAULT_LOAD_UNIT * CustomerCustAccCombined.MAX_ACCOUNTS_PER_CUST;
    
    /*
     * this lists hold pre-generated holding/trade history rows
     * the corresponding counters iterate through them in next()
     */
    private final List<Object[]> holdingHistory = new ArrayList<Object[]>();
    private final List<Object[]> tradeHistory = new ArrayList<Object[]>();
    private int holdHistoryCounter;
    private int tradeHistoryCounter;

    private final long totalTrades;
    private final int tradesPerWorkDay;
    private final double meanBetweenTrades;
    
    private boolean hasNextTrade; // if we can generate another trade after the current one
    private boolean hasNextHoldingSummary;
    private boolean hasNextHolding;
    
    private int currentLoadUnit = -1;
    private final int totalLoadUnits;
    private String currTable;
    private State currState = State.stNone;
    
    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TRADE_GEN);
    private final CustomerSelection custSelection;
    private final HoldingsAndTrades holdsGenerator;
    private final MEESecurity meeSecurity;
    private final CustomerCustAccCombined custAccGenerator;
   
    private final SecurityGenerator secGenerator;
    private final NewBrokerGenerator brokerGenerator;
    
  //  private final InputFileHandler chargeFile;
    private final InputFileHandler tradeTypeFile;
   // private final InputFileHandler commissionRateFile;
    private final InputFileHandler statusTypeFile;
    private final SecurityHandler secHandler;
 

    private long currCompletedTrades, currInitiatedTrades;
    private TradeInfo newTrade;
    private final AddTradeInfo addTrade = new AddTradeInfo(); // generated once for every trade row
    
    //private List<List<HoldingInfo>> customerHoldings = new ArrayList<List<HoldingInfo>>();
    private final List<HoldingInfo>[][] customerHoldings;
    private int holdingIterator; // it is not an iterator, just an index in the lisr pointed by (currAccountForHolding, currSecurityForHolding) 
    
    private int currAccountForHolding;
    private int currSecurityForHolding;
    private int currAccountForHoldingSummary;
    private int currSecurityForHoldingSummary;
    
    private long startFromAccount; // starting account number for customers
    private long startFromCustomer; // starting customer number
    
    private double currentSimulatedTime;
    private Date startTime;
    private long currentTradeId;
    
    // trade priority queue and comparator
    private class TradeInfoComparator implements Comparator<TradeInfo> {
        @Override
        public int compare(TradeInfo t1, TradeInfo t2) {
            if (t1.completionTime < t2.completionTime) {
                return -1;
            }
            else if (t1.completionTime > t2.completionTime) {
                return 1;
            }
            else {
                return 0;
            }
        }
    }
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
        
        customerHoldings = (List<HoldingInfo>[][])new ArrayList[LOAD_UNIT_ACCOUNT_COUNT][HoldingsAndTrades.MAX_SECURITIES_PER_ACCOUNT];
        for (int i = 0; i < LOAD_UNIT_ACCOUNT_COUNT; i++) {
            for (int j = 0; j < HoldingsAndTrades.MAX_SECURITIES_PER_ACCOUNT; j++) {
                customerHoldings[i][j] = new ArrayList<HoldingInfo>();
            }
        }
        
        currentTradeId = hoursOfInitialTrades * 3600 * (generator.getStartCustomer() - TPCEConstants.DEFAULT_START_CUSTOMER_ID) /
                scalingFactor * HoldingsAndTrades.ABORT_TRADE / 100 + TPCEConstants.TRADE_SHIFT;
        
        custSelection = new CustomerSelection(rnd, 0, 0, 100, generator.getStartCustomer(), TPCEConstants.DEFAULT_LOAD_UNIT);
        holdsGenerator = new HoldingsAndTrades(generator);
        secGenerator = (SecurityGenerator)generator.getTableGen(TPCEConstants.TABLENAME_SECURITY, null);
        brokerGenerator = new NewBrokerGenerator(catalogDb.getTables().get(TPCEConstants.TABLENAME_BROKER), generator);
        
        meeSecurity = new MEESecurity();
        
        startFromCustomer = generator.getStartCustomer() + TPCEConstants.IDENT_SHIFT;
        
        this.catalogDb = catalogDb;
        
        startTime = EGenDate.getDateFromTime(TPCEConstants.initialTradePopulationBaseYear,
                TPCEConstants.initialTradePopulationBaseMonth, 
                TPCEConstants.initialTradePopulationBaseDay,
                TPCEConstants.initialTradePopulationBaseHour,
                TPCEConstants.initialTradePopulationBaseMinute,
                TPCEConstants.initialTradePopulationBaseSecond,
                TPCEConstants.initialTradePopulationBaseFraction);

        custAccGenerator = (CustomerCustAccCombined) generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_INFO, null);
       
      //  chargeFile = generator.getInputFile(InputFile.CHARGE);
        tradeTypeFile = generator.getInputFile(InputFile.TRADETYPE);
        statusTypeFile = generator.getInputFile(InputFile.STATUS);
       // commissionRateFile = generator.getInputFile(InputFile.COMMRATE);
        secHandler = new SecurityHandler(generator);
        
        
        totalLoadUnits = (int)(generator.getCustomersNum() / TPCEConstants.DEFAULT_LOAD_UNIT);
    }
    
    private void initNextLoadUnit() {
     
        currCompletedTrades = 0;
        
        // empty customer holdings
        for (int i = 0; i < LOAD_UNIT_ACCOUNT_COUNT; i++) {
            for (int j = 0; j < HoldingsAndTrades.MAX_SECURITIES_PER_ACCOUNT; j++) {
                customerHoldings[i][j].clear();
            }
        }
        
        currAccountForHolding = 0;
        currSecurityForHolding = 0;
        
        currAccountForHoldingSummary = 0;
        currSecurityForHoldingSummary = -1; // for findNextHoldingList() correctness
        
        if (currentLoadUnit > 0) { // second and further load units
            startFromCustomer += TPCEConstants.DEFAULT_LOAD_UNIT;
        }
        
        startFromAccount = CustomerCustAccCombined.getStartingAccId(startFromCustomer);
        //System.out.println("Start from acc in trade:" + startFromAccount);
        currentSimulatedTime = 0;
        currInitiatedTrades = 0;
        
        brokerGenerator.initNextLoadUnit(TPCEConstants.DEFAULT_LOAD_UNIT, startFromCustomer - TPCEConstants.IDENT_SHIFT);
        
        // rnd generator
        long skipCount = startFromCustomer / TPCEConstants.DEFAULT_LOAD_UNIT * totalTrades;
        rnd.setSeedNth(EGenRandom.RNG_SEED_TRADE_GEN, skipCount * RNG_SKIP_ONE_TRADE);
        holdsGenerator.initNextLoadUnit(skipCount);
            
        if (currentLoadUnit > 0) { // second and further load units
            custSelection.setPartitionRange(startFromCustomer, TPCEConstants.DEFAULT_LOAD_UNIT);
        }
        
        meeSecurity.init(0, null, null, MEAN_IN_THE_MONEY_SUBMISSION_DELAY);
        
        hasNextHoldingSummary = true;
        hasNextHolding = true;
    }
    
    private double generateDelay() {
        return rnd.doubleIncrRange(0.0, meanBetweenTrades - 0.001, 0.001);
    }
    
    private void generateNewTrade() {
       // System.out.println("Generating new trade");
        newTrade = new TradeInfo();
       // System.out.println("created new trade");
        newTrade.tradeId = ++currentTradeId;
        //System.out.println("incremented id");
        Object[] custIdAndTier = custSelection.genRandomCustomer();
        newTrade.customer = (Long)custIdAndTier[0]; 
        newTrade.customerTier = (TierId)custIdAndTier[1];
        //System.out.println("set tier");
        long[] accIdAndSecs = holdsGenerator.generateRandomAccSecurity(newTrade.customer, newTrade.customerTier);
        newTrade.accId = accIdAndSecs[0];
        System.out.println("set accID");
        System.out.println("accID" + newTrade.accId);
        newTrade.secAccIndex = (int)accIdAndSecs[1];
        newTrade.secFileIndex = accIdAndSecs[2];
       // System.out.println("secFileIndex");
        newTrade.tradeType = generateTradeType();
       // System.out.println("generated type");
        
        newTrade.tradeStatus = StatusTypeId.E_COMPLETED; // always "completed" for the loading phase
       // System.out.println("set status");
        newTrade.bidPrice = new EGenMoney(rnd.doubleIncrRange(TPCEConstants.minSecPrice, TPCEConstants.maxSecPrice, 0.01));
       // System.out.println("double Incr range done");
        
        newTrade.tradeQty = HoldingsAndTrades.TRADE_QTY_SIZES[rnd.intRange(0, HoldingsAndTrades.TRADE_QTY_SIZES.length - 1)];
       // System.out.println("tradeQtydone");
        
        if (newTrade.tradeType == TradeType.eMarketBuy || newTrade.tradeType == TradeType.eMarketSell) {
          //  System.out.println("if statement");
            newTrade.submissionTime = currentSimulatedTime;
            newTrade.bidPrice = meeSecurity.calculatePrice(newTrade.secFileIndex, currentSimulatedTime); // correcting the price
           // System.out.println("bidPrice done");
        }
        else { // limit order
            //System.out.println("in else");
            newTrade.pendingTime = currentSimulatedTime;
            newTrade.submissionTime = meeSecurity.getSubmissionTime(newTrade.secFileIndex, newTrade.pendingTime,
                    newTrade.bidPrice, newTrade.tradeType);
           // System.out.println("got submission time");
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
        }
        
        // get a completion time and a trade price
        Object[] completionTimeAndPrice = meeSecurity.getCompletionTimeAndPrice(newTrade.secFileIndex, newTrade.submissionTime);
        newTrade.completionTime = (Double)completionTimeAndPrice[0];
        newTrade.tradePrice = (EGenMoney)completionTimeAndPrice[1];
        
        // Make sure the trade has the right price based on the type of trade.
        if ((newTrade.tradeType == TradeType.eLimitBuy && newTrade.bidPrice.lessThan(newTrade.tradePrice)) ||
            (newTrade.tradeType == TradeType.eLimitSell && newTrade.tradePrice.lessThan(newTrade.bidPrice))) {
            newTrade.tradePrice = newTrade.bidPrice;
            //System.out.println("tradePriceRight");
        }
       // System.out.println("beforeLIFO");
        // determine if the trade is a LIFO one
        if (rnd.rndPercent(PERCENT_TRADE_IS_LIFO)) {
            newTrade.isLIFO = true;
        }
        else {
            newTrade.isLIFO = false;
        }

        currInitiatedTrades++;
        //System.out.println("done");
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
   
    private void generateSettlementAmount() {
        String[] ttRow = tradeTypeFile.getTupleByIndex(newTrade.tradeType.ordinal());
        EGenMoney chargeVal = new EGenMoney(10);
        EGenMoney commVal = new EGenMoney(10);
        if (Integer.valueOf(ttRow[2]) == 1) { // is sell?
            addTrade.settlement = EGenMoney.mulMoneyByInt(newTrade.tradePrice, newTrade.tradeQty);
            addTrade.settlement.sub(chargeVal);
            addTrade.settlement.sub(commVal);
        }
        else {
            addTrade.settlement = EGenMoney.mulMoneyByInt(newTrade.tradePrice, newTrade.tradeQty);
            addTrade.settlement.add(chargeVal);
            addTrade.settlement.add(commVal);
            addTrade.settlement.multiplyByInt(-1);            
        }
        
        
    }
    
    private void generateCompleteTrade() {
        generateCompletedTradeInfo();
        // EGen functions generating rows were moved to next()
        
        // update broker info
        brokerGenerator.updateTradeAndCommissionYTD(addTrade.brokerId);
        
        currCompletedTrades++;
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
                newHolding.symbolIndex = newTrade.secFileIndex;
                
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
                newHolding.symbolIndex = newTrade.secFileIndex;
                
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
    
    private boolean findNextHolding() {
        List<HoldingInfo> holdList = customerHoldings[currAccountForHolding][currSecurityForHolding];
        
        do {
            if (holdingIterator == holdList.size()) {
                // have to get a new list
                currSecurityForHolding++;
                if (currSecurityForHolding == HoldingsAndTrades.MAX_SECURITIES_PER_ACCOUNT) {
                    // no more securities, so move the account counter
                    currAccountForHolding++;
                    currSecurityForHolding = 0;
                    
                    if (currAccountForHolding == LOAD_UNIT_ACCOUNT_COUNT) {
                        // no more holdings
                        return false;
                    }
                }
            
                holdList = customerHoldings[currAccountForHolding][currSecurityForHolding];
                holdingIterator = 0;
            }
        } while (holdingIterator == holdList.size());
        
        return true;        
    }
    
    private boolean findNextHoldingList() {
        List<HoldingInfo> holdList;
        
        do {
            currSecurityForHoldingSummary++;
            if (currSecurityForHoldingSummary == HoldingsAndTrades.MAX_SECURITIES_PER_ACCOUNT) {
                // no more securities, so move the account counter
                currAccountForHoldingSummary++;
                currSecurityForHoldingSummary = 0;
                
                if (currAccountForHoldingSummary == LOAD_UNIT_ACCOUNT_COUNT) {
                    // no more holdings
                    return false;
                }
            }
            
            holdList = customerHoldings[currAccountForHoldingSummary][currSecurityForHoldingSummary];
        } while (holdList.isEmpty());
        
        return true;        
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
    
    
    private void generateCompletedTradeInfo() {
       
        addTrade.brokerId = custAccGenerator.generateBrokerId(newTrade.accId);

      //  generateTradeCharge();
        generateSettlementAmount();
        
    }

    private void generateNextTrade() {
        //System.out.println("in gen next trade");
        if (currCompletedTrades < totalTrades) {
            //System.out.println("less than");
            /*
             * While the earliest completion time is before the current
             * simulated ('Trade Order') time, keep creating new
             * incomplete trades putting them on the queue and
             * incrementing the current simulated time.
             */
            while ((currCompletedTrades + currentTrades.size() < totalTrades) &&
                   (currentTrades.isEmpty() || currentSimulatedTime < currentTrades.peek().completionTime)) {
                //System.out.println("in while");
                currentSimulatedTime = (currInitiatedTrades / tradesPerWorkDay) * 3600 * 24 + // seconds per day
                        (currInitiatedTrades % tradesPerWorkDay) * meanBetweenTrades + generateDelay();
                //System.out.println("fixed time");
                generateNewTrade();
                //System.out.println("gen new trade");
                // ignore aborted trades
                if (HoldingsAndTrades.isAbortedTrade(currInitiatedTrades)) {
                    continue;
                }
                
                currentTrades.add(newTrade);
            }
            //System.out.println("out of while");
            newTrade = currentTrades.remove();
            
            updateHoldings();
            //System.out.println("updated holdings");
            generateCompleteTrade();
            //System.out.println("gen completed trade");
            hasNextTrade = currCompletedTrades < totalTrades;
        }
        else {
            hasNextTrade = false;
            //System.out.println("was in else");
        }
        
        if (!hasNextTrade) {
            // find next holding for the holding generator 
            holdingIterator = 0;
            findNextHolding();
            //System.out.println("find next holding");
            // find a non-empty holding list for the holding summary
            findNextHoldingList(); 
            //System.out.println("find next holding list");
            assert currentTrades.size() == 0;
        }
    }
    
    private Object[] generateTradeRow() {
        Object[] tuple = new Object[getColumnNum(TPCEConstants.TABLENAME_TRADE)];
        currTable = TPCEConstants.TABLENAME_TRADE;
        
        tuple[0] = newTrade.tradeId; // t_id
       // System.out.println("inserted id: "+ newTrade.tradeId);
        tuple[1] = new TimestampType(getCurrentTradeCompletionTime()); // t_dts
        tuple[2] = statusTypeFile.getTupleByIndex(newTrade.tradeStatus.ordinal())[0]; // t_st_id
        tuple[3] = tradeTypeFile.getTupleByIndex(newTrade.tradeType.ordinal())[0]; // t_tt_id
        
        // is it a cash trade?
        addTrade.isCash = true;

        
        tuple[4] = addTrade.isCash ? 1 : 0; // t_is_cash
        tuple[5] = secHandler.createSymbol(newTrade.secFileIndex, 15); // t_s_symb; CHAR(15)
        //System.out.println("Possible Symbol" + tuple[5]);
        tuple[6] = newTrade.tradeQty; // t_qty
        tuple[7] = newTrade.bidPrice.getDollars(); // t_bid_price
        tuple[8] = newTrade.accId; // t_ca_id
        //System.out.println("Account ID inserted into trade:" + tuple[8]);
        
        tuple[9] = newTrade.tradePrice.getDollars(); // t_trade_price
        tuple[10] = 10; //addTrade.charge.getDollars(); // t_chrg
        tuple[11] = 10; //addTrade.commission.getDollars(); // t_comm
        tuple[12] =10; //t_tax is const
        
       
        
        tuple[13] = newTrade.isLIFO ? 1 : 0; // t_lifo
        
        return tuple;        
    }
   
    private Object[] generateCashRow() {
        Object[] tuple = new Object[getColumnNum(TPCEConstants.TABLENAME_CASH_TRANSACTION)];
        currTable = TPCEConstants.TABLENAME_CASH_TRANSACTION;
        
        tuple[0] = newTrade.tradeId; // ct_t_id
        tuple[1] = new TimestampType(getCurrentTradeCompletionTime()); // ct_dts
        tuple[2] = addTrade.settlement.getDollars(); // ct_amt
        tuple[3] = tradeTypeFile.getTupleByIndex(newTrade.tradeType.ordinal())[1] + " " +
                Integer.toString(newTrade.tradeQty) + " shares of " + secGenerator.createName(newTrade.secFileIndex); // ct_name
        
        return tuple;        
    }
    
    private void generateTradeHistoryRows() {
        tradeHistory.clear();
        currTable = TPCEConstants.TABLENAME_TRADE_HISTORY;
        int columnsNum = getColumnNum(TPCEConstants.TABLENAME_TRADE_HISTORY);
        
        if (newTrade.tradeType == TradeType.eStopLoss || newTrade.tradeType == TradeType.eLimitSell ||
                newTrade.tradeType == TradeType.eLimitBuy) {
            for (int i = 0; i < 3; i++) {
                Object[] tuple = new Object[columnsNum];
                
                tuple[0] = newTrade.tradeId; // th_t_id
                
                switch (i) {
                    case 0:
                        tuple[1] = new TimestampType(getCurrentTradePendingTime()); // th_dts
                        tuple[2] = statusTypeFile.getTupleByIndex(StatusTypeId.E_PENDING.ordinal())[0]; // th_st_id
                        break;
                        
                    case 1:
                        tuple[1] = new TimestampType(getCurrentTradeSubmissionTime()); // th_dts
                        tuple[2] = statusTypeFile.getTupleByIndex(StatusTypeId.E_SUBMITTED.ordinal())[0]; // th_st_id
                        break;
                        
                    case 2:
                        tuple[1] = new TimestampType(getCurrentTradeCompletionTime()); // th_dts
                        tuple[2] = statusTypeFile.getTupleByIndex(StatusTypeId.E_COMPLETED.ordinal())[0]; // th_st_id
                        break;
                        
                    default:
                        assert false;
                }
                
                tradeHistory.add(tuple);
            }
        }
        else {
            for (int i = 0; i < 2; i++) {
                Object[] tuple = new Object[columnsNum];
                
                tuple[0] = newTrade.tradeId; // th_t_id
                
                switch (i) {
                    case 0:
                        tuple[1] = new TimestampType(getCurrentTradeSubmissionTime()); // th_dts
                        tuple[2] = statusTypeFile.getTupleByIndex(StatusTypeId.E_SUBMITTED.ordinal())[0]; // th_st_id
                        break;
                        
                    case 1:
                        tuple[1] = new TimestampType(getCurrentTradeCompletionTime()); // th_dts
                        tuple[2] = statusTypeFile.getTupleByIndex(StatusTypeId.E_COMPLETED.ordinal())[0]; // th_st_id
                        break;
                        
                    default:
                        assert false;
                }
                
                tradeHistory.add(tuple);
            }
        }
    }
    
    private Object[] generateNextHoldingSummaryRow() {
        // we should have at least one tuple here
        assert currAccountForHoldingSummary < LOAD_UNIT_ACCOUNT_COUNT;
        
        Object[] tuple = new Object[getColumnNum(TPCEConstants.TABLENAME_HOLDING_SUMMARY)];
        currTable = TPCEConstants.TABLENAME_HOLDING_SUMMARY;
        
        tuple[0] = newTrade.accId;//currAccountForHoldingSummary + startFromAccount; // hs_ca_id
        //System.out.println("Account for holding summary:" + tuple[0]);
        // symbol
        long secFlatFileIndex = holdsGenerator.getSecurityFlatFileIndex(currAccountForHoldingSummary + startFromAccount,
                currSecurityForHoldingSummary + 1);
        tuple[1] = secHandler.createSymbol(secFlatFileIndex, 15); // hs_s_symb; CHAR(15)
        
        // total quantity over holdings
        int hsQty = 0;
        for (HoldingInfo hold: customerHoldings[currAccountForHoldingSummary][currSecurityForHoldingSummary]) {
            hsQty += hold.tradeQty;
        }
        tuple[2] = hsQty; // hs_qty
        
        hasNextHoldingSummary = findNextHoldingList();
        
        return tuple;      
    }
    

    private Object[] generateNextHoldingRow() {
        // we should have at least one tuple here
        assert currAccountForHolding < LOAD_UNIT_ACCOUNT_COUNT;
        
        Object[] tuple = new Object[getColumnNum(TPCEConstants.TABLENAME_HOLDING)];
        currTable = TPCEConstants.TABLENAME_HOLDING;
        
        HoldingInfo holding = customerHoldings[currAccountForHolding][currSecurityForHolding].get(holdingIterator);
        
        tuple[0] = holding.tradeId; // h_t_id
        tuple[1] = currAccountForHolding + startFromAccount; // h_ca_id
        tuple[2] = secHandler.createSymbol(holding.symbolIndex, 15); // h_s_symb; CHAR(15)
        tuple[3] = new TimestampType(holding.buyDTS); // h_dts
        tuple[4] = holding.tradePrice.getDollars(); // h_price
        tuple[5] = holding.tradeQty; // h_qty
        
        holdingIterator++;
        hasNextHolding = findNextHolding();
        
        return tuple;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        if (currState == State.stNone) {
            
            currentLoadUnit++;
            if (currentLoadUnit == totalLoadUnits) { // all units are loaded
                return false;
            }
            
            initNextLoadUnit();
            currState = State.stTrade;
            return true;
        }
        
        // we always have tuples in other states
        return true;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public Object[] next() {
        Object[] tuple = null;
       // System.out.println("in next");
        //System.out.println(currState);
        switch (currState) {
            case stNone:
               
                assert false;
                tuple = null;
               // System.out.println("stNone");
                break;
               
            case stTrade:
               
                generateNextTrade(); // generating all the necessary info about the trade
                //System.out.println("trade generated");
                tuple = generateTradeRow();
                currState = State.stTradeHistory;
                //System.out.println("stTradeHistory");
                break;
                
            case stTradeHistory:
              
                if (tradeHistoryCounter == 0) {
                 
                    generateTradeHistoryRows();
                }
                
                tuple = tradeHistory.get(tradeHistoryCounter++);
               
                if (tradeHistoryCounter == tradeHistory.size()) {
                    tradeHistoryCounter = 0;
                    currState = State.stCash;
                }
                break;
   
            case stCash:
               // System.out.println("stCash");
                tuple = generateCashRow();
                currState = State.stHoldHistory;
                break;
                
            case stHoldHistory:
                // we always have at least one tuple here
                //System.out.println("stholdinghistory");
                assert holdingHistory.size() != 0;
                
                currTable = TPCEConstants.TABLENAME_HOLDING_HISTORY;
                tuple = holdingHistory.get(holdHistoryCounter++);
                
                if (holdHistoryCounter == holdingHistory.size()) {
                    holdHistoryCounter = 0;
                    currState = hasNextTrade ? State.stTrade : State.stBroker;
                
                }
                break;
                
            case stBroker:
                //System.out.println("stbroker");
                // we always have at least one BROKER tuple
                assert brokerGenerator.hasNext();
                
                currTable = TPCEConstants.TABLENAME_BROKER;
                tuple = brokerGenerator.next();
                
                if (!brokerGenerator.hasNext()) {
                    currState = State.stHoldSummary;
                }
                break;
                
            case stHoldSummary:
                //System.out.println("stholdingsummary");
                // we always have at least one tuple
                assert hasNextHoldingSummary;
                
                tuple = generateNextHoldingSummaryRow();
                
                if (!hasNextHoldingSummary) {
                    currState = State.stHolding;
                }
                break;
                
            case stHolding:
                // we always have at least one tuple
                //System.out.println("stholding");
                assert hasNextHolding;
                
                tuple = generateNextHoldingRow();
                
                if (!hasNextHolding) {
                    currState = State.stNone; // finished with the unit
                }
                break;
        }
        
        return tuple;
    }
    
    /**
     * Returns the table for which the previous next() call just
     * returned the tuple.
     * 
     * @return Table name for the last generated tuple
     */
    public String getCurrentTable() {
        return currTable;
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
    
    private Date getCurrentTradePendingTime() {
        int daysFromStart  = (int)(newTrade.pendingTime / (3600 * 24));
        int msecsFromStart = (int)((newTrade.pendingTime - daysFromStart * 3600 * 24) * 1000);
        
        return EGenDate.addDaysMsecs(startTime, daysFromStart, msecsFromStart, true); // add days and msec and adjust for weekend
    }
    
    private Date getCurrentTradeSubmissionTime() {
        int daysFromStart  = (int)(newTrade.submissionTime / (3600 * 24));
        int msecsFromStart = (int)((newTrade.submissionTime - daysFromStart * 3600 * 24) * 1000);
        
        return EGenDate.addDaysMsecs(startTime, daysFromStart, msecsFromStart, true); // add days and msec and adjust for weekend
    }
}
