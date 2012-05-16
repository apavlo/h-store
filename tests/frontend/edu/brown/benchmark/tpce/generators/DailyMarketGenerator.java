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

import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class DailyMarketGenerator extends TableGenerator {
    private final int rngSkipOneRowDailyMarket = 2; // for the random generated to skip
    private final int tradeDaysInYear = 261;   //the number of trading days in a year (for DAILY_MARKET)
    private final int dailyMarketYears = 5;    //number of years of history in DAILY_MARKET
    public static final int iDailyMarketTotalRows = 261 * 5;
    /*
     * Different constants for generating fake market values
     */
    private final double  dailyMarketCloseMin = TPCEConstants.minSecPrice;
    private final double  dailyMarketCloseMax = TPCEConstants.maxSecPrice;
    private final double  dailyMarketHighRelativeToClose = 1.05;
    private final double  dailyMarketLowRelativeToClose  = 0.92;
    private final int     dailyMarketVolumeMax = 10000;
    private final int     dailyMarketVolumeMin = 1000;
    
    private final SecurityHandler sec;
    private final EGenRandom rnd;
    
    private final long securitiesNum;
    private final long securitiesStart;
    private final int rowsToGenerateForSecurity = TPCEConstants.dailyMarketYears * tradeDaysInYear; // we generate a bunch of tuples for each security
    private int rowsGeneratedForSecurity = rowsToGenerateForSecurity; // to force reset at the very first record
    private final int startDayNo; // current day number for the security
    private int dayInc;
    private String currentSymbol;
    
    private long counter;

    public DailyMarketGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        sec = new SecurityHandler(generator);
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
        
        securitiesNum = SecurityHandler.getSecurityNum(generator.getCustomersNum());
        securitiesStart = SecurityHandler.getSecurityStart(generator.getStartCustomer());
        
        counter = securitiesStart;
        
        startDayNo = EGenDate.getDayNo(TPCEConstants.dailyMarketBaseYear, TPCEConstants.dailyMarketBaseMonth,
                TPCEConstants.dailyMarketBaseDay);
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, counter * rngSkipOneRowDailyMarket);
    }
    
    @Override
    public boolean hasNext() {
        // either we have more securities or we have more rows for the last security
        return (counter < securitiesNum + securitiesStart) || (rowsGeneratedForSecurity < rowsToGenerateForSecurity); 
    }

    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        // check if we have to start a new security
        if (rowsGeneratedForSecurity == rowsToGenerateForSecurity) {
            if (counter % TPCEConstants.DEFAULT_SECURITIES_PER_UNIT == 0) {
                initNextLoadUnit();
            }
            
            currentSymbol = sec.createSymbol(counter, 15); // since CHAR(15); TODO: get the limit from the catalog
            
            rowsGeneratedForSecurity = 0;
            dayInc = 0;
            counter++;
        }
        
        /*
         *  Skipping weekends.
         *  We start from Monday as 0.
         */
        if (dayInc % 7 == TPCEConstants.daysPerWorkWeek) {
            dayInc += 2;
        }
        
        double dailyClose = rnd.doubleIncrRange(dailyMarketCloseMin, dailyMarketCloseMax, 0.01);
        
        tuple[0] = new TimestampType(EGenDate.getDateFromDayNo(startDayNo + dayInc)); // dm_date
        tuple[1] = currentSymbol; // dm_s_symb
        tuple[2] = dailyClose; // dm_close
        tuple[3] = dailyClose * dailyMarketHighRelativeToClose; // dm_high
        tuple[4] = dailyClose * dailyMarketLowRelativeToClose; // dm_low
        tuple[5] = rnd.intRange(dailyMarketVolumeMin, dailyMarketVolumeMax); // dm_vol
        
        rowsGeneratedForSecurity++;
        dayInc++;
        
        return tuple;
    }

}
