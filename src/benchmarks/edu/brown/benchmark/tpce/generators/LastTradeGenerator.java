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

import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.EGenDate;

public class LastTradeGenerator extends TableGenerator {
    private final long securityNum;
    private final long securityStart;
    private final Date tradeDate;
    private final MEESecurity meeSec;
    
    private long counter;
    
    private final SecurityHandler secHandler;
    

    /**
     * @param catalog_tbl
     * @param generator
     */
    public LastTradeGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        secHandler = new SecurityHandler(generator);
        securityNum = SecurityHandler.getSecurityNum(generator.getCustomersNum());
        securityStart = SecurityHandler.getSecurityStart(generator.getStartCustomer());
        
        counter = securityStart;
        
        tradeDate = EGenDate.addDaysMsecs(EGenDate.getDateFromTime(TPCEConstants.initialTradePopulationBaseYear,
                                                                   TPCEConstants.initialTradePopulationBaseMonth,
                                                                   TPCEConstants.initialTradePopulationBaseDay,
                                                                   TPCEConstants.initialTradePopulationBaseHour,
                                                                   TPCEConstants.initialTradePopulationBaseMinute,
                                                                   TPCEConstants.initialTradePopulationBaseSecond, 0 /* msec */),
                                          generator.getInitTradeDays(), 0, true);
        
        meeSec = new MEESecurity();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return counter < securityStart + securityNum;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        // we want time in seconds here; plus work days have only 8 hours in them
        meeSec.init(generator.getInitTradeDays() * 8 * 3600, null, null, 0); 
        double price = meeSec.calculatePrice(counter, 0).getDollars();
        
        tuple[0] = secHandler.createSymbol(counter, 15); // lt_s_symb; because of CHAR(15) in the schema
        tuple[1] = new TimestampType(tradeDate); // lt_dts
        tuple[2] = price; // lt_price
        tuple[3] = price; // lt_open_price
        tuple[4] = 0; // lt_volume
        
        counter++;
        
        return tuple;
    }
}
