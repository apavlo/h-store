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

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class WatchListGenerator extends TableGenerator {
    // Percentage of customers that have watch lists (100% is taken from EGen)
    private final static int PERCENT_WATCH_LIST = 100;
    private final static int RNG_SKIP_ONE_ROW_WATCH_LIST = 15; // real max count in v3.5: 13
    
    // Note: these parameters are dependent on the load unit size (EGen)
    private final static int WATCH_LIST_ID_PRIME = 631;
    private final static int WATCH_LIST_ID_OFFSET = 97;
    
    private final CustomerGenerator custGen;
    protected final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    
    /* 
     * having another list is tricky -- it depends on random generator.
     * Although it is 100% for now by default, but in general, not every customer has a list
     */
    private boolean hasAnotherCust = false;
    private boolean needRandomReset = false;
    private long custId;

    /**
     * @param catalog_tbl
     * @param generator
     */
    public WatchListGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        custGen = (CustomerGenerator)generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER, null);
        
        generateNewCustId();        
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, custId * RNG_SKIP_ONE_ROW_WATCH_LIST);
    }
    
    private void generateNewCustId() {
        hasAnotherCust = false;
        
        while (custGen.hasNext()) {
            if (custGen.getCurrentCId() % TPCEConstants.DEFAULT_LOAD_UNIT == 0) {
                needRandomReset = true;
            }
            
            custId = custGen.generateCustomerId();
            
            if (rnd.rndPercent(PERCENT_WATCH_LIST)) {
                hasAnotherCust = true;
                break;
            }
        }
        
        if (needRandomReset) {
            initNextLoadUnit();
            needRandomReset = false;
        }
    }
    
    protected long generateWLId() {
        // ??? EGen is probably wrong here, since the starting customer changes for multiple loads
        long customerId = custId - TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        
        long res =  (customerId / TPCEConstants.DEFAULT_LOAD_UNIT) * TPCEConstants.DEFAULT_LOAD_UNIT + // effect: stripping last 3 digits
                    (customerId * WATCH_LIST_ID_PRIME + WATCH_LIST_ID_OFFSET) %
                    TPCEConstants.DEFAULT_LOAD_UNIT + TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        
        // move on to the next customer
        generateNewCustId();
        
        return res;
    }
    
    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return hasAnotherCust;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        Object[] tuple = new Object[columnsNum];
        
        tuple[1] = custId; // wl_c_id
        tuple[0] = generateWLId(); // wl_id
        
        return tuple;
    }
}
