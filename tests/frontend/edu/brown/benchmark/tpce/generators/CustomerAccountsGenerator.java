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
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class CustomerAccountsGenerator extends TableGenerator {

    private int accsToGenerate; // every customer can have a different number of accs
    private int accsGenerated; // the number of accs already generated for the current customer
    private final CustomerGenerator customerGenerator; // is used for geenrating just customer ids
    private final EGenRandom rnd;
    
    /*
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private final static int rngSkipOneRowCustomerAccount = 10;   // real max count in v3.5: 7
    private final static int maxAccountsPerCust = 10;
    
    public CustomerAccountsGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        // do not need the Table for this. Nasty, though. Have to use type casting since we need specific functions
        customerGenerator = (CustomerGenerator)generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER, null);
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT,
                customerGenerator.getCurrentCId() * maxAccountsPerCust * rngSkipOneRowCustomerAccount); 
    }
    
    private long generateAccountId() {
        if (customerGenerator.getCurrentCId() % TPCEConstants.DEFAULT_LOAD_UNIT == 0) {
            initNextLoadUnit();
        }
        
        if (accsToGenerate == accsGenerated) {
            customerGenerator.getCustomerId();
            
        }
        
        return 0;

    }
    
    @Override
    public boolean hasNext() {
        // we either have more customers or are still generating acounts for the last one
        return customerGenerator.hasNext() || accsGenerated < accsToGenerate;
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[this.catalog_tbl.getColumns().size()];
        
        return null;       
    }
}
