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

import edu.brown.benchmark.tpce.util.EGenRandom;

public class PersonHandler {
    private static final String TAX_ID_FORMAT = "nnnaannnnaannn";
    private static final int PERCENT_GENDER_MALE = 49;

    private final InputFileHandler lastNames;
    private final InputFileHandler femFirstNames;
    private final InputFileHandler maleFirstNames;
    
    private final EGenRandom rnd;
    
    public PersonHandler(InputFileHandler lastNames, InputFileHandler femFirstNames, InputFileHandler maleFirstNames) {
        this.lastNames = lastNames;
        this.femFirstNames = femFirstNames;
        this.maleFirstNames = maleFirstNames;
        
        this.rnd = new EGenRandom();
    }
    
    public String getLastName(long cid) {
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_LAST_NAME, cid);
        int key = rnd.intRange(0, lastNames.getMaxKey());
        
        return lastNames.getTupleByKey(key)[0];
    }
    
    public String getFirstName(long cid) {
        String res;
        
        if (isMalGender(cid)) { // seed base is changed here!
            rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_FIRST_NAME, cid);
            int key = rnd.intRange(0, maleFirstNames.getMaxKey());
            res = maleFirstNames.getTupleByKey(key)[0];
        }
        else {
            rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_FIRST_NAME, cid);
            int key = rnd.intRange(0, femFirstNames.getMaxKey());
            res = femFirstNames.getTupleByKey(key)[0];
        }
        
        return res;
    }
    
    public String getMiddleName(long cid) {
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_MIDDLE_INITIAL, cid);
        return rnd.rndAlphaNumFormatted("a");
    }
    
    public String getGender(long cid) {
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_GENDER, cid);
        
        if (rnd.rndPercent(PERCENT_GENDER_MALE)) {
            return "M";
        }
        else {
            return "F";
        }
    }
    
    public boolean isMalGender(long cid) {
        return getGender(cid).equals("M");
    }
    
    public String getTaxID(long cid) {
        /*
         * NOTE: the call to RndAlphaNumFormatted "consumes" an RNG value
         * for EACH character in the format string. Therefore, to avoid getting
         * tax ID's that overlap N-1 out of N characters, multiply the offset into
         * the sequence by N to get a unique range of values.
         */
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_TAX_ID, cid * TAX_ID_FORMAT.length());
        return rnd.rndAlphaNumFormatted(TAX_ID_FORMAT);
    }

    public String[] getFirstNameLastNameTaxID(long cid) {
        String[] res = new String[3];
        
        res[0] = getFirstName(cid);
        res[1] = getLastName(cid);
        res[2] = getTaxID(cid);
        
        return res;
    }
}
