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

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;

public class SecurityHandler {
    /* 
     * EGen uses a small set of values for 26 raised to a power, so store them in
     * a constant array to save doing calls to pow( 26.0, ? )
     */
    private static final int[] power26 = {1, 26, 676, 17576, 456976, 11881376, 308915776};

    // For index i > 0, this array holds the sum of 26^0 ... 26^(i-1)
    private static final long[] power26Sum = {0, 1, 27, 703, 18279, 475255, 12356631, 321272407, 8353082583L};
    private static final char suffSep = '-';
    
    private final InputFileHandler secFile;
    private final int secRecords;
    
    public SecurityHandler(TPCEGenerator generator) {
        secFile = generator.getInputFile(InputFile.SECURITY);
        secRecords = secFile.getRecordsNum();
    }
    
    /**
     * @param maxLen The maximum length for the suffix 
     * @see #createSymbol(long, int)
     */
    private static String createSuffix(long part, int maxLen) {
        // compute the number of chars to add
        int charCount = 0;
        while (part - power26Sum[charCount + 1] >= 0) {
            charCount++;
        }

        String suff = "";
        if (charCount + 1 <= maxLen) { // 1 extra for the separator
            suff += suffSep;
            /*           
             * charCount is the number of letters needed in the suffix
             * The base string is a string of 'a's of length charCount
             * Find the offset from the base value represented by the string
             * of 'a's to the desired number, and modify the base string
             * accordingly.
             */
            long offset = part - power26Sum[charCount];
            while (charCount > 0) {
                long lclIndex = offset / power26[charCount - 1]; // lower case letter index
                suff += ('a' + lclIndex); // assuming lower case letters go continuously
                offset -= (lclIndex * power26[charCount - 1]);
                charCount--;
            }
        }
        else {
            // Not enough room in the string -- go with separators
            charCount = maxLen;
            while (charCount-- > 0) {
                suff += suffSep;
            }
        }
        
        return suff;
    }
    
    /**
     * @param maxLen The maximum length of the returned symbol (we have SQL type restrictions on that)
     *               For example, daily_market table has a restriction of 15 chars. 
     */
    public String createSymbol(long index, int maxLen) {
        int fileInd = (int)(index % secRecords);
        long part = index / secRecords;
        
        String res = secFile.getTupleByIndex(fileInd)[2]; // symbol is a 2nd field
        
        if (part > 0) {
            res += createSuffix(part, maxLen - res.length());
        }
        
        assert(res.length() <= maxLen);
        
        return res;
    }
    
    public long getSecurityNum(long customersNum) {
        return customersNum / TPCEConstants.DEFAULT_LOAD_UNIT * TPCEConstants.DEFAULT_SECURITIES_PER_UNIT;
    }
    
    public long getSecurityStart(long customerStart) {
        return customerStart / TPCEConstants.DEFAULT_LOAD_UNIT * TPCEConstants.DEFAULT_SECURITIES_PER_UNIT;
    }
}
