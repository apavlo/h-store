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

import java.util.HashMap;
import java.util.Map;

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
    private final int compRecords;
    
    private final Map<String, Long> symbolToIdMap = new HashMap<String, Long>();
    
    public SecurityHandler(TPCEGenerator generator) {
        secFile = generator.getInputFile(InputFile.SECURITY);
        secRecords = secFile.getRecordsNum();
        compRecords = generator.getInputFile(InputFile.COMPANY).getRecordsNum();
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
    
    private long parseSuffix(String suffix) {
        int suffixLen = suffix.length();
        long multiplier = power26Sum[suffixLen]; 

        for (int i = 0; i < suffix.length(); i++) {
            multiplier += power26[suffixLen - 1] * (suffix.charAt(i) - 'a'); // assuming lower case letters go continuously
            suffixLen--;
        }
        
        return multiplier;
    }
    
    public long getCompanyId(long counter) {
        
        return Long.valueOf(getSecRecord(counter)[5]) + TPCEConstants.IDENT_SHIFT +
                + counter / secRecords * compRecords;
    }
    
    /**
     * Return the record based on the counter. The file wraps around.
     */
    public String[] getSecRecord(long counter) {
        return secFile.getTupleByIndex((int)(counter % secRecords));
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
    
    public static long getSecurityNum(long customersNum) {
        return customersNum / TPCEConstants.DEFAULT_LOAD_UNIT * TPCEConstants.DEFAULT_SECURITIES_PER_UNIT;
    }
    
    public static long getSecurityStart(long customerStart) {
        return customerStart / TPCEConstants.DEFAULT_LOAD_UNIT * TPCEConstants.DEFAULT_SECURITIES_PER_UNIT;
    }
    
    public void loadSymbolToIdMap() {
        if (symbolToIdMap.isEmpty()) {
            for (int i = 0; i < secFile.getRecordsNum(); i++) {
                String[] secTuple = secFile.getTupleByIndex(i);
                symbolToIdMap.put(secTuple[2], Long.valueOf(secTuple[0]));
            }
        }
    }
    
    public long getId(String symbol) {
        if (symbolToIdMap.isEmpty()) {
            loadSymbolToIdMap();
        }
        
        // first, look for the separator -- '-'
        int sepIndex = symbol.indexOf('-');
        
        if (sepIndex == -1) {
            // simple name, no suffix
            return symbolToIdMap.get(symbol);
        }
        
        // suffix present, have to parse it
        long baseId = symbolToIdMap.get(symbol.substring(0, sepIndex));
        long multiplier = parseSuffix(symbol.substring(sepIndex + 1));
        
        return multiplier * secFile.getRecordsNum() + baseId;      
    }
    public long getCompanyIndex(long counter){
    	return getCompanyId(counter) - 1 - TPCEConstants.IDENT_SHIFT;
	}
    public long getIndex( String pSymbol )
    {
        // Indices and Id's are offset by 1
        return( getId( pSymbol ) - 1 );
    }
}
