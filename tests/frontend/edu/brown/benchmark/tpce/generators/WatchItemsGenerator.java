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

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;

/**
 * @author akalinin
 *
 */
public class WatchItemsGenerator extends WatchListGenerator {
    // Min number of items in one watch list
    private static final int MIN_ITEMS_IN_WL = 50;
    //  Max number of items in one watch list
    private static final int MAX_ITEMS_IN_WL = 150;
    
    private static final int MIN_SEC_IDX = 0;   //this should always be 0 (EGen)

    private final SecurityHandler secHandler;
    private final long maxSecIdx;
    private long itemsToGenerate;
    private long watchListId;
    private final Set<Long> listItems = new HashSet<Long>(); // for de-duplication purposes only

    /**
     * @param catalog_tbl
     * @param generator
     */
    public WatchItemsGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator); // weird, since WatchListGenerator works for another table, but we need columnsNum
        
        secHandler = new SecurityHandler(generator);
        maxSecIdx = SecurityHandler.getSecurityNum(generator.getTotalCustomers()) - 1; // 0-based
    }
    
    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return super.hasNext() || itemsToGenerate > 0;
    }
    
    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        if (itemsToGenerate == 0) {
            itemsToGenerate = rnd.intRange(MIN_ITEMS_IN_WL, MAX_ITEMS_IN_WL);
            
            while (listItems.size() < itemsToGenerate) {
                listItems.add(rnd.int64Range(MIN_SEC_IDX, maxSecIdx)); 
            }
            
            // it is crucial to make this call here if we want to be 100% on par with EGen (rnd calls ordering is important)
            watchListId = generateWLId();
        }
        
        Object[] tuple = new Object[columnsNum];
        
        assert(listItems.size() != 0);
        long secId = listItems.iterator().next();
        listItems.remove(secId);
                
        tuple[0] = watchListId; // wi_wl_id
        tuple[1] = secHandler.createSymbol(secId, 15); // wi_s_symb; CHAR(15)
        
        itemsToGenerate--;
        
        return tuple;
    }
}
