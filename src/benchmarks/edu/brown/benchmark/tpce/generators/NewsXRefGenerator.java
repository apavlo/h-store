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

/**
 * @author akalinin
 *
 */
public class NewsXRefGenerator extends TableGenerator {
    private final NewsItemGenerator newsGen;
    private final CompanyGenerator compGen;
    
    private int newsGenerated = TPCEConstants.newsItemsPerCompany;
    private long compId;
    
    /**
     * @param catalog_tbl
     * @param generator
     */
    public NewsXRefGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        newsGen = (NewsItemGenerator)generator.getTableGen(TPCEConstants.TABLENAME_NEWS_ITEM, null);
        compGen = (CompanyGenerator)generator.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return compGen.hasNext() || newsGenerated < TPCEConstants.newsItemsPerCompany;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        Object[] tuple = new Object[columnsNum];
        
        if (newsGenerated == TPCEConstants.newsItemsPerCompany) {
            compId = compGen.generateCompId();
            newsGenerated = 0;
        }
        
        tuple[0] = newsGen.generateNewsId(); // nx_ni_id
        tuple[1] = compId; // nx_co_id
        
        newsGenerated++;
        
        return tuple;
    }
}
