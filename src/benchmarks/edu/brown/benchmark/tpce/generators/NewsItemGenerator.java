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
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class NewsItemGenerator extends TableGenerator {
    private final static int newsItemMaxDaysAgo = 50;   // how many days ago can a news item be dated
    
    private static final int NEWS_HEAD_LEN = 80;   // maximum length of the news headline field (see tpce-ddl.sql)
    private static final int NEWS_SUMM_LEN = 255;  // maximum length of the news summary field (see tpce-ddl.sql)
    private static final int NEWS_ITEM_LEN = 1024; // maximum length of the news item field (see tpce-ddl.sql)
    private final static int rngSkipOneRowNews = 4 + NEWS_ITEM_LEN; // number of RNG calls for one row
    
    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    
    private final long startNews;
    private final long totalNews;
    private long counter;
    
    private final InputFileHandler newsFile;
    private final InputFileHandler lastNamesFile;
    private final Date newsbaseDate;
    
    /**
     * @param catalog_tbl
     * @param generator
     */
    public NewsItemGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        newsFile = generator.getInputFile(InputFile.LNAME); // we use last names for generating random words
        lastNamesFile = generator.getInputFile(InputFile.LNAME);
        
        startNews = generator.getCompanyCount(generator.getStartCustomer()) * TPCEConstants.newsItemsPerCompany;
        totalNews = generator.getCompanyCount(generator.getCustomersNum()) * TPCEConstants.newsItemsPerCompany;
        
        newsbaseDate = EGenDate.addDaysMsecs(EGenDate.getDateFromTime(TPCEConstants.initialTradePopulationBaseYear,
                                                                      TPCEConstants.initialTradePopulationBaseMonth,
                                                                      TPCEConstants.initialTradePopulationBaseDay,
                                                                      TPCEConstants.initialTradePopulationBaseHour,
                                                                      TPCEConstants.initialTradePopulationBaseMinute,
                                                                      TPCEConstants.initialTradePopulationBaseSecond,
                                                                      TPCEConstants.initialTradePopulationBaseFraction),
                    TPCEConstants.DEFAULT_INITIAL_DAYS, 0, true);

    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, counter * rngSkipOneRowNews);
    }
    
    public long generateNewsId() {
        counter++;
        return startNews + counter;
    }
    
    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return counter < totalNews;
    }
    
    private String[] generateHeadineSummaryItem() {
        String[] res = new String[3];
        
        StringBuilder newsItem = new StringBuilder();
        while (newsItem.length() < NEWS_ITEM_LEN) {
            int key = rnd.intRange(0, newsFile.getMaxKey());
            String word = newsFile.getTupleByKey(key)[0];
            
            if (newsItem.length() + word.length() <= NEWS_ITEM_LEN) {
                newsItem.append(word);
            }
            else {
                newsItem.append(word.substring(0, NEWS_ITEM_LEN - newsItem.length()));
            }
            
            if (newsItem.length() < NEWS_ITEM_LEN) {
                newsItem.append(' ');
            }
        }
        
        assert(newsItem.length() == NEWS_ITEM_LEN);
        
        // headline
        res[0] = newsItem.substring(0, NEWS_HEAD_LEN);
        // summary
        res[1] = newsItem.substring(0, NEWS_SUMM_LEN);
        // item
        res[2] = newsItem.toString();
        
        return res;       
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        Object[] tuple = new Object[columnsNum];
        
        if (counter % (TPCEConstants.DEFAULT_COMPANIES_PER_UNIT * TPCEConstants.newsItemsPerCompany) == 0) {
            initNextLoadUnit();
        }
        
        long newsId = generateNewsId();
        String[] newsHeadSumItem = generateHeadineSummaryItem();
        
        tuple[0] = newsId; // ni_id
        tuple[1] = newsHeadSumItem[0]; // ni_headline
        tuple[2] = newsHeadSumItem[1]; // ni_summary
        tuple[3] = newsHeadSumItem[2]; // ni_item
        
        // generating date
        int dayAdd = rnd.intRange(0, newsItemMaxDaysAgo);
        int msecAdd = rnd.intRange(0, 3600 * 24 * 1000); // msecs per day
        tuple[4] = new TimestampType(EGenDate.addDaysMsecs(newsbaseDate, (-1) * dayAdd, (-1) * msecAdd, false)); // ni_dts
        
        // source and author
        int authorKey = rnd.intRange(0, lastNamesFile.getMaxKey());
        int sourceKey = rnd.intRange(0, lastNamesFile.getMaxKey());
        tuple[5] = lastNamesFile.getTupleByKey(authorKey)[0]; // ni_author
        tuple[6] = lastNamesFile.getTupleByKey(sourceKey)[0]; // ni_source       
        
        return tuple;
    }
}
