/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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
package edu.brown.benchmark.auctionmark;

import java.util.Random;

import org.voltdb.catalog.Database;

import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.AbstractTableStatisticsGenerator;
import edu.brown.utils.ProjectType;

/**
 * AuctionMark Initial Table Sizes
 * 
 * @author pavlo
 */
public class AuctionMarkTableStatisticsGenerator extends AbstractTableStatisticsGenerator {
    
    private static final int NUM_SAMPLES = 1000;
    private static final Random rand = new Random(0);

    /**
     * @param catalogDb
     * @param projectType
     * @param scaleFactor
     */
    public AuctionMarkTableStatisticsGenerator(Database catalog_db, double scale_factor) {
        super(catalog_db, ProjectType.AUCTIONMARK, scale_factor);
    }

    @Override
    public void createProfiles() {
        TableProfile p = null;
        
        // CATEGORY
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_CATEGORY, true, 19500);
        this.addTableProfile(p);
        
        // REGION
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_REGION, true, AuctionMarkConstants.TABLESIZE_REGION);
        this.addTableProfile(p);

        // GLOBAL_ATTRIBUTE_GROUP
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, true, AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_GROUP);
        this.addTableProfile(p);

        // GLOBAL_ATTRIBUTE_VALUE
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP);
        this.addTableProfile(p);
        
        // USER
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_USER, false, AuctionMarkConstants.TABLESIZE_USER);
        this.addTableProfile(p);
        
        // USER_ATTRIBUTES
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_USER, 2.5);
        this.addTableProfile(p);
        
        // USER_ITEM 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_USER_ITEM, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_USER, 1.0); // TODO
        this.addTableProfile(p);
        
        // USER_WATCH 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_USER_WATCH, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_USER,
                new RandomDistribution.Zipf(rand, AuctionMarkConstants.ITEM_MIN_WATCHES_PER_DAY, AuctionMarkConstants.ITEM_MAX_WATCHES_PER_DAY, 2.0001).calculateMean(NUM_SAMPLES));
        this.addTableProfile(p);
        
        // ITEM 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_USER, 10.0);
        this.addTableProfile(p);
        
        // ITEM_ATTRIBUTE 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM,
                new RandomDistribution.Zipf(rand, AuctionMarkConstants.ITEM_MIN_GLOBAL_ATTRS, AuctionMarkConstants.ITEM_MAX_GLOBAL_ATTRS, 1.0001).calculateMean(NUM_SAMPLES));
        this.addTableProfile(p);
        
        // ITEM_IMAGE 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM_IMAGE, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM,
                new RandomDistribution.Zipf(rand, AuctionMarkConstants.ITEM_MIN_IMAGES, AuctionMarkConstants.ITEM_MAX_IMAGES, 1.0001).calculateMean(NUM_SAMPLES));
        this.addTableProfile(p);
        
        // ITEM_COMMENT 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM_COMMENT, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM,
                new RandomDistribution.Zipf(rand, AuctionMarkConstants.ITEM_MIN_COMMENTS, AuctionMarkConstants.ITEM_MAX_COMMENTS, 1.0001).calculateMean(NUM_SAMPLES));
        this.addTableProfile(p);
        
        // ITEM_FEEDBACK 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_USER_FEEDBACK, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM, 1.0); // TODO
        this.addTableProfile(p);
        
        // ITEM_BID 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM_BID, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM, 
                new RandomDistribution.Zipf(rand, AuctionMarkConstants.ITEM_MIN_BIDS_PER_DAY, AuctionMarkConstants.ITEM_MAX_BIDS_PER_DAY, 2.0001).calculateMean(NUM_SAMPLES));
        this.addTableProfile(p);
        
        // ITEM_MAX_BID 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM_MAX_BID, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM, 0.75); // TODO
        this.addTableProfile(p);
        
        // ITEM_PURCHASE 
        p = new TableProfile(this.catalog_db, AuctionMarkConstants.TABLENAME_ITEM_PURCHASE, true);
        p.addMultiplicativeDependency(catalog_db, AuctionMarkConstants.TABLENAME_ITEM, 0.70); // TODO
        this.addTableProfile(p);
    }
}