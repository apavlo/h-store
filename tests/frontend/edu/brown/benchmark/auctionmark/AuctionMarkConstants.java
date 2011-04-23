/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
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

import java.util.*;

public abstract class AuctionMarkConstants {
    
    public enum BidType {
        AUCTION,
        BUYNOW
    } 

    // ----------------------------------------------------------------
    // DATA INFORMATION
    // ----------------------------------------------------------------

    public static final int STATUS_ITEM_OPEN                    = 0;
    public static final int STATUS_ITEM_WAITING_FOR_PURCHASE    = 1;
    public static final int STATUS_ITEM_CLOSED                  = 2;
    
    // ----------------------------------------------------------------
    // STORED PROCEDURE INFORMATION
    // ----------------------------------------------------------------
    
    
    // the maximum number of clients that this benchmark can handle
    public static final int MAXIMUM_NUM_CLIENTS = 1000;
    
    // the maximum number of IDs that can be generated in each client (for each table)
    public static final long MAXIMUM_CLIENT_IDS = 100000000000000l;
    
    
    public static final long INTERVAL_CHECK_WINNING_BIDS = 10000; // Check winning bid's frequency in millisecond
    public static final boolean ENABLE_CHECK_WINNING_BIDS   = true;
    
    // Non-standard txns
    public static final int FREQUENCY_CHECK_WINNING_BIDS    = -1; // called at regular intervals
    public static final int FREQUENCY_POST_AUCTION          = -1; // called after CHECK_WINNING_BIDS
    
    // Regular Txn Mix
    
    public static final int FREQUENCY_GET_ITEM              = 40;
    public static final int FREQUENCY_GET_USER_INFO         = 10;
    public static final int FREQUENCY_GET_WATCHED_ITEMS     = 5;
    public static final int FREQUENCY_NEW_BID               = 18;
    public static final int FREQUENCY_NEW_COMMENT           = 2; // total FREQUENCY_GET_COMMENT = FREQUENCY_GET_COMMENT + FREQUENCY_NEW_COMMENT_RESPONSE because FREQUENCY_NEW_COMMENT_RESPONSE depend on FREQUENCY_GET_COMMENT 
    public static final int FREQUENCY_GET_COMMENT           = 2;
    public static final int FREQUENCY_NEW_COMMENT_RESPONSE  = 1;
    public static final int FREQUENCY_NEW_FEEDBACK          = 3;
    public static final int FREQUENCY_NEW_ITEM              = 10;
    public static final int FREQUENCY_NEW_PURCHASE          = 2;
    public static final int FREQUENCY_NEW_USER              = 5;
    public static final int FREQUENCY_UPDATE_ITEM           = 2;
    
//    public static final int FREQUENCY_GET_ITEM              = 0;
//    public static final int FREQUENCY_GET_USER_INFO         = 100;
//    public static final int FREQUENCY_GET_WATCHED_ITEMS     = 0;
//    public static final int FREQUENCY_NEW_BID               = 0;
//    public static final int FREQUENCY_NEW_COMMENT           = 0;
//    public static final int FREQUENCY_GET_COMMENT           = 0;
//    public static final int FREQUENCY_NEW_COMMENT_RESPONSE  = 0;
//    public static final int FREQUENCY_NEW_FEEDBACK          = 0;
//    public static final int FREQUENCY_NEW_ITEM              = 0;
//    public static final int FREQUENCY_NEW_PURCHASE          = 0;
//    public static final int FREQUENCY_NEW_USER              = 0;
//    public static final int FREQUENCY_UPDATE_ITEM           = 0;

    
    // ----------------------------------------------------------------
    // DEFAULT TABLE SIZES
    // ----------------------------------------------------------------
    public static final long TABLESIZE_REGION = 75;
    public static final long TABLESIZE_GLOBAL_ATTRIBUTE_GROUP = 100;
    public static final long TABLESIZE_GLOBAL_ATTRIBUTE_VALUE = TABLESIZE_GLOBAL_ATTRIBUTE_GROUP * 10;
    public static final long TABLESIZE_USER = 100000;
    public static final long TABLESIZE_ITEM = TABLESIZE_USER * 10;
    
    // ----------------------------------------------------------------
    // USER PARAMETERS
    // ----------------------------------------------------------------
    public static final int USER_MIN_ATTRIBUTES = 0;
    public static final int USER_MAX_ATTRIBUTES = 5;
        
    // ----------------------------------------------------------------
    // SELLER PARAMETERS
    // ---------------------------------------------------------------- 
    public static final float SELLER_PERCENT = 10.0f;
    public static final int SELLER_MAX_ITEM = 1000;
    
    // ----------------------------------------------------------------
    // ITEM PARAMETERS
    // ----------------------------------------------------------------
    public static final int ITEM_MIN_INITIAL_PRICE = 1;
    public static final int ITEM_MAX_INITIAL_PRICE = 1000000;
    public static final int ITEM_MAX_FINAL_PRICE = 10000000;
    public static final int ITEM_MIN_ITEMS_PER_SELLER = 1;
    public static final int ITEM_MAX_ITEMS_PER_SELLER = 10000;
    public static final int ITEM_MIN_BIDS_PER_DAY = 0;
    public static final int ITEM_MAX_BIDS_PER_DAY = 12;
    public static final int ITEM_MIN_WATCHES_PER_DAY = 0;
    public static final int ITEM_MAX_WATCHES_PER_DAY = 20;
    public static final int ITEM_MIN_IMAGES = 1;
    public static final int ITEM_MAX_IMAGES = 10;
    public static final int ITEM_MIN_COMMENTS = 0;
    public static final int ITEM_MAX_COMMENTS = 5;
    public static final int ITEM_MIN_GLOBAL_ATTRS = 1;
    public static final int ITEM_MAX_GLOBAL_ATTRS = 10;
    public static final float ITEM_BID_PERCENT_STEP = 0.05f;
    
    public static final int ITEM_MAX_PURCHASE_DURATION_DAYS = 7;
    
    //public static final int ITEM_PERCENT_SOLD = 20;
    // Duration in days that expired bids are preserved
    public static final int ITEM_PRESERVE_DAYS = 30;
    // Maximum duration in days for each auction
    public static final int ITEM_MAX_DURATION_DAYS = 7;
    public static final int ITEM_MAX_PURCHASE_DAY = 7;
    
    // ----------------------------------------------------------------
    // DEFAULT BATCH SIZES
    // ----------------------------------------------------------------
    public static final long BATCHSIZE_REGION                   = 5000;
    public static final long BATCHSIZE_GLOBAL_ATTRIBUTE_GROUP   = 5000;
    public static final long BATCHSIZE_GLOBAL_ATTRIBUTE_VALUE   = 5000;
    public static final long BATCHSIZE_CATEGORY                 = 5000;
    public static final long BATCHSIZE_USER                     = 1000;
    public static final long BATCHSIZE_USER_ATTRIBUTES          = 5000;
    public static final long BATCHSIZE_ITEM                     = 1000;
    public static final long BATCHSIZE_ITEM_ATTRIBUTE           = 5000;
    public static final long BATCHSIZE_ITEM_IMAGE               = 5000;
    public static final long BATCHSIZE_ITEM_COMMENT             = 1000;
    public static final long BATCHSIZE_ITEM_FEEDBACK            = 1000;
    public static final long BATCHSIZE_ITEM_BID                 = 5000;
    public static final long BATCHSIZE_ITEM_MAX_BID             = 5000;
    public static final long BATCHSIZE_ITEM_PURCHASE            = 5000;
    public static final long BATCHSIZE_USER_ITEM                = 5000;
    public static final long BATCHSIZE_USER_WATCH               = 5000;
    
    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_REGION                 = "REGION";
    public static final String TABLENAME_USER                   = "USER";
    public static final String TABLENAME_USER_ATTRIBUTES        = "USER_ATTRIBUTES";
    public static final String TABLENAME_USER_ITEM              = "USER_ITEM";
    public static final String TABLENAME_USER_WATCH             = "USER_WATCH";
    public static final String TABLENAME_CATEGORY               = "CATEGORY";
    public static final String TABLENAME_GLOBAL_ATTRIBUTE_GROUP = "GLOBAL_ATTRIBUTE_GROUP";
    public static final String TABLENAME_GLOBAL_ATTRIBUTE_VALUE = "GLOBAL_ATTRIBUTE_VALUE";
    public static final String TABLENAME_ITEM                   = "ITEM";
    public static final String TABLENAME_ITEM_ATTRIBUTE         = "ITEM_ATTRIBUTE";
    public static final String TABLENAME_ITEM_IMAGE             = "ITEM_IMAGE";
    public static final String TABLENAME_ITEM_COMMENT           = "ITEM_COMMENT";
    public static final String TABLENAME_ITEM_FEEDBACK          = "ITEM_FEEDBACK";
    public static final String TABLENAME_ITEM_BID               = "ITEM_BID";
    public static final String TABLENAME_ITEM_MAX_BID           = "ITEM_MAX_BID";
    public static final String TABLENAME_ITEM_PURCHASE          = "ITEM_PURCHASE";

    public static final String TABLENAMES[] = {
        AuctionMarkConstants.TABLENAME_REGION,
        AuctionMarkConstants.TABLENAME_CATEGORY,
        AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
        AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE,
        AuctionMarkConstants.TABLENAME_USER,
        AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES,
        AuctionMarkConstants.TABLENAME_USER_ITEM,
        AuctionMarkConstants.TABLENAME_USER_WATCH,
        AuctionMarkConstants.TABLENAME_ITEM,
        AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE,
        AuctionMarkConstants.TABLENAME_ITEM_IMAGE,
        AuctionMarkConstants.TABLENAME_ITEM_COMMENT,
        AuctionMarkConstants.TABLENAME_ITEM_FEEDBACK,
        AuctionMarkConstants.TABLENAME_ITEM_BID,
        AuctionMarkConstants.TABLENAME_ITEM_MAX_BID,
        AuctionMarkConstants.TABLENAME_ITEM_PURCHASE,
    };
    
    // ----------------------------------------------------------------
    // TABLE DATA SOURCES
    // ----------------------------------------------------------------
    
    // If a table exists in this set, then the number of tuples loaded into the table
    // should not be modified by the scale factor
    public static final Set<String> FIXED_TABLES = new HashSet<String>();
    static {
        AuctionMarkConstants.FIXED_TABLES.add(AuctionMarkConstants.TABLENAME_REGION);
        AuctionMarkConstants.FIXED_TABLES.add(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        AuctionMarkConstants.FIXED_TABLES.add(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
    }
    
    public static final Set<String> DYNAMIC_TABLES = new HashSet<String>();
    static {
    	AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES);
    	AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_IMAGE);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_COMMENT);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_FEEDBACK);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_BID);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_USER_ITEM);
        AuctionMarkConstants.DYNAMIC_TABLES.add(AuctionMarkConstants.TABLENAME_USER_WATCH);
    }
    
    // These tables are loaded from static data files
    public static final Set<String> DATAFILE_TABLES = new HashSet<String>();
    static {
    	AuctionMarkConstants.DATAFILE_TABLES.add(AuctionMarkConstants.TABLENAME_CATEGORY);
    }
}
