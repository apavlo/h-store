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

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.auctionmark.procedures.*;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class AuctionMarkProjectBuilder extends AbstractProjectBuilder {
    
    public static final ProjectType type = ProjectType.AUCTIONMARK;

    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        CheckWinningBids.class,
        GetComment.class,
        GetItem.class,
        GetUserInfo.class,
        GetWatchedItems.class,
        NewBid.class,
        NewComment.class,
        NewCommentResponse.class,
        NewFeedback.class,
        NewItem.class,
        NewPurchase.class,
        NewUser.class,
        PostAuction.class,
        UpdateItem.class,
    };
    
    public static final String PARTITIONING[][] = new String[][] {
        {AuctionMarkConstants.TABLENAME_USER, "U_ID"},
        {AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES, "UA_U_ID"},
        {AuctionMarkConstants.TABLENAME_USER_ITEM, "UI_U_ID"},
        {AuctionMarkConstants.TABLENAME_USER_WATCH, "UW_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM, "I_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE, "IA_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_IMAGE, "II_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_COMMENT, "IC_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_FEEDBACK, "IF_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_BID, "IB_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_MAX_BID, "IMB_U_ID"},
        {AuctionMarkConstants.TABLENAME_ITEM_PURCHASE, "IP_IB_U_ID"},
    };
    
    public static final Class<?> SUPPLMENTALS[] = new Class<?>[] {
        // TODO(visawee)
    };
    
    public AuctionMarkProjectBuilder() {
        super("auctionmark", AuctionMarkProjectBuilder.class, PROCEDURES, PARTITIONING, SUPPLMENTALS, true);
    }
}
