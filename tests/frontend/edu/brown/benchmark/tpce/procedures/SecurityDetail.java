/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package edu.brown.benchmark.tpce.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

/**
 * Security-Detail Transaction <br/>
 * TPC-E Section 3.3.8
 * 
 * H-Store quirks:
 *   1) Many parameters are not required as the transaction's output. We execute complete statements, but do not retrieve them.
 *   2) Constants are used for LIMIT instead of parameters (getInfo2, getInfo3, getInfo6, getInfo7).
 *      The constants' values are predefined in the specification.
 *   3) getInfo4 has the same LIMIT issue. We use 20 for the constant since it is the maximum value.
 *      The real one will be a parameter from 5 to 20.
 */
public class SecurityDetail extends VoltProcedure {
    private final VoltTable ret_template = new VoltTable(
            new VoltTable.ColumnInfo("last_vol", VoltType.BIGINT),
            new VoltTable.ColumnInfo("news_len", VoltType.INTEGER)
    );

    // should be used as a limit for getInfo2
    private static final int max_comp_len = 3;

    // should be used as a limit for getInfo3 
    private static final int max_fin_len = 20;

    // should be used as a limit for getInfo6 and getInfo7 
    private static final int max_news_len = 2;


/*  commented out for now because the planer freaks out!
 *     public final SQLStmt getInfo1 = new SQLStmt(
            "select S_NAME, CO_ID, CO_NAME, CO_SP_RATE, CO_CEO, CO_DESC, CO_OPEN_DATE, CO_ST_ID, " +
                "CA.AD_LINE1, CA.AD_LINE2, ZCA.ZC_TOWN, ZCA.ZC_DIV, CA.AD_ZC_CODE, CA.AD_CTRY, " +
                "S_NUM_OUT, S_START_DATE, S_EXCH_DATE, S_PE, S_52WK_HIGH, S_52WK_HIGH_DATE, " +
                "S_52WK_LOW, S_52WK_LOW_DATE, S_DIVIDEND, S_YIELD, ZEA.ZC_DIV, EA.AD_CTRY, EA.AD_LINE1, EA.AD_LINE2, " +
                "ZEA.ZC_TOWN, EA.AD_ZC_CODE, EX_CLOSE, EX_DESC, EX_NAME, EX_NUM_SYMB, EX_OPEN " +
            "from SECURITY, COMPANY, ADDRESS CA, ADDRESS EA, ZIP_CODE ZCA, ZIP_CODE ZEA, EXCHANGE " +
            "where S_SYMB = ? and CO_ID = S_CO_ID and CA.AD_ID = CO_AD_ID and EA.AD_ID = EX_AD_ID and " +
                "EX_ID = S_EX_ID AND CA.AD_ZC_CODE = ZCA.ZC_CODE and EA.AD_ZC_CODE = ZEA.ZC_CODE");
*/
    public final SQLStmt getInfo1 = new SQLStmt(
            "select S_NAME, CO_ID " +
            "from SECURITY, COMPANY " +
            "where S_SYMB = ? and CO_ID = S_CO_ID");
    
    public final SQLStmt getInfo2 = new SQLStmt("select CO_NAME, IN_NAME from COMPANY_COMPETITOR, COMPANY, INDUSTRY " +
            "where CP_CO_ID = ? and CO_ID = CP_COMP_CO_ID and IN_ID = CP_IN_ID limit 3");

    public final SQLStmt getInfo3 = new SQLStmt("select FI_YEAR, FI_QTR, FI_QTR_START_DATE, FI_REVENUE, FI_NET_EARN, " +
            "FI_BASIC_EPS, FI_DILUT_EPS, FI_MARGIN, FI_INVENTORY, FI_ASSETS, FI_LIABILITY, FI_OUT_BASIC, FI_OUT_DILUT " +
            "from FINANCIAL where FI_CO_ID = ? order by FI_YEAR asc, FI_QTR limit 20");
    
    public final SQLStmt getInfo4 = new SQLStmt("select DM_DATE, DM_CLOSE, DM_HIGH, DM_LOW, DM_VOL " +
            "from DAILY_MARKET where DM_S_SYMB = ? and DM_DATE >= ? order by DM_DATE asc limit 20");
                                                                                                                                                                                                // ?
    public final SQLStmt getInfo5 = new SQLStmt("select LT_PRICE, LT_OPEN_PRICE, LT_VOL from LAST_TRADE where LT_S_SYMB = ?");

    public final SQLStmt getInfo6 = new SQLStmt("select NI_ITEM, NI_DTS, NI_SOURCE, NI_AUTHOR " +
            "from NEWS_XREF, NEWS_ITEM where NI_ID = NX_NI_ID and NX_CO_ID = ? limit 2");
    
    public final SQLStmt getInfo7 = new SQLStmt("select NI_DTS, NI_SOURCE, NI_AUTHOR, NI_HEADLINE, NI_SUMMARY " +
            "from NEWS_XREF, NEWS_ITEM where NI_ID = NX_NI_ID and NX_CO_ID = ? limit 2");

    public VoltTable[] run(long max_rows_to_return, long access_lob_flag, TimestampType start_day, String symbol) throws VoltAbortException {
        voltQueueSQL(getInfo1, symbol);
        VoltTable sec_detail = voltExecuteSQL()[0];
        
        assert sec_detail.getRowCount() == 1;
        long co_id = sec_detail.fetchRow(0).getLong("CO_ID"); // only co_id is really required
        
        voltQueueSQL(getInfo2, co_id);
        voltQueueSQL(getInfo3, co_id);
        voltQueueSQL(getInfo4, symbol, start_day);
        voltExecuteSQL(); // do not really need the results
        
        voltQueueSQL(getInfo5, symbol);
        VoltTable last_trade = voltExecuteSQL()[0];
        
        assert last_trade.getRowCount() == 1;
        long last_vol = last_trade.fetchRow(0).getLong("LT_VOL");
        
        if (access_lob_flag == 1) {
            voltQueueSQL(getInfo6, co_id);
        }
        else {
            voltQueueSQL(getInfo7, co_id);
        }
        
        VoltTable news = voltExecuteSQL()[0];
        int news_len = news.getRowCount();
        
        VoltTable res = ret_template.clone(64);
        res.addRow(last_vol, news_len);
        
        return new VoltTable[] {res};
    }
}
