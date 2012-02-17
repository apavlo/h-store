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

import java.util.HashMap;
import java.util.Map;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEClient;
import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * Security-Detail Transaction <br/>
 * TPC-E Section 3.3.8
 */
public class SecurityDetail extends VoltProcedure {

    private static final int max_comp_len = 3;

    private static final int max_fin_len = 20;

    private static final int max_news_len = 2;

    // Note: "ADDRESS AS CA"... not supportd
    public final SQLStmt getInfo1 = new SQLStmt(
    // "select S_NAME, CO_ID, CO_NAME, CO_SP_RATE, CO_CEO, CO_DESC, CO_OPEN_DATE, CO_ST_ID, "
    // +
    // "       CA.AD_LINE1, CA.AD_LINE2, ZCA.ZC_TOWN, ZCA.ZC_DIV, CA.AD_ZC_CODE, CA.AD_CTRY, "
    // +
    // "       S_NUM_OUT, S_START_DATE, S_EXCH_DATE, S_PE, S_52WK_HIGH, S_52WK_HIGH_DATE, "
    // +
    // "       S_52WK_LOW, S_52WK_LOW_DATE, S_DIVIDEND, S_YIELD, ZEA.ZC_DIV, EA.AD_CTRY, "
    // +
    // "       EA.AD_LINE1, EA.AD_LINE2, ZEA.ZC_TOWN, EA.AD_ZC_CODE, EX_CLOSE, EX_DESC, "
    // +
    // "       EX_NAME, EX_NUM_SYMB, EX_OPEN " +
    // " FROM SECURITY, COMPANY, ADDRESS AS CA, ADDRESS AS EA, ZIP_CODE AS ZCA, ZIP_CODE AS ZEA, EXCHANGE "
    // +
    // " WHERE S_SYMB = ? and CO_ID = S_CO_ID and CA.AD_ID = CO_AD_ID " +
    // "   AND EA.AD_ID = EX_AD_ID AND EX_ID = S_EX_ID AND CA.AD_ZC_CODE = ZCA.ZC_CODE "
    // +
    // "   AND EA.AD_ZC_CODE = ZEA.ZC_CODE");
            "select S_NAME, CO_ID, CO_NAME, CO_SP_RATE, CO_CEO, CO_DESC, CO_OPEN_DATE, CO_ST_ID, " + "       AD_LINE1, AD_LINE2, ZC_TOWN, ZC_DIV, AD_ZC_CODE, AD_CTRY, "
                    + "       S_NUM_OUT, S_START_DATE, S_EXCH_DATE, S_PE, S_52WK_HIGH, S_52WK_HIGH_DATE, " + "       S_52WK_LOW, S_52WK_LOW_DATE, S_DIVIDEND, S_YIELD, " + "       EX_CLOSE, EX_DESC, "
                    + "       EX_NAME, EX_NUM_SYMB, EX_OPEN " + " FROM SECURITY, COMPANY, ADDRESS, ZIP_CODE, EXCHANGE " + " WHERE S_SYMB = ? and CO_ID = S_CO_ID and AD_ID = CO_AD_ID "
                    + "   AND EX_ID = S_EX_ID AND AD_ZC_CODE = ZC_CODE ");

    // Note: The parameter max_comp_len should be used for the LIMIT value but
    // we don't support parameters in the LIMIT clause. Hardcoding to 3 for now.
    public final SQLStmt getInfo2 = new SQLStmt("select CO_NAME, IN_NAME from COMPANY_COMPETITOR, COMPANY, INDUSTRY " + "where CP_CO_ID = ? and CO_ID = CP_COMP_CO_ID and IN_ID = CP_IN_ID limit 3"); // limit
    // ?

    // Note: The parameter max_fin_len should be used for the LIMIT value but
    // we don't support parameters in the LIMIT clause. Hardcoding to 20 for
    // now.
    public final SQLStmt getInfo3 = new SQLStmt("select FI_YEAR, FI_QTR, FI_QTR_START_DATE, FI_REVENUE, FI_NET_EARN, "
            + "FI_BASIC_EPS, FI_DILUT_EPS, FI_MARGIN, FI_INVENTORY, FI_ASSETS, FI_LIABILITY, " + "FI_OUT_BASIC, FI_OUT_DILUT "
            + "from FINANCIAL where FI_CO_ID = ? order by FI_YEAR asc, FI_QTR limit 20"); // limit
    // ?

    // Note: The parameter max_rows_to_return should be used for the LIMIT value
    // but
    // we don't support parameters in the LIMIT clause. Hardcoding to 20 for
    // now.
    public final SQLStmt getInfo4 = new SQLStmt("select DM_DATE, DM_CLOSE, DM_HIGH, DM_LOW, DM_VOL " + "from DAILY_MARKET where DM_S_SYMB = ? and DM_DATE >= ? order by DM_DATE asc limit 20"); // limit
                                                                                                                                                                                                // ?

    public final SQLStmt getInfo5 = new SQLStmt("select LT_PRICE, LT_OPEN_PRICE, LT_VOL from LAST_TRADE where LT_S_SYMB = ?");

    // Note: The parameter max_news_len should be used for the LIMIT value but
    // we don't support parameters in the LIMIT clause. Hardcoding to 2 for
    // now.
    public final SQLStmt getInfo6 = new SQLStmt("select NI_ITEM, NI_DTS, NI_SOURCE, NI_AUTHOR " + "from NEWS_XREF, NEWS_ITEM where NI_ID = NX_NI_ID and NX_CO_ID = ? limit 2"); // limit
    // ?

    // Note: The parameter max_news_len should be used for the LIMIT value but
    // we don't support parameters in the LIMIT clause. Hardcoding to 2 for
    // now.
    public final SQLStmt getInfo7 = new SQLStmt("select NI_DTS, NI_SOURCE, NI_AUTHOR, NI_HEADLINE, NI_SUMMARY " + "from NEWS_XREF, NEWS_ITEM where NI_ID = NX_NI_ID and NX_CO_ID = ? limit 2"); // limit

    // ?
    public VoltTable[] run(long max_rows_to_return, long access_lob_flag, TimestampType start_day, String symbol) throws VoltAbortException {
        Map<String, Object[]> ret = new HashMap<String, Object[]>();
        ret.put("status", new Object[] { TPCEClient.STATUS_SUCCESS });

        // ProcedureUtil.execute(ret, this, getInfo1, new Object[] { symbol },
        // new String[] { "s_name", "co_id", "co_name", "sp_rate",
        // "ceo_name", "co_desc", "open_date", "co_st_id",
        // "co_ad_line1", "co_ad_line2", "co_ad_town",
        // "co_ad_div", "co_ad_zip", "co_ad_ctry", "num_out",
        // "start_date", "exch_date", "pe_ratio", "52_wk_high",
        // "52_wk_high_date", "52_wk_low", "52_wk_low_date",
        // "divid", "yield", "ex_ad_div", "ex_ad_ctry",
        // "ex_ad_line1", "ex_ad_line2", "ex_ad_town",
        // "ex_ad_zip", "ex_close", "ex_desc", "ex_name",
        // "ex_num_symb", "ex_open" }, new Object[] { "S_NAME",
        // "CO_ID", "CO_NAME", "CO_SP_RATE", "CO_CEO", "CO_DESC",
        // "CO_OPEN_DATE", "CO_ST_ID", "CA.AD_LINE1",
        // "CA.AD_LINE2", "ZCA.ZC_TOWN", "ZCA.ZC_DIV",
        // "CA.AD_ZC_CODE", "CA.AD_CTRY", "S_NUM_OUT",
        // "S_START_DATE", "S_EXCH_DATE", "S_PE", "S_52WK_HIGH",
        // "S_52WK_HIGH_DATE", "S_52WK_LOW", "S_52WK_LOW_DATE",
        // "S_DIVIDEND", "S_YIELD", "ZEA.ZC_DIV", "EA.AD_CTRY",
        // "EA.AD_LINE1", "EA.AD_LINE2", "ZEA.ZC_TOWN",
        // "EA.AD_ZC_CODE", "EX_CLOSE", "EX_DESC", "EX_NAME",
        // "EX_NUM_SYMB", "EX_OPEN" });

        ProcedureUtil.execute(ret, this, getInfo2, new Object[] { ret.get("co_id"), max_comp_len }, new String[] { "cp_co_name", "cp_in_name" }, new Object[] { "CO_NAME", "IN_NAME" });

        int row_count = ProcedureUtil.execute(ret, this, getInfo3, new Object[] { ret.get("co_id"), max_fin_len }, new String[] { "fin.year", "fin.qtr", "fin.start_date", "fin.rev", "fin.net_earn",
                "fin.basic_eps", "fin.dilut_eps", "fin.margin", "fin.invent", "fin.assets", "fin.liab", "fin.out_basic", "fin.out_dilut" }, new Object[] { "FI_YEAR", "FI_QTR", "FI_QTR_START_DATE",
                "FI_REVENUE", "FI_NET_EARN", "FI_BASIC_EPS", "FI_DILUT_EPS", "FI_MARGIN", "FI_INVENTORY", "FI_ASSETS", "FI_LIABILITY", "FI_OUT_BASIC", "FI_OUT_DILUT" });

        ret.put("fin_len", new Object[] { row_count });

        row_count = ProcedureUtil.execute(ret, this, getInfo4, new Object[] { symbol, start_day, max_rows_to_return }, new String[] { "day.date", "day.close", "day.high", "day.low", "day.vol" },
                new Object[] { "DM_DATE", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL" });

        ret.put("t_day_len", new Object[] { row_count });

        ProcedureUtil.execute(ret, this, getInfo5, new Object[] { symbol }, new String[] { "last_price", "last_open", "last_vol" }, new Object[] { "LT_PRICE", "LT_OPEN_PRICE", "LT_VOL" });

        if (access_lob_flag != 0) {
            row_count = ProcedureUtil.execute(ret, this, getInfo6, new Object[] { ret.get("co_id"), max_news_len }, new String[] { "news.item", "news.dts", "news.src", "news.auth", "news.headline",
                    "news.summary" }, new Object[] { "NI_ITEM", "NI_DTS", "NI_SOURCE", "NI_AUTHOR", "", "" });
        } else {
            row_count = ProcedureUtil.execute(ret, this, getInfo7, new Object[] { ret.get("co_id"), max_news_len }, new String[] { "news.item", "news.dts", "news.src", "news.auth", "news.headline",
                    "news.summary" }, new Object[] { "", "NI_DTS", "NI_SOURCE", "NI_AUTHOR", "NI_HEADLINE", "NI_SUMMARY" });
        }

        ret.put("t_news_len", new Object[] { row_count });

        return ProcedureUtil.mapToTable(ret);
    }

}
