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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;

import edu.brown.benchmark.tpce.TPCEClient;
import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * Market-Feed Transaction. <br>
 * TPC-E section: 3.3.9
 */
public class MarketFeed extends VoltProcedure {

    protected static class TradeRequest {
        public String symbol;
        public long trade_id;
        public double price_quote;
        public int trade_qty;
        public String trade_type;

        public TradeRequest(String symbol, long trade_id, double price_quote, int trade_qty, String trade_type) {
            this.symbol = symbol;
            this.trade_id = trade_id;
            this.price_quote = price_quote;
            this.trade_qty = trade_qty;
            this.trade_type = trade_type;
        }
    }

    
    private static int MAX_FEED_LEN = 20;
    private static int MAX_SEND_LEN = 40;

    public final SQLStmt updateLastTrade = new SQLStmt(
            "update LAST_TRADE set LT_PRICE = ?, LT_VOL = LT_VOL + ?, LT_DTS = ? where LT_S_SYMB = ?");

    public final SQLStmt getRequestList = new SQLStmt(
            "select TR_T_ID, TR_BID_PRICE, TR_TT_ID, TR_QTY from TRADE_REQUEST "
                    + "where TR_S_SYMB = ? and "
                    + "((TR_TT_ID = ? and TR_BID_PRICE >= ?) or "
                    + "(TR_TT_ID = ? and TR_BID_PRICE <= ?) or "
                    + "(TR_TT_ID = ? and TR_BID_PRICE >= ?))");

    public final SQLStmt updateTrade = new SQLStmt(
            "update TRADE set T_DTS = ?, T_ST_ID = ? where T_ID = ?");

    public final SQLStmt deleteTradeRequest = new SQLStmt(
            "delete from TRADE_REQUEST where TR_T_ID = ? and TR_BID_PRICE = ? and TR_TT_ID = ? and TR_QTY = ?");

    public final SQLStmt insertTradeHistory = new SQLStmt(
            "insert into TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)");

    public VoltTable[] run(
            double[] price_quotes,
            String status_submitted,
            String[] symbols,
            long[] trade_qtys,
            String type_limit_buy,
            String type_limit_sell,
            String type_stop_loss) throws VoltAbortException {
        Date now_dts = Calendar.getInstance().getTime();
        List<TradeRequest> TradeRequestBuffer = new LinkedList<TradeRequest>();
        int s = 0;

        Map<String, Object[]> ret = new HashMap<String, Object[]>();

        for (int i = 0; i <= MAX_FEED_LEN; i++) {
            ProcedureUtil.execute(this, updateLastTrade, new Object[] {
                    price_quotes[i], trade_qtys[i], now_dts, symbols[i] });

            voltQueueSQL(getRequestList,
                         symbols[i],
                         type_stop_loss, price_quotes[i],
                         type_limit_sell, price_quotes[i],
                         type_limit_buy, price_quotes[i]);
            VoltTable request_list = voltExecuteSQL()[0];

            for (int j = 0; j < request_list.getRowCount() || s >= MAX_SEND_LEN; j++, s++) {
                VoltTableRow row = request_list.fetchRow(j);
                long trade_id = row.getLong("TR_T_ID");
                double price_quote = row.getDouble("TR_BID_PRICE");
                String trade_type = row.getString("TR_TT_ID");
                int trade_qty = (int) row.getLong("TR_QTY");

                voltQueueSQL(updateTrade, now_dts, status_submitted, trade_id);
                voltQueueSQL(deleteTradeRequest, trade_id, price_quote, trade_type, trade_qty);
                voltQueueSQL(insertTradeHistory, trade_id, now_dts, status_submitted);
                voltExecuteSQL();

                TradeRequestBuffer.add(new TradeRequest(symbols[i], trade_id,
                        price_quote, trade_qty, trade_type));
            }
        }

        int send_len = s;

        ret.put("send_len", new Object[] { send_len });
        ret.put("status", new Object[] { TPCEClient.STATUS_SUCCESS });

        // TODO
        // for (TradeRequest tr : TradeRequestBuffer) {
        // send_to_market(tr.symbol, tr.trade_id, tr.price_quote,
        // tr.trade_qty, tr.trade_type);
        // }

        return ProcedureUtil.mapToTable(ret);
    }

}
