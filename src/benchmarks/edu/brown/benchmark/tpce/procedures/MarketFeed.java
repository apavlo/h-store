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
 *  Modifications by:                                                      *
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
package edu.brown.benchmark.tpce.procedures;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

/**
 * Market-Feed Transaction. <br>
 * TPC-E section: 3.3.9
 * 
 * H-Store quirks:
 *   1) Instead of using the send_to_market interface, we return the result as a table. Then the calling MEE client
 *      will use it to do its job. 
 */
public class MarketFeed extends VoltProcedure {
    private final VoltTable stm_template = new VoltTable(
            new VoltTable.ColumnInfo("symbol", VoltType.STRING),
            new VoltTable.ColumnInfo("trade_id", VoltType.BIGINT),
            new VoltTable.ColumnInfo("price_quote", VoltType.FLOAT),
            new VoltTable.ColumnInfo("trade_qty", VoltType.INTEGER),
            new VoltTable.ColumnInfo("trade_type", VoltType.STRING)
    );

    private static class TradeRequest {
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

    public final SQLStmt updateLastTrade = new SQLStmt("update LAST_TRADE set LT_PRICE = ?, LT_VOL = LT_VOL + ?, LT_DTS = ? where LT_S_SYMB = ?");

    public final SQLStmt getRequestList = new SQLStmt("select TR_T_ID, TR_BID_PRICE, TR_TT_ID, TR_QTY from TRADE_REQUEST " +
            "where TR_S_SYMB = ? and ((TR_TT_ID = ? and TR_BID_PRICE >= ?) or " +
            "(TR_TT_ID = ? and TR_BID_PRICE <= ?) or " +
            "(TR_TT_ID = ? and TR_BID_PRICE >= ?))");

    public final SQLStmt updateTrade = new SQLStmt("update TRADE set T_DTS = ?, T_ST_ID = ? where T_ID = ?");

    public final SQLStmt deleteTradeRequest = new SQLStmt("delete from TRADE_REQUEST where TR_T_ID = ?");

    public final SQLStmt insertTradeHistory = new SQLStmt("insert into TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)");

    public VoltTable[] run(double[] price_quotes, String status_submitted, String[] symbols, long[] trade_qtys, String type_limit_buy, String type_limit_sell, String type_stop_loss)
            throws VoltAbortException {
        
        Date now_dts = Calendar.getInstance().getTime();
        List<TradeRequest> tradeRequestBuffer = new ArrayList<TradeRequest>();
        
        // let's do the updates first in a batch
        for (int i = 0; i <= MAX_FEED_LEN; i++) {
            voltQueueSQL(updateLastTrade, price_quotes[i], trade_qtys[i], now_dts, symbols[i]);
        }
        voltExecuteSQL();
        
        // then, see about pending trades
        for (int i = 0; i <= MAX_FEED_LEN; i++) {
            voltQueueSQL(getRequestList, symbols[i], type_stop_loss, price_quotes[i],
                    type_limit_sell, price_quotes[i],
                    type_limit_buy, price_quotes[i]);
            VoltTable reqs = voltExecuteSQL()[0];
            
            for (int j = 0; j < reqs.getRowCount() && tradeRequestBuffer.size() < MAX_SEND_LEN; j++) {
                VoltTableRow req = reqs.fetchRow(j);
                
                long trade_id = req.getLong("TR_T_ID");
                double price_quote = req.getDouble("TR_BID_PRICE");
                String trade_type = req.getString("TR_TT_ID");
                int trade_qty = (int)req.getLong("TR_QTY");
                
                voltQueueSQL(updateTrade, now_dts, status_submitted, trade_id);
                voltQueueSQL(deleteTradeRequest, trade_id);
                voltQueueSQL(insertTradeHistory, trade_id, now_dts, status_submitted);
                voltExecuteSQL();
                
                TradeRequest tr = new TradeRequest(symbols[i], trade_id, price_quote, trade_qty, trade_type);
                tradeRequestBuffer.add(tr);
            }
        }
        
        // creating send_to_market info
        VoltTable stm = stm_template.clone(512);
        for (TradeRequest req: tradeRequestBuffer) {
            stm.addRow(req.symbol, req.trade_id, req.price_quote, req.trade_qty, req.trade_type);
        }
        
        return new VoltTable[] {stm};
    }
}
