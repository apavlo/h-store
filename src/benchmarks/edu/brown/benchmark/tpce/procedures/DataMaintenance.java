/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
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

import java.util.Calendar;
import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;

/**
 * DataMaintenance Transaction <br/>
 * TPC-E Section 3.3.11
 * 
 * H-Store quirks:
 *  1) CUSTOMER_TAXRATE part uses the MySQL version instead of the official one.
 *  2) For DAILY_MARKET it is hard to get the day of the month in the SQL query itself. So, we go through them one-by-one, starting
 *     from the initial year (specified in TPCEConstants) until the last history year (also in TPCEConstants).
 *  3) For EXCHANGE, substring is, again, unsupported. So, the table is scanned and updated (it contains 4 rows only).
 *  4) FINANCIAL has the same problem -- no substring. The row is retrieved and then the decision is made.
 *  5) For NEWS_ITEM it is impossible to increment the date. So, we retrieve the date via join and update separately.
 *  6) SECURITY the same as for NEWS_ITEM, except join is not needed.
 *  7) For WATCH_LIST, selectMaxCommonWatchList selects the maximum id for a "common" watch list. It is done differently in the official
 *     specification, but this should work too. The problem is that sub-queries and distinct? are not supported. 
 */
public class DataMaintenance extends VoltProcedure {

    public static final long DAY_MICROSECONDS = 86400000000l;

    public final SQLStmt selectAccountPermission = new SQLStmt("SELECT AP_ACL FROM ACCOUNT_PERMISSION WHERE AP_CA_ID = ? ORDER BY ap_acl DESC LIMIT 1");

    public final SQLStmt updateAccountPermission = new SQLStmt("UPDATE ACCOUNT_PERMISSION SET AP_ACL = ? WHERE AP_CA_ID = ?");

    public final SQLStmt selectCustomerAddress = new SQLStmt("SELECT ad_line2, ad_id FROM address, customer WHERE ad_id = c_ad_id AND c_id = ?");

    public final SQLStmt selectCompanyAddress = new SQLStmt("SELECT ad_line2, ad_id FROM address, company WHERE ad_id = co_ad_id AND co_id = ?");

    public final SQLStmt updateAddress = new SQLStmt("UPDATE address SET ad_line2 = ? WHERE ad_id = ?");

    public final SQLStmt selectCompany = new SQLStmt("SELECT CO_SP_RATE FROM COMPANY WHERE CO_ID = ?");

    public final SQLStmt updateCompany = new SQLStmt("UPDATE COMPANY SET CO_SP_RATE = ? WHERE CO_ID = ?");

    public final SQLStmt selectCustomer = new SQLStmt("SELECT C_EMAIL_2 FROM CUSTOMER WHERE C_ID = ?");

    public final SQLStmt updateCustomer = new SQLStmt("UPDATE CUSTOMER SET C_EMAIL_2 = ? WHERE C_ID = ?");

    public final SQLStmt selectCustomerTaxrate = new SQLStmt(
    // Benchmark Spec Version
    // "SELECT count(*) FROM customer_taxrate WHERE cx_c_id = ? AND cx_tx_id = ?");
    // MySQL Version
            "SELECT cx_tx_id FROM customer_taxrate WHERE cx_c_id = ?"); // AND
                                                                        // (cx_tx_id
                                                                        // LIKE
                                                                        // 'US%'
                                                                        // OR
                                                                        // cx_tx_id
                                                                        // LIKE
                                                                        // 'CN%')");

    public final SQLStmt updateCustomerTaxrate = new SQLStmt("UPDATE customer_taxrate SET cx_tx_id = ? WHERE cx_c_id = ? AND cx_tx_id = ?");

    public final SQLStmt insertCustomerTaxrate = new SQLStmt("INSERT INTO customer_taxrate (cx_tx_id, cx_c_id) VALUES (?, ?)");

    public final SQLStmt deleteCustomerTaxrate = new SQLStmt("DELETE FROM customer_taxrate WHERE cx_tx_id = ? AND cx_c_id = ?");

    public final SQLStmt updateDailyMarket = new SQLStmt("UPDATE daily_market SET dm_vol = dm_vol + ? WHERE dm_s_symb = ? AND dm_date = ?");

    public final SQLStmt selectExchange = new SQLStmt("SELECT ex_id, ex_desc FROM EXCHANGE");

    public final SQLStmt updateExchange = new SQLStmt("UPDATE EXCHANGE SET EX_DESC = ? WHERE EX_ID = ?");

    public final SQLStmt selectFinancial = new SQLStmt("SELECT FI_QTR_START_DATE FROM FINANCIAL WHERE FI_CO_ID = ?");

    public final SQLStmt updateFinancial = new SQLStmt("UPDATE FINANCIAL SET FI_QTR_START_DATE = ? WHERE FI_CO_ID = ?");

    public final SQLStmt selectNewsXref = new SQLStmt("SELECT nx_ni_id, ni_dts FROM news_xref, news_item WHERE nx_co_id = ? and nx_ni_id = ni_id");

    public final SQLStmt updateNewsItem = new SQLStmt("UPDATE news_item SET ni_dts = ? WHERE ni_id = ?");

    public final SQLStmt selectSecurity = new SQLStmt("SELECT S_EXCH_DATE FROM SECURITY WHERE S_SYMB = ?");

    public final SQLStmt updateSecurity = new SQLStmt("UPDATE SECURITY SET S_EXCH_DATE = ? WHERE S_SYMB = ?");

    public final SQLStmt selectTaxRate = new SQLStmt("SELECT TX_NAME FROM TAXRATE WHERE TX_ID = ?");

    public final SQLStmt updateTaxRate = new SQLStmt("UPDATE taxrate SET tx_rate = ? WHERE tx_id = ?");

    public final SQLStmt updateTaxRateName = new SQLStmt("UPDATE taxrate SET tx_name = ? WHERE tx_id = ?");

    public final SQLStmt selectWatchItemListCount = new SQLStmt("SELECT COUNT(wi_wl_id) FROM watch_item, watch_list WHERE wl_c_id = ? AND wi_wl_id = wl_id");

    public final SQLStmt selectWatchListCustMax = new SQLStmt("SELECT max(WL_ID) FROM WATCH_LIST WHERE WL_C_ID = ?");

    public final SQLStmt selectWatchListMax = new SQLStmt("SELECT max(WL_ID) FROM WATCH_LIST");
    
    public final SQLStmt selectWatchItemMax = new SQLStmt("SELECT max(WI_S_SYMB) FROM WATCH_ITEM WHERE WI_WL_ID = ? AND WI_S_SYMB != ? AND WI_S_SYMB != ? AND WI_S_SYMB != ?");

    public final SQLStmt insertWatchList = new SQLStmt("insert into WATCH_LIST(WL_ID, WL_C_ID) values (?, ?)");
    
    public final SQLStmt insertWatchItem = new SQLStmt("insert into WATCH_ITEM(WI_WL_ID, WI_S_SYMB) values (?, ?)");
    
    public final SQLStmt selectMaxCommonWatchList = new SQLStmt("select max(WI_WL_ID) from WATCH_ITEM " +
            "where WI_S_SYMB != ? and WI_S_SYMB != ? and WI_S_SYMB != ? group by WI_WL_ID");
    
    public final SQLStmt deleteWatchListItems = new SQLStmt("delete from WATCH_ITEM where WI_WL_ID > ?");
    
    public final SQLStmt deleteWatchList = new SQLStmt("delete from WATCH_LIST where WL_ID > ?");
    
    /**
     * @param acct_id
     * @param c_id
     * @param co_id
     * @param day_of_month
     * @param symbol
     * @param table_name
     * @param tx_id
     * @param vol_incr
     * @return
     */
    public VoltTable[] run(long acct_id, long c_id, long co_id, long day_of_month, String symbol, String table_name, String tx_id, long vol_incr) {
        // ACCOUNT_PERMISSION
        if (table_name.equals("ACCOUNT_PERMISSION")) {

            // Update the AP_ACL to "1111" or "0011" in rows for
            // an account of in_acct_id.
            voltQueueSQL(selectAccountPermission, acct_id);
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());
            String acl = results[0].getString(0);

            // ACL is "1111" change it to "0011"
            acl = (!acl.equals("1111") ? "1111" : "0011");
            voltQueueSQL(updateAccountPermission, acl, acct_id);

            // ADDRESS
        } else if (table_name.equals("ADDRESS")) {
            // Change AD_LINE2 in the ADDRESS table for
            // the CUSTOMER with C_ID of c_id.
            if (c_id != 0) {
                voltQueueSQL(selectCustomerAddress, c_id);
            } else {
                voltQueueSQL(selectCompanyAddress, co_id);
            }
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());

            String line2 = results[0].getString(0);
            long addr_id = results[0].getLong(1);

            if (!line2.equals("Apt. 10C")) {
                voltQueueSQL(updateAddress, "Apt. 10C", addr_id);
            } else {
                voltQueueSQL(updateAddress, "Apt. 22", addr_id);
            }

            // COMPANY
        } else if (table_name.equals("COMPANY")) {
            // Update a row in the COMPANY table identified
            // by co_id, set the company's Standard and Poor
            // credit rating to "ABA" or to "AAA".

            voltQueueSQL(selectCompany, co_id);
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());
            String sprate = results[0].getString(0);

            if (!sprate.equals("ABA")) {
                voltQueueSQL(updateCompany, "ABA", co_id);
            } else {
                voltQueueSQL(updateCompany, "AAA", co_id);
            }

            // CUSTOMER
        } else if (table_name.equals("CUSTOMER")) {
            // Update the second email address of a CUSTOMER
            // identified by c_id. Set the ISP part of the customer's
            // second email address to "@mindspring.com"
            // or "@earthlink.com".

            int lenMindspring = "@mindspring.com".length();

            voltQueueSQL(selectCustomer, c_id);
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());

            String email2 = results[0].getString(0);
            int len = email2.length() - lenMindspring;
            String new_email = null;

            // Set to @earthlink.com
            if (len > 0 && email2.substring(len + 1).equals("@mindspring.com")) {
                new_email = email2.substring(0, email2.indexOf("@")) + "earthlink.com";
                // Set to @mindspring.com
            } else {
                new_email = email2.substring(0, email2.indexOf("@")) + "mindspring.com";
            }
            voltQueueSQL(updateCustomer, new_email, c_id);

            // CUSTOMER_TAXRATE
        } else if (table_name.equals("CUSTOMER_TAXRATE")) {
            // A tax rate identified by "999" will be inserted into
            // the CUSTOMER_TAXRATE table for the CUSTOMER identified
            // by c_id.If the customer already has the "999" tax
            // rate, the tax Rate will be deleted. To preserve for
            // foreign key integrity The "999" tax rate must exist
            // in the TAXRATE table.

            String tax_id = "999";
            voltQueueSQL(selectTaxRate, tax_id);
            VoltTable[] results = voltExecuteSQL();
            assert results.length == 1;
            assert results[0].advanceRow();

            double tax_rate = results[0].getDouble(0);
            double new_tax_rate = (tax_rate == 0.11 ? 0.13 : 0.11);

            // Update customer
            // So this is what the TPC-E spec has, but the MySQL version does
            // something else
            // voltQueueSQL(selectCustomerTaxrate, c_id, tax_id);
            // results = voltExecuteSQL();
            // assert results.length == 1;
            // assert results[0].advanceRow();
            // long count = results[0].getLong(0);
            //
            // if (count == 0) {
            // voltQueueSQL(insertCustomerTaxrate, tax_id, c_id);
            // } else {
            // voltQueueSQL(deleteCustomerTaxrate, tax_id, c_id);
            // }
            //
            voltQueueSQL(selectCustomerTaxrate, c_id);
            results = voltExecuteSQL();

            while (results[0].advanceRow()) {
                String old_tax_id = results[0].getString(0);
                String new_tax_id = null;

                if (old_tax_id.startsWith("US")) {
                    if (old_tax_id.equals("US5")) {
                        new_tax_id = "US1";
                    } else if (old_tax_id.equals("US4")) {
                        new_tax_id = "US5";
                    } else if (old_tax_id.equals("US3")) {
                        new_tax_id = "US4";
                    } else if (old_tax_id.equals("US2")) {
                        new_tax_id = "US3";
                    } else {
                        new_tax_id = "US2";
                    }
                } else if (old_tax_id.startsWith("CN")) {
                    if (old_tax_id.equals("CN4")) {
                        new_tax_id = "CN1";
                    } else if (old_tax_id.equals("CN3")) {
                        new_tax_id = "CN4";
                    } else if (old_tax_id.equals("CN2")) {
                        new_tax_id = "CN3";
                    } else {
                        new_tax_id = "CN2";
                    }
                }
                if (new_tax_id != null)
                    voltQueueSQL(updateCustomerTaxrate, new_tax_id, c_id, old_tax_id);
            } // WHILE

            // Don't forget this from above
            voltQueueSQL(updateTaxRate, new_tax_rate, tax_id);

            // DAILY_MARKET
        } else if (table_name.equals("DAILY_MARKET")) {
            // See the comment in the header for this table
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.DAY_OF_MONTH, (int)day_of_month);
            
            for (int year = 0; year < TPCEConstants.dailyMarketYears; year++) {
                cal.set(Calendar.YEAR, TPCEConstants.dailyMarketBaseYear + year);
                for (int month = 0; month < 12; month++) {
                    cal.set(Calendar.MONTH, month); 
                    TimestampType t = new TimestampType(cal.getTime());
                    voltQueueSQL(updateDailyMarket, vol_incr, symbol, t);
                }
            }
            // EXCHANGE
        } else if (table_name.equals("EXCHANGE")) {
            // Other than the table_name, no additional
            // parameters are used when the table_name is EXCHANGE.
            // There are only four rows in the EXCHANGE table. Every
            // row will have its EX_DESC updated. If EX_DESC does not
            // already end with "LAST UPDATED " and a date and time,
            // that string will be appended to EX_DESC. Otherwise the
            // date and time at the end of EX_DESC will be updated
            // to the current date and time.

            // PAVLO (2010-06-15)
            // The substring replacement functions from the spec isn't
            // supported, so we are just going
            // to pull down all the exchange descriptions and update one at a
            // time
            voltQueueSQL(selectExchange);
            VoltTable[] results = voltExecuteSQL();

            final TimestampType now = new TimestampType();
            final String last_updated = " LAST UPDATED ";
            while (results[0].advanceRow()) {
                long ex_id = results[0].getLong(0);
                String ex_desc = results[0].getString(1);

                // Update the Last Update timestamp
                if (ex_desc.contains(last_updated)) {
                    int start_idx = ex_desc.indexOf(last_updated) + last_updated.length();
                    ex_desc = ex_desc.substring(0, start_idx) + now;
                    // Add the Last Updated string + timestamp
                } else {
                    ex_desc += last_updated + now;
                }
                voltQueueSQL(updateExchange, ex_desc, ex_id);
            } // WHILE

            // FINANCIAL
        } else if (table_name.equals("FINANCIAL")) {
            // Update the FINANCIAL table for a company identified by
            // co_id. That company's FI_QTR_START_DATEs will be
            // updated to the second of the month or to the first of
            // the month if the dates were already the second of the
            // month.

            // PAVLO (2010-06-15)
            // Again, the date substring tricks in the spec aren't supported, so
            // we'll pull
            // down the data and make the decision on what to do here
            voltQueueSQL(selectFinancial, co_id);
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());

            TimestampType orig_start_timestamp = results[0].getTimestampAsTimestamp(0);
            Date qtr_start_date = orig_start_timestamp.asApproximateJavaDate();
            Calendar c = Calendar.getInstance();
            c.setTime(qtr_start_date);
            int qtr_start_day = c.get(Calendar.DAY_OF_MONTH);
            long delta = DAY_MICROSECONDS;

            // Decrement by one day
            if (qtr_start_day != 1)
                delta *= -1;

            TimestampType new_start_date = new TimestampType(orig_start_timestamp.getTime() + delta);
            voltQueueSQL(updateFinancial, new_start_date, co_id);

            // NEWS_ITEM
        } else if (table_name.equals("NEWS_ITEM")) {
            // Update the news items for a specified company.
            // Change the NI_DTS by 1 day.

            voltQueueSQL(selectNewsXref, co_id);
            VoltTable[] results = voltExecuteSQL();
            while (results[0].advanceRow()) {
                long ni_id = results[0].getLong(0);
                TimestampType ni_dts = results[0].getTimestampAsTimestamp(1);
                
                voltQueueSQL(updateNewsItem, ni_dts.getTime() + DAY_MICROSECONDS, ni_id);
             } // WHILE

            // SECURITY
        } else if (table_name.equals("SECURITY")) {
            // Update a security identified symbol, increment
            // S_EXCH_DATE by 1 day.

            voltQueueSQL(selectSecurity, symbol);
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());

            TimestampType exch_date = results[0].getTimestampAsTimestamp(0);
            assert (exch_date != null);
            exch_date = new TimestampType(exch_date.getTime() + DAY_MICROSECONDS);

            voltQueueSQL(updateSecurity, exch_date, symbol);

            // TAXRATE
        } else if (table_name.equals("TAXRATE")) {
            // Update a TAXRATE identified by tx_id. The tax rate's
            // TX_NAME Will be updated to end with the word "rate",
            // or the word "rate" will be removed from the end of the
            // TX_NAME if TX_NAME already ends with the word "rate".

            voltQueueSQL(selectTaxRate, tx_id);
            VoltTable[] results = voltExecuteSQL();
            assert (results[0].advanceRow());

            String tax_name = results[0].getString(0);
            int pos = tax_name.indexOf(" rate");
            if (pos != -1) {
                tax_name = tax_name.substring(0, pos);
            } else {
                tax_name += " rate";
            }

            voltQueueSQL(updateTaxRateName, tax_name, tx_id);

            // WATCH_ITEM
        } else if (table_name.equals("WATCH_ITEM")) {
            // A WATCH_LIST containing the WATCH_ITEMs with security
            // symbols "AA", "ZAPS" and "ZONS" will be added for the
            // customer identified by c_id, if the customer does not
            // already have a watch list with those items. If the
            // customer already has a watch list with those items,
            // the watch list will be deleted.

            voltQueueSQL(selectWatchListCustMax, c_id);
            VoltTable[] results = voltExecuteSQL();
            
            String symbol1 = "AA";
            String symbol2 = "ZAPS";
            String symbol3 = "ZONS";
            
            if (results[0].getRowCount() > 0) {
                assert (results[0].advanceRow());
                long wl_id = results[0].getLong(0);
    
                // If the CUSTOMER identified by c_id has a watch
                // list with "AA", "ZAPS", "ZONS", it would have the
                // highest WL_ID of that customer's watch lists.
                voltQueueSQL(selectWatchItemMax, wl_id, symbol1, symbol2, symbol3);
                results = voltExecuteSQL();
    
                if (results[0].getRowCount() > 0) {
                    // no "AA", "ZAPS", "ZONS" list
                    voltQueueSQL(selectWatchListMax);
                    VoltTable wl = voltExecuteSQL()[0];
                    
                    assert wl.getRowCount() == 1;
                    long last_wl_id = wl.fetchRow(0).getLong(0);
                    
                    voltQueueSQL(insertWatchList, last_wl_id + 1, c_id);
                    voltQueueSQL(insertWatchItem, last_wl_id + 1, symbol1);
                    voltQueueSQL(insertWatchItem, last_wl_id + 1, symbol2);
                    voltQueueSQL(insertWatchItem, last_wl_id + 1, symbol3);
                }
                else {
                    // the customer has the list -- remove it for this and all other customers
                    voltQueueSQL(selectMaxCommonWatchList, symbol1, symbol2, symbol3);
                    VoltTable wl = voltExecuteSQL()[0];
                    
                    assert wl.getRowCount() == 1;
                    long max_wl_id = wl.fetchRow(0).getLong(0);
                    
                    // have to remove all lists with ids greater that that one
                    voltQueueSQL(deleteWatchListItems, max_wl_id);
                    voltQueueSQL(deleteWatchList, max_wl_id);
                }
            }
            else {
                // no watch lists for the customer -- insert this as the only one
                voltQueueSQL(selectWatchListMax);
                VoltTable wl = voltExecuteSQL()[0];
                
                assert wl.getRowCount() == 1;
                long last_wl_id = wl.fetchRow(0).getLong(0);
                
                voltQueueSQL(insertWatchList, last_wl_id + 1, c_id);
                voltQueueSQL(insertWatchItem, last_wl_id + 1, symbol1);
                voltQueueSQL(insertWatchItem, last_wl_id + 1, symbol2);
                voltQueueSQL(insertWatchItem, last_wl_id + 1, symbol3);
            }
        }
        return (voltExecuteSQL());
    }
}
