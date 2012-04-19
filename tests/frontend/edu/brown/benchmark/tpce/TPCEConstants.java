/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
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
package edu.brown.benchmark.tpce;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class TPCEConstants {

    // Parameter that
    public static final String PARAM_EGENLOADER_HOME = "EGENLOADER_HOME";

    // Helpers
    public static final int TRUE = 1;
    public static final int FALSE = 0;

    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // TPC-E Specification 6.2.2.1
    // ----------------------------------------------------------------
    public static final int FREQUENCY_BROKER_VOLUME = 5;
    public static final int FREQUENCY_DATA_MAINTENANCE = -1;
    public static final int FREQUENCY_CUSTOMER_POSITION = 13;
    public static final int FREQUENCY_MARKET_FEED = 1;
    public static final int FREQUENCY_MARKET_WATCH = 18;
    public static final int FREQUENCY_SECURITY_DETAIL = 14;
    public static final int FREQUENCY_TRADE_CLEANUP = -1;
    public static final int FREQUENCY_TRADE_LOOKUP = 8;
    public static final int FREQUENCY_TRADE_ORDER = 10;
    public static final int FREQUENCY_TRADE_RESULT = 10;
    public static final int FREQUENCY_TRADE_STATUS = 19;
    public static final int FREQUENCY_TRADE_UPDATE = 2;

    // ----------------------------------------------------------------
    // TABLE INFORMATION
    // ----------------------------------------------------------------

    // The EGen generator defines tables as being one of the following types
    public enum TableType {
        FIXED, SCALING, GROWING,
    }

    // Data Parameters
    public static final int DEFAULT_NUM_CUSTOMERS = 5000; // Default total number of customers (EGen uses 'long' here) 
    public static final int DEFAULT_SCALE_FACTOR = 500; // Using 2880 causes the
                                                        // EGenClientDriver to
                                                        // have problems
    
    /*
     * Miscellaneous loader parameters
     */
    public static final int  DEFAULT_INITIAL_DAYS = 300;
    public static final int  DEFAULT_LOAD_UNIT    = 1000; // unit size in customers
    public static final long IDENT_SHIFT = 4300000000L;  // All ids are shifted by this
    public static final long DEFAULT_START_CUSTOMER_ID = 1;
    
    /*
     * Parameters for scaling tables
     */
    public static final long DEFAULT_COMPANIES_PER_UNIT = 500;
    public static final long DEFAULT_COMPANY_COMPETITORS_PER_UNIT  = 3 * DEFAULT_COMPANIES_PER_UNIT;
    public static final long DEFAULT_SECURITIES_PER_UNIT = 685;

    public static final int  BROKERS_DIV = 100;  // by what number to divide the customer count to get the broker count

    /*
     * Some importand dates for the generator
     */
    public static final int dailyMarketBaseYear    = 2000;
    public static final int dailyMarketBaseMonth   = 0; // January, since months are zero-based in Java
    public static final int dailyMarketBaseDay     = 3; // it should be Monday, since skipping weekends depends on this
    public static final int dailyMarketBaseHour    = 0;
    public static final int dailyMarketBaseMinute  = 0;
    public static final int dailyMarketBaseSecond  = 0;
    public static final int dailyMarketBaseMsec    = 0;
    
    public static final int initialTradePopulationBaseYear      = 2005;
    public static final int initialTradePopulationBaseMonth     = 0; // January, since months are zero-based in Java
    public static final int initialTradePopulationBaseDay       = 3;
    public static final int initialTradePopulationBaseHour      = 9;
    public static final int initialTradePopulationBaseMinute    = 0;
    public static final int initialTradePopulationBaseSecond    = 0;
    public static final int initialTradePopulationBaseFraction  = 0;

    public static final int daysPerWorkWeek = 5;
    public static final int newsItemsPerCompany = 2;
    
    /*
     * Constants for securities
     */
    public static final double minSecPrice = 20.00;
    public static final double maxSecPrice = 30.00;

    //
    // Table Names
    //
    public static final String TABLENAME_ZIP_CODE = "ZIP_CODE";
    public static final String TABLENAME_ADDRESS = "ADDRESS";
    public static final String TABLENAME_STATUS_TYPE = "STATUS_TYPE";
    public static final String TABLENAME_TAXRATE = "TAXRATE";
    public static final String TABLENAME_ACCOUNT_PERMISSION = "ACCOUNT_PERMISSION";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_EXCHANGE = "EXCHANGE";
    public static final String TABLENAME_SECTOR = "SECTOR";
    public static final String TABLENAME_INDUSTRY = "INDUSTRY";
    public static final String TABLENAME_COMPANY = "COMPANY";
    public static final String TABLENAME_COMPANY_COMPETITOR = "COMPANY_COMPETITOR";
    public static final String TABLENAME_SECURITY = "SECURITY";
    public static final String TABLENAME_DAILY_MARKET = "DAILY_MARKET";
    public static final String TABLENAME_FINANCIAL = "FINANCIAL";
    public static final String TABLENAME_LAST_TRADE = "LAST_TRADE";
    public static final String TABLENAME_NEWS_ITEM = "NEWS_ITEM";
    public static final String TABLENAME_NEWS_XREF = "NEWS_XREF";
    public static final String TABLENAME_BROKER = "BROKER";
    public static final String TABLENAME_CUSTOMER_ACCOUNT = "CUSTOMER_ACCOUNT";
    public static final String TABLENAME_CUSTOMER_TAXRATE = "CUSTOMER_TAXRATE";
    public static final String TABLENAME_TRADE_TYPE = "TRADE_TYPE";
    public static final String TABLENAME_TRADE = "TRADE";
    public static final String TABLENAME_SETTLEMENT = "SETTLEMENT";
    public static final String TABLENAME_TRADE_HISTORY = "TRADE_HISTORY";
    public static final String TABLENAME_HOLDING_SUMMARY = "HOLDING_SUMMARY";
    public static final String TABLENAME_HOLDING = "HOLDING";
    public static final String TABLENAME_HOLDING_HISTORY = "HOLDING_HISTORY";
    public static final String TABLENAME_WATCH_LIST = "WATCH_LIST";
    public static final String TABLENAME_WATCH_ITEM = "WATCH_ITEM";
    public static final String TABLENAME_CASH_TRANSACTION = "CASH_TRANSACTION";
    public static final String TABLENAME_CHARGE = "CHARGE";
    public static final String TABLENAME_COMMISSION_RATE = "COMMISSION_RATE";
    public static final String TABLENAME_TRADE_REQUEST = "TRADE_REQUEST";

    //
    // Table Categories
    //

    public static final Set<String> FIXED_TABLES = new HashSet<String>();
    static {
        FIXED_TABLES.add(TPCEConstants.TABLENAME_CHARGE);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_COMMISSION_RATE);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_EXCHANGE);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_INDUSTRY);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_SECTOR);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_STATUS_TYPE);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_TAXRATE);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_TRADE_TYPE);
        FIXED_TABLES.add(TPCEConstants.TABLENAME_ZIP_CODE);
    };

    public static final Set<String> SCALING_TABLES = new HashSet<String>();
    static {
        SCALING_TABLES.add(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_ADDRESS);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_COMPANY_COMPETITOR);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_COMPANY);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_CUSTOMER_TAXRATE);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_CUSTOMER);
//        SCALING_TABLES.add(TPCEConstants.TABLENAME_DAILY_MARKET);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_FINANCIAL);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_LAST_TRADE);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_NEWS_ITEM);
        SCALING_TABLES.add(TPCEConstants.TABLENAME_NEWS_XREF);
//        SCALING_TABLES.add(TPCEConstants.TABLENAME_SECURITY);
//        SCALING_TABLES.add(TPCEConstants.TABLENAME_WATCH_ITEM);
//        SCALING_TABLES.add(TPCEConstants.TABLENAME_WATCH_LIST);
    };

    public static final Set<String> GROWING_TABLES = new HashSet<String>();
    static {
        GROWING_TABLES.add(TPCEConstants.TABLENAME_BROKER);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_CASH_TRANSACTION);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_HOLDING_HISTORY);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_HOLDING_SUMMARY);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_HOLDING);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_SETTLEMENT);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_TRADE_HISTORY);
        GROWING_TABLES.add(TPCEConstants.TABLENAME_TRADE);
    };

    public static final Map<String, TableType> TABLE_TYPES = new HashMap<String, TableType>();
    static {
        for (String table_name : FIXED_TABLES) {
            TABLE_TYPES.put(table_name, TableType.FIXED);
        }
        for (String table_name : SCALING_TABLES) {
            TABLE_TYPES.put(table_name, TableType.SCALING);
        }
        for (String table_name : GROWING_TABLES) {
            TABLE_TYPES.put(table_name, TableType.GROWING);
        }
    };
    
    /*
     * These are tables that are loaded together, by one generator
     */
    public static final Map<String, String> MIXED_TABLES = new HashMap<String, String>();
    static {
        MIXED_TABLES.put(TABLENAME_ACCOUNT_PERMISSION, TABLENAME_CUSTOMER_ACCOUNT);
    }

    public static TableType getTableType(String table_name) {
        return (TPCEConstants.TABLE_TYPES.get(table_name));
    }

}
