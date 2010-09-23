/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Christopher Chan-Nui
 */

#include <iostream>
#include <boost/test/auto_unit_test.hpp>
#include "../inc/DriverParamSettings.h"

using boost::unit_test::test_suite;

// These macros simplify the testing of the CheckValid/CheckCompliant tests in DriverParamSettings.h
// The parameters for each of the macros are
//     cls     - The class being tested (must be in the TPCE namespace)
//     num     - Id which makes the test unique for each class (otherwise we have namespace collisions)
//     expr    - An expression to evaluate before performing the tests.  This is used to set members
//               to particular values in order to force valid or invalid conditions.
//     err_loc - The expected ErrorLoc() return of a failing check exception
//     err_msg - The expected what() return of a failing check exception
//     member  - A particular member of class cls to test
//     dft     - Default value of specified member
//     val     - Value to set member to in order to force a failed compliance test

// Checks that the class is compliant.  If the class isn't compliant it will
// print out an appropriate test line to replace the test with.  This makes it
// much faster to generate the invalid test cases.
#define CHECK_COMPLIANT(cls, num, expr)                                                                                                          \
    BOOST_AUTO_TEST_CASE( test_##cls##_##num ) {                                                                                                 \
        TPCE::cls v;                                                                                                                             \
        expr;                                                                                                                                    \
        try {                                                                                                                                    \
            v.CheckValid();                                                                                                                      \
        } catch (TPCE::CCheckErr& e) {                                                                                                           \
            std::cout << "CHECK_INVALID( " #cls ", " #num ", " #expr ", \"" << e.ErrorLoc() << "\", \"" << e.what() << "\");" << std::endl;      \
            BOOST_FAIL("Unexpected Failure of CheckValid");                                                                                      \
        }                                                                                                                                        \
        try {                                                                                                                                    \
            v.CheckCompliant();                                                                                                                  \
        } catch (TPCE::CCheckErr& e) {                                                                                                           \
            std::cout << "CHECK_NONCOMPLIANT( " #cls ", " #num ", " #expr ", \"" << e.ErrorLoc() << "\", \"" << e.what() << "\");" << std::endl; \
            BOOST_FAIL("Unexpected Failure of CheckValid");                                                                                      \
        }                                                                                                                                        \
        BOOST_CHECK_EQUAL(v.IsValid(), true);                                                                                                    \
        BOOST_CHECK_EQUAL(v.IsCompliant(), true);                                                                                                \
    }

// Checks that the class is valid but not compliant.  This one also produces an
// invalid testcase if the check fails.
#define CHECK_VALID(cls, num, expr) \
    BOOST_AUTO_TEST_CASE( test_##cls##_##num ) {                                                                                            \
        TPCE::cls v;                                                                                                                        \
        expr;                                                                                                                               \
        try {                                                                                                                               \
            v.CheckValid();                                                                                                                 \
        } catch (TPCE::CCheckErr& e) {                                                                                                      \
            std::cout << "CHECK_INVALID( " #cls ", " #num ", " #expr ", \"" << e.ErrorLoc() << "\", \"" << e.what() << "\");" << std::endl; \
            BOOST_FAIL("Unexpected Failure of CheckValid");                                                                                 \
        }                                                                                                                                   \
        BOOST_CHECK_EQUAL(v.IsValid(), true);                                                                                               \
        BOOST_CHECK_EQUAL(v.IsCompliant(), false);                                                                                          \
    }

// Checks to make sure that the class reports that it is invalid.  It also
// verifies the Exception location and error message.
#define CHECK_INVALID(cls, num, expr, err_loc, err_msg)                                                                                         \
    BOOST_AUTO_TEST_CASE( test_##cls##_##num ) {                                                                                                \
        TPCE::cls v;                                                                                                                            \
        expr;                                                                                                                                   \
        try {                                                                                                                                   \
            v.CheckValid();                                                                                                                     \
            BOOST_FAIL("CHECK_INVALID tests should not pass CheckValid()");                                                                     \
        } catch (TPCE::CCheckErr& e) {                                                                                                          \
            BOOST_CHECK_EQUAL(e.ErrorLoc(), err_loc);                                                                                           \
            BOOST_CHECK_EQUAL(e.what(), err_msg);                                                                                               \
            if (strcmp(e.ErrorLoc(), err_loc) != 0 || strcmp(e.what(), err_msg) !=0) {                                                          \
                std::cout << "CHECK_INVALID( " #cls ", " #num ", " #expr ", \"" << e.ErrorLoc() << "\", \"" << e.what() << "\");" << std::endl; \
            }                                                                                                                                   \
        }                                                                                                                                       \
        BOOST_CHECK_EQUAL(v.IsValid(), false);                                                                                                  \
        BOOST_CHECK_EQUAL(v.IsCompliant(), false);                                                                                              \
    }

// Checks to make sure that the class reports that it is valid but not compliant.  It also
// verifies the Exception location and error message.
#define CHECK_NONCOMPLIANT(cls, num, expr, err_loc, err_msg) \
    BOOST_AUTO_TEST_CASE( test_##cls##_##num ) { \
        TPCE::cls v; \
        expr; \
        try { \
            v.CheckCompliant(); \
            BOOST_FAIL("CHECK_NONCOMPLIANT tests should not pass CheckCompliant()"); \
        } catch (TPCE::CCheckErr& e) { \
            BOOST_CHECK_EQUAL(e.ErrorLoc(), err_loc); \
            BOOST_CHECK_EQUAL(e.what(), err_msg); \
            if (strcmp(e.ErrorLoc(), err_loc) != 0 || strcmp(e.what(), err_msg) !=0) { \
                std::cout << "CHECK_NONCOMPLIANT( " #cls ", " #num ", " #expr ", \"" << e.ErrorLoc() << "\", \"" << e.what() << "\");" << std::endl; \
            } \
        } \
        BOOST_CHECK_EQUAL(v.IsValid(), true);\
        BOOST_CHECK_EQUAL(v.IsCompliant(), false);\
    }

// Checks to make sure that a particular member of the class cls can only be
// set to valid percentage values
#define CHECK_PCT(cls, num, member, expr)        \
    BOOST_AUTO_TEST_CASE( test_##cls##_##num ) { \
        TPCE::cls v;                             \
        expr;                                    \
        v.cur.member = 0;                        \
        BOOST_CHECK_EQUAL(v.IsValid(), true);    \
        v.cur.member = 100;                      \
        BOOST_CHECK_EQUAL(v.IsValid(), true);    \
        v.cur.member = -1;                       \
        BOOST_CHECK_EQUAL(v.IsValid(), false);   \
        v.cur.member = 101;                      \
        BOOST_CHECK_EQUAL(v.IsValid(), false);   \
    }

// Checks to make sure that a particular member of the class cls can not be set
// set to values outside of 0-100.  Many percentage values tend to have
// relationships to others so we have to test those manually elsewhere.
#define CHECK_PCT_OUT(cls, num, member)                                      \
    BOOST_AUTO_TEST_CASE( test_##cls##_##num ) {                             \
        TPCE::cls v;                                                         \
        v.cur.member = -1;                                                   \
        try {                                                                \
            v.CheckValid();                                                  \
            BOOST_FAIL("CHECK_CPT_OUT tests should not pass CheckValid()");  \
        } catch (TPCE::CCheckErr& e) {                                       \
            BOOST_CHECK_EQUAL(e.ErrorLoc(), #member);                        \
            BOOST_CHECK_EQUAL(e.what(), "cur." #member "(-1) < 0(0)" );      \
        }                                                                    \
        BOOST_CHECK_EQUAL(v.IsValid(), false);                               \
        v.cur.member = 101;                                                  \
        try {                                                                \
            v.CheckValid();                                                  \
            BOOST_FAIL("CHECK_CPT_OUT tests should not pass CheckValid()");  \
        } catch (TPCE::CCheckErr& e) {                                       \
            BOOST_CHECK_EQUAL(e.ErrorLoc(), #member);                        \
            BOOST_CHECK_EQUAL(e.what(), "cur." #member "(101) > 100(100)" ); \
        }                                                                    \
        BOOST_CHECK_EQUAL(v.IsValid(), false);                               \
    }

// Checks that if a particular default value is changed that the class won't
// report itself as compliant.
#define CHECK_DEFAULT(cls, num, member, dft, val, expr) \
    CHECK_NONCOMPLIANT(cls, num, (v.cur.member = val, (expr)), #member, #member "(" #val ") != " #dft );

CHECK_COMPLIANT(CBrokerVolumeSettings, 1, true);

CHECK_COMPLIANT(  CCustomerPositionSettings, 1, true);
CHECK_INVALID(    CCustomerPositionSettings, 2, (v.cur.by_cust_id = 20), "by_*_id total", "cur.by_cust_id + cur.by_tax_id(70) != 100(100)");
CHECK_VALID(      CCustomerPositionSettings, 3, (v.cur.by_cust_id=0, v.cur.by_tax_id=100));
CHECK_VALID(      CCustomerPositionSettings, 4, (v.cur.by_cust_id=100, v.cur.by_tax_id=0));
CHECK_PCT_OUT(    CCustomerPositionSettings, 5, by_cust_id );
CHECK_PCT_OUT(    CCustomerPositionSettings, 6, by_tax_id );
CHECK_PCT(        CCustomerPositionSettings, 7, get_history, true );
CHECK_DEFAULT(    CCustomerPositionSettings, 8, by_cust_id,   50, 40, (v.cur.by_tax_id=60));
//CHECK_DEFAULT(    CCustomerPositionSettings, 9, by_tax_id,   50, 40, (v.cur.by_cust_id=60)); // Impossible to test
CHECK_DEFAULT(    CCustomerPositionSettings, 10, get_history, 50, 40, true );

CHECK_COMPLIANT(  CMarketWatchSettings, 1, true);
CHECK_VALID(      CMarketWatchSettings, 2, (v.cur.by_acct_id=45, v.cur.by_watch_list=55, v.cur.by_industry=0));
CHECK_PCT_OUT(    CMarketWatchSettings, 3, by_acct_id);
CHECK_PCT_OUT(    CMarketWatchSettings, 4, by_industry);
CHECK_PCT_OUT(    CMarketWatchSettings, 5, by_watch_list);
CHECK_INVALID(    CMarketWatchSettings, 6, (v.cur.by_acct_id = 40), "by_* total", "cur.by_acct_id + cur.by_industry + cur.by_watch_list(105) != 100(100)");
CHECK_DEFAULT(    CMarketWatchSettings, 7, by_acct_id, 35, 40, (v.cur.by_watch_list=55) );
CHECK_DEFAULT(    CMarketWatchSettings, 8, by_industry, 5, 10, (v.cur.by_watch_list=55) );
//CHECK_DEFAULT(    CMarketWatchSettings, 9, by_watch_list, 60, 65, (v.cur.by_acct_id=30) ); // Impossible to test

CHECK_COMPLIANT(  CSecurityDetailSettings, 1, true);
CHECK_PCT(        CSecurityDetailSettings, 2, LOBAccessPercentage, true);
CHECK_DEFAULT(    CSecurityDetailSettings, 3, LOBAccessPercentage, 1, 10, true);

CHECK_COMPLIANT(  CTradeLookupSettings, 1, true);
CHECK_VALID(      CTradeLookupSettings, 2, (v.cur.do_frame1=10, v.cur.do_frame2=10, v.cur.do_frame3=10, v.cur.do_frame4=70));
CHECK_PCT_OUT(    CTradeLookupSettings, 3, do_frame1);
CHECK_PCT_OUT(    CTradeLookupSettings, 4, do_frame2);
CHECK_PCT_OUT(    CTradeLookupSettings, 5, do_frame3);
CHECK_PCT_OUT(    CTradeLookupSettings, 6, do_frame4);
CHECK_INVALID(    CTradeLookupSettings, 7, (v.cur.do_frame1=10),      "do_frame* total", "cur.do_frame1 + cur.do_frame2 + cur.do_frame3 + cur.do_frame4(80) != 100(100)");
CHECK_INVALID(    CTradeLookupSettings, 8, (v.cur.MaxRowsFrame1=21),  "MaxRowsFrame1", "cur.MaxRowsFrame1(21) > TradeLookupFrame1MaxRows(20)");
CHECK_INVALID(    CTradeLookupSettings, 9, (v.cur.MaxRowsFrame2=21),  "MaxRowsFrame2", "cur.MaxRowsFrame2(21) > TradeLookupFrame2MaxRows(20)");
CHECK_INVALID(    CTradeLookupSettings, 10, (v.cur.MaxRowsFrame3=21), "MaxRowsFrame3", "cur.MaxRowsFrame3(21) > TradeLookupFrame3MaxRows(20)");
CHECK_INVALID(    CTradeLookupSettings, 11, (v.cur.MaxRowsFrame4=21), "MaxRowsFrame4", "cur.MaxRowsFrame4(21) > TradeLookupFrame4MaxRows(20)");
CHECK_DEFAULT(    CTradeLookupSettings, 12, do_frame1, 30, 10, (v.cur.do_frame4 = 30));
CHECK_DEFAULT(    CTradeLookupSettings, 13, do_frame2, 30, 10, (v.cur.do_frame4 = 30));
CHECK_DEFAULT(    CTradeLookupSettings, 14, do_frame3, 30, 10, (v.cur.do_frame4 = 30));
//CHECK_DEFAULT(    CTradeLookupSettings, 15, do_frame4, 10, 30, (v.cur.do_frame1 = 10));
CHECK_DEFAULT(    CTradeLookupSettings, 16, MaxRowsFrame1, 20, 10, true);
CHECK_DEFAULT(    CTradeLookupSettings, 17, MaxRowsFrame2, 20, 10, true);
CHECK_DEFAULT(    CTradeLookupSettings, 18, MaxRowsFrame3, 20, 10, true);
CHECK_DEFAULT(    CTradeLookupSettings, 19, MaxRowsFrame4, 20, 10, true);
CHECK_DEFAULT(    CTradeLookupSettings, 20, BackOffFromEndTimeFrame2, 115200, 10, true);
CHECK_DEFAULT(    CTradeLookupSettings, 21, BackOffFromEndTimeFrame3, 12000, 10, true);
CHECK_DEFAULT(    CTradeLookupSettings, 22, BackOffFromEndTimeFrame4, 30000, 10, true);

CHECK_COMPLIANT(    CTradeOrderSettings, 1, true);
CHECK_VALID(        CTradeOrderSettings, 2, (v.cur.market=40, v.cur.limit=60));
CHECK_VALID(        CTradeOrderSettings, 3, (v.cur.security_by_name=60, v.cur.security_by_symbol=40));
CHECK_VALID(        CTradeOrderSettings, 4, (v.cur.buy_orders=60, v.cur.sell_orders=40));
CHECK_INVALID(      CTradeOrderSettings, 5, (v.cur.market=40),           "market or limit total", "cur.market + cur.limit(80) != 100(100)");
CHECK_INVALID(      CTradeOrderSettings, 6, (v.cur.security_by_name=60), "security_by_* total", "cur.security_by_name + cur.security_by_symbol(120) != 100(100)");
CHECK_INVALID(      CTradeOrderSettings, 7, (v.cur.buy_orders=60),       "*_orders total", "cur.buy_orders + cur.sell_orders(110) != 100(100)");
CHECK_PCT_OUT(      CTradeOrderSettings, 8, market);
CHECK_PCT_OUT(      CTradeOrderSettings, 9, limit);
CHECK_PCT(          CTradeOrderSettings, 10, stop_loss, true);
CHECK_PCT_OUT(      CTradeOrderSettings, 11, security_by_name);
CHECK_PCT_OUT(      CTradeOrderSettings, 12, security_by_symbol);
CHECK_PCT_OUT(      CTradeOrderSettings, 13, buy_orders);
CHECK_PCT_OUT(      CTradeOrderSettings, 14, sell_orders);
CHECK_PCT(          CTradeOrderSettings, 15, lifo, true);
CHECK_PCT(          CTradeOrderSettings, 16, exec_is_owner, true);
CHECK_PCT(          CTradeOrderSettings, 17, rollback, true);
CHECK_PCT(          CTradeOrderSettings, 18, type_is_margin, true);
CHECK_NONCOMPLIANT( CTradeOrderSettings, 19, ( v.cur.exec_is_owner = 10), "exec_is_owner", "cur.exec_is_owner(10) < 60(60)");
CHECK_DEFAULT(      CTradeOrderSettings, 20, market,             60, 10, (v.cur.limit=90) );
//CHECK_DEFAULT(      CTradeOrderSettings, 21, limit,              60, 10, (v.cur.market=90) );
CHECK_DEFAULT(      CTradeOrderSettings, 22, stop_loss,          50, 10, true );
CHECK_DEFAULT(      CTradeOrderSettings, 23, security_by_name,   40, 50, (v.cur.security_by_symbol=50) );
//CHECK_DEFAULT(      CTradeOrderSettings, 24, security_by_symbol, 40, 50, (v.cur.security_by_name=50) );
CHECK_DEFAULT(      CTradeOrderSettings, 25, buy_orders,         50, 40, (v.cur.sell_orders=60) );
//CHECK_DEFAULT(      CTradeOrderSettings, 26, sell_orders,        50, 40, (v.cur.buy_orders=60) );
CHECK_DEFAULT(      CTradeOrderSettings, 27, lifo,               35, 10, true );
CHECK_DEFAULT(      CTradeOrderSettings, 28, exec_is_owner,      90, 60, true );
CHECK_DEFAULT(      CTradeOrderSettings, 29, rollback,           1,  10, true );
CHECK_DEFAULT(      CTradeOrderSettings, 30, type_is_margin,     8,  10, true );

CHECK_COMPLIANT(  CTradeUpdateSettings, 1, true);
CHECK_VALID(      CTradeUpdateSettings, 2, (v.cur.do_frame1=10, v.cur.do_frame2=10, v.cur.do_frame3=80));
CHECK_PCT_OUT(    CTradeUpdateSettings, 3, do_frame1);
CHECK_PCT_OUT(    CTradeUpdateSettings, 4, do_frame2);
CHECK_PCT_OUT(    CTradeUpdateSettings, 5, do_frame3);
CHECK_INVALID(    CTradeUpdateSettings, 6, (v.cur.do_frame1=10),     "do_frame* total", "cur.do_frame1 + cur.do_frame2 + cur.do_frame3(77) != 100(100)");
CHECK_INVALID(    CTradeUpdateSettings, 7, (v.cur.MaxRowsFrame1=21), "MaxRowsFrame1",   "cur.MaxRowsFrame1(21) > TradeUpdateFrame1MaxRows(20)");
CHECK_INVALID(    CTradeUpdateSettings, 8, (v.cur.MaxRowsFrame2=21), "MaxRowsFrame2",   "cur.MaxRowsFrame2(21) > TradeUpdateFrame2MaxRows(20)");
CHECK_INVALID(    CTradeUpdateSettings, 9, (v.cur.MaxRowsFrame3=21), "MaxRowsFrame3",   "cur.MaxRowsFrame3(21) > TradeUpdateFrame3MaxRows(20)");
CHECK_DEFAULT(    CTradeUpdateSettings, 10, do_frame1, 33, 10, (v.cur.do_frame3 = 57));
CHECK_DEFAULT(    CTradeUpdateSettings, 11, do_frame2, 33, 10, (v.cur.do_frame3 = 57));
//CHECK_DEFAULT(    CTradeUpdateSettings, 12, do_frame3, 34, 57, (v.cur.do_frame1 = 0));
CHECK_DEFAULT(    CTradeUpdateSettings, 13, MaxRowsFrame1, 20, 10, true);
CHECK_DEFAULT(    CTradeUpdateSettings, 14, MaxRowsFrame2, 20, 10, true);
CHECK_DEFAULT(    CTradeUpdateSettings, 15, MaxRowsFrame3, 20, 10, true);
CHECK_DEFAULT(    CTradeUpdateSettings, 16, BackOffFromEndTimeFrame2, 115200, 10, true);
CHECK_DEFAULT(    CTradeUpdateSettings, 17, BackOffFromEndTimeFrame3, 12000, 10, true);

CHECK_COMPLIANT(  CTxnMixGeneratorSettings, 1, true);
CHECK_INVALID(    CTxnMixGeneratorSettings, 2, (v.cur.BrokerVolumeMixLevel     = -1), "BrokerVolumeMixLevel",     "cur.BrokerVolumeMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 3, (v.cur.CustomerPositionMixLevel = -1), "CustomerPositionMixLevel", "cur.CustomerPositionMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 4, (v.cur.MarketWatchMixLevel      = -1), "MarketWatchMixLevel",      "cur.MarketWatchMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 5, (v.cur.SecurityDetailMixLevel   = -1), "SecurityDetailMixLevel",   "cur.SecurityDetailMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 6, (v.cur.TradeLookupMixLevel      = -1), "TradeLookupMixLevel",      "cur.TradeLookupMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 7, (v.cur.TradeOrderMixLevel       = -1), "TradeOrderMixLevel",       "cur.TradeOrderMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 8, (v.cur.TradeStatusMixLevel      = -1), "TradeStatusMixLevel",      "cur.TradeStatusMixLevel(-1) < 0(0)");
CHECK_INVALID(    CTxnMixGeneratorSettings, 9, (v.cur.TradeUpdateMixLevel      = -1), "TradeUpdateMixLevel",      "cur.TradeUpdateMixLevel(-1) < 0(0)");
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 10, BrokerVolumeMixLevel,       49, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 11, CustomerPositionMixLevel,  130, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 12, MarketWatchMixLevel,       180, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 13, SecurityDetailMixLevel,    140, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 14, TradeLookupMixLevel,        80, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 15, TradeOrderMixLevel,        101, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 16, TradeStatusMixLevel,       190, 10, true);
CHECK_DEFAULT(    CTxnMixGeneratorSettings, 17, TradeUpdateMixLevel,        20, 10, true);

CHECK_COMPLIANT(    CLoaderSettings, 1, true);
CHECK_INVALID(      CLoaderSettings, 2, (v.cur.iConfiguredCustomerCount = 900, v.cur.iActiveCustomerCount = 900), "iConfiguredCustomerCount", "cur.iConfiguredCustomerCount(900) < 1000(1000)");
CHECK_INVALID(      CLoaderSettings, 3, (v.cur.iActiveCustomerCount = 900),      "iActiveCustomerCount", "cur.iActiveCustomerCount(900) < 1000(1000)");
CHECK_INVALID(      CLoaderSettings, 4, (v.cur.iActiveCustomerCount = 8000),     "iActiveCustomerCount", "cur.iActiveCustomerCount(8000) > cur.iConfiguredCustomerCount(5000)");
CHECK_INVALID(      CLoaderSettings, 5, (v.cur.iConfiguredCustomerCount = 5001), "iConfiguredCustomerCount", "cur.iConfiguredCustomerCount % 1000(1) != 0(0)");
CHECK_INVALID(      CLoaderSettings, 6, (v.cur.iStartingCustomer = 0),           "iStartingCustomer", "cur.iStartingCustomer(0) < 1(1)");
CHECK_INVALID(      CLoaderSettings, 7, (v.cur.iStartingCustomer = 1002, v.cur.iCustomerCount=1000), "iStartingCustomer", "cur.iStartingCustomer % 1000(2) != 1(1)");
CHECK_INVALID(      CLoaderSettings, 8, (v.cur.iCustomerCount = 1001),    "iCustomerCount", "cur.iCustomerCount % 1000(1) != 0(0)");
CHECK_INVALID(      CLoaderSettings, 9, (v.cur.iStartingCustomer = 1001), "iCustomerCount", "cur.iCustomerCount + cur.iStartingCustomer - 1(6000) > cur.iConfiguredCustomerCount(5000)");
CHECK_NONCOMPLIANT( CLoaderSettings, 10, (v.cur.iConfiguredCustomerCount = 4000, v.cur.iActiveCustomerCount = 4000, v.cur.iCustomerCount= 4000), "iConfiguredCustomerCount", "cur.iConfiguredCustomerCount(4000) < 5000(5000)");
CHECK_NONCOMPLIANT( CLoaderSettings, 11, (v.cur.iActiveCustomerCount = 4000),     "iActiveCustomerCount", "cur.iActiveCustomerCount(4000) < 5000(5000)");
CHECK_NONCOMPLIANT( CLoaderSettings, 12, (v.cur.iConfiguredCustomerCount = 6000), "iActiveCustomerCount", "cur.iActiveCustomerCount(5000) != cur.iConfiguredCustomerCount(6000)");
CHECK_DEFAULT(      CLoaderSettings, 13, iScaleFactor,         500, 400, true);
CHECK_DEFAULT(      CLoaderSettings, 14, iDaysOfInitialTrades, 300, 200, true );

CHECK_COMPLIANT(    CDriverGlobalSettings, 1, true);
CHECK_INVALID(      CDriverGlobalSettings, 2, (v.cur.iConfiguredCustomerCount = 900, v.cur.iActiveCustomerCount = 900), "iConfiguredCustomerCount", "cur.iConfiguredCustomerCount(900) < 1000(1000)");
CHECK_INVALID(      CDriverGlobalSettings, 3, (v.cur.iActiveCustomerCount = 900),      "iActiveCustomerCount",     "cur.iActiveCustomerCount(900) < 1000(1000)");
CHECK_INVALID(      CDriverGlobalSettings, 4, (v.cur.iActiveCustomerCount = 8000),     "iActiveCustomerCount",     "cur.iActiveCustomerCount(8000) > cur.iConfiguredCustomerCount(5000)");
CHECK_INVALID(      CDriverGlobalSettings, 5, (v.cur.iConfiguredCustomerCount = 5001), "iConfiguredCustomerCount", "cur.iConfiguredCustomerCount % 1000(1) != 0(0)");
CHECK_NONCOMPLIANT( CDriverGlobalSettings, 6, (v.cur.iConfiguredCustomerCount = 4000, v.cur.iActiveCustomerCount = 4000), "iConfiguredCustomerCount", "cur.iConfiguredCustomerCount(4000) < 5000(5000)");
CHECK_NONCOMPLIANT( CDriverGlobalSettings, 7, (v.cur.iActiveCustomerCount = 4000),     "iActiveCustomerCount", "cur.iActiveCustomerCount(4000) < 5000(5000)");
CHECK_NONCOMPLIANT( CDriverGlobalSettings, 8, (v.cur.iConfiguredCustomerCount = 6000), "iActiveCustomerCount", "cur.iActiveCustomerCount(5000) != cur.iConfiguredCustomerCount(6000)");
CHECK_DEFAULT(      CDriverGlobalSettings, 9,  iScaleFactor,         500, 400, true);
CHECK_DEFAULT(      CDriverGlobalSettings, 10, iDaysOfInitialTrades, 300, 200, true );

CHECK_COMPLIANT(    CDriverCESettings,  1, true);

CHECK_COMPLIANT( CDriverCEPartitionSettings, 1, true);
CHECK_COMPLIANT( CDriverCEPartitionSettings, 2, (v.cur.iMyStartingCustomerId = 0, v.cur.iMyCustomerCount = 0, v.cur.iPartitionPercent = 0));
CHECK_COMPLIANT( CDriverCEPartitionSettings, 3, (v.cur.iMyStartingCustomerId = 1, v.cur.iMyCustomerCount = 5000, v.cur.iPartitionPercent = 50));
CHECK_PCT(       CDriverCEPartitionSettings, 4, iPartitionPercent, (v.cur.iMyStartingCustomerId = 1, v.cur.iMyCustomerCount = 5000));
CHECK_INVALID(   CDriverCEPartitionSettings, 5, (v.cur.iMyStartingCustomerId = 2, v.cur.iMyCustomerCount = 5000, v.cur.iPartitionPercent = 50), "iMyStartingCustomerId", "cur.iMyStartingCustomerId % 1000(2) != 1(1)");
CHECK_INVALID(   CDriverCEPartitionSettings, 6, (v.cur.iMyStartingCustomerId = 1, v.cur.iMyCustomerCount = 900, v.cur.iPartitionPercent = 50), "iMyCustomerCount", "cur.iMyCustomerCount(900) < 1000(1000)");
CHECK_INVALID(   CDriverCEPartitionSettings, 7, (v.cur.iMyStartingCustomerId = 1, v.cur.iMyCustomerCount = 5001, v.cur.iPartitionPercent = 50), "iMyCustomerCount", "cur.iMyCustomerCount % 1000(1) != 0(0)");
CHECK_DEFAULT(   CDriverCEPartitionSettings, 8, iPartitionPercent, 50, 40, (v.cur.iMyStartingCustomerId = 1, v.cur.iMyCustomerCount = 5000));

CHECK_COMPLIANT(    CDriverMEESettings, 1, true);

CHECK_COMPLIANT(    CDriverDMSettings,  1, true);

// Define TDriverCETxnSettings as a type so that our macros will work on it.
namespace TPCE
{
    typedef struct TDriverCETxnSettings TDriverCETxnSettings;
}

CHECK_COMPLIANT(    TDriverCETxnSettings, 1,  true);
CHECK_INVALID(      TDriverCETxnSettings, 2,  (v.CP_settings.cur.by_cust_id=-1),          "by_cust_id",                     "cur.by_cust_id(-1) < 0(0)");
CHECK_INVALID(      TDriverCETxnSettings, 3,  (v.MW_settings.cur.by_watch_list=-1),       "by_watch_list",                  "cur.by_watch_list(-1) < 0(0)");
CHECK_INVALID(      TDriverCETxnSettings, 4,  (v.SD_settings.cur.LOBAccessPercentage=-1), "LOBAccessPercentage",            "cur.LOBAccessPercentage(-1) < 0(0)");
CHECK_INVALID(      TDriverCETxnSettings, 5,  (v.TL_settings.cur.do_frame1=-1),           "do_frame1",                      "cur.do_frame1(-1) < 0(0)");
CHECK_INVALID(      TDriverCETxnSettings, 6,  (v.TO_settings.cur.rollback=-1),            "rollback",                       "cur.rollback(-1) < 0(0)");
CHECK_INVALID(      TDriverCETxnSettings, 7,  (v.TU_settings.cur.do_frame1=-1),           "do_frame1",                      "cur.do_frame1(-1) < 0(0)");
CHECK_NONCOMPLIANT( TDriverCETxnSettings, 8,  (v.CP_settings.cur.get_history=10),         "get_history",                    "get_history(10) != 50");
CHECK_NONCOMPLIANT( TDriverCETxnSettings, 9,  (v.MW_settings.cur.by_watch_list=0, v.MW_settings.cur.by_acct_id=95), "by_acct_id", "by_acct_id(95) != 35");
CHECK_NONCOMPLIANT( TDriverCETxnSettings, 10, (v.SD_settings.cur.LOBAccessPercentage=10), "LOBAccessPercentage",            "LOBAccessPercentage(10) != 1");
CHECK_NONCOMPLIANT( TDriverCETxnSettings, 11, (v.TL_settings.cur.MaxRowsFrame1=10),       "MaxRowsFrame1",                  "MaxRowsFrame1(10) != 20");
CHECK_NONCOMPLIANT( TDriverCETxnSettings, 12, (v.TO_settings.cur.rollback=10),            "rollback",                       "rollback(10) != 1");
CHECK_NONCOMPLIANT( TDriverCETxnSettings, 13, (v.TU_settings.cur.MaxRowsFrame1=10),       "MaxRowsFrame1",                  "MaxRowsFrame1(10) != 20");

