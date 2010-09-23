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
 * - Sergey Vasilevskiy
 */

/*
*   Contains structure definitions for all transactions.
*/
#ifndef TXN_HARNESS_STRUCTS_H
#define TXN_HARNESS_STRUCTS_H

#include "EGenStandardTypes.h"
#include "DateTime.h"
#include "MiscConsts.h"
#include "TableConsts.h"
#include "MEETradeRequestActions.h"

namespace TPCE
{

//declare the < operator for timestamps
bool operator< (const TIMESTAMP_STRUCT& ts1, const TIMESTAMP_STRUCT& ts2);

const INT32 iFinYears = 5;
const INT32 iFinQtrPerYear = 4;
const INT32 iMaxDailyHistory = 10;
const INT32 iMaxNews = 10;

const INT32 min_broker_list_len = 20;   // for Broker-Volume
const INT32 max_broker_list_len = 40;   // for Broker-Volume
const INT32 max_acct_len = 20;      // for Customer-Position
const INT32 max_hist_len = 30;      // for Customer-Position
const INT32 max_feed_len = 20;      // for Market-Feed
const INT32 max_send_len = 40;      // for Market-Feed
const INT32 max_day_len = 20;       // for Security-Detail
const INT32 max_fin_len = 20;       // for Security-Detail
const INT32 max_news_len = 2;       // for Security-Detail
const INT32 max_comp_len = 3;       // for Security-Detail
const INT32 max_trade_status_len = 50;  // for Trade-Status

const INT32 max_table_name = 30;    // for Data Maintenance

/*
*   Broker-Volume
*/
typedef struct TBrokerVolumeTxnInput
{
    // Transaction level inputs
    char            broker_list[max_broker_list_len][cB_NAME_len+1];
    char            sector_name[cSC_NAME_len+1];
}   *PBrokerVolumeTxnInput,
     TBrokerVolumeFrame1Input,  // Single-Frame transaction
    *PBrokerVolumeFrame1Input;  // Single-Frame transaction

typedef struct TBrokerVolumeTxnOutput
{
    // Transaction level outputs
    double          volume[max_broker_list_len];
    INT32           list_len;
    INT32           status;
}   *PBrokerVolumeTxnOutput;

typedef struct TBrokerVolumeFrame1Output
{
    // Frame level outputs
    double          volume[max_broker_list_len];
    INT32           list_len;
    INT32           status;
    char            broker_name[max_broker_list_len][cB_NAME_len+1];
}   *PBrokerVolumeFrame1Output;
/*
*   Customer-Position
*/
typedef struct TCustomerPositionTxnInput
{
    TIdent      acct_id_idx;
    TIdent      cust_id;
    bool        get_history;
    char        tax_id[cTAX_ID_len+1];
} *PCustomerPositionTxnInput;

typedef struct TCustomerPositionTxnOutput
{
    double              asset_total[max_acct_len];
    double              cash_bal[max_acct_len];
    TIdent              acct_id[max_acct_len];
    TTrade              trade_id[max_hist_len];
    TIdent              c_ad_id;
    INT32               qty[max_hist_len];
    INT32               acct_len;
    INT32               hist_len;
    INT32               status;
    TIMESTAMP_STRUCT    hist_dts[max_hist_len];
    TIMESTAMP_STRUCT    c_dob;
    char                symbol[max_hist_len][cSYMBOL_len+1];
    char                trade_status[max_hist_len][cST_NAME_len+1];
    char                c_area_1[cAREA_len+1];
    char                c_area_2[cAREA_len+1];
    char                c_area_3[cAREA_len+1];
    char                c_ctry_1[cCTRY_len+1];
    char                c_ctry_2[cCTRY_len+1];
    char                c_ctry_3[cCTRY_len+1];
    char                c_email_1[cEMAIL_len+1];
    char                c_email_2[cEMAIL_len+1];
    char                c_ext_1[cEXT_len+1];
    char                c_ext_2[cEXT_len+1];
    char                c_ext_3[cEXT_len+1];
    char                c_f_name[cF_NAME_len+1];
    char                c_gndr[cGNDR_len+1];
    char                c_l_name[cL_NAME_len+1];
    char                c_local_1[cLOCAL_len+1];
    char                c_local_2[cLOCAL_len+1];
    char                c_local_3[cLOCAL_len+1];
    char                c_m_name[cM_NAME_len+1];
    char                c_st_id[cST_ID_len+1];
    char                c_tier;
} *PCustomerPositionTxnOutput;

typedef struct TCustomerPositionFrame1Input
{
    TIdent              cust_id;
    char                tax_id[cTAX_ID_len+1];
} *PCustomerPositionFrame1Input;

typedef struct TCustomerPositionFrame1Output
{
    double              asset_total[max_acct_len];
    double              cash_bal[max_acct_len];
    TIdent              acct_id[max_acct_len];
    TIdent              c_ad_id;
    TIdent              cust_id;
    INT32               acct_len;
    INT32               status;
    TIMESTAMP_STRUCT    c_dob;
    char                c_area_1[cAREA_len+1];
    char                c_area_2[cAREA_len+1];
    char                c_area_3[cAREA_len+1];
    char                c_ctry_1[cCTRY_len+1];
    char                c_ctry_2[cCTRY_len+1];
    char                c_ctry_3[cCTRY_len+1];
    char                c_email_1[cEMAIL_len+1];
    char                c_email_2[cEMAIL_len+1];
    char                c_ext_1[cEXT_len+1];
    char                c_ext_2[cEXT_len+1];
    char                c_ext_3[cEXT_len+1];
    char                c_f_name[cF_NAME_len+1];
    char                c_gndr[cGNDR_len+1];
    char                c_l_name[cL_NAME_len+1];
    char                c_local_1[cLOCAL_len+1];
    char                c_local_2[cLOCAL_len+1];
    char                c_local_3[cLOCAL_len+1];
    char                c_m_name[cM_NAME_len+1];
    char                c_st_id[cST_ID_len+1];
    char                c_tier;
} *PCustomerPositionFrame1Output;

typedef struct TCustomerPositionFrame2Input
{
    TIdent              acct_id;
} *PCustomerPositionFrame2Input;

typedef struct TCustomerPositionFrame2Output
{
    TTrade              trade_id[max_hist_len];
    INT32               qty[max_hist_len];
    INT32               hist_len;
    INT32               status;
    TIMESTAMP_STRUCT    hist_dts[max_hist_len];
    char                symbol[max_hist_len][cSYMBOL_len+1];
    char                trade_status[max_hist_len][cST_NAME_len+1];
} *PCustomerPositionFrame2Output;

typedef struct TCustomerPositionFrame3Output
{
    INT32           status;
} *PCustomerPositionFrame3Output;


/*
*   Data-Maintenance
*/
typedef struct TDataMaintenanceTxnInput
{
    TIdent  acct_id;
    TIdent  c_id;
    TIdent  co_id;
    INT32   day_of_month;
    INT32   vol_incr;
    char    symbol[cSYMBOL_len+1];
    char    table_name[max_table_name+1];
    char    tx_id[cTAX_ID_len+1];
}   *PDataMaintenanceTxnInput,
     TDataMaintenanceFrame1Input,   // Single-Frame transaction
    *PDataMaintenanceFrame1Input;   // Single-Frame transaction

typedef struct TDataMaintenanceTxnOutput
{
    INT32   status;
}   *PDataMaintenanceTxnOutput,
     TDataMaintenanceFrame1Output,  // Single-Frame transaction
    *PDataMaintenanceFrame1Output;  // Single-Frame transaction


/*
*   Market-Feed
*/
// MEE populates this structure
typedef struct TStatusAndTradeType
{
    char    status_submitted[cST_ID_len+1];
    char    type_limit_buy[cTT_ID_len+1];
    char    type_limit_sell[cTT_ID_len+1];
    char    type_stop_loss[cTT_ID_len+1];
} *PTStatusAndTradeType;

//Incomming order from SendToMarket interface.
typedef struct TTradeRequest
{
    double              price_quote;
    TTrade              trade_id;
    INT32               trade_qty;
    eMEETradeRequestAction      eAction;
    char                symbol[cSYMBOL_len+1];
    char                trade_type_id[cTT_ID_len+1];
} *PTradeRequest;

//A single entry on the ticker tape feed.
typedef struct TTickerEntry
{
    double              price_quote;
    INT32               trade_qty;
    char                symbol[cSYMBOL_len+1];
} *PTickerEntry;

//Market-Feed data sent from MEE to sponsor provided SUT interface
typedef struct TMarketFeedTxnInput
{
    TStatusAndTradeType StatusAndTradeType;
    char                zz_padding[4];
    TTickerEntry        Entries[max_feed_len];
}   *PMarketFeedTxnInput,
     TMarketFeedFrame1Input,    // Single-Frame transaction
    *PMarketFeedFrame1Input;    // Single-Frame transaction

typedef struct TMarketFeedTxnOutput
{
    INT32           send_len;
    INT32           status;
}   *PMarketFeedTxnOutput;

typedef struct TMarketFeedFrame1Output
{
    INT32           send_len;
    INT32           status;
}   *PMarketFeedFrame1Output;


/*
*   Market-Watch
*/
typedef struct TMarketWatchTxnInput
{
    TIdent              acct_id;
    TIdent              c_id;
    TIdent              ending_co_id;
    TIdent              starting_co_id;
    TIMESTAMP_STRUCT    start_day;
    char    industry_name[cIN_NAME_len+1];
}   *PMarketWatchTxnInput,
     TMarketWatchFrame1Input,   // Single-Frame transaction
    *PMarketWatchFrame1Input;   // Single-Frame transaction

typedef struct TMarketWatchTxnOutput
{
    double  pct_change;
    INT32   status;
}   *PMarketWatchTxnOutput,
     TMarketWatchFrame1Output,  // Single-Frame transaction
    *PMarketWatchFrame1Output;  // Single-Frame transaction


/*
*   Security-Detail
*/
typedef struct TFinInfo
{
    double              assets;
    double              basic_eps;
    double              dilut_eps;
    double              invent;
    double              liab;
    double              margin;
    double              net_earn;
    double              out_basic;
    double              out_dilut;
    double              rev;
    INT32               qtr;
    INT32               year;
    TIMESTAMP_STRUCT    start_date;
    DB_INDICATOR        assets_ind;
    DB_INDICATOR        basic_eps_ind;
    DB_INDICATOR        dilut_eps_ind;
    DB_INDICATOR        invent_ind;
    DB_INDICATOR        liab_ind;
    DB_INDICATOR        margin_ind;
    DB_INDICATOR        net_earn_ind;
    DB_INDICATOR        out_basic_ind;
    DB_INDICATOR        out_dilut_ind;
    DB_INDICATOR        qtr_ind;
    DB_INDICATOR        rev_ind;
    DB_INDICATOR        start_date_ind;
    DB_INDICATOR        year_ind;
} *PFinInfo;

typedef struct TDailyHistory
{
    double              close;
    double              high;
    double              low;
    INT64               vol;
    TIMESTAMP_STRUCT    date;
    DB_INDICATOR        close_ind;
    DB_INDICATOR        date_ind;
    DB_INDICATOR        high_ind;
    DB_INDICATOR        low_ind;
    DB_INDICATOR        vol_ind;
} *PDailyHistory;

typedef struct TLastPrice
{
    double          open_price;
    double          price;
    INT64           vol_today;
    DB_INDICATOR    open_price_ind;
    DB_INDICATOR    price_ind;
    DB_INDICATOR    vol_today_ind;
} *PLastPrice;

typedef struct TNews
{
    TIMESTAMP_STRUCT    dts;
    char                auth[cNI_AUTHOR_len+1];
    char                headline[cNI_HEADLINE_len+1];
    char                item[cNI_ITEM_len+1];
    char                src[cNI_SOURCE_len+1];
    char                summary[cNI_SUMMARY_len+1];
    DB_INDICATOR        auth_ind;
} *PNews;

typedef struct TSecurityDetailTxnInput
{
    INT32               max_rows_to_return;
    bool                access_lob_flag;
    TIMESTAMP_STRUCT    start_day;
    char                symbol[cSYMBOL_len+1];
}   *PSecurityDetailTxnInput,
     TSecurityDetailFrame1Input,    // Single-Frame transaction
    *PSecurityDetailFrame1Input;    // Single-Frame transaction

typedef struct TSecurityDetailTxnOutput
{
    INT64               last_vol;
    INT32               news_len;
    INT32               status;
}   *PSecurityDetailTxnOutput;

typedef struct TSecurityDetailFrame1Output
{
    double              divid;
    double              last_open;
    double              last_price;
    double              pe_ratio;
    double              s52_wk_high;
    double              s52_wk_low;
    double              yield;
    INT64               last_vol;
    INT64               num_out;
    INT32               day_len;
    INT32               ex_close;
    INT32               ex_num_symb;
    INT32               ex_open;
    INT32               fin_len;
    INT32               news_len;
    INT32               status;
    TIMESTAMP_STRUCT    ex_date;
    TIMESTAMP_STRUCT    open_date;
    TIMESTAMP_STRUCT    s52_wk_high_date;
    TIMESTAMP_STRUCT    s52_wk_low_date;
    TIMESTAMP_STRUCT    start_date;
    TDailyHistory       day[max_day_len];
    TFinInfo            fin[max_fin_len];
    TNews               news[max_news_len];
    char                cp_co_name[max_comp_len][cCO_NAME_len+1];
    char                cp_in_name[max_comp_len][cIN_NAME_len+1];
    char                ceo_name[cCEO_NAME_len+1];
    char                co_ad_cty[cAD_CTRY_len+1];
    char                co_ad_div[cAD_DIV_len+1];
    char                co_ad_line1[cAD_LINE_len+1];
    char                co_ad_line2[cAD_LINE_len+1];
    char                co_ad_town[cAD_TOWN_len+1];
    char                co_ad_zip[cAD_ZIP_len+1];
    char                co_desc[cCO_DESC_len+1];
    char                co_name[cCO_NAME_len+1];
    char                co_st_id[cST_ID_len+1];
    char                ex_ad_cty[cAD_CTRY_len+1];
    char                ex_ad_div[cAD_DIV_len+1];
    char                ex_ad_line1[cAD_LINE_len+1];
    char                ex_ad_line2[cAD_LINE_len+1];
    char                ex_ad_town[cAD_TOWN_len+1];
    char                ex_ad_zip[cAD_ZIP_len+1];
    char                ex_desc[cEX_DESC_len+1];
    char                ex_name[cEX_NAME_len+1];
    char                s_name[cS_NAME_len+1];
    char                sp_rate[cSP_RATE_len+1];
}   *PSecurityDetailFrame1Output;


/*
*   Trade-Lookup
*/
typedef struct TTradeLookupTxnInput
{
    TTrade              trade_id[TradeLookupFrame1MaxRows];
    TIdent              acct_id;
    TIdent              max_acct_id;
    INT32               frame_to_execute;           // which of the frames to execute
    INT32               max_trades;
    TIMESTAMP_STRUCT    end_trade_dts;
    TIMESTAMP_STRUCT    start_trade_dts;
    char                symbol[cSYMBOL_len+1];
} *PTradeLookupTxnInput;
typedef struct TTradeLookupTxnOutput
{
    TTrade              trade_list[TradeLookupMaxRows];
    INT32               frame_executed;             // confirmation of which frame was executed
    INT32               num_found;
    INT32               status;
    bool                is_cash[TradeLookupMaxRows];
    bool                is_market[TradeLookupMaxRows];
} *PTradeLookupTxnOutput;

typedef struct TTradeLookupFrame1Input
{
    TTrade              trade_id[TradeLookupFrame1MaxRows];
    INT32               max_trades;
} *PTradeLookupFrame1Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame1TradeInfo
{
    double              bid_price;
    double              cash_transaction_amount;
    double              settlement_amount;
    double              trade_price;
    bool                is_cash;
    bool                is_market;
    TIMESTAMP_STRUCT    trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
    TIMESTAMP_STRUCT    cash_transaction_dts;
    TIMESTAMP_STRUCT    settlement_cash_due_date;
    char                trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
    char                cash_transaction_name[cCT_NAME_len+1];
    char                exec_name[cEXEC_NAME_len+1];
    char                settlement_cash_type[cSE_CASH_TYPE_len+1];
    DB_INDICATOR        trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
    DB_INDICATOR        trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
    DB_INDICATOR        bid_price_ind;
    DB_INDICATOR        cash_transaction_amount_ind;
    DB_INDICATOR        cash_transaction_dts_ind;
    DB_INDICATOR        cash_transaction_name_ind;
    DB_INDICATOR        exec_name_ind;
    DB_INDICATOR        is_cash_ind;
    DB_INDICATOR        is_market_ind;
    DB_INDICATOR        settlement_amount_ind;
    DB_INDICATOR        settlement_cash_due_date_ind;
    DB_INDICATOR        settlement_cash_type_ind;
    DB_INDICATOR        trade_price_ind;
} *PTradeLookupFrame1TradeInfo;

typedef struct TTradeLookupFrame1Output
{
    INT32                       num_found;
    INT32                       status;
    TTradeLookupFrame1TradeInfo trade_info[TradeLookupFrame1MaxRows];
} *PTradeLookupFrame1Output;

typedef struct TTradeLookupFrame2Input
{
    TIdent              acct_id;
    INT32               max_trades;
    TIMESTAMP_STRUCT    end_trade_dts;
    TIMESTAMP_STRUCT    start_trade_dts;
} *PTradeLookupFrame2Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame2TradeInfo
{
    double              bid_price;
    double              cash_transaction_amount;
    double              settlement_amount;
    double              trade_price;
    TTrade              trade_id;
    bool                is_cash;
    TIMESTAMP_STRUCT    trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
    TIMESTAMP_STRUCT    cash_transaction_dts;
    TIMESTAMP_STRUCT    settlement_cash_due_date;
    char                trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
    char                cash_transaction_name[cCT_NAME_len+1];
    char                exec_name[cEXEC_NAME_len+1];
    char                settlement_cash_type[cSE_CASH_TYPE_len+1];
    DB_INDICATOR        trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
    DB_INDICATOR        trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
    DB_INDICATOR        bid_price_ind;
    DB_INDICATOR        cash_transaction_amount_ind;
    DB_INDICATOR        cash_transaction_dts_ind;
    DB_INDICATOR        cash_transaction_name_ind;
    DB_INDICATOR        exec_name_ind;
    DB_INDICATOR        is_cash_ind;
    DB_INDICATOR        settlement_amount_ind;
    DB_INDICATOR        settlement_cash_due_date_ind;
    DB_INDICATOR        settlement_cash_type_ind;
    DB_INDICATOR        trade_id_ind;
    DB_INDICATOR        trade_price_ind;
} *PTradeLookupFrame2TradeInfo;

typedef struct TTradeLookupFrame2Output
{
    INT32                       num_found;
    INT32                       status;
    TTradeLookupFrame2TradeInfo trade_info[TradeLookupFrame2MaxRows];
} *PTradeLookupFrame2Output;

typedef struct TTradeLookupFrame3Input
{
    TIdent              max_acct_id;
    INT32               max_trades;
    TIMESTAMP_STRUCT    end_trade_dts;
    TIMESTAMP_STRUCT    start_trade_dts;
    char                symbol[cSYMBOL_len+1];
} *PTradeLookupFrame3Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame3TradeInfo
{
    double              cash_transaction_amount;
    double              price;
    double              settlement_amount;
    TIdent              acct_id;
    TTrade              trade_id;
    INT32               quantity;
    bool                is_cash;
    TIMESTAMP_STRUCT    trade_history_dts[TradeLookupMaxTradeHistoryRowsReturned];
    TIMESTAMP_STRUCT    cash_transaction_dts;
    TIMESTAMP_STRUCT    settlement_cash_due_date;
    TIMESTAMP_STRUCT    trade_dts;
    char                trade_history_status_id[TradeLookupMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
    char                cash_transaction_name[cCT_NAME_len+1];
    char                exec_name[cEXEC_NAME_len+1];
    char                settlement_cash_type[cSE_CASH_TYPE_len+1];
    char                trade_type[cTT_ID_len+1];
    DB_INDICATOR        trade_history_dts_ind[TradeLookupMaxTradeHistoryRowsReturned];
    DB_INDICATOR        trade_history_status_id_ind[TradeLookupMaxTradeHistoryRowsReturned];
    DB_INDICATOR        acct_id_ind;
    DB_INDICATOR        cash_transaction_amount_ind;
    DB_INDICATOR        cash_transaction_dts_ind;
    DB_INDICATOR        cash_transaction_name_ind;
    DB_INDICATOR        exec_name_ind;
    DB_INDICATOR        is_cash_ind;
    DB_INDICATOR        price_ind;
    DB_INDICATOR        quantity_ind;
    DB_INDICATOR        settlement_amount_ind;
    DB_INDICATOR        settlement_cash_due_date_ind;
    DB_INDICATOR        settlement_cash_type_ind;
    DB_INDICATOR        trade_dts_ind;
    DB_INDICATOR        trade_id_ind;
    DB_INDICATOR        trade_type_ind;
} *PTradeLookupFrame3TradeInfo;

typedef struct TTradeLookupFrame3Output
{
    INT32                       num_found;
    INT32                       status;
    TTradeLookupFrame3TradeInfo trade_info[TradeLookupFrame3MaxRows];
} *PTradeLookupFrame3Output;

typedef struct TTradeLookupFrame4Input
{
    TIdent              acct_id;
    TIMESTAMP_STRUCT    trade_dts;
} *PTradeLookupFrame4Input;

// Structure to hold one trade information row
//
typedef struct TTradeLookupFrame4TradeInfo
{
    TTrade              holding_history_id;
    TTrade              holding_history_trade_id;
    INT32               quantity_after;
    INT32               quantity_before;
    DB_INDICATOR        holding_history_id_ind;
    DB_INDICATOR        holding_history_trade_id_ind;
    DB_INDICATOR        quantity_after_ind;
    DB_INDICATOR        quantity_before_ind;
} *PTradeLookupFrame4TradeInfo;

typedef struct TTradeLookupFrame4Output
{
    TTrade                      trade_id;
    INT32                       num_found;
    INT32                       status;
    TTradeLookupFrame4TradeInfo trade_info[TradeLookupFrame4MaxRows];
} *PTradeLookupFrame4Output;



/*
*   Trade-Order
*/
typedef struct TTradeOrderTxnInput
{
    double          requested_price;
    TIdent          acct_id;
    INT32           is_lifo;
    INT32           roll_it_back;
    INT32           trade_qty;
    INT32           type_is_margin;
    char            co_name[cCO_NAME_len+1];
    char            exec_f_name[cF_NAME_len+1];
    char            exec_l_name[cL_NAME_len+1];
    char            exec_tax_id[cTAX_ID_len+1];
    char            issue[cS_ISSUE_len+1];
    char            st_pending_id[cST_ID_len+1];
    char            st_submitted_id[cST_ID_len+1];
    char            symbol[cSYMBOL_len+1];
    char            trade_type_id[cTT_ID_len+1];
} *PTradeOrderTxnInput;
typedef struct TTradeOrderTxnOutput
{
    double  buy_value;
    double  sell_value;
    double  tax_amount;
    TTrade  trade_id;
    INT32   status;
} *PTradeOrderTxnOutput;

typedef struct TTradeOrderFrame1Input
{
    TIdent  acct_id;
} *PTradeOrderFrame1Input;

typedef struct TTradeOrderFrame1Output
{
    TIdent  broker_id;
    TIdent  cust_id;
    INT32   cust_tier;
    INT32   status;
    INT32   tax_status;
    char    acct_name[cCA_NAME_len+1];
    char    broker_name[cB_NAME_len+1];
    char    cust_f_name[cF_NAME_len+1];
    char    cust_l_name[cL_NAME_len+1];
    char    tax_id[cTAX_ID_len+1];
} *PTradeOrderFrame1Output;

typedef struct TTradeOrderFrame2Input
{
    TIdent  acct_id;
    char    exec_f_name[cF_NAME_len+1];
    char    exec_l_name[cL_NAME_len+1];
    char    exec_tax_id[cTAX_ID_len+1];
} *PTradeOrderFrame2Input;

typedef struct TTradeOrderFrame2Output
{
    INT32       status;
    char        ap_acl[cACL_len+1];
} *PTradeOrderFrame2Output;

typedef struct TTradeOrderFrame3Input
{
    double  requested_price;                // IN-OUT parameter
    TIdent  acct_id;
    TIdent  cust_id;
    INT32   cust_tier;
    INT32   is_lifo;
    INT32   tax_status;
    INT32   trade_qty;
    INT32   type_is_margin;
    char    co_name[cCO_NAME_len+1];        // IN-OUT parameter
    char    issue[cS_ISSUE_len+1];
    char    st_pending_id[cST_ID_len+1];
    char    st_submitted_id[cST_ID_len+1];
    char    symbol[cSYMBOL_len+1];          // IN-OUT parameter
    char    trade_type_id[cTT_ID_len+1];
} *PTradeOrderFrame3Input;

typedef struct TTradeOrderFrame3Output
{
    double  buy_value;
    double  charge_amount;
    double  comm_rate;
    double  cust_assets;
    double  market_price;
    double  requested_price;            // IN-OUT parameter
    double  sell_value;
    double  tax_amount;
    INT32   status;
    INT32   type_is_market;
    INT32   type_is_sell;
    char    co_name[cCO_NAME_len+1];    // IN-OUT parameter
    char    s_name[cS_NAME_len+1];
    char    status_id[cST_ID_len+1];
    char    symbol[cSYMBOL_len+1];      // IN-OUT parameter
} *PTradeOrderFrame3Output;

typedef struct TTradeOrderFrame4Input
{
    double  charge_amount;
    double  comm_amount;
    double  requested_price;
    TIdent  acct_id;
    TIdent  broker_id;
    INT32   is_cash;
    INT32   is_lifo;
    INT32   trade_qty;
    INT32   type_is_market;
    char    exec_name[cEXEC_NAME_len+1];
    char    status_id[cST_ID_len+1];
    char    symbol[cSYMBOL_len+1];
    char    trade_type_id[cTT_ID_len+1];
} *PTradeOrderFrame4Input;

typedef struct TTradeOrderFrame4Output
{
    TTrade  trade_id;
    INT32   status;
} *PTradeOrderFrame4Output;

typedef struct TTradeOrderFrame5Output
{
    INT32       status;
} *PTradeOrderFrame5Output;

typedef struct TTradeOrderFrame6Output
{
    INT32       status;
} *PTradeOrderFrame6Output;


/*
*   Trade-Result
*/
//Trade-Result data sent from MEE to sponsor provided SUT interface
typedef struct TTradeResultTxnInput
{
    double      trade_price;
    TTrade      trade_id;
} *PTradeResultTxnInput;

typedef struct TTradeResultTxnOutput
{
    double      acct_bal;
    TIdent      acct_id;
    INT32       status;
} *PTradeResultTxnOutput;

typedef struct TTradeResultFrame1Input
{
    TTrade      trade_id;
} *PTradeResultFrame1Input;

typedef struct TTradeResultFrame1Output
{
    double  charge;
    TIdent  acct_id;
    INT32   hs_qty;
    INT32   is_lifo;
    INT32   status;
    INT32   trade_is_cash;
    INT32   trade_qty;
    INT32   type_is_market;
    INT32   type_is_sell;
    char    symbol[cSYMBOL_len+1];
    char    type_id[cTT_ID_len+1];
    char    type_name[cTT_NAME_len+1];
} *PTradeResultFrame1Output;

typedef struct TTradeResultFrame2Input
{
    double              trade_price;
    TIdent              acct_id;
    TTrade              trade_id;
    INT32               hs_qty;
    INT32               is_lifo;
    INT32               trade_qty;
    INT32               type_is_sell;
    char                symbol[cSYMBOL_len+1];
} *PTradeResultFrame2Input;

typedef struct TTradeResultFrame2Output
{
    double              buy_value;
    double              sell_value;
    TIdent              broker_id;
    TIdent              cust_id;
    INT32               status;
    INT32               tax_status;
    TIMESTAMP_STRUCT    trade_dts;
} *PTradeResultFrame2Output;

typedef struct TTradeResultFrame3Input
{
    double  buy_value;
    double  sell_value;
    TIdent  cust_id;
    TTrade  trade_id;
} *PTradeResultFrame3Input;

typedef struct TTradeResultFrame3Output
{
    double  tax_amount;
    INT32   status;
} *PTradeResultFrame3Output;

typedef struct TTradeResultFrame4Input
{
    TIdent  cust_id;
    INT32   trade_qty;
    char    symbol[cSYMBOL_len+1];
    char    type_id[cTT_ID_len+1];
} *PTradeResultFrame4Input;

typedef struct TTradeResultFrame4Output
{
    double  comm_rate;
    INT32   status;
    char    s_name[cS_NAME_len+1];
} *PTradeResultFrame4Output;

typedef struct TTradeResultFrame5Input
{
    double              comm_amount;
    double              trade_price;
    TIdent              broker_id;
    TTrade              trade_id;
    TIMESTAMP_STRUCT    trade_dts;
    char                st_completed_id[cST_ID_len+1];
} *PTradeResultFrame5Input;

typedef struct TTradeResultFrame5Output
{
    INT32       status;
} *PTradeResultFrame5Output;

typedef struct TTradeResultFrame6Input
{
    double              se_amount;
    TIdent              acct_id;
    TTrade              trade_id;
    INT32               trade_is_cash;
    INT32               trade_qty;
    TIMESTAMP_STRUCT    due_date;
    TIMESTAMP_STRUCT    trade_dts;
    char                s_name[cS_NAME_len+1];
    char                type_name[cTT_NAME_len+1];
} *PTradeResultFrame6Input;

typedef struct TTradeResultFrame6Output
{
    double      acct_bal;
    INT32       status;
} *PTradeResultFrame6Output;


/*
*   Trade-Status
*/
typedef struct TTradeStatusTxnInput
{
    TIdent              acct_id;
}   *PTradeStatusTxnInput,
     TTradeStatusFrame1Input,   // Single-Frame transaction
    *PTradeStatusFrame1Input;   // Single-Frame transaction

typedef struct TTradeStatusTxnOutput
{
    TTrade              trade_id[max_trade_status_len];
    INT32               status;
    char                status_name[max_trade_status_len][cST_NAME_len+1];
}   *PTradeStatusTxnOutput;

typedef struct TTradeStatusFrame1Output
{
    double              charge[max_trade_status_len];
    TTrade              trade_id[max_trade_status_len];
    INT32               trade_qty[max_trade_status_len];
    INT32               status;
    TIMESTAMP_STRUCT    trade_dts[max_trade_status_len];
    char                ex_name[max_trade_status_len][cEX_NAME_len+1];
    char                exec_name[max_trade_status_len][cEXEC_NAME_len+1];
    char                s_name[max_trade_status_len][cS_NAME_len+1];
    char                status_name[max_trade_status_len][cST_NAME_len+1];
    char                symbol[max_trade_status_len][cSYMBOL_len+1];
    char                type_name[max_trade_status_len][cTT_NAME_len+1];
    char                broker_name[cB_NAME_len+1];
    char                cust_f_name[cF_NAME_len+1];
    char                cust_l_name[cL_NAME_len+1];
}   *PTradeStatusFrame1Output;


/*
*   Trade-Update
*/
typedef struct TTradeUpdateTxnInput
{
    TTrade              trade_id[TradeUpdateFrame1MaxRows];
    TIdent              acct_id;
    TIdent              max_acct_id;
    INT32               frame_to_execute;                   // which of the frames to execute
    INT32               max_trades;
    INT32               max_updates;
    TIMESTAMP_STRUCT    end_trade_dts;
    TIMESTAMP_STRUCT    start_trade_dts;
    char                symbol[cSYMBOL_len+1];
} *PTradeUpdateTxnInput;
typedef struct TTradeUpdateTxnOutput
{
    TTrade              trade_list[TradeUpdateMaxRows];
    INT32               frame_executed;                     // confirmation of which frame was executed
    INT32               num_found;
    INT32               num_updated;
    INT32               status;
    bool                is_cash[TradeUpdateMaxRows];
    bool                is_market[TradeUpdateMaxRows];
} *PTradeUpdateTxnOutput;

typedef struct TTradeUpdateFrame1Input
{
    TTrade              trade_id[TradeUpdateFrame1MaxRows];
    INT32               max_trades;
    INT32               max_updates;
} *PTradeUpdateFrame1Input;

typedef struct TTradeUpdateFrame1TradeInfo
{
    double              bid_price;
    double              cash_transaction_amount;
    double              settlement_amount;
    double              trade_price;
    bool                is_cash;
    bool                is_market;
    TIMESTAMP_STRUCT    trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
    TIMESTAMP_STRUCT    cash_transaction_dts;
    TIMESTAMP_STRUCT    settlement_cash_due_date;
    char                trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
    char                cash_transaction_name[cCT_NAME_len+1];
    char                exec_name[cEXEC_NAME_len+1];
    char                settlement_cash_type[cSE_CASH_TYPE_len+1];
    DB_INDICATOR        trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
    DB_INDICATOR        trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
    DB_INDICATOR        bid_price_ind;
    DB_INDICATOR        cash_transaction_amount_ind;
    DB_INDICATOR        cash_transaction_dts_ind;
    DB_INDICATOR        cash_transaction_name_ind;
    DB_INDICATOR        exec_name_ind;
    DB_INDICATOR        is_cash_ind;
    DB_INDICATOR        is_market_ind;
    DB_INDICATOR        settlement_amount_ind;
    DB_INDICATOR        settlement_cash_due_date_ind;
    DB_INDICATOR        settlement_cash_type_ind;
    DB_INDICATOR        trade_price_ind;
} *PTradeUpdateFrame1TradeInfo;

typedef struct TTradeUpdateFrame1Output
{
    INT32                       num_found;
    INT32                       num_updated;
    INT32                       status;
    char                        zz_padding[4];
    TTradeUpdateFrame1TradeInfo trade_info[TradeUpdateFrame1MaxRows];
} *PTradeUpdateFrame1Output;

typedef struct TTradeUpdateFrame2Input
{
    TIdent              acct_id;
    INT32               max_trades;
    INT32               max_updates;
    TIMESTAMP_STRUCT    end_trade_dts;
    TIMESTAMP_STRUCT    start_trade_dts;
} *PTradeUpdateFrame2Input;

typedef struct TTradeUpdateFrame2TradeInfo
{
    double              bid_price;
    double              cash_transaction_amount;
    double              settlement_amount;
    double              trade_price;
    TTrade              trade_id;
    bool                is_cash;
    TIMESTAMP_STRUCT    trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
    TIMESTAMP_STRUCT    cash_transaction_dts;
    TIMESTAMP_STRUCT    settlement_cash_due_date;
    char                trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
    char                cash_transaction_name[cCT_NAME_len+1];
    char                exec_name[cEXEC_NAME_len+1];
    char                settlement_cash_type[cSE_CASH_TYPE_len+1];
    DB_INDICATOR        trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
    DB_INDICATOR        trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
    DB_INDICATOR        bid_price_ind;
    DB_INDICATOR        cash_transaction_amount_ind;
    DB_INDICATOR        cash_transaction_dts_ind;
    DB_INDICATOR        cash_transaction_name_ind;
    DB_INDICATOR        exec_name_ind;
    DB_INDICATOR        is_cash_ind;
    DB_INDICATOR        settlement_amount_ind;
    DB_INDICATOR        settlement_cash_due_date_ind;
    DB_INDICATOR        settlement_cash_type_ind;
    DB_INDICATOR        trade_id_ind;
    DB_INDICATOR        trade_price_ind;
} *PTradeUpdateFrame2TradeInfo;

typedef struct TTradeUpdateFrame2Output
{
    INT32                       num_found;
    INT32                       num_updated;
    INT32                       status;
    TTradeUpdateFrame2TradeInfo trade_info[TradeUpdateFrame2MaxRows];
} *PTradeUpdateFrame2Output;

typedef struct TTradeUpdateFrame3Input
{
    TIdent              max_acct_id;
    INT32               max_trades;
    INT32               max_updates;
    TIMESTAMP_STRUCT    end_trade_dts;
    TIMESTAMP_STRUCT    start_trade_dts;
    char                symbol[cSYMBOL_len+1];
} *PTradeUpdateFrame3Input;

typedef struct TTradeUpdateFrame3TradeInfo
{
    double              cash_transaction_amount;
    double              price;
    double              settlement_amount;
    TIdent              acct_id;
    TTrade              trade_id;
    INT32               quantity;
    bool                is_cash;
    TIMESTAMP_STRUCT    trade_history_dts[TradeUpdateMaxTradeHistoryRowsReturned];
    TIMESTAMP_STRUCT    cash_transaction_dts;
    TIMESTAMP_STRUCT    settlement_cash_due_date;
    TIMESTAMP_STRUCT    trade_dts;
    char                trade_history_status_id[TradeUpdateMaxTradeHistoryRowsReturned][cTH_ST_ID_len+1];
    char                cash_transaction_name[cCT_NAME_len+1];
    char                exec_name[cEXEC_NAME_len+1];
    char                s_name[cS_NAME_len+1];
    char                settlement_cash_type[cSE_CASH_TYPE_len+1];
    char                trade_type[cTT_ID_len+1];
    char                type_name[cTT_NAME_len+1];
    DB_INDICATOR        trade_history_dts_ind[TradeUpdateMaxTradeHistoryRowsReturned];
    DB_INDICATOR        trade_history_status_id_ind[TradeUpdateMaxTradeHistoryRowsReturned];
    DB_INDICATOR        acct_id_ind;
    DB_INDICATOR        cash_transaction_amount_ind;
    DB_INDICATOR        cash_transaction_dts_ind;
    DB_INDICATOR        cash_transaction_name_ind;
    DB_INDICATOR        exec_name_ind;
    DB_INDICATOR        is_cash_ind;
    DB_INDICATOR        price_ind;
    DB_INDICATOR        quantity_ind;
    DB_INDICATOR        s_name_ind;
    DB_INDICATOR        settlement_amount_ind;
    DB_INDICATOR        settlement_cash_due_date_ind;
    DB_INDICATOR        settlement_cash_type_ind;
    DB_INDICATOR        trade_dts_ind;
    DB_INDICATOR        trade_id_ind;
    DB_INDICATOR        trade_type_ind;
    DB_INDICATOR        type_name_ind;
} *PTradeUpdateFrame3TradeInfo;

typedef struct TTradeUpdateFrame3Output
{
    INT32                       num_found;
    INT32                       num_updated;
    INT32                       status;
    TTradeUpdateFrame3TradeInfo trade_info[TradeUpdateFrame3MaxRows];
} *PTradeUpdateFrame3Output;

/*
*   Trade-Cleanup
*/
typedef struct TTradeCleanupTxnInput
{
    TTrade              start_trade_id;
    char                st_canceled_id[cST_ID_len+1];
    char                st_pending_id[cST_ID_len+1];
    char                st_submitted_id[cST_ID_len+1];
}   *PTradeCleanupTxnInput,
     TTradeCleanupFrame1Input,  // Single-Frame transaction
    *PTradeCleanupFrame1Input;  // Single-Frame transaction

typedef struct TTradeCleanupTxnOutput
{
    INT32               status;
}   *PTradeCleanupTxnOutput;

typedef struct TTradeCleanupFrame1Output
{
    INT32               status;
}   *PTradeCleanupFrame1Output;


}   // namespace TPCE

#endif  // #ifndef TXN_HARNESS_STRUCTS_H
