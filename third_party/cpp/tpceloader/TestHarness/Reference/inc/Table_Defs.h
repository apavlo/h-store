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
 * - Sergey Vasilevskiy, Doug Johnson, Larry Loen
 */

/******************************************************************************
*   Description:        Table definitions.
******************************************************************************/

#ifndef TABLE_DEFS_H
#define TABLE_DEFS_H

#include <fstream>

namespace TPCE
{

//Base abstract structure for an input file row
//Must be able to read itself from a file.
typedef struct TBaseInputRow
{
    void Load(istream &file);   //loads itself (one row) from the input stream
    virtual ~TBaseInputRow() {}
} *PBaseInputRow;


//For nullable columns
enum OLTPNullValueIndicator {
    OLTP_VALUE_IS_NULL =0,
    OLTP_VALUE_IS_NOT_NULL
};

typedef struct OLTPNullableFloat
{
    OLTPNullValueIndicator  iIndicator;
    float                   value;
} *POLTPNullableFloat;

typedef struct OLTPNullableDateTime
{
    OLTPNullValueIndicator  iIndicator;
    CDateTime               value;
} *POLTPNullableDateTime;

typedef struct OLTPNullable52WkHighLow
{
    OLTPNullValueIndicator  iIndicator;
    float                   HIGH;
    CDateTime               HIGH_DATE;
    float                   LOW;
    CDateTime               LOW_DATE;
} *POLTPNullable52WkHighLow;

// ACCOUNT_PERMISSION table
typedef struct ACCOUNT_PERMISSION_ROW
{
    TIdent                  AP_CA_ID;
    char                    AP_ACL[ cACL_len+1 ];   //binary column in the table
    char                    AP_TAX_ID[ cTAX_ID_len+1 ];
    char                    AP_L_NAME[ cL_NAME_len+1 ];
    char                    AP_F_NAME[ cF_NAME_len+1 ];
} *PACCOUNT_PERMISSION_ROW;
const char AccountPermissionRowFmt[] = "%" PRId64 "|%s|%s|%s|%s\n";

// ADDRESS table
typedef struct ADDRESS_ROW
{
    TIdent                  AD_ID;
    char                    AD_LINE1[ cAD_LINE_len+1];
    char                    AD_LINE2[ cAD_LINE_len+1];
    char                    AD_ZC_CODE[ cAD_ZIP_len+1 ];
    char                    AD_CTRY[ cAD_CTRY_len+1 ];
} *PADDRESS_ROW;
const char AddressRowFmt[] = "%" PRId64 "|%s|%s|%s|%s\n";

// BROKER table
typedef struct BROKER_ROW
{
    TIdent                  B_ID;
    char                    B_ST_ID[ cST_ID_len+1 ];
    char                    B_NAME[ cB_NAME_len+1 ];
    int                     B_NUM_TRADES;
    double                  B_COMM_TOTAL;
} *PBROKER_ROW;
const char BrokerRowFmt[] = "%" PRId64 "|%s|%s|%d|%.2f\n";

// CASH_TRANSACTION table
typedef struct CASH_TRANSACTION_ROW
{
    TTrade                  CT_T_ID;
    CDateTime               CT_DTS;
    double                  CT_AMT;
    char                    CT_NAME[cCT_NAME_len+1];
} *PCASH_TRANSACTION_ROW;
const char CashTransactionRowFmt[] = "%" PRId64 "|%s|%.2f|%s\n";

// CHARGE table
typedef struct CHARGE_ROW : TBaseInputRow
{
    char                    CH_TT_ID[cTT_ID_len+1];
    int                     CH_C_TIER;
    double                  CH_CHRG;

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PCHARGE_ROW;
const char ChargeRowFmt[] = "%s|%d|%.2f\n";

// COMMISSION_RATE table
typedef struct COMMISSION_RATE_ROW : TBaseInputRow
{
    int                     CR_C_TIER;
    char                    CR_TT_ID[cTT_ID_len+1];
    char                    CR_EX_ID[cEX_ID_len+1];
    double                  CR_FROM_QTY;
    double                  CR_TO_QTY;
    double                  CR_RATE;

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PCOMMISSION_RATE_ROW;
const char CommissionRateRowFmt[] = "%d|%s|%s|%.0f|%.0f|%.2f\n";

// COMPANY table
typedef struct COMPANY_ROW
{
    TIdent                  CO_ID;
    char                    CO_ST_ID[ cST_ID_len+1 ];
    char                    CO_NAME[ cCO_NAME_len+1 ];
    char                    CO_IN_ID[ cIN_ID_len+1 ];
    char                    CO_SP_RATE[ cSP_RATE_len+1 ];
    char                    CO_CEO[ cCEO_NAME_len+1 ];
    TIdent                  CO_AD_ID;
    char                    CO_DESC[ cCO_DESC_len+1 ];
    CDateTime               CO_OPEN_DATE;
} *PCOMPANY_ROW;
const char CompanyRowFmt[] = "%" PRId64 "|%s|%s|%s|%s|%s|%" PRId64 "|%s|%s\n";

// COMPANY_COMPETITOR table
typedef struct COMPANY_COMPETITOR_ROW : TBaseInputRow
{
    TIdent                  CP_CO_ID;
    TIdent                  CP_COMP_CO_ID;
    char                    CP_IN_ID[cIN_ID_len+1];
} *PCOMPANY_COMPETITOR_ROW;
const char CompanyCompetitorRowFmt[] = "%" PRId64 "|%" PRId64 "|%s\n";

// CUSTOMER table
typedef struct CUSTOMER_ROW
{
    TIdent                  C_ID;
    char                    C_TAX_ID[ cTAX_ID_len+1 ];
    char                    C_ST_ID[ cST_ID_len+1 ];
    char                    C_L_NAME[ cL_NAME_len+1 ];
    char                    C_F_NAME[ cF_NAME_len+1 ];
    char                    C_M_NAME[ cM_NAME_len+1 ];
    char                    C_GNDR;
    char                    C_TIER;
    CDateTime               C_DOB;
    TIdent                  C_AD_ID;
    char                    C_CTRY_1[ cCTRY_len+1 ];
    char                    C_AREA_1[ cAREA_len+1 ];
    char                    C_LOCAL_1[ cLOCAL_len+1 ];
    char                    C_EXT_1[ cEXT_len+1 ];
    char                    C_CTRY_2[ cCTRY_len+1 ];
    char                    C_AREA_2[ cAREA_len+1 ];
    char                    C_LOCAL_2[ cLOCAL_len+1 ];
    char                    C_EXT_2[ cEXT_len+1 ];
    char                    C_CTRY_3[ cCTRY_len+1 ];
    char                    C_AREA_3[ cAREA_len+1 ];
    char                    C_LOCAL_3[ cLOCAL_len+1 ];
    char                    C_EXT_3[ cEXT_len+1 ];
    char                    C_EMAIL_1[ cEMAIL_len+1 ];
    char                    C_EMAIL_2[ cEMAIL_len+1 ];

    CUSTOMER_ROW()
    : C_ID(0)
    {};

} *PCUSTOMER_ROW;
const char CustomerRowFmt[] = "%" PRId64 "|%s|%s|%s|%s|%s|%c|%d|%s|%" PRId64 "|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n";

// CUSTOMER_ACCOUNT table
typedef struct CUSTOMER_ACCOUNT_ROW
{
    TIdent                  CA_ID;
    TIdent                  CA_B_ID;
    TIdent                  CA_C_ID;
    char                    CA_NAME[ cCA_NAME_len+1 ];
    char                    CA_TAX_ST;
    double                  CA_BAL;
} *PCUSTOMER_ACCOUNT_ROW;
const char CustomerAccountRowFmt[] = "%" PRId64 "|%" PRId64 "|%" PRId64 "|%s|%d|%.2f\n";

// CUSTOMER_TAXRATE table
typedef struct CUSTOMER_TAXRATE_ROW
{
    char                    CX_TX_ID[ cTX_ID_len+1 ];
    TIdent                  CX_C_ID;
} *PCUSTOMER_TAXRATE_ROW;
const char CustomerTaxrateRowFmt[] = "%s|%" PRId64 "\n";

// DAILY_MARKET table
typedef struct DAILY_MARKET_ROW
{
    CDateTime               DM_DATE;
    char                    DM_S_SYMB[cSYMBOL_len+1];
    double                  DM_CLOSE;
    double                  DM_HIGH;
    double                  DM_LOW;
    int                     DM_VOL;
} *PDAILY_MARKET_ROW;
const char DailyMarketRowFmt[] = "%s|%s|%.2f|%.2f|%.2f|%d\n";

// EXCHANGE table
typedef struct EXCHANGE_ROW
{
    char                    EX_ID[ cEX_ID_len+1 ];
    char                    EX_NAME[ cEX_NAME_len+1 ];
    int                     EX_NUM_SYMB;
    int                     EX_OPEN;
    int                     EX_CLOSE;
    char                    EX_DESC[ cEX_DESC_len+1 ];
    TIdent                  EX_AD_ID;

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PEXCHANGE_ROW;
const char ExchangeRowFmt[] = "%s|%s|%d|%d|%d|%s|%" PRId64 "\n";

// FINANCIAL table
typedef struct FINANCIAL_ROW
{
    TIdent                  FI_CO_ID;
    int                     FI_YEAR;
    int                     FI_QTR;
    CDateTime               FI_QTR_START_DATE;
    double                  FI_REVENUE;
    double                  FI_NET_EARN;
    double                  FI_BASIC_EPS;
    double                  FI_DILUT_EPS;
    double                  FI_MARGIN;
    double                  FI_INVENTORY;
    double                  FI_ASSETS;
    double                  FI_LIABILITY;
    double                  FI_OUT_BASIC;
    double                  FI_OUT_DILUT;
} *PFINANCIAL_ROW;
const char FinancialRowFmt[] = "%" PRId64 "|%d|%d|%s|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f|%.0f\n";

// HOLDING table
typedef struct HOLDING_ROW
{
    TTrade                  H_T_ID;
    TIdent                  H_CA_ID;
    char                    H_S_SYMB[ cSYMBOL_len+1 ];
    CDateTime               H_DTS;
    double                  H_PRICE;
    int                     H_QTY;
} *PHOLDING_ROW;
const char HoldingRowFmt[] = "%" PRId64 "|%" PRId64 "|%s|%s|%.2f|%d\n";

// HOLDING_HISTORY table
typedef struct HOLDING_HISTORY_ROW
{
    TTrade                  HH_H_T_ID;
    TTrade                  HH_T_ID;
    int                     HH_BEFORE_QTY;
    int                     HH_AFTER_QTY;
} *PHOLDING_HISTORY_ROW;
const char HoldingHistoryRowFmt[] = "%" PRId64 "|%" PRId64 "|%d|%d\n";

// HOLDING_SUMMARY table
typedef struct HOLDING_SUMMARY_ROW
{
    TIdent                  HS_CA_ID;
    char                    HS_S_SYMB[ cSYMBOL_len+1 ];
    int                     HS_QTY;
} *PHOLDING_SUMMARY_ROW;
const char HoldingSummaryRowFmt[] = "%" PRId64 "|%s|%d\n";

// INDUSTRY table
typedef struct INDUSTRY_ROW : TBaseInputRow
{
    char                    IN_ID[cIN_ID_len+1];
    char                    IN_NAME[cIN_NAME_len+1];
    char                    IN_SC_ID[cSC_ID_len+1];

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PINDUSTRY_ROW;
const char IndustryRowFmt[] = "%s|%s|%s\n";

// LAST_TRADE table
typedef struct LAST_TRADE_ROW
{
    char                    LT_S_SYMB[cSYMBOL_len+1];
    CDateTime               LT_DTS;
    double                  LT_PRICE;
    double                  LT_OPEN_PRICE;
    int                     LT_VOL;
} *PLAST_TRADE_ROW;
const char LastTradeRowFmt[] = "%s|%s|%.2f|%.2f|%d\n";

// NEWS_ITEM table
typedef struct NEWS_ITEM_ROW
{
    TIdent                  NI_ID;
    char                    NI_HEADLINE[cNI_HEADLINE_len+1];
    char                    NI_SUMMARY[cNI_SUMMARY_len+1];
    char                    NI_ITEM[cNI_ITEM_len+1];
    CDateTime               NI_DTS;
    char                    NI_SOURCE[cNI_SOURCE_len+1];
    char                    NI_AUTHOR[cNI_AUTHOR_len+1];
} *PNEWS_ITEM_ROW;
const char NewsItemRowFmt[] = "%" PRId64 "|%s|%s|%s|%s|%s|%s\n";

// NEWS_XREF table
typedef struct NEWS_XREF_ROW
{
    TIdent                  NX_NI_ID;
    TIdent                  NX_CO_ID;
} *PNEWS_XREF_ROW;
const char NewsXRefRowFmt[] = "%" PRId64 "|%" PRId64 "\n";

// SECTOR table
typedef struct SECTOR_ROW : TBaseInputRow
{
    char                    SC_ID[cSC_ID_len+1];
    char                    SC_NAME[cSC_NAME_len+1];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PSECTOR_ROW;
const char SectorRowFmt[] = "%s|%s\n";

// SECURITY table
typedef struct SECURITY_ROW
{
    char                    S_SYMB[ cSYMBOL_len+1 ];
    char                    S_ISSUE[ cS_ISSUE_len+1 ];
    char                    S_ST_ID[ cST_ID_len+1 ];
    char                    S_NAME[ cS_NAME_len+1 ];
    char                    S_EX_ID[ cEX_ID_len+1 ];
    TIdent                  S_CO_ID;
    double                  S_NUM_OUT;
    CDateTime               S_START_DATE;
    CDateTime               S_EXCH_DATE;
    double                  S_PE;
    OLTPNullable52WkHighLow S_52WK; // S_52WK_HIGH, S_52WK_HIGH_DATE, S_52WK_LOW and S_52WK_LOW_DATE
    double                  S_DIVIDEND;
    double                  S_YIELD;
}   *PSECURITY_ROW;
const char SecurityRowFmt_1[] = "%s|%s|%s|%s|%s|%" PRId64 "|%.0f|%s|%s|%.2f|";
const char SecurityRowFmt_2[] = "%.2f|%s|%.2f|%s|"; // Used for (S_52WK_HIGH, S_52WK_HIGH_DATE) and (S_52WK_LOW, S_52WK_LOW_DATE) when not null.
const char SecurityRowFmt_3[] = "%.2f|%.2f\n";

// SETTLEMENT table
typedef struct SETTLEMENT_ROW
{
    TTrade                  SE_T_ID;
    char                    SE_CASH_TYPE[cSE_CASH_TYPE_len+1];
    CDateTime               SE_CASH_DUE_DATE;
    double                  SE_AMT;
} *PSETTLEMENT_ROW;
const char SettlementRowFmt[] = "%" PRId64 "|%s|%s|%.2f\n";

// STATUS_TYPE table
typedef struct STATUS_TYPE_ROW : TBaseInputRow
{
    char                    ST_ID[cST_ID_len+1];
    char                    ST_NAME[cST_NAME_len+1];

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PSTATUS_TYPE_ROW;
const char StatusTypeRowFmt[] = "%s|%s\n";

// TAXRATE table
typedef struct TAXRATE_ROW
{
    char                    TX_ID[ cTX_ID_len+1 ];
    char                    TX_NAME[ cTX_NAME_len+1 ];
    double                  TX_RATE;
} *PTAXRATE_ROW;
const char TaxrateRowFmt[] = "%s|%s|%.5f\n";

// TRADE table
typedef struct TRADE_ROW
{
    TTrade                  T_ID;
    CDateTime               T_DTS;
    char                    T_ST_ID[ cST_ID_len+1 ];
    char                    T_TT_ID[ cTT_ID_len+1 ];
    int                     T_IS_CASH;
    char                    T_S_SYMB[ cSYMBOL_len+1 ];
    int                     T_QTY;
    double                  T_BID_PRICE;
    TIdent                  T_CA_ID;
    char                    T_EXEC_NAME[ cEXEC_NAME_len+1 ];
    double                  T_TRADE_PRICE;
    double                  T_CHRG;
    double                  T_COMM;
    double                  T_TAX;
    int                     T_LIFO;
} *PTRADE_ROW;
const char TradeRowFmt[] = "%" PRId64 "|%s|%s|%s|%d|%s|%d|%.2f|%" PRId64 "|%s|%.2f|%.2f|%.2f|%.2f|%d\n";

// TRADE_HISTORY table
typedef struct TRADE_HISTORY_ROW
{
    TTrade                  TH_T_ID;
    CDateTime               TH_DTS;
    char                    TH_ST_ID[cST_ID_len+1];
} *PTRADE_HISTORY_ROW;
const char TradeHistoryRowFmt[] = "%" PRId64 "|%s|%s\n";

// TRADE_REQUEST table
typedef struct TRADE_REQUEST_ROW
{
    TTrade                  TR_T_ID;
    char                    TR_TT_ID[ cTT_ID_len+1 ];
    char                    TR_S_SYMB[ cSYMBOL_len+1 ];
    int                     TR_QTY;
    double                  TR_BID_PRICE;
    TIdent                  TR_B_ID;
} *PTRADE_REQUEST_ROW;
const char TradeRequestRowFmt[] = "%" PRId64 "|%s|%s|%d|%.2f|%" PRId64 "\n";

// TRADE_TYPE table
typedef struct TRADE_TYPE_ROW
{
    char                    TT_ID[ cTT_ID_len+1 ];
    char                    TT_NAME[ cTT_NAME_len+1 ];
    int                     TT_IS_SELL;
    int                     TT_IS_MRKT;

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PTRADE_TYPE_ROW;
const char TradeTypeRowFmt[] = "%s|%s|%d|%d\n";

// WATCH_ITEM table
typedef struct WATCH_ITEM_ROW
{
    TIdent                  WI_WL_ID;
    char                    WI_S_SYMB[ cSYMBOL_len+1 ];
} *PWATCH_ITEM_ROW;
const char WatchItemRowFmt[] = "%" PRId64 "|%s\n";

// WATCH_LIST table
typedef struct WATCH_LIST_ROW
{
    TIdent                  WL_ID;
    TIdent                  WL_C_ID;
} *PWATCH_LIST_ROW;
const char WatchListRowFmt[] = "%" PRId64 "|%" PRId64 "\n";

// ZIP_CODE table
typedef struct ZIP_CODE_ROW : public TBaseInputRow
{
    char                    ZC_CODE[cZC_CODE_len+1];
    char                    ZC_TOWN[cZC_TOWN_len+1];
    char                    ZC_DIV[cZC_DIV_len+1];

    void                    Load(istream &file);    //loads itself (one row) from the input stream
} *PZIP_CODE_ROW;
const char ZipCodeRowFmt[] = "%s|%s|%s\n";

}   // namespace TPCE

#endif  // #ifndef TABLE_DEFS_H
