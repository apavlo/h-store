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
 * - Doug Johnson
 */

#include "../inc/FlatFileLoad_stdafx.h"

using namespace TPCE;

namespace TPCE
{
//Explicit Instantiation to generate code
template class CFlatFileLoader<ACCOUNT_PERMISSION_ROW>;
template class CFlatFileLoader<ADDRESS_ROW>;
template class CFlatFileLoader<BROKER_ROW>;
template class CFlatFileLoader<CASH_TRANSACTION_ROW>;
template class CFlatFileLoader<CHARGE_ROW>;
template class CFlatFileLoader<COMMISSION_RATE_ROW>;
template class CFlatFileLoader<COMPANY_ROW>;
template class CFlatFileLoader<COMPANY_COMPETITOR_ROW>;
template class CFlatFileLoader<CUSTOMER_ROW>;
template class CFlatFileLoader<CUSTOMER_ACCOUNT_ROW>;
template class CFlatFileLoader<CUSTOMER_TAXRATE_ROW>;
template class CFlatFileLoader<DAILY_MARKET_ROW>;
template class CFlatFileLoader<EXCHANGE_ROW>;
template class CFlatFileLoader<FINANCIAL_ROW>;
template class CFlatFileLoader<HOLDING_ROW>;
template class CFlatFileLoader<HOLDING_HISTORY_ROW>;
template class CFlatFileLoader<HOLDING_SUMMARY_ROW>;
template class CFlatFileLoader<INDUSTRY_ROW>;
template class CFlatFileLoader<LAST_TRADE_ROW>;
template class CFlatFileLoader<NEWS_ITEM_ROW>;
template class CFlatFileLoader<NEWS_XREF_ROW>;
template class CFlatFileLoader<SECTOR_ROW>;
template class CFlatFileLoader<SECURITY_ROW>;
template class CFlatFileLoader<SETTLEMENT_ROW>;
template class CFlatFileLoader<STATUS_TYPE_ROW>;
template class CFlatFileLoader<TAXRATE_ROW>;
template class CFlatFileLoader<TRADE_ROW>;
template class CFlatFileLoader<TRADE_HISTORY_ROW>;
template class CFlatFileLoader<TRADE_REQUEST_ROW>;
template class CFlatFileLoader<TRADE_TYPE_ROW>;
template class CFlatFileLoader<WATCH_ITEM_ROW>;
template class CFlatFileLoader<WATCH_LIST_ROW>;
template class CFlatFileLoader<ZIP_CODE_ROW>;
} // namespace TPCE
