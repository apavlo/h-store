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

#ifndef EGEN_TABLES_STDAFX_H
#define EGEN_TABLES_STDAFX_H

#define WIN32_LEAN_AND_MEAN     // Exclude rarely-used stuff from Windows headers

#include <assert.h>
#include <math.h>
#include <fstream>
#include <set>
#include <map>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>
#include <queue>

// TODO: reference additional headers your program requires here
using namespace std;

//Define unsigned type for convenience
typedef unsigned int UINT;

#include "EGenStandardTypes.h"

namespace TPCE
{

static const TIdent iDefaultStartFromCustomer = 1;

// Minimum number of customers in a database.
// Broker-Volume transations requires 40 broker names as input,
// which translates into minimum 4000 customers in a database.
//
const UINT          iDefaultCustomerCount = 5000;

const int           iBrokersDiv = 100;  // by what number to divide the customer count to get the broker count

// Number of customers in default load unit.
// Note: this value must not be changed. EGen code depends
// on load unit consisting of exactly 1000 customers.
//
const UINT          iDefaultLoadUnitSize = 1000;

// Number by which all IDENT_T columns (C_ID, CA_ID, etc.) are shifted.
//
const TIdent iTIdentShift = UINT64_CONST(4300000000);       // 4.3 billion
//const TIdent iTIdentShift = UINT64_CONST(0);

// Number by which all TRADE_T columns (T_ID, TH_T_ID, etc.) are shifted.
//
const TTrade iTTradeShift = UINT64_CONST(200000000000000);  // 200 trillion (2 * 10^14)
//const TTrade iTTradeShift = UINT64_CONST(0);

}   // namespace TPCE

#include "EGenUtilities_stdafx.h"
#include "InputFile.h"
#include "InputFileNoWeight.h"
#include "FlatFile.h"
#include "Table_Defs.h"
#include "InputFlatFilesDeclarations.h"
#include "SecurityFile.h"
#include "CompanyFile.h"
#include "CompanyCompetitorFile.h"
#include "InputFlatFilesStructure.h"
#include "TableTemplate.h"
#include "Person.h"
#include "CustomerSelection.h"
#include "CustomerTable.h"
#include "CompanyTable.h"   //must be before Address and Financial tables
#include "FinancialTable.h"
#include "AddressTable.h"
#include "CustomerAccountsAndPermissionsTable.h"
#include "CustomerTaxratesTable.h"
#include "HoldingsAndTradesTable.h"
#include "WatchListsAndItemsTable.h"
#include "SecurityTable.h"
#include "DailyMarketTable.h"
#include "Brokers.h"
#include "SectorTable.h"
#include "ChargeTable.h"
#include "ExchangeTable.h"
#include "CommissionRateTable.h"
#include "IndustryTable.h"
#include "StatusTypeTable.h"
#include "TaxrateTable.h"
#include "TradeTypeTable.h"
#include "CompanyCompetitorTable.h"
#include "ZipCodeTable.h"
#include "NewsItemAndXRefTable.h"
#include "MEESecurity.h"    // must be before LastTradeTable.h
#include "LastTradeTable.h"
#include "TradeGen.h"

#endif  // #ifndef EGEN_TABLES_STDAFX_H
