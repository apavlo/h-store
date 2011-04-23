/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
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

#ifndef EGEN_TEST_SUITES_STDAFX_H
#define EGEN_TEST_SUITES_STDAFX_H

#define WIN32_LEAN_AND_MEAN     // Exclude rarely-used stuff from Windows headers
#include <stddef.h>
#include <stdio.h>
#include <assert.h>
#include <fstream>
#include <set>
#include <map>
#include <iostream>
#include <algorithm>
#include <string>
#include <vector>

//  Include Reference EGen code under RefTPCE namespace.
//
#define TPCE RefTPCE

#include "../../Reference/inc/EGenUtilities_stdafx.h"
#include "../../Reference/inc/EGenTables_stdafx.h"
//#include "../../Reference/inc/EGenBaseLoader_stdafx.h"
#include "../../Reference/inc/EGenGenerateAndLoadBaseOutput.h"
#include "../../Reference/inc/EGenGenerateAndLoadStandardOutput.h"
#include "../../Reference/inc/DriverParamSettings.h"
#include "../../Reference/inc/EGenLogger.h"
#include "../../Reference/inc/CE.h"
#include "../../Reference/inc/DM.h"

#undef TPCE

//  Remove #ifndef checks in includes because they have been defined by Reference includes.
//
#undef EGEN_UTILITIES_STDAFX_H
#undef EGEN_STANDARD_TYPES_H
#undef DATE_TIME_H
#undef RANDOM_H
#undef ERROR_H
#undef TABLE_CONSTS_H
#undef MISC_CONSTS_H
#undef FIXED_MAP_H
#undef FIXED_ARRAY_H
#undef RNGSEEDS_H
#undef EGEN_VERSION_H
#undef MONEY_H

#undef EGEN_TABLES_STDAFX_H
#undef INPUT_FILE_H
#undef INPUT_FILE_NO_WEIGHT_H
#undef FLAT_FILE_H
#undef TABLE_DEFS_H
#undef INPUT_FLAT_FILE_DECLARATIONS_H
#undef SECURITY_FILE_H
#undef COMPANY_FILE_H
#undef COMPANY_COMPETITOR_FILE_H
#undef INPUT_FLAT_FILE_STRUCTURE_H
#undef DRIVER_TYPES_H
#undef BASE_TABLE_H
#undef TABLE_TEMPLATE_H
#undef PERSON_H
#undef CUSTOMER_SELECTION_H
#undef CUSTOMER_TABLE_H
#undef COMPANY_TABLE_H
#undef FINANCIAL_TABLE_H
#undef ADDRESS_TABLE_H
#undef CUSTOMER_ACCOUNTS_AND_PERMISSIONS_TABLE_H
#undef CUSTOMER_TAXRATES_TABLE_H
#undef HOLDINGS_AND_TRADES_TABLE_H
#undef WATCH_LIST_AND_ITEMS_TABLE_H
#undef SECURITY_TABLE_H
#undef DAILY_MARKET_TABLE_H
#undef BROKERS_H
#undef SECTOR_TABLE_H
#undef CHARGE_TABLE_H
#undef EXCHANGE_TABLE_H
#undef COMMISSION_RATE_TABLE_H
#undef INDUSTRY_TABLE_H
#undef STATUS_TYPE_H
#undef TAXRATE_TABLE_H
#undef TRADE_TYPE_TABLE_H
#undef COMPANY_COMPETITOR_TABLE_H
#undef ZIP_CODE_TABLE_H
#undef NEWS_ITEM_AND_XREG_TABLE_H
#undef MEE_SECURITY_H
#undef LAST_TRADE_TABLE_H
#undef TRADE_GEN_H
#undef SECURITY_PRICE_RANGE_H
#undef TRADE_TYPE_IDS_H
#undef SYNCLOCK_H
#undef WIN_ERROR_H

#undef EGEN_GENERATE_AND_LOAD_BASE_OUTPUT_H

#undef EGEN_GENERATE_AND_LOAD_STANDARD_OUTPUT_H

#undef DRIVER_PARAM_SETTINGS_H

#undef EGEN_LOGGER_H
#undef BASE_LOGGER_H
#undef BASE_LOG_FORMATTER_H
#undef EGEN_LOG_FORMATTER_H

#undef CE_H
#undef CE_TXN_INPUT_GENERATOR_H
#undef CE_TXN_MIX_GENERATOR_H
#undef CE_SUT_INTERFACE_H
#undef TXN_HARNESS_STRUCTS_H
#undef MEE_TRADE_REQUEST_ACTIONS_H

#undef DM_H
#undef DM_SUT_INTERFACE_H

//  Include current code being tested. It is included under TPCE (unchanged) namespace.
//
#include "../../../inc/EGenUtilities_stdafx.h"
#include "../../../inc/EGenTables_stdafx.h"
//#include "../../../inc/EGenBaseLoader_stdafx.h"
#include "../../../inc/EGenGenerateAndLoadBaseOutput.h"
#include "../../../inc/EGenGenerateAndLoadStandardOutput.h"
#include "../../../inc/DriverParamSettings.h"
#include "../../../inc/EGenLogger.h"
#include "../../../inc/CE.h"
#include "../../../inc/DM.h"

///////////////////////////////////////////////////
//  Include EGen Test code.
///////////////////////////////////////////////////
#include "EGenTestLoader.h"
#include "EGenTestSUT.h"

#include "EGenTestError.h"
#include "EGenBaseTestSuite.h"
#include "EGenTestSuiteOne.h"
#include "EGenTestSuiteTwo.h"


#endif // EGEN_TEST_SUITES_STDAFX_H