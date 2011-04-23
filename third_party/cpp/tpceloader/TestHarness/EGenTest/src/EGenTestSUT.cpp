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

/*
*   Test suite Two implementation.
*
*   This suite runs one instance of reference Driver code with one instance
*   of the current Driver code. 
*
*   It is intended to test runtime transaction parameter generation.
*/

#include "../inc/EGenTestSuites_stdafx.h"

//  String representation of TXN_TYPES.
//
namespace TPCETest
{
const char* g_szTxnTypes[] = {
    "Broker Volume",
    "Customer Position",
    "Market Feed",
    "Market Watch",
    "Security Detail",
    "Trade Lookup",
    "Trade Order",
    "Trade Result",
    "Trade Status",
    "Trade Update",
    "Data Maintenance",
    "Trade Cleanup"
};
}   // namespace TPCETest

using namespace TPCETest;

//  The constructor.
//
CEGenTestSUT::CEGenTestSUT()
{
    memset(&m_RefInput, 0, sizeof(m_RefInput));
    memset(&m_CurrentInput, 0, sizeof(m_CurrentInput));

    m_RefInput.eTxnType = INVALID_TRANSACTION;

    m_CurrentInput.eTxnType = INVALID_TRANSACTION;
}

// RefTPCE::CCESUTInterface
//
bool CEGenTestSUT::BrokerVolume( RefTPCE::PBrokerVolumeTxnInput       pTxnInput )
{
    m_RefInput.eTxnType = BROKER_VOLUME;

    m_RefInput.BVInput = *pTxnInput;    // copy to local storage

    return true;
}

bool CEGenTestSUT::CustomerPosition( RefTPCE::PCustomerPositionTxnInput pTxnInput )
{
    m_RefInput.eTxnType = CUSTOMER_POSITION;

    m_RefInput.CPInput = *pTxnInput;    // copy to local storage

    return true;
}

bool CEGenTestSUT::MarketWatch( RefTPCE::PMarketWatchTxnInput         pTxnInput )
{
    m_RefInput.eTxnType = MARKET_WATCH;

    m_RefInput.MWInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::SecurityDetail( RefTPCE::PSecurityDetailTxnInput   pTxnInput )
{
    m_RefInput.eTxnType = SECURITY_DETAIL;

    m_RefInput.SDInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeLookup( RefTPCE::PTradeLookupTxnInput         pTxnInput )
{
    m_RefInput.eTxnType = TRADE_LOOKUP;

    m_RefInput.TLInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeOrder( RefTPCE::PTradeOrderTxnInput           pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner )
{
    m_RefInput.eTxnType = TRADE_ORDER;

    m_RefInput.TOInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeStatus( RefTPCE::PTradeStatusTxnInput         pTxnInput )
{
    m_RefInput.eTxnType = TRADE_STATUS;

    m_RefInput.TSInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeUpdate( RefTPCE::PTradeUpdateTxnInput         pTxnInput )
{
    m_RefInput.eTxnType = TRADE_UPDATE;

    m_RefInput.TUInput = *pTxnInput;    // copy to local storage
    
    return true;
}


// RefTPCE::CDMSUTInterface
//
bool CEGenTestSUT::DataMaintenance( RefTPCE::PDataMaintenanceTxnInput pTxnInput )
{
    m_RefInput.eTxnType = DATA_MAINTENANCE;

    m_RefInput.DMInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeCleanup( RefTPCE::PTradeCleanupTxnInput       pTxnInput )
{
    m_RefInput.eTxnType = TRADE_CLEANUP;

    m_RefInput.TCInput = *pTxnInput;    // copy to local storage
    
    return true;
}


// TPCE::CCESUTInterface
//
bool CEGenTestSUT::BrokerVolume( TPCE::PBrokerVolumeTxnInput          pTxnInput )
{
    m_CurrentInput.eTxnType = BROKER_VOLUME;

    m_CurrentInput.BVInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::CustomerPosition( TPCE::PCustomerPositionTxnInput  pTxnInput )
{
    m_CurrentInput.eTxnType = CUSTOMER_POSITION;

    m_CurrentInput.CPInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::MarketWatch( TPCE::PMarketWatchTxnInput            pTxnInput )
{
    m_CurrentInput.eTxnType = MARKET_WATCH;

    m_CurrentInput.MWInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::SecurityDetail( TPCE::PSecurityDetailTxnInput      pTxnInput )
{
    m_CurrentInput.eTxnType = SECURITY_DETAIL;

    m_CurrentInput.SDInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeLookup( TPCE::PTradeLookupTxnInput            pTxnInput )
{
    m_CurrentInput.eTxnType = TRADE_LOOKUP;

    m_CurrentInput.TLInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeOrder( TPCE::PTradeOrderTxnInput              pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner )
{
    m_CurrentInput.eTxnType = TRADE_ORDER;

    m_CurrentInput.TOInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeStatus( TPCE::PTradeStatusTxnInput            pTxnInput )
{
    m_CurrentInput.eTxnType = TRADE_STATUS;

    m_CurrentInput.TSInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeUpdate( TPCE::PTradeUpdateTxnInput            pTxnInput )
{
    m_CurrentInput.eTxnType = TRADE_UPDATE;

    m_CurrentInput.TUInput = *pTxnInput;    // copy to local storage
    
    return true;
}


// TPCE::CDMSUTInterface
//
bool CEGenTestSUT::DataMaintenance( TPCE::PDataMaintenanceTxnInput    pTxnInput )
{
    m_CurrentInput.eTxnType = DATA_MAINTENANCE;

    m_CurrentInput.DMInput = *pTxnInput;    // copy to local storage
    
    return true;
}

bool CEGenTestSUT::TradeCleanup( TPCE::PTradeCleanupTxnInput          pTxnInput )
{
    m_CurrentInput.eTxnType = TRADE_CLEANUP;

    m_CurrentInput.TCInput = *pTxnInput;    // copy to local storage
    
    return true;
}

/*
*  Routine to compare two versions of a transaction inputs.
*  Both transaction type and transaction input data are compared.
*
*  PARAMETERS:
*           none.
*
*  RETURNS:
*           true    - if either the transaction type is different or input data is different.
*           false   - if both the transaction type and input data are the same.
*/
bool CEGenTestSUT::InputDiffers()
{
    if (m_RefInput.eTxnType != m_CurrentInput.eTxnType)
    {
        return false;   // transaction types don't match
    }

    switch(m_RefInput.eTxnType)
    {   // TO DO: need to check structure sizes here too
    case BROKER_VOLUME:
        return memcmp(&m_RefInput.BVInput, &m_CurrentInput.BVInput, sizeof(m_RefInput.BVInput)) != 0;
    case CUSTOMER_POSITION:
        return memcmp(&m_RefInput.CPInput, &m_CurrentInput.CPInput, sizeof(m_RefInput.CPInput)) != 0;
    case MARKET_WATCH:
        return memcmp(&m_RefInput.MWInput, &m_CurrentInput.MWInput, sizeof(m_RefInput.MWInput)) != 0;
    case SECURITY_DETAIL:
        return memcmp(&m_RefInput.SDInput, &m_CurrentInput.SDInput, sizeof(m_RefInput.SDInput)) != 0;
    case TRADE_LOOKUP:
        return memcmp(&m_RefInput.TLInput, &m_CurrentInput.TLInput, sizeof(m_RefInput.TLInput)) != 0;
    case TRADE_ORDER:
        return memcmp(&m_RefInput.TOInput, &m_CurrentInput.TOInput, sizeof(m_RefInput.TOInput)) != 0;
    case TRADE_STATUS:
        return memcmp(&m_RefInput.TSInput, &m_CurrentInput.TSInput, sizeof(m_RefInput.TSInput)) != 0;
    case TRADE_UPDATE:
        return memcmp(&m_RefInput.TUInput, &m_CurrentInput.TUInput, sizeof(m_RefInput.TUInput)) != 0;
    case DATA_MAINTENANCE:
        return memcmp(&m_RefInput.DMInput, &m_CurrentInput.DMInput, sizeof(m_RefInput.DMInput)) != 0;
    case TRADE_CLEANUP:
        return memcmp(&m_RefInput.TCInput, &m_CurrentInput.TCInput, sizeof(m_RefInput.TCInput)) != 0;
    default:
        return false;   // false by default
    }
}
