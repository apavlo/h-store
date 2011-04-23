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

#ifndef EGEN_TEST_SUT_H
#define EGEN_TEST_SUT_H

/*
*   Test suite Two implementation.
*
*   This suite runs one instance of reference Driver code with one instance
*   of the current Driver code. 
*
*   It is intended to test runtime transaction parameter generation.
*/

#include "EGenTestSuites_stdafx.h"

namespace TPCETest
{

enum TXN_TYPES
{
    INVALID_TRANSACTION = -1,
    BROKER_VOLUME        = 0,    
    CUSTOMER_POSITION,  //1
    MARKET_FEED,        //2
    MARKET_WATCH,       //3    
    SECURITY_DETAIL,    //4
    TRADE_LOOKUP,       //5
    TRADE_ORDER,        //6
    TRADE_RESULT,       //7
    TRADE_STATUS,       //8
    TRADE_UPDATE,       //9
    DATA_MAINTENANCE,   //10
    TRADE_CLEANUP,      //11
    NUM_OF_TXN_TYPES    //12    
};

//  String representation of TXN_TYPES.
//  Defined in EGenTestSUT.cpp.
//
extern const char* g_szTxnTypes[];

typedef struct TTxnInputTPCE
{
    TXN_TYPES            eTxnType;
    union 
    {
        TPCE::TBrokerVolumeTxnInput         BVInput;
        TPCE::TCustomerPositionTxnInput     CPInput;
        TPCE::TMarketFeedTxnInput           MFInput;
        TPCE::TMarketWatchTxnInput          MWInput;
        TPCE::TSecurityDetailTxnInput       SDInput;
        TPCE::TTradeLookupTxnInput          TLInput;
        TPCE::TTradeOrderTxnInput           TOInput;
        TPCE::TTradeResultTxnInput          TRInput;
        TPCE::TTradeStatusTxnInput          TSInput;
        TPCE::TTradeUpdateTxnInput          TUInput;
        TPCE::TDataMaintenanceTxnInput      DMInput;
        TPCE::TTradeCleanupTxnInput         TCInput;
    };
} *PTxnInputTPCE;

typedef struct TTxnInputRefTPCE
{
    TXN_TYPES            eTxnType;
    union 
    {
        RefTPCE::TBrokerVolumeTxnInput          BVInput;
        RefTPCE::TCustomerPositionTxnInput      CPInput;
        RefTPCE::TMarketFeedTxnInput            MFInput;
        RefTPCE::TMarketWatchTxnInput           MWInput;
        RefTPCE::TSecurityDetailTxnInput        SDInput;
        RefTPCE::TTradeLookupTxnInput           TLInput;
        RefTPCE::TTradeOrderTxnInput            TOInput;
        RefTPCE::TTradeResultTxnInput           TRInput;
        RefTPCE::TTradeStatusTxnInput           TSInput;
        RefTPCE::TTradeUpdateTxnInput           TUInput;
        RefTPCE::TDataMaintenanceTxnInput       DMInput;
        RefTPCE::TTradeCleanupTxnInput          TCInput;
    };
} *PTxnInputRefTPCE;

class CEGenTestSUT: public RefTPCE::CCESUTInterface, public RefTPCE::CDMSUTInterface, public TPCE::CCESUTInterface, public TPCE::CDMSUTInterface
{
    TTxnInputRefTPCE    m_RefInput;
    TTxnInputTPCE       m_CurrentInput;

public:

    CEGenTestSUT();

    // RefTPCE::CCESUTInterface
    //
    virtual bool BrokerVolume( RefTPCE::PBrokerVolumeTxnInput           pTxnInput );    // return whether it was successful
    virtual bool CustomerPosition( RefTPCE::PCustomerPositionTxnInput   pTxnInput );    // return whether it was successful    
    virtual bool MarketWatch( RefTPCE::PMarketWatchTxnInput             pTxnInput );    // return whether it was successful
    virtual bool SecurityDetail( RefTPCE::PSecurityDetailTxnInput       pTxnInput );    // return whether it was successful
    virtual bool TradeLookup( RefTPCE::PTradeLookupTxnInput             pTxnInput );    // return whether it was successful
    virtual bool TradeOrder( RefTPCE::PTradeOrderTxnInput               pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner );    // return whether it was successful
    virtual bool TradeStatus( RefTPCE::PTradeStatusTxnInput             pTxnInput );    // return whether it was successful
    virtual bool TradeUpdate( RefTPCE::PTradeUpdateTxnInput             pTxnInput );    // return whether it was successful

    // RefTPCE::CDMSUTInterface
    //
    virtual bool DataMaintenance( RefTPCE::PDataMaintenanceTxnInput     pTxnInput );    // return whether it was successful
    virtual bool TradeCleanup( RefTPCE::PTradeCleanupTxnInput           pTxnInput );    // return whether it was successful

    // TPCE::CCESUTInterface
    //
    virtual bool BrokerVolume( TPCE::PBrokerVolumeTxnInput              pTxnInput );    // return whether it was successful
    virtual bool CustomerPosition( TPCE::PCustomerPositionTxnInput      pTxnInput );    // return whether it was successful    
    virtual bool MarketWatch( TPCE::PMarketWatchTxnInput                pTxnInput );    // return whether it was successful
    virtual bool SecurityDetail( TPCE::PSecurityDetailTxnInput          pTxnInput );    // return whether it was successful
    virtual bool TradeLookup( TPCE::PTradeLookupTxnInput                pTxnInput );    // return whether it was successful
    virtual bool TradeOrder( TPCE::PTradeOrderTxnInput                  pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner );    // return whether it was successful
    virtual bool TradeStatus( TPCE::PTradeStatusTxnInput                pTxnInput );    // return whether it was successful
    virtual bool TradeUpdate( TPCE::PTradeUpdateTxnInput                pTxnInput );    // return whether it was successful

    // TPCE::CDMSUTInterface
    //
    virtual bool DataMaintenance( TPCE::PDataMaintenanceTxnInput        pTxnInput );    // return whether it was successful
    virtual bool TradeCleanup( TPCE::PTradeCleanupTxnInput              pTxnInput );    // return whether it was successful

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
    bool InputDiffers();

    /*
    *  Helper routine to get reference transaction type.
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           transaction type last generated by the reference code
    */
    TXN_TYPES GetRefTxnType()    { return m_RefInput.eTxnType; }

    /*
    *  Helper routine to get current code transaction type.
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           transaction type last generated by the current code
    */
    TXN_TYPES GetCurTxnType()    { return m_CurrentInput.eTxnType; }
};

}   // namespace TPCETest

#endif // EGEN_TEST_SUT_H
