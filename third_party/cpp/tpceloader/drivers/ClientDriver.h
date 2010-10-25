/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
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
#ifndef TPCECLIENTDRIVER_H
#define TPCECLIENTDRIVER_H

#include <string>

#include <TxnHarnessStructs.h>
#include <CETxnInputGenerator.h>
#include <DM.h>
#include <MEE.h>

using namespace std;

namespace TPCE {

class ClientDriver {
    public:
        ClientDriver(string dataPath, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays);
        ~ClientDriver();
        
        TBrokerVolumeTxnInput&       generateBrokerVolumeInput();
        TCustomerPositionTxnInput&   generateCustomerPositionInput();
        TDataMaintenanceTxnInput&    generateDataMaintenanceInput();
        TMarketFeedTxnInput&         generateMarketFeedInput();
        TMarketWatchTxnInput&        generateMarketWatchInput();
        TSecurityDetailTxnInput&     generateSecurityDetailInput();
        TTradeCleanupTxnInput&       generateTradeCleanupInput();
        TTradeLookupTxnInput&        generateTradeLookupInput();
        TTradeOrderTxnInput&         generateTradeOrderInput(INT32 &iTradeType, bool &bExecutorIsAccountOwner);
        TTradeResultTxnInput&        generateTradeResultInput();
        TTradeStatusTxnInput&        generateTradeStatusInput();
        TTradeUpdateTxnInput&        generateTradeUpdateInput();
        
    private:
        string m_dataPath;
        int m_configuredCustomerCount;
        int m_totalCustomerCount;
        int m_scaleFactor;
        int m_initialDays;
    
        TDriverCETxnSettings    m_DriverCETxnSettings;
        CLogFormatTab           m_LogFormat;
        CBaseLogger*            m_Logger;
        CCETxnInputGenerator*   m_TxnInputGenerator;
        CDM*                    m_DataMaintenanceGenerator;
        CDMSUTInterface*        m_DataMaintenanceCallback;
        CMEE*                   m_MarketExchangeGenerator;
        CMEESUTInterface*       m_MarketExchangeCallback;
    
        TBrokerVolumeTxnInput       m_BrokerVolumeTxnInput;
        TCustomerPositionTxnInput   m_CustomerPositionTxnInput;
        TDataMaintenanceTxnInput    m_DataMaintenanceTxnInput;
        TMarketFeedTxnInput         m_MarketFeedTxnInput;
        TMarketWatchTxnInput        m_MarketWatchTxnInput;
        TSecurityDetailTxnInput     m_SecurityDetailTxnInput;
        TTradeCleanupTxnInput       m_TradeCleanupTxnInput;
        TTradeLookupTxnInput        m_TradeLookupTxnInput;
        TTradeOrderTxnInput         m_TradeOrderTxnInput;
        TTradeResultTxnInput        m_TradeResultTxnInput;
        TTradeStatusTxnInput        m_TradeStatusTxnInput;
        TTradeUpdateTxnInput        m_TradeUpdateTxnInput;
};

}

#endif
