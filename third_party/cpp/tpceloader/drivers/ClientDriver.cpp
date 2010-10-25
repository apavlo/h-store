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
#include "ClientDriver.h"

#include <InputFlatFilesStructure.h>
#include <TxnHarnessStructs.h>
#include <EGenLogger.h>

namespace TPCE {

class DataMaintenanceCallback : public CDMSUTInterface {
    public:
        DataMaintenanceCallback(PDataMaintenanceTxnInput dmTxnInput, 
                                PTradeCleanupTxnInput tcTxnInput) :
            m_DataMaintenanceTxnInput(dmTxnInput),
            m_TradeCleanupTxnInput(tcTxnInput) {
            // Nothing else...
        }
    
        virtual bool DataMaintenance( PDataMaintenanceTxnInput pTxnInput ) {
            #ifdef DEBUG
            fprintf(stderr, "%s callback!\n", __FUNCTION__);
            #endif
        
            m_DataMaintenanceTxnInput->acct_id = pTxnInput->acct_id;
            m_DataMaintenanceTxnInput->c_id = pTxnInput->c_id;
            m_DataMaintenanceTxnInput->co_id = pTxnInput->co_id;
            m_DataMaintenanceTxnInput->day_of_month = pTxnInput->day_of_month;
            m_DataMaintenanceTxnInput->vol_incr = pTxnInput->vol_incr;
            strncpy(m_DataMaintenanceTxnInput->symbol, pTxnInput->symbol, sizeof(pTxnInput->symbol));
            strncpy(m_DataMaintenanceTxnInput->table_name, pTxnInput->table_name, sizeof(pTxnInput->table_name));
            strncpy(m_DataMaintenanceTxnInput->tx_id, pTxnInput->tx_id, sizeof(pTxnInput->tx_id));
            
            return (true);
        }
        
        virtual bool TradeCleanup( PTradeCleanupTxnInput pTxnInput ) {
            #ifdef DEBUG
            fprintf(stderr, "%s callback!\n", __FUNCTION__);
            #endif
            
            m_TradeCleanupTxnInput->start_trade_id = pTxnInput->start_trade_id;
            strncpy(m_TradeCleanupTxnInput->st_canceled_id, pTxnInput->st_canceled_id, sizeof(pTxnInput->st_canceled_id));
            strncpy(m_TradeCleanupTxnInput->st_pending_id, pTxnInput->st_pending_id, sizeof(pTxnInput->st_pending_id));
            strncpy(m_TradeCleanupTxnInput->st_submitted_id, pTxnInput->st_submitted_id, sizeof(pTxnInput->st_submitted_id));
            
            return (true);
        }
        
    private:
        PDataMaintenanceTxnInput m_DataMaintenanceTxnInput;
        PTradeCleanupTxnInput    m_TradeCleanupTxnInput;
};

class MarketExchangeCallback : public CMEESUTInterface {
    public:
        MarketExchangeCallback(PTradeResultTxnInput trTxnInput, 
                               PMarketFeedTxnInput mfTxnInput) :
            m_TradeResultTxnInput(trTxnInput),
            m_MarketFeedTxnInput(mfTxnInput) {
            // Nothing else...
        }
    
        virtual bool TradeResult( PTradeResultTxnInput pTxnInput ) {
            #ifdef DEBUG
            fprintf(stderr, "%s callback!\n", __FUNCTION__);
            #endif
        
            m_TradeResultTxnInput->trade_id = pTxnInput->trade_id;
            m_TradeResultTxnInput->trade_price = pTxnInput->trade_price;
            // strncpy(m_TradeResultTxnInput->st_completed_id, pTxnInput->st_completed_id, sizeof(pTxnInput->st_completed_id));
            
            return (true);
        }
        
        virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput ) {
            #ifdef DEBUG
            fprintf(stderr, "%s callback!\n", __FUNCTION__);
            #endif
            
            for (int i = 0; i < max_feed_len; i++) {
                m_MarketFeedTxnInput->Entries[i] = pTxnInput->Entries[i];
            } // FOR
            m_MarketFeedTxnInput->StatusAndTradeType = pTxnInput->StatusAndTradeType;
            
            return (true);
        }
        
    private:
        PTradeResultTxnInput    m_TradeResultTxnInput;
        PMarketFeedTxnInput     m_MarketFeedTxnInput;
};


ClientDriver::ClientDriver(string dataPath, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays) :
        m_dataPath(dataPath),
        m_configuredCustomerCount(configuredCustomerCount),
        m_totalCustomerCount(totalCustomerCount),
        m_scaleFactor(scaleFactor),
        m_initialDays(initialDays) {
        
    #ifdef DEBUG
    fprintf(stderr, "%-15s %s\n", "m_dataPath", m_dataPath.c_str());
    fprintf(stderr, "%-15s %d\n", "m_configuredCustomerCount", m_configuredCustomerCount);
    fprintf(stderr, "%-15s %d\n", "m_totalCustomerCount", m_totalCustomerCount);
    fprintf(stderr, "%-15s %d\n", "m_scaleFactor", m_scaleFactor);
    fprintf(stderr, "%-15s %d\n", "m_initialDays", m_initialDays);
    #endif
        
    const char *filename = "/tmp/EGenClientDriver.log";
    m_Logger = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_LogFormat);

    // Setup the input generator object that we will use
    CInputFiles inputFiles;
    inputFiles.Initialize(eDriverCE, m_configuredCustomerCount, m_totalCustomerCount, m_dataPath.c_str());
    
    m_TxnInputGenerator = new CCETxnInputGenerator(inputFiles, m_configuredCustomerCount, m_totalCustomerCount, m_scaleFactor, m_initialDays * HoursPerWorkDay, m_Logger, &m_DriverCETxnSettings);
    m_TxnInputGenerator->UpdateTunables();
    
    // We also need a DataMaintenance input generator
    m_DataMaintenanceCallback = new DataMaintenanceCallback(&m_DataMaintenanceTxnInput, &m_TradeCleanupTxnInput);
    m_DataMaintenanceGenerator = new CDM(m_DataMaintenanceCallback, m_Logger, inputFiles, m_configuredCustomerCount, m_totalCustomerCount, m_scaleFactor, m_initialDays, 1);
    
    // As well as our trusty ol' MarketExchange input generator
    m_MarketExchangeCallback = new MarketExchangeCallback(&m_TradeResultTxnInput, &m_MarketFeedTxnInput);
    m_MarketExchangeGenerator = new CMEE(0, m_MarketExchangeCallback, m_Logger, inputFiles.Securities, 1);
    m_MarketExchangeGenerator->EnableTickerTape();
}

/**
 *
 **/
ClientDriver::~ClientDriver() {
    delete m_TxnInputGenerator;
    delete m_Logger;
}

TBrokerVolumeTxnInput& ClientDriver::generateBrokerVolumeInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );
    return (m_BrokerVolumeTxnInput);
}

TCustomerPositionTxnInput& ClientDriver::generateCustomerPositionInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateCustomerPositionInput( m_CustomerPositionTxnInput );
    return (m_CustomerPositionTxnInput);
}

TDataMaintenanceTxnInput& ClientDriver::generateDataMaintenanceInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_DataMaintenanceGenerator->DoTxn();
    return (m_DataMaintenanceTxnInput);
}

TMarketFeedTxnInput& ClientDriver::generateMarketFeedInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    // m_TxnInputGenerator->GenerateMarketFeedInput( m_MarketFeedTxnInput );
    return (m_MarketFeedTxnInput);
}

TMarketWatchTxnInput& ClientDriver::generateMarketWatchInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateMarketWatchInput( m_MarketWatchTxnInput );
    return (m_MarketWatchTxnInput);
}

TSecurityDetailTxnInput& ClientDriver::generateSecurityDetailInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateSecurityDetailInput( m_SecurityDetailTxnInput );
    return (m_SecurityDetailTxnInput);
}

TTradeCleanupTxnInput& ClientDriver::generateTradeCleanupInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_DataMaintenanceGenerator->DoCleanupTxn();
    return (m_TradeCleanupTxnInput);
}

TTradeLookupTxnInput& ClientDriver::generateTradeLookupInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateTradeLookupInput( m_TradeLookupTxnInput );
    return (m_TradeLookupTxnInput);
}

TTradeOrderTxnInput& ClientDriver::generateTradeOrderInput(INT32 &iTradeType, bool &bExecutorIsAccountOwner) {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateTradeOrderInput( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
    return (m_TradeOrderTxnInput);
}

TTradeResultTxnInput& ClientDriver::generateTradeResultInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_MarketExchangeGenerator->GenerateTradeResult();
    return (m_TradeResultTxnInput);
}

TTradeStatusTxnInput& ClientDriver::generateTradeStatusInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateTradeStatusInput( m_TradeStatusTxnInput );
    return (m_TradeStatusTxnInput);
}

TTradeUpdateTxnInput& ClientDriver::generateTradeUpdateInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", __FUNCTION__);
    #endif
    m_TxnInputGenerator->GenerateTradeUpdateInput( m_TradeUpdateTxnInput );
    return (m_TradeUpdateTxnInput);
}

}