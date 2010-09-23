/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
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
#include <EGenLogger.h>

namespace TPCE {

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
        
    //
    // Setup the input generator object that we will use
    //
    CInputFiles inputFiles;
    inputFiles.Initialize(eDriverCE, m_configuredCustomerCount, m_totalCustomerCount, m_dataPath.c_str());
    
    const char *filename = "/tmp/EGenClientDriver.log";
    m_Logger = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_LogFormat);

    m_TxnInputGenerator = new CCETxnInputGenerator(inputFiles, m_configuredCustomerCount, m_totalCustomerCount, m_scaleFactor, m_initialDays * HoursPerWorkDay, m_Logger, &m_DriverCETxnSettings);
    m_TxnInputGenerator->UpdateTunables();
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
    fprintf(stderr, "Executing %s...\n", "generateBrokerVolumeInput");
    #endif
    m_TxnInputGenerator->GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );
    return (m_BrokerVolumeTxnInput);
}

TCustomerPositionTxnInput& ClientDriver::generateCustomerPositionInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateCustomerPositionInput");
    #endif
    m_TxnInputGenerator->GenerateCustomerPositionInput( m_CustomerPositionTxnInput );
    return (m_CustomerPositionTxnInput);
}

TMarketWatchTxnInput& ClientDriver::generateMarketWatchInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateMarketWatchInput");
    #endif
    m_TxnInputGenerator->GenerateMarketWatchInput( m_MarketWatchTxnInput );
    return (m_MarketWatchTxnInput);
}

TSecurityDetailTxnInput& ClientDriver::generateSecurityDetailInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateSecurityDetailInput");
    #endif
    m_TxnInputGenerator->GenerateSecurityDetailInput( m_SecurityDetailTxnInput );
    return (m_SecurityDetailTxnInput);
}

TTradeLookupTxnInput& ClientDriver::generateTradeLookupInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateTradeLookupInput");
    #endif
    m_TxnInputGenerator->GenerateTradeLookupInput( m_TradeLookupTxnInput );
    return (m_TradeLookupTxnInput);
}

TTradeOrderTxnInput& ClientDriver::generateTradeOrderInput(INT32 &iTradeType, bool &bExecutorIsAccountOwner) {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateTradeOrderInput");
    #endif
    m_TxnInputGenerator->GenerateTradeOrderInput( m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner );
    return (m_TradeOrderTxnInput);
}

TTradeStatusTxnInput& ClientDriver::generateTradeStatusInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateTradeStatusInput");
    #endif
    m_TxnInputGenerator->GenerateTradeStatusInput( m_TradeStatusTxnInput );
    return (m_TradeStatusTxnInput);
}

TTradeUpdateTxnInput& ClientDriver::generateTradeUpdateInput() {
    #ifdef DEBUG
    fprintf(stderr, "Executing %s...\n", "generateTradeUpdateInput");
    #endif
    m_TxnInputGenerator->GenerateTradeUpdateInput( m_TradeUpdateTxnInput );
    return (m_TradeUpdateTxnInput);
}

}