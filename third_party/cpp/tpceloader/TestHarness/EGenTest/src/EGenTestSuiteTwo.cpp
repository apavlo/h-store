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
*   Test suite one implementation.
*
*   This suite runs one instance of reference loader code with one instance
*   of current loader code. 
*
*   It is intended to simulate a single instance EGenLoader configuration.
*/

#include "../inc/EGenTestSuites_stdafx.h"

namespace TPCETest
{

const INT32     g_iCEUniqueId = 1;  // instance number for CE constructor
const INT32     g_iDMUniqueId = 2;  // instance number for DM constructor
const UINT64    g_TxnMixRNGSeed = 87944308;   // taken from RNGSeeds.h
const UINT64    g_TxnInputRNGSeed = 80534927; // taken from RNGSeeds.h

const int       g_iTCIterationCount = 10;
}

using namespace TPCETest;

/*
*   Constructor.
*
*  PARAMETERS:
*           IN  RefInputFiles       - in-memory representation of Reference input flat files
*           IN  inputFiles          - in-memory representation of Current input flat files
*           IN  iCustomerCount      - number of customers to build (for this class instance)
*           IN  iStartFromCustomer  - first customer id
*           IN  iTotalCustomerCount - total number of customers in the database
*           IN  iLoadUnitSize       - minimal number of customers that can be build (should always be 1000)
*           IN  iScaleFactor        - number of customers for 1tpsE
*           IN  iDaysOfInitialTrades- number of 8-hour days of initial trades per customer
*           IN  pRefLogger          - parameter logging interface (from reference code)
*           IN  pLogger             - parameter logging interface
*           IN  pOutput             - interface to output information to a user during the build process
*           IN  iCEIterationCount   - number of CE transactions to generate
*           IN  iDMIterationCount   - number of DM (and TC) transactions to generate
*
*  RETURNS:
*           not applicable.
*/
CEGenTestSuiteTwo::CEGenTestSuiteTwo(RefTPCE::CInputFiles           RefInputFiles,
                                TPCE::CInputFiles                   inputFiles,
                                TIdent                              iCustomerCount,
                                TIdent                              iStartFromCustomer,
                                TIdent                              iTotalCustomerCount,
                                int                                 iLoadUnitSize,
                                int                                 iScaleFactor,
                                int                                 iDaysOfInitialTrades,
                                RefTPCE::CBaseLogger*               pRefLogger,
                                TPCE::CBaseLogger*                  pLogger,
                                TPCE::CGenerateAndLoadBaseOutput*   pOutput,
                                int                                 iCEIterationCount,
                                int                                 iDMIterationCount)
: m_RefInputFiles(RefInputFiles)
, m_inputFiles(inputFiles)
, m_iStartFromCustomer(iStartFromCustomer)
, m_iCustomerCount(iCustomerCount)
, m_iTotalCustomerCount(iTotalCustomerCount)
, m_iLoadUnitSize(iLoadUnitSize)
, m_iScaleFactor(iScaleFactor)
, m_iDaysOfInitialTrades(iDaysOfInitialTrades)
, m_pOutput(pOutput)
, m_pRefLogger(pRefLogger)
, m_pLogger(pLogger)
, m_iCEIterationCount(iCEIterationCount)
, m_iDMIterationCount(iDMIterationCount)
{
};

/*
*   Run all the tests in the suite.
*   This function is part of the base interface.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteTwo::Run()
{
    m_pLogger->SendToLogger("Running suite Two (match transaction inputs).");

    TestCE();
    TestDM();
    TestTC();

    m_pLogger->SendToLogger("Suite Two is done.");
}

/*
*   Generate and test Customer Emulator driver.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteTwo::TestCE()
{
    char                        szMsg[iMaxTestErrMsg];
    int                         i;
    //  Reference code.
    //
    RefTPCE::CCE      RefCE(&m_SUT,
                            m_pRefLogger,
                            m_RefInputFiles,
                            m_iCustomerCount,
                            m_iCustomerCount, // Active customers
                            m_iScaleFactor,
                            m_iDaysOfInitialTrades,
                            g_iCEUniqueId,
                            g_TxnMixRNGSeed,
                            g_TxnInputRNGSeed);
    //  Current code.
    //
    TPCE::CCE         CE(   &m_SUT,
                            m_pLogger,
                            m_inputFiles,
                            m_iCustomerCount,
                            m_iCustomerCount, // Active customers
                            m_iScaleFactor,
                            m_iDaysOfInitialTrades,
                            g_iCEUniqueId,
                            g_TxnMixRNGSeed,
                            g_TxnInputRNGSeed);

    //  Set zero buffer flag. Otherwise, bitwise comparison will not work.
    //
    RefCE.SetClearBufferOption(true);
    CE.SetClearBufferOption(true);

    m_pOutput->OutputStart("Testing Customer Emulator...");

    for (i = 0; i < m_iCEIterationCount; ++i)
    {
        RefCE.DoTxn();
        CE.DoTxn();

        //  Compare two transactions.
        //
        if (m_SUT.InputDiffers())
        {
            if (m_SUT.GetRefTxnType() != m_SUT.GetCurTxnType())
            {
                sprintf(szMsg, "Transaction type (%s) for transaction #%d is different from reference (%s).",
                        g_szTxnTypes[m_SUT.GetCurTxnType()], i, g_szTxnTypes[m_SUT.GetRefTxnType()]);
            }
            else
            {
                sprintf(szMsg, "Transaction input for transaction #%d (%s) is different from reference.", 
                        i, g_szTxnTypes[m_SUT.GetRefTxnType()]);
            }

            throw CEGenTestErr(szMsg);
        }
    };

    sprintf(szMsg, "done (%d transactions).", i);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test Data Maintenance driver.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteTwo::TestDM()
{
    char                        szMsg[iMaxTestErrMsg];
    int                         i;
    //  Reference code.
    //
    RefTPCE::CDM      RefDM(&m_SUT,
                            m_pRefLogger,
                            m_RefInputFiles,
                            m_iCustomerCount,
                            m_iCustomerCount, // Active customers
                            m_iScaleFactor,
                            m_iDaysOfInitialTrades,
                            g_iDMUniqueId,
                            g_TxnInputRNGSeed);
    //  Current code.
    //
    TPCE::CDM         DM(   &m_SUT,
                            m_pLogger,
                            m_inputFiles,
                            m_iCustomerCount,
                            m_iCustomerCount, // Active customers
                            m_iScaleFactor,
                            m_iDaysOfInitialTrades,
                            g_iDMUniqueId,
                            g_TxnInputRNGSeed);


    m_pOutput->OutputStart("Testing Data Maintenance...");

    for (i = 0; i < m_iDMIterationCount; ++i)
    {
        RefDM.DoTxn();
        DM.DoTxn();

        //  Compare two transactions.
        //
        if (m_SUT.InputDiffers())
        {
            if (m_SUT.GetRefTxnType() != m_SUT.GetCurTxnType())
            {
                sprintf(szMsg, "Transaction type for transaction #%d is different from reference.", i);
            }
            else
            {
                sprintf(szMsg, "Transaction input for transaction #%d is different from reference.", i);
            }

            throw CEGenTestErr(szMsg);
        }
    };

    sprintf(szMsg, "done (%d transactions).", i);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test Trade Cleanup in Data Maintenance driver.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteTwo::TestTC()
{
    char                        szMsg[iMaxTestErrMsg];
    int                         i;
    //  Reference code.
    //
    RefTPCE::CDM      RefDM(&m_SUT,
                            m_pRefLogger,
                            m_RefInputFiles,
                            m_iCustomerCount,
                            m_iCustomerCount, // Active customers
                            m_iScaleFactor,
                            m_iDaysOfInitialTrades,
                            g_iDMUniqueId,
                            g_TxnInputRNGSeed);
    //  Current code.
    //
    TPCE::CDM         DM(   &m_SUT,
                            m_pLogger,
                            m_inputFiles,
                            m_iCustomerCount,
                            m_iCustomerCount, // Active customers
                            m_iScaleFactor,
                            m_iDaysOfInitialTrades,
                            g_iDMUniqueId,
                            g_TxnInputRNGSeed);


    m_pOutput->OutputStart("Testing Trade Cleanup...");

    for (i = 0; i < g_iTCIterationCount; ++i)   // no need to run many Trade Cleanup (input doesn't vary)
    {
        RefDM.DoCleanupTxn();
        DM.DoCleanupTxn();

        //  Compare two transactions.
        //
        if (m_SUT.InputDiffers())
        {
            if (m_SUT.GetRefTxnType() != m_SUT.GetCurTxnType())
            {
                sprintf(szMsg, "Transaction type for transaction #%d is different from reference.", i);
            }
            else
            {
                sprintf(szMsg, "Transaction input for transaction #%d is different from reference.", i);
            }

            throw CEGenTestErr(szMsg);
        }
    };

    sprintf(szMsg, "done (%d transactions).", i);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}