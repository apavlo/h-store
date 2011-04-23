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
*   Test harness to run two versions of EGen (Reference and Current) in lockstep
*   and compare the results. Useful for regression testing.
*/

//  Include EGen Test code.
//
#include "../inc/EGenTestHarness.h"

using namespace std;

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
*           IN  szInDir             - input flat file directory needed for tables loaded from flat files
*           IN  eSuite              - test suite to run (one-to-one or one-to-many instances)
*
*  RETURNS:
*           not applicable.
*/
CEGenTestHarness::CEGenTestHarness(RefTPCE::CInputFiles             RefInputFiles,
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
                                char*                               szRefInDir, // directory for input flat files
                                char*                               szInDir,
                                eTestSuite                          eSuite)
: m_RefInputFiles(RefInputFiles)
, m_inputFiles(inputFiles)
, m_iStartFromCustomer(iStartFromCustomer)
, m_iCustomerCount(iCustomerCount)
, m_iTotalCustomerCount(iTotalCustomerCount)
, m_iLoadUnitSize(iLoadUnitSize)
, m_iScaleFactor(iScaleFactor)
, m_iDaysOfInitialTrades(iDaysOfInitialTrades)
, m_pOutput(pOutput)
, m_pLogger(pLogger)
, m_LoaderSettings(iTotalCustomerCount, iTotalCustomerCount, iStartFromCustomer, iCustomerCount, iScaleFactor, iDaysOfInitialTrades )
, m_eTestSuite(eSuite)
{
    // Copy input flat file directory needed for tables loaded from flat files.
    strncpy( m_szRefInDir, szRefInDir, sizeof(m_szRefInDir)-1);
    strncpy( m_szInDir, szInDir, sizeof(m_szInDir)-1);

    // Log Parameters
    m_pLogger->SendToLogger(m_LoaderSettings);
};

/*
*   Run test suite One.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestHarness::RunSuiteOne()
{
    CEGenTestSuiteOne   Suite(
            m_RefInputFiles,
            m_inputFiles,
            m_iCustomerCount,
            m_iStartFromCustomer,
            m_iTotalCustomerCount,
            m_iLoadUnitSize,
            m_iScaleFactor,
            m_iDaysOfInitialTrades,
            m_pLogger,
            m_pOutput,
            m_szRefInDir,
            m_szInDir);

    Suite.Run();
}

/*
*   Run test suite Two.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestHarness::RunSuiteTwo()
{
    CEGenTestSuiteTwo   Suite(
            m_RefInputFiles,
            m_inputFiles,
            m_iCustomerCount,
            m_iStartFromCustomer,
            m_iTotalCustomerCount,
            m_iLoadUnitSize,
            m_iScaleFactor,
            m_iDaysOfInitialTrades,
            m_pLogger,
            m_pOutput,
            m_szRefInDir,
            m_szInDir);

    Suite.Run();
}

/*
*   Run all test suites.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestHarness::RunTestSuites()
{
    if (m_eTestSuite == eTestSuiteAll || m_eTestSuite == eTestSuiteOne)
    {
        RunSuiteOne();
    }

    if (m_eTestSuite == eTestSuiteAll || m_eTestSuite == eTestSuiteTwo)
    {
        RunSuiteTwo();
    }
}