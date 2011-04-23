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

#ifndef EGEN_TEST_HARNESS_H
#define EGEN_TEST_HARNESS_H

/*
*   Test harness to run two versions of EGen (Reference and Current) in lockstep
*   and compare the results. Useful for regression testing.
*/

#include "EGenTestSuites_stdafx.h"

namespace TPCETest
{

class CEGenTestHarness
{
    // Structure containing references to input files loaded into memory
    RefTPCE::CInputFiles        m_RefInputFiles;    // reference code input files
    TPCE::CInputFiles           m_inputFiles;
    // Ordinal position (1-based) of the first customer in the sequence
    TIdent                      m_iStartFromCustomer;
    // The number of customers to generate from the starting position
    TIdent                      m_iCustomerCount;
    // Total number of customers in the database
    TIdent                      m_iTotalCustomerCount;
    // Number of customers in one load unit for generating initial trades
    int                         m_iLoadUnitSize;
    // Number of customers per 1 tpsE
    int                         m_iScaleFactor;
    // Time period for which to generate initial trades
    int                         m_iDaysOfInitialTrades;

    // External class used to output load progress
    TPCE::CGenerateAndLoadBaseOutput* m_pOutput;
    // Input flat file directory for tables loaded via flat files
    char                        m_szRefInDir[RefTPCE::iMaxPath];
    char                        m_szInDir[TPCE::iMaxPath];
    // Logger instance
    TPCE::CBaseLogger*          m_pLogger;
    // Parameter instance
    TPCE::CLoaderSettings       m_LoaderSettings;

    // Test suite to run.
    eTestSuite                  m_eTestSuite;

    /*
    *   Run test suite One.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void RunSuiteOne();

    /*
    *   Run test suite Two.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void RunSuiteTwo();

public:
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
    *           IN  szRefInDir          - input flat file directory needed for tables loaded from flat files (Reference)
    *           IN  szInDir             - input flat file directory needed for tables loaded from flat files (Current)
    *           IN  eSuite              - test suite to run (one-to-one or one-to-many instances)
    *
    *  RETURNS:
    *           not applicable.
    */
    CEGenTestHarness(RefTPCE::CInputFiles               RefInputFiles,
                    TPCE::CInputFiles                   inputFiles,
                    TIdent                              iCustomerCount,
                    TIdent                              iStartFromCustomer,
                    TIdent                              iTotalCustomerCount,
                    int                                 iLoadUnitSize,
                    int                                 iScaleFactor,
                    int                                 iDaysOfInitialTrades,
                    TPCE::CBaseLogger*                  pLogger,
                    TPCE::CGenerateAndLoadBaseOutput*   pOutput,
                    char*                               szRefInDir, // directory for input flat files
                    char*                               szInDir,
                    eTestSuite                          eSuite);

    /*
    *   Run all test suites.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void RunTestSuites();
};

}   // namespace TPCETest

#endif // EGEN_TEST_HARNESS_H
