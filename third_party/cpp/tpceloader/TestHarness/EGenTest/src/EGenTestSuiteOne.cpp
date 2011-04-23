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
*   This suite runs one instance of reference loader code with two instances
*   of current loader code. 
*
*   It is intended to simulate a multiple-instance EGenLoader configuration.
*/

#include "../inc/EGenTestSuites_stdafx.h"

using namespace TPCETest;

/*
*   Constructor.
*
*  PARAMETERS:
*           IN  RefInputFiles       - in-memory representation reference of input flat files
*           IN  inputFiles          - in-memory representation of current input flat files
*           IN  iCustomerCount      - number of customers to build (for this class instance)
*           IN  iStartFromCustomer  - first customer id
*           IN  iTotalCustomerCount - total number of customers in the database
*           IN  iLoadUnitSize       - minimal number of customers that can be build (should always be 1000)
*           IN  iScaleFactor        - number of customers for 1tpsE
*           IN  iDaysOfInitialTrades- number of 8-hour days of initial trades per customer
*           IN  pLogger             - parameter logging interface
*           IN  pOutput             - interface to output information to a user during the build process
*           IN  szRefInDir          - input flat file directory needed for tables loaded from flat files (Reference)
*           IN  szInDir             - input flat file directory needed for tables loaded from flat files
*
*  RETURNS:
*           not applicable.
*/
CEGenTestSuiteOne::CEGenTestSuiteOne(RefTPCE::CInputFiles           RefInputFiles,
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
                                char*                               szInDir)
: m_RefInputFiles(RefInputFiles)
, m_inputFiles(inputFiles)
, m_iStartFromCustomer(iStartFromCustomer)
, m_iCustomerCount(iCustomerCount)
, m_iTotalCustomerCount(iTotalCustomerCount)
, m_iLoadUnitSize(iLoadUnitSize)
, m_iScaleFactor(iScaleFactor)
, m_iRefHoursOfInitialTrades(iDaysOfInitialTrades * RefTPCE::HoursPerWorkDay)
, m_iHoursOfInitialTrades(iDaysOfInitialTrades * TPCE::HoursPerWorkDay)
, m_pOutput(pOutput)
, m_pLogger(pLogger)
, m_iStartFromCustomerFirstInstance(m_iStartFromCustomer)
{
    // Copy input flat file directory needed for tables loaded from flat files.
    //
    strncpy( m_szRefInDir, szRefInDir, sizeof(m_szRefInDir)-1);
    strncpy( m_szInDir, szInDir, sizeof(m_szInDir)-1);

    // Split customer range between two instances.
    //
    m_iCustomerCountFirstInstance = (m_iCustomerCount / (2 * m_iLoadUnitSize)) * m_iLoadUnitSize;
    m_iStartFromCustomerSecondInstance = m_iStartFromCustomerFirstInstance + m_iCustomerCountFirstInstance;
    m_iCustomerCountSecondInstance = m_iCustomerCount - m_iCustomerCountFirstInstance;
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
void CEGenTestSuiteOne::Run()
{
    char    szMsg[iMaxTestErrMsg];

    sprintf(szMsg, "Running suite One (match one Reference loader with two Current loaders (customers %" PRId64 "-%" PRId64 ",%" PRId64 "-%" PRId64 ")).",
            m_iStartFromCustomerFirstInstance, 
            m_iStartFromCustomerFirstInstance + m_iCustomerCountFirstInstance - 1,
            m_iStartFromCustomerSecondInstance, 
            m_iStartFromCustomerSecondInstance + m_iCustomerCountSecondInstance - 1);
    m_pLogger->SendToLogger(szMsg);
    
    TestAddress();
    TestCharge();
    TestCommissionRate();
    TestCompanyCompetitor();
    TestCompany();
    TestCustomerAccountAndPermission();
    TestCustomer();
    TestCustomerTaxrate();
    TestDailyMarket();
    TestExchange();
    TestFinancial();
    TestIndustry();
    TestLastTrade();
    NewsItemAndXRef();
    TestSector();
    TestSecurity();
    TestStatusType();
    TestTaxrate();
    TestTradeGen();
    TestTradeType();
    TestWatchListsAndItems();
    TestZipCode();

    m_pLogger->SendToLogger("Suite One is done.");
}

/*
*   Generate and test ADDRESS table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestAddress()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CAddressTable      RefTable(m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer,
                                        // do not generate exchange and company addresses
                                        // if the starting customer is not 1
                                        m_iStartFromCustomer != RefTPCE::iDefaultStartFromCustomer);

    CTestLoader<RefTPCE::ADDRESS_ROW, TPCE::ADDRESS_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing ADDRESS table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CAddressTable         TableFirstInstance(
                                    m_inputFiles, m_iCustomerCountFirstInstance, m_iStartFromCustomerFirstInstance,
                                    // do not generate exchange and company addresses
                                    // if the starting customer is not 1
                                    m_iStartFromCustomerFirstInstance != TPCE::iDefaultStartFromCustomer);
        do
        {


            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CAddressTable         TableSecondInstance(
                                    m_inputFiles, m_iCustomerCountSecondInstance, m_iStartFromCustomerSecondInstance,
                                    // do not generate exchange and company addresses
                                    // if the starting customer is not 1
                                    m_iStartFromCustomerSecondInstance != TPCE::iDefaultStartFromCustomer);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test CHARGE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCharge()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CChargeTable      RefTable(m_szRefInDir);

    CTestLoader<RefTPCE::CHARGE_ROW, TPCE::CHARGE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing CHARGE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CChargeTable         TableFirstInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        };
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CChargeTable         TableSecondInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

        };
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test COMMISSION_RATE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCommissionRate()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CCommissionRateTable      RefTable(m_szRefInDir);

    CTestLoader<RefTPCE::COMMISSION_RATE_ROW, TPCE::COMMISSION_RATE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing COMMISSION_RATE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCommissionRateTable         TableFirstInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

        };
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCommissionRateTable         TableSecondInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

        };
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test COMPANY_COMPETITOR table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCompanyCompetitor()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CCompanyCompetitorTable      RefTable(
                                            m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer);

    CTestLoader<RefTPCE::COMPANY_COMPETITOR_ROW, TPCE::COMPANY_COMPETITOR_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing COMPANY_COMPETITOR table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCompanyCompetitorTable         TableFirstInstance(
                                            m_inputFiles, m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCompanyCompetitorTable         TableSecondInstance(
                                            m_inputFiles, m_iCustomerCountSecondInstance, 
                                            m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test COMPANY table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCompany()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CCompanyTable      RefTable(
                                            m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer);
    

    

    CTestLoader<RefTPCE::COMPANY_ROW, TPCE::COMPANY_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing COMPANY table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCompanyTable         TableFirstInstance(
                                            m_inputFiles, m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCompanyTable         TableSecondInstance(
                                    m_inputFiles, m_iCustomerCountSecondInstance, 
                                    m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test CUSTOMER_ACCOUNT and ACCOUNT_PERMISSION tables.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCustomerAccountAndPermission()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CCustomerAccountsAndPermissionsTable      RefTable(
                                            m_RefInputFiles, m_iLoadUnitSize, 
                                            m_iCustomerCount, m_iStartFromCustomer);    

    CTestLoader<RefTPCE::CUSTOMER_ACCOUNT_ROW, TPCE::CUSTOMER_ACCOUNT_ROW>          CALoad;
    CTestLoader<RefTPCE::ACCOUNT_PERMISSION_ROW, TPCE::ACCOUNT_PERMISSION_ROW>      APLoad;
    INT64                       iCnt=0;
    int                         i;

    m_pOutput->OutputStart("Testing CUSTOMER_ACCOUNT and ACCOUNT_PERMISSION tables...");

    {
        //  Current table code.
        //
        TPCE::CCustomerAccountsAndPermissionsTable         TableFirstInstance(
                                            m_inputFiles, 
                                            m_iLoadUnitSize, 
                                            m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            //  Verify CUSTOMER_ACCOUNT row.
            //
            CALoad.WriteNextRefRecord(RefTable.GetCARow());

            CALoad.WriteNextRecord(TableFirstInstance.GetCARow());

            if (++iCnt % 10000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (CALoad.RowsDiffer())
            {
                sprintf(szMsg, "CA row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Verify ACCOUNT_PERMISSION rows.
            //

            //  First, verify that the number of rows match.
            //
            if (RefTable.GetCAPermsCount() != TableFirstInstance.GetCAPermsCount())
            {
                sprintf(szMsg, "Number of AP rows (%d) for CA row %" PRId64 " is different from reference (%d).",
                    TableFirstInstance.GetCAPermsCount(), iCnt, RefTable.GetCAPermsCount());

                throw CEGenTestErr(szMsg);
            }

            //  Verify ACCOUNT_PERMISSION row data.
            //
            for(i=0; i<RefTable.GetCAPermsCount(); ++i)
            {
                APLoad.WriteNextRefRecord(RefTable.GetAPRow(i));

                APLoad.WriteNextRecord(TableFirstInstance.GetAPRow(i));

                if (APLoad.RowsDiffer())
                {
                    sprintf(szMsg, "AP row %d for CA row %" PRId64 " is different from reference.", i, iCnt);

                    throw CEGenTestErr(szMsg);
                }
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current table code.
        //
        TPCE::CCustomerAccountsAndPermissionsTable         TableSecondInstance(
                                                            m_inputFiles, 
                                                            m_iLoadUnitSize, 
                                                            m_iCustomerCountSecondInstance, 
                                                            m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            //  Verify CUSTOMER_ACCOUNT row.
            //
            CALoad.WriteNextRefRecord(RefTable.GetCARow());

            CALoad.WriteNextRecord(TableSecondInstance.GetCARow());

            if (++iCnt % 10000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two CUSTOMER_ACCOUNT rows.
            //
            if (CALoad.RowsDiffer())
            {
                sprintf(szMsg, "CA row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Verify ACCOUNT_PERMISSION rows.
            //

            //  First, verify that the number of rows match.
            //
            if (RefTable.GetCAPermsCount() != TableSecondInstance.GetCAPermsCount())
            {
                sprintf(szMsg, "Number of AP rows (%d) for CA row %" PRId64 " is different from reference (%d).",
                    TableSecondInstance.GetCAPermsCount(), iCnt, RefTable.GetCAPermsCount());

                throw CEGenTestErr(szMsg);
            }

            //  Verify ACCOUNT_PERMISSION row data.
            //
            for(i=0; i<RefTable.GetCAPermsCount(); ++i)
            {
                APLoad.WriteNextRefRecord(RefTable.GetAPRow(i));

                APLoad.WriteNextRecord(TableSecondInstance.GetAPRow(i));

                if (APLoad.RowsDiffer())
                {
                    sprintf(szMsg, "AP row %d for CA row %" PRId64 " is different from reference.", i, iCnt);

                    throw CEGenTestErr(szMsg);
                }
            }
        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " CA rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test CUSTOMER table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCustomer()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CCustomerTable      RefTable(
                                            m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer);
    

    

    CTestLoader<RefTPCE::CUSTOMER_ROW, TPCE::CUSTOMER_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing CUSTOMER table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCustomerTable         TableFirstInstance(
                                            m_inputFiles, m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCustomerTable         TableSecondInstance(
                                        m_inputFiles, m_iCustomerCountSecondInstance, 
                                        m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test CUSTOMER_TAXRATE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestCustomerTaxrate()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CCustomerTaxratesTable      RefTable(
                                            m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer);

    CTestLoader<RefTPCE::CUSTOMER_TAXRATE_ROW, TPCE::CUSTOMER_TAXRATE_ROW>   Load;
    INT64                       iCnt=0, iTotalCnt=0;
    int                         i;

    m_pOutput->OutputStart("Testing CUSTOMER_TAXRATE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCustomerTaxratesTable         TableFirstInstance(
                                            m_inputFiles, m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Check that the number of rows match.
            //
            if (RefTable.GetTaxRatesCount() != TableFirstInstance.GetTaxRatesCount())
            {
                sprintf(szMsg, "Number of CT rows (%d) for customer %" PRId64 " is different from reference (%d).",
                    TableFirstInstance.GetTaxRatesCount(), iCnt, RefTable.GetTaxRatesCount());

                throw CEGenTestErr(szMsg);
            }

            //  Compare CUSTOMER_TAXRATE rows.
            //
            for (i=0; i<RefTable.GetTaxRatesCount(); ++i)
            {
                Load.WriteNextRefRecord(RefTable.GetRowByIndex(i));

                Load.WriteNextRecord(TableFirstInstance.GetRowByIndex(i));

                ++iTotalCnt;

                if (Load.RowsDiffer())
                {
                    sprintf(szMsg, "Row %d for customer %" PRId64 " is different from reference.", i, iCnt);

                    throw CEGenTestErr(szMsg);
                }
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CCustomerTaxratesTable         TableSecondInstance(
                                            m_inputFiles, m_iCustomerCountSecondInstance, 
                                            m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Check that the number of rows match.
            //
            if (RefTable.GetTaxRatesCount() != TableSecondInstance.GetTaxRatesCount())
            {
                sprintf(szMsg, "Number of CT rows (%d) for customer %" PRId64 " is different from reference (%d).",
                    TableSecondInstance.GetTaxRatesCount(), iCnt, RefTable.GetTaxRatesCount());

                throw CEGenTestErr(szMsg);
            }

            //  Compare CUSTOMER_TAXRATE rows.
            //
            for (i=0; i<RefTable.GetTaxRatesCount(); ++i)
            {
                Load.WriteNextRefRecord(RefTable.GetRowByIndex(i));

                Load.WriteNextRecord(TableSecondInstance.GetRowByIndex(i));

                iTotalCnt;

                if (Load.RowsDiffer())
                {
                    sprintf(szMsg, "Row %d for customer %" PRId64 " is different from reference.", i, iCnt);

                    throw CEGenTestErr(szMsg);
                }
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iTotalCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test DAILY_MARKET table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestDailyMarket()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CDailyMarketTable      RefTable(
                                            m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer);
    

    

    CTestLoader<RefTPCE::DAILY_MARKET_ROW, TPCE::DAILY_MARKET_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing DAILY_MARKET table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CDailyMarketTable         TableFirstInstance(
                                            m_inputFiles, m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CDailyMarketTable         TableSecondInstance(
                                        m_inputFiles, m_iCustomerCountSecondInstance, 
                                        m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test EXCHANGE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestExchange()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CExchangeTable      RefTable(m_szRefInDir, m_iTotalCustomerCount);

    CTestLoader<RefTPCE::EXCHANGE_ROW, TPCE::EXCHANGE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing EXCHANGE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CExchangeTable         TableFirstInstance(m_szInDir, m_iTotalCustomerCount);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CExchangeTable         TableSecondInstance(m_szInDir, m_iTotalCustomerCount);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {   
            Load.WriteNextRefRecord(RefTable.GetRow());
            
            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();
        }
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test FINANCIAL table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestFinancial()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CFinancialTable      RefTable(m_RefInputFiles, m_iCustomerCount, m_iStartFromCustomer);

    CTestLoader<RefTPCE::FINANCIAL_ROW, TPCE::FINANCIAL_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing FINANCIAL table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CFinancialTable         TableFirstInstance(m_inputFiles, m_iCustomerCountFirstInstance, 
                                                        m_iStartFromCustomerFirstInstance);

        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CFinancialTable         TableSecondInstance(m_inputFiles, m_iCustomerCountSecondInstance, 
                                                        m_iStartFromCustomerSecondInstance);

        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test INDUSTRY table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestIndustry()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CIndustryTable      RefTable(m_szRefInDir);

    CTestLoader<RefTPCE::INDUSTRY_ROW, TPCE::INDUSTRY_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing INDUSTRY table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CIndustryTable         TableFirstInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {

            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CIndustryTable         TableSecondInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {            
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();
        }
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test LAST_TRADE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestLastTrade()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CLastTradeTable      RefTable(m_RefInputFiles, m_iCustomerCount, 
                                        m_iStartFromCustomer, m_iHoursOfInitialTrades);

    CTestLoader<RefTPCE::LAST_TRADE_ROW, TPCE::LAST_TRADE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing LAST_TRADE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CLastTradeTable         TableFirstInstance(m_inputFiles, m_iCustomerCountFirstInstance, 
                                                        m_iStartFromCustomerFirstInstance, 
                                                        m_iHoursOfInitialTrades);
        do
        {

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CLastTradeTable         TableSecondInstance(m_inputFiles, m_iCustomerCountSecondInstance, 
                                        m_iStartFromCustomerSecondInstance, m_iHoursOfInitialTrades);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test NEWS_ITEM and NEWS_XREF tables.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::NewsItemAndXRef()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    //  Allocated on the heap because contains 100KB item.
    RefTPCE::CNewsItemAndXRefTable*      RefTable = new RefTPCE::CNewsItemAndXRefTable(m_RefInputFiles, 
                                                    m_iCustomerCount,
                                                    m_iStartFromCustomer,
                                                    m_iHoursOfInitialTrades);

    CTestLoader<RefTPCE::NEWS_ITEM_ROW, TPCE::NEWS_ITEM_ROW>   NILoad;
    CTestLoader<RefTPCE::NEWS_XREF_ROW, TPCE::NEWS_XREF_ROW>   NXLoad;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing NEWS_ITEM and NEWS_XREF tables...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CNewsItemAndXRefTable*         TableFirstInstance = new TPCE::CNewsItemAndXRefTable(m_inputFiles, 
                                                        m_iCustomerCountFirstInstance, 
                                                        m_iStartFromCustomerFirstInstance,
                                                        m_iHoursOfInitialTrades);

        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable->GenerateNextRecord();

            NILoad.WriteNextRefRecord(RefTable->GetNewsItemRow());
            NXLoad.WriteNextRefRecord(RefTable->GetNewsXRefRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance->GenerateNextRecord();

            NILoad.WriteNextRecord(TableFirstInstance->GetNewsItemRow());
            NXLoad.WriteNextRecord(TableFirstInstance->GetNewsXRefRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (NILoad.RowsDiffer())
            {
                sprintf(szMsg, "NI row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            if (NXLoad.RowsDiffer())
            {
                sprintf(szMsg, "NX row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);

        delete TableFirstInstance;
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CNewsItemAndXRefTable*         TableSecondInstance = new TPCE::CNewsItemAndXRefTable(
                                                        m_inputFiles, 
                                                        m_iCustomerCountSecondInstance, 
                                                        m_iStartFromCustomerSecondInstance,
                                                        m_iHoursOfInitialTrades);

        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable->GenerateNextRecord();

            NILoad.WriteNextRefRecord(RefTable->GetNewsItemRow());
            NXLoad.WriteNextRefRecord(RefTable->GetNewsXRefRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance->GenerateNextRecord();

            NILoad.WriteNextRecord(TableSecondInstance->GetNewsItemRow());
            NXLoad.WriteNextRecord(TableSecondInstance->GetNewsXRefRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (NILoad.RowsDiffer())
            {
                sprintf(szMsg, "NI row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            if (NXLoad.RowsDiffer())
            {
                sprintf(szMsg, "NX row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);

        delete TableSecondInstance;
    }

    sprintf(szMsg, "done (%" PRId64 " NI rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test SECTOR table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestSector()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CSectorTable      RefTable(m_szRefInDir);

    CTestLoader<RefTPCE::SECTOR_ROW, TPCE::SECTOR_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing SECTOR table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CSectorTable         TableFirstInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {

            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CSectorTable         TableSecondInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {            
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();
        }
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test SECURITY table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestSecurity()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CSecurityTable      RefTable(m_RefInputFiles, m_iCustomerCount, 
                                        m_iStartFromCustomer);

    CTestLoader<RefTPCE::SECURITY_ROW, TPCE::SECURITY_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing SECURITY table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CSecurityTable         TableFirstInstance(m_inputFiles, m_iCustomerCountFirstInstance, 
                                                        m_iStartFromCustomerFirstInstance);
        do
        {

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CSecurityTable         TableSecondInstance(m_inputFiles, m_iCustomerCountSecondInstance, 
                                        m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test STATUS_TYPE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestStatusType()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CStatusTypeTable      RefTable(m_szRefInDir);

    CTestLoader<RefTPCE::STATUS_TYPE_ROW, TPCE::STATUS_TYPE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing STATUS_TYPE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CStatusTypeTable         TableFirstInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {

            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CStatusTypeTable         TableSecondInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {            
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();
        }
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test TAXRATE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestTaxrate()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CTaxrateTable      RefTable(m_RefInputFiles);

    CTestLoader<RefTPCE::TAXRATE_ROW, TPCE::TAXRATE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing TAXRATE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CTaxrateTable         TableFirstInstance(m_inputFiles);
        do
        {

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CTaxrateTable         TableSecondInstance(m_inputFiles);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            Load.WriteNextRefRecord(RefTable.GetRow());

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test BROKER, CASH_TRANSACTION, HOLDING, HOLDING_HISTORY, SETTLEMENT, TRADE, TRADE_HISTORY tables.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestTradeGen()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet, bBrokerRefRet, bHoldingSummaryRefRet, bHoldingRefRet;
    bool                        bRet, bBrokerRet, bHoldingSummaryRet, bHoldingRet;
    int                         iCurrentLoadUnit = 1;
    //  Reference code.
    //
    RefTPCE::CTradeGen      RefTable(
                                    m_RefInputFiles,
                                    m_iCustomerCount, 
                                    m_iStartFromCustomer,
                                    m_iTotalCustomerCount,
                                    m_iLoadUnitSize,
                                    m_iScaleFactor, 
                                    m_iHoursOfInitialTrades);    

    CTestLoader<RefTPCE::HOLDING_ROW, TPCE::HOLDING_ROW>                    HoldingLoad;
    CTestLoader<RefTPCE::HOLDING_HISTORY_ROW, TPCE::HOLDING_HISTORY_ROW>    HoldingHistoryLoad;
    CTestLoader<RefTPCE::HOLDING_SUMMARY_ROW, TPCE::HOLDING_SUMMARY_ROW>    HoldingSummaryLoad;
    CTestLoader<RefTPCE::TRADE_ROW, TPCE::TRADE_ROW>                        TradeLoad;
    CTestLoader<RefTPCE::SETTLEMENT_ROW, TPCE::SETTLEMENT_ROW>              SettlementLoad;
    CTestLoader<RefTPCE::TRADE_HISTORY_ROW, TPCE::TRADE_HISTORY_ROW>        TradeHistoryLoad;
    CTestLoader<RefTPCE::CASH_TRANSACTION_ROW, TPCE::CASH_TRANSACTION_ROW>  CashTransactionLoad;
    CTestLoader<RefTPCE::BROKER_ROW, TPCE::BROKER_ROW>                      BrokerLoad;
    INT64                       iCnt=0, iTotalCnt=0;
    int                         i;

    m_pOutput->OutputStart("Testing BROKER, CASH_TRANSACTION, HOLDING, HOLDING_HISTORY, SETTLEMENT, TRADE, TRADE_HISTORY tables...");

    {
        //  Current table code.
        //
        TPCE::CTradeGen         TableFirstInstance(
                                            m_inputFiles,
                                            m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance,
                                            m_iTotalCustomerCount,
                                            m_iLoadUnitSize,
                                            m_iScaleFactor, 
                                            m_iHoursOfInitialTrades);
        do  // load unit loop
        {
            do
            {
                //  Generate reference data.
                //
                bRefRet = RefTable.GenerateNextTrade();

                //  Generate current data.
                //
                bRet = TableFirstInstance.GenerateNextTrade();

                ++iCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                //  Verify TRADE row.
                //
                TradeLoad.WriteNextRefRecord(RefTable.GetTradeRow());

                TradeLoad.WriteNextRecord(TableFirstInstance.GetTradeRow());

                //  Compare two rows.
                //
                if (TradeLoad.RowsDiffer())
                {
                    sprintf(szMsg, "TRADE row %" PRId64 " is different from reference.", iCnt);

                    throw CEGenTestErr(szMsg);
                }

                //  Verify TRADE_HISTORY rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetTradeHistoryRowCount() != TableFirstInstance.GetTradeHistoryRowCount())
                {
                    sprintf(szMsg, 
                        "Number of TRADE_HISTORY rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableFirstInstance.GetTradeHistoryRowCount(), iCnt, RefTable.GetTradeHistoryRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify TRADE_HISTORY row data.
                //
                for(i=0; i<RefTable.GetTradeHistoryRowCount(); ++i)
                {
                    TradeHistoryLoad.WriteNextRefRecord(RefTable.GetTradeHistoryRow(i));

                    TradeHistoryLoad.WriteNextRecord(TableFirstInstance.GetTradeHistoryRow(i));

                    if (TradeHistoryLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "TRADE_HISTORY row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

                //  Verify SETTLEMENT rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetSettlementRowCount() != TableFirstInstance.GetSettlementRowCount())
                {
                    sprintf(szMsg, 
                        "Number of SETTLEMENT rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableFirstInstance.GetSettlementRowCount(), iCnt, RefTable.GetSettlementRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify SETTLEMENT row data.
                //
                for(i=0; i<RefTable.GetSettlementRowCount(); ++i)
                {
                    SettlementLoad.WriteNextRefRecord(RefTable.GetSettlementRow());

                    SettlementLoad.WriteNextRecord(TableFirstInstance.GetSettlementRow());

                    if (SettlementLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "SETTLEMENT row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

                //  Verify CASH_TRANSACTION rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetCashTransactionRowCount() != TableFirstInstance.GetCashTransactionRowCount())
                {
                    sprintf(szMsg, 
                        "Number of CASH_TRANSACTION rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableFirstInstance.GetCashTransactionRowCount(), iCnt, RefTable.GetCashTransactionRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify CASH_TRANSACTION row data.
                //
                for(i=0; i<RefTable.GetCashTransactionRowCount(); ++i)
                {
                    CashTransactionLoad.WriteNextRefRecord(RefTable.GetCashTransactionRow());

                    CashTransactionLoad.WriteNextRecord(TableFirstInstance.GetCashTransactionRow());

                    if (CashTransactionLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "CASH_TRANSACTION row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

                //  Verify HOLDING_HISTORY rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetHoldingHistoryRowCount() != TableFirstInstance.GetHoldingHistoryRowCount())
                {
                    sprintf(szMsg, 
                        "Number of HOLDING_HISTORY rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableFirstInstance.GetHoldingHistoryRowCount(), iCnt, RefTable.GetHoldingHistoryRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify HOLDING_HISTORY row data.
                //
                for(i=0; i<RefTable.GetHoldingHistoryRowCount(); ++i)
                {
                    HoldingHistoryLoad.WriteNextRefRecord(RefTable.GetHoldingHistoryRow(i));

                    HoldingHistoryLoad.WriteNextRecord(TableFirstInstance.GetHoldingHistoryRow(i));

                    if (HoldingHistoryLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "HOLDING_HISTORY row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

            } while (bRefRet && bRet);

            //  Make sure the trades in the current code instance are exhausted.
            //
            if (bRet && !bRefRet)
            {
                sprintf(szMsg, "Number of trades in the first instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }

            //  Generate and verify BROKER rows.
            //
            TIdent iBrokerCnt = 0;
            do
            {
                //  Generate reference data.
                //
                bBrokerRefRet = RefTable.GenerateNextBrokerRecord();

                //  Generate current data.
                //
                bBrokerRet = TableFirstInstance.GenerateNextBrokerRecord();

                ++iBrokerCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                BrokerLoad.WriteNextRefRecord(RefTable.GetBrokerRow());

                BrokerLoad.WriteNextRecord(TableFirstInstance.GetBrokerRow());

                if (BrokerLoad.RowsDiffer())
                {
                    sprintf(szMsg, "BROKER row %" PRId64 " is different from reference.", i, iBrokerCnt);

                    throw CEGenTestErr(szMsg);
                }
            } while (bBrokerRefRet && bBrokerRet);

            //  Make sure the brokers in the current code instance are exhausted.
            //
            if (bBrokerRet && !bBrokerRefRet)
            {
                sprintf(szMsg, "Number of brokers in the first instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }

            m_pOutput->OutputProgress("t"); // indicate that trades and brokers have finished

            //  Generate and verify HOLDING_SUMMARY rows.
            //
            TIdent iHoldingSummaryCnt = 0;
            do
            {
                //  Generate reference data.
                //
                bHoldingSummaryRefRet = RefTable.GenerateNextHoldingSummaryRow();

                //  Generate current data.
                //
                bHoldingSummaryRet = TableFirstInstance.GenerateNextHoldingSummaryRow();

                ++iHoldingSummaryCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                HoldingSummaryLoad.WriteNextRefRecord(RefTable.GetHoldingSummaryRow());

                HoldingSummaryLoad.WriteNextRecord(TableFirstInstance.GetHoldingSummaryRow());

                if (HoldingSummaryLoad.RowsDiffer())
                {
                    sprintf(szMsg, "HOLDING_SUMMARY row %" PRId64 " is different from reference.", i, iHoldingSummaryCnt);

                    throw CEGenTestErr(szMsg);
                }
            } while (bHoldingSummaryRefRet && bHoldingSummaryRet);

            //  Make sure the holding summary rows in the current code instance are exhausted.
            //
            if (bHoldingSummaryRet && !bHoldingSummaryRefRet)
            {
                sprintf(szMsg, "Number of HOLDING_SUMMARY rows in the first instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }

            //  Generate and verify HOLDING rows.
            //
            TIdent iHoldingCnt = 0;
            do
            {
                //  Generate reference data.
                //
                bHoldingRefRet = RefTable.GenerateNextHolding();

                //  Generate current data.
                //
                bHoldingRet = TableFirstInstance.GenerateNextHolding();

                ++iHoldingCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                HoldingLoad.WriteNextRefRecord(RefTable.GetHoldingRow());

                HoldingLoad.WriteNextRecord(TableFirstInstance.GetHoldingRow());

                if (HoldingLoad.RowsDiffer())
                {
                    sprintf(szMsg, "HOLDING row %" PRId64 " is different from reference.", i, iHoldingCnt);

                    throw CEGenTestErr(szMsg);
                }
            } while (bHoldingRefRet && bHoldingRet);

            //  Make sure the holdings in the current code instance are exhausted.
            //
            if (bHoldingRet && !bHoldingRefRet)
            {
                sprintf(szMsg, "Number of HOLDING rows in the first instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }

            // Output unit number for information.
            //
            sprintf(szMsg, "%d", iCurrentLoadUnit++);

            m_pOutput->OutputProgress(szMsg);

        } while ((bRefRet = RefTable.InitNextLoadUnit()) && (bRet = TableFirstInstance.InitNextLoadUnit()));

        //  Make sure it is the current code that triggered the exit. 
        //  Reference should have more load units to run.
        //
        if (!bRefRet)
        {
            sprintf(szMsg, "Number of load units in the first instance is more than in reference.");

            throw CEGenTestErr(szMsg);
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current table code.
        //
        TPCE::CTradeGen         TableSecondInstance(
                                                    m_inputFiles, 
                                                    m_iCustomerCountSecondInstance, 
                                                    m_iStartFromCustomerSecondInstance,
                                                    m_iTotalCustomerCount,
                                                    m_iLoadUnitSize,
                                                    m_iScaleFactor, 
                                                    m_iHoursOfInitialTrades);
        do  // load unit loop
        {
            do
            {
                //  Generate reference data.
                //
                bRefRet = RefTable.GenerateNextTrade();

                //  Generate current data.
                //
                bRet = TableSecondInstance.GenerateNextTrade();

                ++iCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                //  Verify TRADE row.
                //
                TradeLoad.WriteNextRefRecord(RefTable.GetTradeRow());

                TradeLoad.WriteNextRecord(TableSecondInstance.GetTradeRow());

                //  Compare two rows.
                //
                if (TradeLoad.RowsDiffer())
                {
                    sprintf(szMsg, "TRADE row %" PRId64 " is different from reference.", iCnt);

                    throw CEGenTestErr(szMsg);
                }

                //  Verify TRADE_HISTORY rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetTradeHistoryRowCount() != TableSecondInstance.GetTradeHistoryRowCount())
                {
                    sprintf(szMsg, 
                        "Number of TRADE_HISTORY rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableSecondInstance.GetTradeHistoryRowCount(), iCnt, RefTable.GetTradeHistoryRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify TRADE_HISTORY row data.
                //
                for(i=0; i<RefTable.GetTradeHistoryRowCount(); ++i)
                {
                    TradeHistoryLoad.WriteNextRefRecord(RefTable.GetTradeHistoryRow(i));

                    TradeHistoryLoad.WriteNextRecord(TableSecondInstance.GetTradeHistoryRow(i));

                    if (TradeHistoryLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "TRADE_HISTORY row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

                //  Verify SETTLEMENT rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetSettlementRowCount() != TableSecondInstance.GetSettlementRowCount())
                {
                    sprintf(szMsg, 
                        "Number of SETTLEMENT rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableSecondInstance.GetSettlementRowCount(), iCnt, RefTable.GetSettlementRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify SETTLEMENT row data.
                //
                for(i=0; i<RefTable.GetSettlementRowCount(); ++i)
                {
                    SettlementLoad.WriteNextRefRecord(RefTable.GetSettlementRow());

                    SettlementLoad.WriteNextRecord(TableSecondInstance.GetSettlementRow());

                    if (SettlementLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "SETTLEMENT row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

                //  Verify CASH_TRANSACTION rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetCashTransactionRowCount() != TableSecondInstance.GetCashTransactionRowCount())
                {
                    sprintf(szMsg, 
                        "Number of CASH_TRANSACTION rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableSecondInstance.GetCashTransactionRowCount(), iCnt, RefTable.GetCashTransactionRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify CASH_TRANSACTION row data.
                //
                for(i=0; i<RefTable.GetCashTransactionRowCount(); ++i)
                {
                    CashTransactionLoad.WriteNextRefRecord(RefTable.GetCashTransactionRow());

                    CashTransactionLoad.WriteNextRecord(TableSecondInstance.GetCashTransactionRow());

                    if (CashTransactionLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "CASH_TRANSACTION row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

                //  Verify HOLDING_HISTORY rows.
                //

                //  First, verify that the number of rows match.
                //
                if (RefTable.GetHoldingHistoryRowCount() != TableSecondInstance.GetHoldingHistoryRowCount())
                {
                    sprintf(szMsg, 
                        "Number of HOLDING_HISTORY rows (%d) for TRADE row %" PRId64 " is different from reference (%d).",
                        TableSecondInstance.GetHoldingHistoryRowCount(), iCnt, RefTable.GetHoldingHistoryRowCount());

                    throw CEGenTestErr(szMsg);
                }

                //  Verify HOLDING_HISTORY row data.
                //
                for(i=0; i<RefTable.GetHoldingHistoryRowCount(); ++i)
                {
                    HoldingHistoryLoad.WriteNextRefRecord(RefTable.GetHoldingHistoryRow(i));

                    HoldingHistoryLoad.WriteNextRecord(TableSecondInstance.GetHoldingHistoryRow(i));

                    if (HoldingHistoryLoad.RowsDiffer())
                    {
                        sprintf(szMsg, "HOLDING_HISTORY row %d for TRADE row %" PRId64 " is different from reference.", i, iCnt);

                        throw CEGenTestErr(szMsg);
                    }
                }

            } while (bRefRet && bRet);

            //  Make sure the trades in the current code instance are exhausted.
            //
            if (bRet && !bRefRet)
            {
                sprintf(szMsg, "Number of trades in the second instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }

            //  Generate and verify BROKER rows.
            //
            TIdent iBrokerCnt = 0;
            do
            {
                //  Generate reference data.
                //
                bBrokerRefRet = RefTable.GenerateNextBrokerRecord();

                //  Generate current data.
                //
                bBrokerRet = TableSecondInstance.GenerateNextBrokerRecord();

                ++iBrokerCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                BrokerLoad.WriteNextRefRecord(RefTable.GetBrokerRow());

                BrokerLoad.WriteNextRecord(TableSecondInstance.GetBrokerRow());

                if (BrokerLoad.RowsDiffer())
                {
                    sprintf(szMsg, "BROKER row %" PRId64 " is different from reference.", i, iBrokerCnt);

                    throw CEGenTestErr(szMsg);
                }
            } while (bBrokerRefRet && bBrokerRet);

            //  Make sure the brokers in the current code instance are exhausted.
            //
            if (bBrokerRet && !bBrokerRefRet)
            {
                sprintf(szMsg, "Number of brokers in the second instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }
            if (!bBrokerRet && bBrokerRefRet)
            {
                sprintf(szMsg, "Number of brokers in the second instance is less than in reference.");

                throw CEGenTestErr(szMsg);
            }

            m_pOutput->OutputProgress("t"); // indicate that trades and brokers have finished

            //  Generate and verify HOLDING_SUMMARY rows.
            //
            TIdent iHoldingSummaryCnt = 0;
            do
            {
                //  Generate reference data.
                //
                bHoldingSummaryRefRet = RefTable.GenerateNextHoldingSummaryRow();

                //  Generate current data.
                //
                bHoldingSummaryRet = TableSecondInstance.GenerateNextHoldingSummaryRow();

                ++iHoldingSummaryCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                HoldingSummaryLoad.WriteNextRefRecord(RefTable.GetHoldingSummaryRow());

                HoldingSummaryLoad.WriteNextRecord(TableSecondInstance.GetHoldingSummaryRow());

                if (HoldingSummaryLoad.RowsDiffer())
                {
                    sprintf(szMsg, "HOLDING_SUMMARY row %" PRId64 " is different from reference.", i, iHoldingSummaryCnt);

                    throw CEGenTestErr(szMsg);
                }
            } while (bHoldingSummaryRefRet && bHoldingSummaryRet);

            //  Make sure the holding summary rows in the current code instance are exhausted.
            //
            if (bHoldingSummaryRet && !bHoldingSummaryRefRet)
            {
                sprintf(szMsg, "Number of HOLDING_SUMMARY rows in the second instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }
            if (!bHoldingSummaryRet && bHoldingSummaryRefRet)
            {
                sprintf(szMsg, "Number of HOLDING_SUMMARY rows in the second instance is less than in reference.");

                throw CEGenTestErr(szMsg);
            }


            //  Generate and verify HOLDING rows.
            //
            TIdent iHoldingCnt = 0;
            do
            {
                //  Generate reference data.
                //
                bHoldingRefRet = RefTable.GenerateNextHolding();

                //  Generate current data.
                //
                bHoldingRet = TableSecondInstance.GenerateNextHolding();

                ++iHoldingCnt;
                if (++iTotalCnt % 20000 == 0)
                {   //output progress
                    m_pOutput->OutputProgress(".");
                }

                HoldingLoad.WriteNextRefRecord(RefTable.GetHoldingRow());

                HoldingLoad.WriteNextRecord(TableSecondInstance.GetHoldingRow());

                if (HoldingLoad.RowsDiffer())
                {
                    sprintf(szMsg, "HOLDING row %" PRId64 " is different from reference.", i, iHoldingCnt);

                    throw CEGenTestErr(szMsg);
                }
            } while (bHoldingRefRet && bHoldingRet);

            //  Make sure the holdings in the current code instance are exhausted.
            //
            if (bHoldingRet && !bHoldingRefRet)
            {
                sprintf(szMsg, "Number of HOLDING rows in the second instance is more than in reference.");

                throw CEGenTestErr(szMsg);
            }
            if (!bHoldingRet && bHoldingRefRet)
            {
                sprintf(szMsg, "Number of HOLDING rows in the second instance is less than in reference.");

                throw CEGenTestErr(szMsg);
            }

            // Output unit number for information.
            //
            sprintf(szMsg, "%d", iCurrentLoadUnit++);

            m_pOutput->OutputProgress(szMsg);

        } while ((bRefRet = RefTable.InitNextLoadUnit()) && (bRet = TableSecondInstance.InitNextLoadUnit()));

        //  Make sure load units are exhausted both in the current code and in the reference. 
        //
        if (bRefRet || bRet)
        {
            sprintf(szMsg, "Number of load units doesn't match reference.");

            throw CEGenTestErr(szMsg);
        }
    }

    sprintf(szMsg, "done (%" PRId64 " TRADE rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test TRADE_TYPE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestTradeType()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CTradeTypeTable      RefTable(m_szRefInDir);

    CTestLoader<RefTPCE::TRADE_TYPE_ROW, TPCE::TRADE_TYPE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing TRADE_TYPE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CTradeTypeTable         TableFirstInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {

            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (!bRefRet)   // GenerateNextRecord returns EOF here (inverted return)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CTradeTypeTable         TableSecondInstance(m_szInDir);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (!bRefRet && !bRet)   // GenerateNextRecord returns EOF here (inverted return)
        {            
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();
        }
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test WATCH_LIST and WATCH_ITEM tables.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestWatchListsAndItems()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CWatchListsAndItemsTable      RefTable(
                                            m_RefInputFiles,
                                            m_iCustomerCount, m_iStartFromCustomer);    

    CTestLoader<RefTPCE::WATCH_LIST_ROW, TPCE::WATCH_LIST_ROW>      WLLoad;
    CTestLoader<RefTPCE::WATCH_ITEM_ROW, TPCE::WATCH_ITEM_ROW>      WILoad;
    INT64                       iCnt=0;
    int                         i;

    m_pOutput->OutputStart("Testing WATCH_LIST and WATCH_ITEM tables...");

    {
        //  Current table code.
        //
        TPCE::CWatchListsAndItemsTable         TableFirstInstance(
                                            m_inputFiles,
                                            m_iCustomerCountFirstInstance, 
                                            m_iStartFromCustomerFirstInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();

            //  Verify WATCH_LIST row.
            //
            WLLoad.WriteNextRefRecord(RefTable.GetWLRow());

            WLLoad.WriteNextRecord(TableFirstInstance.GetWLRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (WLLoad.RowsDiffer())
            {
                sprintf(szMsg, "WL row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Verify WATCH_ITEM rows.
            //

            //  First, verify that the number of rows match.
            //
            if (RefTable.GetWICount() != TableFirstInstance.GetWICount())
            {
                sprintf(szMsg, "Number of WI rows (%d) for WL row %" PRId64 " is different from reference (%d).",
                    TableFirstInstance.GetWICount(), iCnt, RefTable.GetWICount());

                throw CEGenTestErr(szMsg);
            }

            //  Verify WATCH_ITEM row data.
            //
            for(i=0; i<RefTable.GetWICount(); ++i)
            {
                WILoad.WriteNextRefRecord(RefTable.GetWIRow(i));

                WILoad.WriteNextRecord(TableFirstInstance.GetWIRow(i));

                if (WILoad.RowsDiffer())
                {
                    sprintf(szMsg, "WI row %d for WL row %" PRId64 " is different from reference.", i, iCnt);

                    throw CEGenTestErr(szMsg);
                }
            }

        } while (bRefRet && bRet);
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current table code.
        //
        TPCE::CWatchListsAndItemsTable         TableSecondInstance(
                                                            m_inputFiles,
                                                            m_iCustomerCountSecondInstance, 
                                                            m_iStartFromCustomerSecondInstance);
        do
        {
            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();

            //  Verify WATCH_LIST row.
            //
            WLLoad.WriteNextRefRecord(RefTable.GetWLRow());

            WLLoad.WriteNextRecord(TableSecondInstance.GetWLRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two WATCH_LIST rows.
            //
            if (WLLoad.RowsDiffer())
            {
                sprintf(szMsg, "WL row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Verify WATCH_ITEM rows.
            //

            //  First, verify that the number of rows match.
            //
            if (RefTable.GetWICount() != TableSecondInstance.GetWICount())
            {
                sprintf(szMsg, "Number of WI rows (%d) for WL row %" PRId64 " is different from reference (%d).",
                    TableSecondInstance.GetWICount(), iCnt, RefTable.GetWICount());

                throw CEGenTestErr(szMsg);
            }

            //  Verify WATCH_ITEM row data.
            //
            for(i=0; i<RefTable.GetWICount(); ++i)
            {
                WILoad.WriteNextRefRecord(RefTable.GetWIRow(i));

                WILoad.WriteNextRecord(TableSecondInstance.GetWIRow(i));

                if (WILoad.RowsDiffer())
                {
                    sprintf(szMsg, "WI row %d for WL row %" PRId64 " is different from reference.", i, iCnt);

                    throw CEGenTestErr(szMsg);
                }
            }
        } while (bRefRet && bRet);
    }

    sprintf(szMsg, "done (%" PRId64 " CA rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}

/*
*   Generate and test ZIP_CODE table.
*
*   PARAMETERS:
*           none.
*
*   RETURNS:
*           none.
*/
void CEGenTestSuiteOne::TestZipCode()
{
    char                        szMsg[iMaxTestErrMsg];
    bool                        bRefRet;
    bool                        bRet;
    //  Reference code.
    //
    RefTPCE::CZipCodeTable      RefTable(m_RefInputFiles);

    CTestLoader<RefTPCE::ZIP_CODE_ROW, TPCE::ZIP_CODE_ROW>   Load;
    INT64                       iCnt=0;

    m_pOutput->OutputStart("Testing ZIP_CODE table...");

    {
        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CZipCodeTable         TableFirstInstance(m_inputFiles);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableFirstInstance.GenerateNextRecord();

        while (bRefRet && bRet)
        {

            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableFirstInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableFirstInstance.GenerateNextRecord();
        }
    }

    //  First instance loader has finished.
    //
    //  Go to the second instance.
    //
    if (bRefRet)
    {
        m_pOutput->OutputProgress("i2.");    // output something to indicate second instance is starting

        //  Current code. 
        //  Declared here to go out of scope for the other instance (to prevent copy-paste bugs).
        //
        TPCE::CZipCodeTable         TableSecondInstance(m_inputFiles);

        //  Generate reference data.
        //
        bRefRet = RefTable.GenerateNextRecord();

        //  Generate current data.
        //
        bRet = TableSecondInstance.GenerateNextRecord();

        while (bRefRet && bRet)
        {            
            Load.WriteNextRefRecord(RefTable.GetRow());

            Load.WriteNextRecord(TableSecondInstance.GetRow());

            if (++iCnt % 20000 == 0)
            {   //output progress
                m_pOutput->OutputProgress(".");
            }

            //  Compare two rows.
            //
            if (Load.RowsDiffer())
            {
                sprintf(szMsg, "Row %" PRId64 " is different from reference.", iCnt);

                throw CEGenTestErr(szMsg);
            }

            //  Generate reference data.
            //
            bRefRet = RefTable.GenerateNextRecord();

            //  Generate current data.
            //
            bRet = TableSecondInstance.GenerateNextRecord();
        }
    }

    sprintf(szMsg, "done (%" PRId64 " rows).", iCnt);
    m_pOutput->OutputComplete(szMsg);
    m_pOutput->OutputNewline();
    m_pOutput->OutputNewline();
}
