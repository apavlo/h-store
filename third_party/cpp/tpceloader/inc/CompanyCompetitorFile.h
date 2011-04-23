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

/******************************************************************************
*   Description:        Implementation of the Company Competitor input file
*                       that scales with the database size.
******************************************************************************/

#ifndef COMPANY_COMPETITOR_FILE_H
#define COMPANY_COMPETITOR_FILE_H

#include <string>
#include "EGenStandardTypes.h"
#include "TableConsts.h"
#include "InputFlatFilesDeclarations.h"
#include "FlatFile.h"

using namespace std;

namespace TPCE
{

class CCompanyCompetitorFile : public CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>
{
    // Configured and active number of companies in the database.
    // Depends on the configured and active number of customers.
    //
    TIdent  m_iConfiguredCompanyCompetitorCount;
    TIdent  m_iActiveCompanyCompetitorCount;

    // Number of base companies (=rows in COMPANY.txt input file).
    //
    UINT    m_iBaseCompanyCount;

public:

    /*
    *  Constructor.
    *
    *  PARAMETERS:
    *       IN  szListFile                  - file name of the CompanyCompetitor input flat file
    *       IN  iConfiguredCustomerCount    - total configured number of customers in the database
    *       IN  iActiveCustomerCount        - active number of customers in the database (provided for engineering purposes)
    *
    *  RETURNS:
    *       not applicable.
    */
    CCompanyCompetitorFile(const char *szListFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
    : CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>(szListFile)
    {
        TCompanyLimits      CompanyInputFileLimits;

        SetConfiguredCompanyCompetitorCount(iConfiguredCustomerCount);
        SetActiveCompanyCompetitorCount(iActiveCustomerCount);

        m_iBaseCompanyCount     = CompanyInputFileLimits.m_iTotalElements;
    }

    /*
    *  Constructor.
    *
    *  PARAMETERS:
    *       IN  str                         - file name of the CompanyCompetitor input flat file
    *       IN  iConfiguredCustomerCount    - total configured number of customers in the database
    *       IN  iActiveCustomerCount        - active number of customers in the database (provided for engineering purposes)
    *
    *  RETURNS:
    *       not applicable.
    */
    CCompanyCompetitorFile(const string &str, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
    : CFlatFile<TCompanyCompetitorInputRow, TCompanyCompetitorLimits>(str)
    {
        TCompanyLimits      CompanyInputFileLimits;

        SetConfiguredCompanyCompetitorCount(iConfiguredCustomerCount);
        SetActiveCompanyCompetitorCount(iActiveCustomerCount);

        m_iBaseCompanyCount     = CompanyInputFileLimits.m_iTotalElements;
    }

    /*
    *  Delayed initialization of the configured number of company competitors.
    *  Made available for situations when the configured number of customers
    *  is not known at construction time.
    *
    *  PARAMETERS:
    *       IN  iConfiguredCustomerCount    - total configured number of customers in the database
    *
    *  RETURNS:
    *       none.
    */
    void SetConfiguredCompanyCompetitorCount(TIdent iConfiguredCustomerCount)
    {
        m_iConfiguredCompanyCompetitorCount = CalculateCompanyCompetitorCount(iConfiguredCustomerCount);
    }

    /*
    *  Delayed initialization of the active number of company competitors.
    *  Made available for situations when the active number of customers
    *  is not known at construction time.
    *
    *  PARAMETERS:
    *       IN  iActiveCustomerCount    - active number of customers in the database (provided for engineering purposes)
    *
    *  RETURNS:
    *       none.
    */
    void SetActiveCompanyCompetitorCount(TIdent iActiveCustomerCount)
    {
        m_iActiveCompanyCompetitorCount = CalculateCompanyCompetitorCount(iActiveCustomerCount);
    }

    /*
    *  Calculate company competitor count for the specified number of customers.
    *  Sort of a static method. Used in parallel generation of company related tables.
    *
    *  PARAMETERS:
    *       IN  iCustomerCount          - number of customers
    *
    *  RETURNS:
    *       number of company competitors.
    */
    TIdent CalculateCompanyCompetitorCount(TIdent iCustomerCount)
    {
        return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitCompanyCompetitorCount;
    }

    /*
    *  Calculate the first company competitor id (0-based) for the specified customer id.
    *
    *  PARAMETERS:
    *       IN  iStartFromCustomer      - customer id
    *
    *  RETURNS:
    *       company competitor id.
    */
    TIdent CalculateStartFromCompanyCompetitor(TIdent iStartFromCustomer)
    {
        return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitCompanyCompetitorCount;
    }

    /*
    *  Return company id for the specified row.
    *  Index can exceed the size of the Company Competitor input file.
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company Competitor file (0-based)
    *
    *  RETURNS:
    *       company id.
    */
    TIdent GetCompanyId(TIdent iIndex)
    {
        // Index wraps around every 15000 companies.
        //
        return m_list[ (int)(iIndex % m_list.size()) ].CP_CO_ID + iTIdentShift
                + iIndex / m_list.size() * m_iBaseCompanyCount;
    }

    /*
    *  Return company competitor id for the specified row.
    *  Index can exceed the size of the Company Competitor input file.
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company Competitor file (0-based)
    *
    *  RETURNS:
    *       company competitor id.
    */
    TIdent GetCompanyCompetitorId(TIdent iIndex)
    {
        // Index wraps around every 5000 companies.
        //
        return m_list[ (int)(iIndex % m_list.size()) ].CP_COMP_CO_ID + iTIdentShift
                + iIndex / m_list.size() * m_iBaseCompanyCount;
    }

    /*
    *  Return industry id for the specified row.
    *  Index can exceed the size of the Company Competitor input file.
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company Competitor file (0-based)
    *
    *  RETURNS:
    *       industry id.
    */
    char* GetIndustryId(TIdent iIndex)
    {
        // Index wraps around every 5000 companies.
        //
        return m_list[ (int)(iIndex % m_list.size()) ].CP_IN_ID;
    }

    /*
    *  Return the number of company competitors in the database for
    *  the configured number of customers.
    *
    *  PARAMETERS:
    *       none.
    *
    *  RETURNS:
    *       configured company competitor count.
    */
    TIdent GetConfiguredCompanyCompetitorCount()
    {
        return m_iConfiguredCompanyCompetitorCount;
    }

    /*
    *  Return the number of company competitors in the database for
    *  the active number of customers.
    *
    *  PARAMETERS:
    *       none.
    *
    *  RETURNS:
    *       active company competitor count.
    */
    TIdent GetActiveCompanyCompetitorCount()
    {
        return m_iActiveCompanyCompetitorCount;
    }

    /*
    *  Overload GetRecord to wrap around indices that
    *  are larger than the flat file
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company Competitor file (0-based)
    *
    *  RETURNS:
    *       reference to the row structure in the Company Competitor file.
    */
    TCompanyCompetitorInputRow* GetRecord(TIdent index) { return &m_list[(int)(index % m_list.size())]; };
};

}   // namespace TPCE

#endif // COMPANY_COMPETITOR_FILE_H

