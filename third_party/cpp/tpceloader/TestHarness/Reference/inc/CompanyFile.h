/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
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
*   Description:        Implementation of the Company input file that scales
*                       with the database size.
******************************************************************************/

#ifndef COMPANY_FILE_H
#define COMPANY_FILE_H

namespace TPCE
{

class CCompanyFile  : public CFlatFile<TCompanyInputRow, TCompanyLimits>
{
    // Configured and active number of companies in the database.
    // Depends on the configured and active number of customers.
    //
    TIdent  m_iConfiguredCompanyCount;
    TIdent  m_iActiveCompanyCount;

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
    CCompanyFile(const char *szListFile, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
    : CFlatFile<TCompanyInputRow, TCompanyLimits>(szListFile)
    {
        SetConfiguredCompanyCount(iConfiguredCustomerCount);
        SetActiveCompanyCount(iActiveCustomerCount);
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
    CCompanyFile(const string &str, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount)
    : CFlatFile<TCompanyInputRow, TCompanyLimits>(str)
    {
        SetConfiguredCompanyCount(iConfiguredCustomerCount);
        SetActiveCompanyCount(iActiveCustomerCount);
    }

    /*
    *  Delayed initialization of the configured number of companies.
    *  Made available for situations when the configured number of customers
    *  is not known at construction time.
    *
    *  PARAMETERS:
    *       IN  iConfiguredCustomerCount    - total configured number of customers in the database
    *
    *  RETURNS:
    *       none.
    */
    void SetConfiguredCompanyCount(TIdent iConfiguredCustomerCount)
    {
        m_iConfiguredCompanyCount = CalculateCompanyCount(iConfiguredCustomerCount);
    }

    /*
    *  Delayed initialization of the active number of companies.
    *  Made available for situations when the active number of customers
    *  is not known at construction time.
    *
    *  PARAMETERS:
    *       IN  iActiveCustomerCount    - active number of customers in the database (provided for engineering purposes)
    *
    *  RETURNS:
    *       none.
    */
    void SetActiveCompanyCount(TIdent iActiveCustomerCount)
    {
        m_iActiveCompanyCount = CalculateCompanyCount(iActiveCustomerCount);
    }

    /*
    *  Calculate company count for the specified number of customers.
    *  Sort of a static method. Used in parallel generation of company related tables.
    *
    *  PARAMETERS:
    *       IN  iCustomerCount          - number of customers
    *
    *  RETURNS:
    *       number of company competitors.
    */
    TIdent CalculateCompanyCount(TIdent iCustomerCount)
    {
        return iCustomerCount / iDefaultLoadUnitSize * iOneLoadUnitCompanyCount;
    }

    /*
    *  Calculate the first company id (0-based) for the specified customer id.
    *
    *  PARAMETERS:
    *       IN  iStartFromCustomer      - customer id
    *
    *  RETURNS:
    *       company competitor id.
    */
    TIdent CalculateStartFromCompany(TIdent iStartFromCustomer)
    {
        return iStartFromCustomer / iDefaultLoadUnitSize * iOneLoadUnitCompanyCount;
    }

    /*
    *  Create company name with appended suffix based on the
    *  load unit number.
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company Competitor file (0-based)
    *       IN  szOutput    - output buffer for company name
    *       IN  iOutputLen  - size of the output buffer
    *
    *  RETURNS:
    *       none.
    */
    void CreateName(TIdent  iIndex,     // row number
                    char*   szOutput,   // output buffer
                    int     iOutputLen) // size of the output buffer
    {
        TIdent      iFileIndex = iIndex % CFlatFile<TCompanyInputRow, TCompanyLimits>::GetSize();
        TIdent      iAdd = iIndex / CFlatFile<TCompanyInputRow, TCompanyLimits>::GetSize();

        if (iAdd > 0)
        {
            sprintf( szOutput, "%s #%" PRId64, GetRecord(iFileIndex)->CO_NAME, iAdd );
        }
        else
        {
            strncpy(szOutput, GetRecord(iFileIndex)->CO_NAME, iOutputLen);
        }
    }

    /*
    *  Return company id for the specified row.
    *  Index can exceed the size of the Company input file.
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company Competitor file (0-based)
    *
    *  RETURNS:
    *       company id.
    */
    TIdent GetCompanyId(TIdent iIndex)
    {
        // Index wraps around every 5000 companies.
        //
        return m_list[ (int)(iIndex % m_list.size()) ].CO_ID + iTIdentShift
                + iIndex / m_list.size() * m_list.size();
    }

    /*
    *  Return the number of companies in the database for
    *  the configured number of customers.
    *
    *  PARAMETERS:
    *       none.
    *
    *  RETURNS:
    *       number of rows in the file.
    */
    TIdent GetSize()
    {
        return m_iConfiguredCompanyCount;
    }

    /*
    *  Return the number of companies in the database for
    *  the configured number of customers.
    *
    *  PARAMETERS:
    *       none.
    *
    *  RETURNS:
    *       configured company count.
    */
    TIdent GetConfiguredCompanyCount()
    {
        return m_iConfiguredCompanyCount;
    }

    /*
    *  Return the number of companies in the database for
    *  the active number of customers.
    *
    *  PARAMETERS:
    *       none.
    *
    *  RETURNS:
    *       active company count.
    */
    TIdent GetActiveCompanyCount()
    {
        return m_iActiveCompanyCount;
    }

    /*
    *  Overload GetRecord to wrap around indices that
    *  are larger than the flat file
    *
    *  PARAMETERS:
    *       IN  iIndex      - row number in the Company file (0-based)
    *
    *  RETURNS:
    *       reference to the row structure in the Company file.
    */
    TCompanyInputRow*   GetRecord(TIdent index) { return &m_list[(int)(index % m_list.size())]; };
};

}   // namespace TPCE

#endif // COMPANY_FILE_H

