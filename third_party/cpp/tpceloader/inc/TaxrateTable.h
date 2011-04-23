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
*   Class representing the Taxrate table.
*/
#ifndef TAXRATE_TABLE_H
#define TAXRATE_TABLE_H

#include "EGenTables_common.h"

namespace TPCE
{

class CTaxrateTable : public TableTemplate<TAXRATE_ROW>
{
    TTaxRatesCountryFile    *m_pCountry;    // in-mem representation of Country Tax Rate file
    TTaxRatesDivisionFile   *m_pDivision;   // in-mem representation of Division Tax Rate file
    UINT                    m_iCountryIndex;    // Country file record index used for generation of the current table record
    UINT                    m_iDivisionIndex;   // Division file record index used for generation of the current table record
    UINT                    m_iCountrySubIndex; // subindex in the vector (record) from Country
    UINT                    m_iDivisionSubIndex;// subindex in the vector (record) from Division

public:
    CTaxrateTable( CInputFiles inputFiles )
        : TableTemplate<TAXRATE_ROW>(),
        m_pCountry(inputFiles.TaxRatesCountry),
        m_pDivision(inputFiles.TaxRatesDivision),
        m_iCountryIndex(0),
        m_iDivisionIndex(0),
        m_iCountrySubIndex(0),
        m_iDivisionSubIndex(0)
    {
    };

    /*
    *   Generates all column values for the next row.
    */
    bool GenerateNextRecord()
    {
        const vector<TTaxRateInputRow>  *pRatesRecord;

        ++m_iLastRowNumber; //increment state info

        // Return a Country row if not all have been returned
        if (m_iCountryIndex < m_pCountry->GetSize())
        {
            pRatesRecord = m_pCountry->GetRecord(m_iCountryIndex);  // get into a separate variable for convenience

            // Copy all the necessary fields from the tax rate row of the input file
            strncpy(m_row.TX_ID, (*pRatesRecord)[m_iCountrySubIndex].TAX_ID, sizeof(m_row.TX_ID));
            strncpy(m_row.TX_NAME, (*pRatesRecord)[m_iCountrySubIndex].TAX_NAME, sizeof(m_row.TX_NAME));
            m_row.TX_RATE = (*pRatesRecord)[m_iCountrySubIndex].TAX_RATE;

            ++m_iCountrySubIndex;   // move to the next element in the record

            if (m_iCountrySubIndex >= m_pCountry->GetRecord(m_iCountryIndex)->size())
            {   // All elements of the current record have been traversed
                // Advance to the next record in the file
                ++m_iCountryIndex;
                m_iCountrySubIndex = 0; // start with the first element in the record's vector
            }
        }
        else
        {   // Must try to return a Division record
            if (m_iDivisionIndex < m_pDivision->GetSize())
            {
                pRatesRecord = m_pDivision->GetRecord(m_iDivisionIndex);    // get into a separate variable for convenience

                // Copy all the necessary fields from the tax rate row of the input file
                strncpy(m_row.TX_ID, (*pRatesRecord)[m_iDivisionSubIndex].TAX_ID, sizeof(m_row.TX_ID));
                strncpy(m_row.TX_NAME, (*pRatesRecord)[m_iDivisionSubIndex].TAX_NAME, sizeof(m_row.TX_NAME));
                m_row.TX_RATE = (*pRatesRecord)[m_iDivisionSubIndex].TAX_RATE;

                ++m_iDivisionSubIndex;  // move to the next element in the record

                if (m_iDivisionSubIndex >= m_pDivision->GetRecord(m_iDivisionIndex)->size())
                {   // All elements of the current record have been traversed
                    // Advance to the next record in the file
                    ++m_iDivisionIndex;
                    m_iDivisionSubIndex = 0;    // start with the first element in the record's vector
                }
            }
        }

        // Done when all the records from both Country and Division input files have been returned
        m_bMoreRecords = (m_iCountryIndex < m_pCountry->GetSize()) || (m_iDivisionIndex < m_pDivision->GetSize());

        return ( MoreRecords() );
    }
};

}   // namespace TPCE

#endif //TAXRATE_TABLE_H
