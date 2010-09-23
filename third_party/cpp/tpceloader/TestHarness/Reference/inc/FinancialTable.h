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

/*
*   Class representing the Financial table.
*/
#ifndef FINANCIAL_TABLE_H
#define FINANCIAL_TABLE_H

namespace TPCE
{

const int iYearsForFins = 5;
const int iQuartersInYear = 4;
const int iFinsPerCompany = iYearsForFins * iQuartersInYear;    //5 years of 4 quaters each year

// multiplier to get the diluted number of shares from outstanding
const double fDilutedSharesMultiplier = 1.1;

// Multipliers for previous quarter to get the current quarter data
const double fFinDataDownMult = 0.9;
const double fFinDataUpMult = 1.15;

const double    fFinancialRevenueMin = 100000.0;
const double    fFinancialRevenueMax = 16000000000.0;

const double    fFinancialEarningsMin = -300000000.0;
const double    fFinancialEarningsMax = 3000000000.0;

const double    fFinancialOutBasicMin = 400000.0;
const double    fFinancialOutBasicMax = 9500000000.0;

const double    fFinancialInventMin = 0.0;
const double    fFinancialInventMax = 2000000000.0;

const double    fFinancialAssetsMin = 100000.0;
const double    fFinancialAssetsMax = 65000000000.0;

const double    fFinancialLiabMin = 100000.0;
const double    fFinancialLiabMax = 35000000000.0;

// Number of RNG calls to skip for one row in order
// to not use any of the random values from the previous row.
//
const int iRNGSkipOneRowFinancial = 6 + iFinsPerCompany * 6;

typedef struct FINANCIAL_GEN_ROW
{
    FINANCIAL_ROW   m_financials[ iFinsPerCompany ];
} *PFINANCIAL_GEN_ROW;

class CFinancialTable : public TableTemplate<FINANCIAL_GEN_ROW>
{
    CCompanyTable   m_CompanyTable;
    int             m_iFinYear;         // first year to generate financials
    int             m_iFinQuarter;      // first quarter to generate financials (0-based)
    // Number of times GenerateNextRecord() was called for the current company data.
    // Needed to decide when to generate next company's data.
    int             m_iRowsGeneratedPerCompany;
    // Stores whether there is another company(s) for which to
    // generate financial data.
    bool            m_bMoreCompanies;
    TIdent          m_iFinancialCountForOneLoadUnit;

    //
    // Generate the financial data for the next company.
    //
    // Return whether there are more companies to generate data for.
    //
    bool GenerateFinancialRows()
    {
        int iFinYear, iFinQuarter;
        int i;
        double fRev, fEarn, fInvent, fAssets, fLiab, fOutBasic;

        // Generate financials
        fRev = m_rnd.RndDoubleIncrRange( fFinancialRevenueMin, fFinancialRevenueMax, 0.01 );
        // Generate earnings no greater than the revenue
        fEarn = m_rnd.RndDoubleIncrRange( fFinancialEarningsMin, fRev < fFinancialEarningsMax ? fRev : fFinancialEarningsMax, 0.01 );
        fOutBasic = m_rnd.RndDoubleIncrRange( fFinancialOutBasicMin, fFinancialOutBasicMax, 0.01 );
        fInvent = m_rnd.RndDoubleIncrRange( fFinancialInventMin, fFinancialInventMax, 0.01 );
        fAssets = m_rnd.RndDoubleIncrRange( fFinancialAssetsMin, fFinancialAssetsMax, 0.01 );
        fLiab = m_rnd.RndDoubleIncrRange( fFinancialLiabMin, fFinancialLiabMax, 0.01 );

        iFinYear = m_iFinYear;
        iFinQuarter = m_iFinQuarter;

        for (i = 0; i<iFinsPerCompany; ++i)
        {
            m_row.m_financials[i].FI_CO_ID = m_CompanyTable.GetCurrentCO_ID();
            m_row.m_financials[i].FI_YEAR = iFinYear;
            m_row.m_financials[i].FI_QTR = iFinQuarter + 1;

            m_row.m_financials[i].FI_QTR_START_DATE.Set(iFinYear, iFinQuarter * 3 + 1, 1);

            m_row.m_financials[i].FI_REVENUE = fRev * m_rnd.RndDoubleRange(fFinDataDownMult, fFinDataUpMult);
            fRev = m_row.m_financials[i].FI_REVENUE;

            m_row.m_financials[i].FI_NET_EARN = fEarn * m_rnd.RndDoubleRange(fFinDataDownMult, fFinDataUpMult);
            if (m_row.m_financials[i].FI_NET_EARN >= m_row.m_financials[i].FI_REVENUE)
            {   // earnings cannot be greater than the revenue
                m_row.m_financials[i].FI_NET_EARN = 0.9 * m_row.m_financials[i].FI_REVENUE;
            }
            fEarn = m_row.m_financials[i].FI_NET_EARN;

            m_row.m_financials[i].FI_OUT_BASIC = fOutBasic * m_rnd.RndDoubleRange(fFinDataDownMult, fFinDataUpMult);
            fOutBasic = m_row.m_financials[i].FI_OUT_BASIC;

            m_row.m_financials[i].FI_OUT_DILUT =
                m_row.m_financials[i].FI_OUT_BASIC * fDilutedSharesMultiplier;

            m_row.m_financials[i].FI_INVENTORY = fInvent * m_rnd.RndDoubleRange(fFinDataDownMult, fFinDataUpMult);
            fInvent = m_row.m_financials[i].FI_INVENTORY;

            m_row.m_financials[i].FI_ASSETS = fAssets * m_rnd.RndDoubleRange(fFinDataDownMult, fFinDataUpMult);
            fAssets = m_row.m_financials[i].FI_ASSETS;

            m_row.m_financials[i].FI_LIABILITY = fLiab * m_rnd.RndDoubleRange(fFinDataDownMult, fFinDataUpMult);
            fLiab = m_row.m_financials[i].FI_LIABILITY;

            m_row.m_financials[i].FI_BASIC_EPS = fEarn / fOutBasic;

            m_row.m_financials[i].FI_DILUT_EPS = fEarn / (fOutBasic * fDilutedSharesMultiplier);

            m_row.m_financials[i].FI_MARGIN = // profit margin can be negative
                    m_row.m_financials[i].FI_NET_EARN /
                    m_row.m_financials[i].FI_REVENUE;

            // increment quarter
            iFinQuarter = iFinQuarter + 1;
            if (iFinQuarter == iQuartersInYear)
            {   // reached the last quarter in the year
                iFinQuarter = 0;    // start from the first quarter
                ++iFinYear; //increment year
            }
        }

        return m_CompanyTable.GenerateNextCO_ID();
    }

    /*
    *   Reset the state for the next load unit.
    *
    *   PARAMETERS:
    *           none.
    *
    *   RETURNS:
    *           none.
    */
    void InitNextLoadUnit()
    {
        m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTableDefault,
            m_iLastRowNumber * iRNGSkipOneRowFinancial));

        ClearRecord();  // this is needed for EGenTest to work
    }

public:
    CFinancialTable(CInputFiles inputFiles,
                    TIdent      iCustomerCount,
                    TIdent      iStartFromCustomer)
        : TableTemplate<FINANCIAL_GEN_ROW>()
        , m_CompanyTable(inputFiles, iCustomerCount, iStartFromCustomer)
        , m_iRowsGeneratedPerCompany(iFinsPerCompany)
        , m_bMoreCompanies(true)
    {
        // Start year to generate financials.
        // Count by quaters
        m_iFinYear = iDailyMarketBaseYear;  // first financial year
        m_iFinQuarter = iDailyMarketBaseMonth / 3;  // first financial quarter in the year (0-based)

        m_bMoreRecords = true;  // initialize once

        m_iFinancialCountForOneLoadUnit = inputFiles.Company->CalculateCompanyCount(iDefaultLoadUnitSize) * iFinsPerCompany;

        m_iLastRowNumber = inputFiles.Company->CalculateStartFromCompany(iStartFromCustomer) * iFinsPerCompany;
    };

    bool GenerateNextRecord()
    {
        // Reset RNG at Load Unit boundary, so that all data is repeatable.
        //
        if (m_iLastRowNumber % m_iFinancialCountForOneLoadUnit == 0)
        {
            InitNextLoadUnit();
        }

        ++m_iLastRowNumber;

        ++m_iRowsGeneratedPerCompany;

        if (m_iRowsGeneratedPerCompany >= iFinsPerCompany)
        {
            if (m_bMoreCompanies)
            {
                // All rows for the current company have been returned
                // therefore move on to the next company
                m_bMoreCompanies = GenerateFinancialRows();

                m_iRowsGeneratedPerCompany = 0;
            }
        }

        // Return false when generated the last row of the last company
        if (!m_bMoreCompanies && (m_iRowsGeneratedPerCompany == iFinsPerCompany - 1))
        {
            m_bMoreRecords = false;
        }

        return MoreRecords();
    }

    PFINANCIAL_ROW    GetRow()
    {
        return &m_row.m_financials[m_iRowsGeneratedPerCompany];
    }
};

}   // namespace TPCE

#endif //FINANCIAL_TABLE_H
