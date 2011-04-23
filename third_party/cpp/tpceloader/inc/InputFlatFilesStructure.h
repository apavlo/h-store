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
 * - Sergey Vasilevskiy, Matt Emmerton
 */

/******************************************************************************
*   Description:        Superstructure that contains all the input flat files used
*                       by the loader and the driver.
******************************************************************************/

#ifndef INPUT_FLAT_FILE_STRUCTURE_H
#define INPUT_FLAT_FILE_STRUCTURE_H

#include "EGenStandardTypes.h"
#include "DriverTypes.h"
#include "InputFlatFilesDeclarations.h"
#include "CompanyFile.h"
#include "CompanyCompetitorFile.h"
#include "SecurityFile.h"

namespace TPCE
{

enum eOutputVerbosity
{
  eOutputQuiet,
  eOutputVerbose
};

//Pointers to all the input files structure
class CInputFiles
{
  public:
    TAreaCodeFile               *AreaCodes;
    TChargeFile                 *Charge;
    TCommissionRateFile         *CommissionRate;
    CCompanyFile                *Company;
    CCompanyCompetitorFile      *CompanyCompetitor;
    TCompanySPRateFile          *CompanySPRate;
    TExchangeFile               *Exchange;
    TFemaleFirstNamesFile       *FemaleFirstNames;
    TIndustryFile               *Industry;
    TLastNamesFile              *LastNames;
    TMaleFirstNamesFile         *MaleFirstNames;
    TNewsFile                   *News;
    TSectorFile                 *Sectors;
    CSecurityFile               *Securities;
    TStatusTypeFile             *StatusType;
    TStreetNamesFile            *Street;
    TStreetSuffixFile           *StreetSuffix;
    TTaxableAccountNameFile     *TaxableAccountName;
    TNonTaxableAccountNameFile  *NonTaxableAccountName;
    TTaxRatesCountryFile        *TaxRatesCountry;
    TTaxRatesDivisionFile       *TaxRatesDivision;
    TTradeTypeFile              *TradeType;
    TZipCodeFile                *ZipCode;

    CInputFiles() 
        : AreaCodes(NULL)
        , Charge(NULL)
        , CommissionRate(NULL)
        , Company(NULL)
        , CompanyCompetitor(NULL)
        , CompanySPRate(NULL)
        , Exchange(NULL)
        , FemaleFirstNames(NULL)
        , Industry(NULL)
        , LastNames(NULL)
        , MaleFirstNames(NULL)
        , News(NULL)
        , Sectors(NULL)
        , Securities(NULL)
        , StatusType(NULL)
        , Street(NULL)
        , StreetSuffix(NULL)
        , TaxableAccountName(NULL)
        , NonTaxableAccountName(NULL)
        , TaxRatesCountry(NULL)
        , TaxRatesDivision(NULL)
        , TradeType(NULL)
        , ZipCode(NULL)
    {};
    ~CInputFiles() {};

        bool Initialize(eDriverType eType, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, const char *szPathName);
};

}   // namespace TPCE

#endif // INPUT_FLAT_FILE_STRUCTURE_H
