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
 * - Sergey Vasilevskiy, Doug Johnson, John Fowler
 */

/*
*   This file contains declarations for input flat files
*   template instantiations.
*/
#ifndef INPUT_FLAT_FILE_DECLARATIONS_H
#define INPUT_FLAT_FILE_DECLARATIONS_H

#include <iostream>
#include "TableConsts.h"
#include "Table_Defs.h"
#include "TradeTypeIDs.h"
#include "FlatFile.h"
#include "InputFile.h"
#include "InputFileNoWeight.h"

using namespace std;

//Structures that represent one row of the input files in memory

namespace TPCE
{
const int cWORD_len = 30;   // for NEWS input file

//TaxableAccountName.txt and NonTaxableAccountName.txt
typedef struct TAccountNameInputRow : public TBaseInputRow
{
    char    NAME[ cCA_NAME_len+1 ];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PAccountNameInputRow;

//AreaCodes.txt
typedef struct TAreaCodeInputRow : public TBaseInputRow
{
    //Phone number area
    char        AREA_CODE[ cAREA_len+1 ];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PAreaCodeInputRow;

//Company.txt
typedef struct TCompanyInputRow : public TBaseInputRow
{
    TIdent      CO_ID;
    char    CO_ST_ID[ cST_ID_len+1 ];
    char    CO_NAME[ cCO_NAME_len+1 ];
    char    CO_IN_ID[ cIN_ID_len+1 ];
    char    CO_DESC[ cCO_DESC_len+1 ];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PCompanyInputRow;

//CompanyCompetitor.txt
typedef struct TCompanyCompetitorInputRow : public TBaseInputRow
{
    TIdent      CP_CO_ID;
    TIdent      CP_COMP_CO_ID;
    char    CP_IN_ID[cIN_ID_len+1];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PCompanyCompetitorInputRow;

//CompanySPRate.txt
typedef struct TCompanySPRateInputRow : public TBaseInputRow
{
    //Company SP Rating
    char        CO_SP_RATE[ cCO_SP_RATE_len+1 ];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PCompanySPRateInputRow;

//MaleFirstNames.txt and FemaleFirstNames.txt
typedef struct TFirstNameInputRow : public TBaseInputRow
{
    char    FIRST_NAME[cF_NAME_len+1];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PFirstNameInputRow;


//LastNames.txt
typedef struct TLastNameInputRow : public TBaseInputRow
{
    char    LAST_NAME[cL_NAME_len+1];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PLastNameInputRow;

//NewsItem.txt
typedef struct TNewsInputRow : public TBaseInputRow
{
    char    WORD[cWORD_len+1];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PNewsInputRow;

//Street Names.txt
typedef struct TStreetNameInputRow : public TBaseInputRow
{
    char        STREET[ cAD_LINE_len+1 ];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PStreetNameInputRow;

//StreetSuffix.txt
typedef struct TStreetSuffixInputRow : public TBaseInputRow
{
    char        SUFFIX[ cAD_LINE_len+1 ];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PStreetSuffixInputRow;

//Security.txt
typedef struct TSecurityInputRow : public TBaseInputRow
{
    TIdent      S_ID;
    char    S_ISSUE[ cS_ISSUE_len+1 ];
    char    S_ST_ID[ cST_ID_len+1 ];
    char    S_SYMB[ cSYMBOL_len+1 ];
    char    S_EX_ID[ cEX_ID_len+1];
    TIdent      S_CO_ID;

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PSecuritiesInputRow;

//TaxRatesDivision.txt and TaxRatesCountry.txt
typedef struct TTaxRateInputRow : public TBaseInputRow
{
    char    TAX_ID[ cTX_ID_len+1 ];
    char    TAX_NAME[ cTX_NAME_len+1 ];
    double  TAX_RATE;   //the actual taxrate - needed to calculate tax for the TRADE table

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PTaxRateInputRow;

//ZipCode.txt
typedef struct TZipCodeInputRow : public TBaseInputRow
{
    UINT    iDivisionTaxKey;
    char    ZC_CODE[cZC_CODE_len+1];
    char    ZC_TOWN[cZC_TOWN_len+1];
    char    ZC_DIV[cZC_DIV_len+1];

    void Load(istream &file);   //loads itself (one row) from the input stream
} *PZipCodeInputRow;


/*
*
*   Limit structures for input files.
*   Limit values are set in default constructors.
*
*/


//Base structure for only the total number of elements
typedef struct TBaseElementsLimits
{
    UINT   m_iTotalElements;

    //Constructor
    TBaseElementsLimits()
        : m_iTotalElements(0)
    {
    };

    UINT   TotalElements() {return m_iTotalElements;}
    virtual ~TBaseElementsLimits() {}
} *PBaseElementsLimits;
//Base structure for the highest key and the total number of elements
typedef struct TBaseKeyElementsLimits : public TBaseElementsLimits
{
    int     m_iHighestKey;

    //Constructor
    TBaseKeyElementsLimits()
        : TBaseElementsLimits(), m_iHighestKey(0)
    {
    };

    int     HighestKey() {return m_iHighestKey;}
} *PBaseKeyElementsLimits;

//Area Codes input file limits
typedef struct TAreaCodesLimits : public TBaseKeyElementsLimits
{
    TAreaCodesLimits()
    {
        m_iHighestKey = 306;    //sum of all weights (first column in the file)
        m_iTotalElements = 284;     //# of rows in the file
    };
} *PAreaCodesLimits;

//Charge input file limits
typedef struct TChargeLimits : public TBaseElementsLimits
{
    TChargeLimits()
    {
        m_iTotalElements = 15;      //# of rows in the file
    };
} *PChargeLimits;

//Commission rate input file limits
typedef struct TCommissionRateLimits : public TBaseElementsLimits
{
    TCommissionRateLimits()
    {
        m_iTotalElements = 240;     //# of rows in the file
    };
} *PCommissionRateLimits;

//Company input file limits
typedef struct TCompanyLimits : public TBaseElementsLimits
{
    TCompanyLimits()
    {
        m_iTotalElements = 5000;        //# of rows in the file
    };
} *PCompanyLimits;

//Company Competitor input file limits
typedef struct TCompanyCompetitorLimits : public TBaseElementsLimits
{
    TCompanyCompetitorLimits()
    {
        m_iTotalElements = 15000;       //# of rows in the file
    };
} *PCompanyCompetitorLimits;

//CompanySPRate input file limits
typedef struct TCompanySPRateLimits : public TBaseKeyElementsLimits
{
    TCompanySPRateLimits()
    {
        m_iHighestKey = 39; //sum of all weights (first column in the file)
        m_iTotalElements = 39;      //# of rows in the file
    };
} *PCompanySPRateLimits;

//Exchange input file limits
typedef struct TExchangeLimits : public TBaseElementsLimits
{
    TExchangeLimits()
    {
        m_iTotalElements = 4;       //# of rows in the file
    };
} *PExchangeLimits;

//Female First Names input file limits
typedef struct TFemaleFirstNamesLimits : public TBaseKeyElementsLimits
{
    TFemaleFirstNamesLimits()
    {
        m_iHighestKey = 11890;  //sum of all weights (first column in the file)
        m_iTotalElements = 4275;        //# of rows in the file
    };
} *PFemaleFirstNamesLimits;

//Industry input file limits
typedef struct TIndustryLimits : public TBaseElementsLimits
{
    TIndustryLimits()
    {
        m_iTotalElements = 102;     //# of rows in the file
    };
} *PIndustryLimits;

//Last Names input file limits
typedef struct TLastNamesLimits : public TBaseKeyElementsLimits
{
    TLastNamesLimits()
    {
        m_iHighestKey = 69195;      //sum of all weights (first column in the file)
        m_iTotalElements = 65000;   //# of rows in the file
    };
} *PLastNamesLimits;

//Male First Names input file limits
typedef struct TMaleFirstNamesLimits : public TBaseKeyElementsLimits
{
    TMaleFirstNamesLimits()
    {
        m_iHighestKey = 9554;   //sum of all weights (first column in the file)
        m_iTotalElements = 1220;        //# of rows in the file
    };
} *PMaleFirstNamesLimits;

//News input file limits
typedef struct TNewsLimits : public TBaseKeyElementsLimits
{
    TNewsLimits()
    {
        m_iHighestKey = 69195;      //sum of all weights (first column in the file)
        m_iTotalElements = 65000;   //# of rows in the file
    };
} *PNewsLimits;

//Sector input file limits
typedef struct TSectorLimits : public TBaseElementsLimits
{
    TSectorLimits()
    {
        m_iTotalElements = 12;      //# of rows in the file
    };
} *PSectorLimits;

//Security input file limits
typedef struct TSecurityLimits : public TBaseElementsLimits
{
    TSecurityLimits()
    {
        m_iTotalElements = 6850;        //# of rows in the file
    };
} *PSecurityLimits;

//StatusType input file limits
typedef struct TStatusTypeLimits : public TBaseElementsLimits
{
    TStatusTypeLimits()
    {
        m_iTotalElements = 5;       //# of rows in the file
    };
} *PStatusTypeLimits;


//Street Names input file limits
typedef struct TStreetNamesLimits : public TBaseKeyElementsLimits
{
    TStreetNamesLimits()
    {
        m_iHighestKey = 1000;   //sum of all weights (first column in the file)
        m_iTotalElements = 1000;        //# of rows in the file
    };
} *PStreetNamesLimits;

//Street Names input file limits
typedef struct TStreetSuffixLimits : public TBaseKeyElementsLimits
{
    TStreetSuffixLimits()
    {
        m_iHighestKey = 17; //sum of all weights (first column in the file)
        m_iTotalElements = 17;      //# of rows in the file
    };
} *PStreetSuffixLimits;

//TaxableAccountName input file limits
typedef struct TTaxableAccountNameLimits : public TBaseElementsLimits
{
    TTaxableAccountNameLimits()
    {
        m_iTotalElements = 13;      //# of rows in the file
    };
} *PTaxableAccountNameLimits;

//NonTaxableAccountName input file limits
typedef struct TNonTaxableAccountNameLimits : public TBaseElementsLimits
{
    TNonTaxableAccountNameLimits()
    {
        m_iTotalElements = 11;      //# of rows in the file
    };
} *PNonTaxableAccountNameLimits;

//Taxrate input file limits
typedef struct TTaxrateLimits : public TBaseElementsLimits
{
    TTaxrateLimits()
    {
        m_iTotalElements = 320;     //# of rows in the file
    };
} *PTaxrateLimits;

//TradeType input file limits
typedef struct TTradeTypeLimits : public TBaseElementsLimits
{
    TTradeTypeLimits()
    {
        m_iTotalElements = 5;       //# of rows in the file
    };
} *PTradeTypeLimits;

//ZipCode input file limits
typedef struct TZipCodeLimits : public TBaseKeyElementsLimits
{
    TZipCodeLimits()
    {
        m_iHighestKey = 23121;  //sum of all weights (first column in the file)
        m_iTotalElements = 14741;       //# of rows in the file
    };
} *PZipCodeLimits;

// Trade Type IDs moved to a separate file (TradeTypeIDs.h)
//

// Status Type IDs corresponding to the StatusType.txt flat file.
// Note: The order of enumeration members must match the order
// of rows in the StatusType.txt flat file.
enum eStatusTypeID
{
    eCompleted = 0,
    eActive,
    eSubmitted,
    ePending,
    eCanceled,

    eMaxStatusTypeID    // should be the last - contains the number of items in the enumeration
};

// Exchange IDs corresponding to the Exchange.txt flat file.
// Note: The order of enumeration members must match the order
// of rows in the Exchange.txt flat file.
enum eExchangeID
{
    eNYSE = 0,
    eNASDAQ,
    eAMEX,
    ePCX
};

// These constants are used by security/company scaling code.
//
const int   iBaseCompanyCount           = 5000;                     // number of base companies in the flat file
const int   iBaseCompanyCompetitorCount = 3 * iBaseCompanyCount;    // number of base company competitor rows
const int   iOneLoadUnitCompanyCount    = 500;
const int   iOneLoadUnitSecurityCount   = 685;
const int   iOneLoadUnitCompanyCompetitorCount  = 3 * iOneLoadUnitCompanyCount;

/*
*
* Input files type declarations.
*
*/

typedef CInputFile<TAreaCodeInputRow, TAreaCodesLimits>             TAreaCodeFile;
typedef CInputFile<TCompanySPRateInputRow, TCompanySPRateLimits>    TCompanySPRateFile;
typedef CInputFile<TFirstNameInputRow, TFemaleFirstNamesLimits>     TFemaleFirstNamesFile;
typedef CInputFile<TLastNameInputRow, TLastNamesLimits>             TLastNamesFile;
typedef CInputFile<TFirstNameInputRow, TMaleFirstNamesLimits>       TMaleFirstNamesFile;
typedef CInputFile<TNewsInputRow, TNewsLimits>                      TNewsFile;
typedef CInputFile<TStreetNameInputRow, TStreetNamesLimits>         TStreetNamesFile;
typedef CInputFile<TStreetSuffixInputRow, TStreetSuffixLimits>      TStreetSuffixFile;
typedef CInputFile<TZipCodeInputRow, TZipCodeLimits>                TZipCodeFile;

typedef CInputFileNoWeight<TTaxRateInputRow>                        TTaxRatesCountryFile;
typedef CInputFileNoWeight<TTaxRateInputRow>                        TTaxRatesDivisionFile;

typedef CFlatFile<CHARGE_ROW, TChargeLimits>                        TChargeFile;
typedef CFlatFile<COMMISSION_RATE_ROW, TCommissionRateLimits>       TCommissionRateFile;
typedef CFlatFile<EXCHANGE_ROW, TExchangeLimits>                    TExchangeFile;
typedef CFlatFile<INDUSTRY_ROW, TIndustryLimits>                    TIndustryFile;
typedef CFlatFile<SECTOR_ROW, TSectorLimits>                        TSectorFile;
typedef CFlatFile<STATUS_TYPE_ROW, TStatusTypeLimits>               TStatusTypeFile;
typedef CFlatFile<TAccountNameInputRow, TTaxableAccountNameLimits>  TTaxableAccountNameFile;
typedef CFlatFile<TAccountNameInputRow, TNonTaxableAccountNameLimits> TNonTaxableAccountNameFile;
typedef CFlatFile<TAXRATE_ROW, TTaxrateLimits>                      TTaxrateFile;
typedef CFlatFile<TRADE_TYPE_ROW, TTradeTypeLimits>                 TTradeTypeFile;

}   // namespace TPCE

#endif //INPUT_FLAT_FILE_DECLARATIONS_H
