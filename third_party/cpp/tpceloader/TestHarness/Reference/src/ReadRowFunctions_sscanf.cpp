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
 * - Christopher Chan-Nui
 */
/*
 * This file is functionally equivalent to the ReadRowFunctions.cpp file.
 * Some iostream implementations preform very poorly, so these versions of the
 * readrow functions fetch a line into a buffer, and then use sscanf to parse
 * it.
 *
 * In order to use this, modify the makefiles in the prj directory to use this
 * file rather than ReadRowFunctions.cpp.
 */

/*
*   This file contains functions that read rows from different input files.
*/

#include <stdexcept>
#include "../inc/EGenTables_stdafx.h"

using namespace TPCE;

/*
*   Function to read customer account names from the input stream.
*/
void TAccountNameInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            NAME);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TAccountNameInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    // need to eat end-of-line or it will be read into NAME
    file>>ws;
    file.get(NAME, sizeof(NAME), '\n');
#endif
}

/*
 *   Function to read phone row from the input stream.
 *   Needed to construct phones list.
 */
void TAreaCodeInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            AREA_CODE);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TAreaCodeInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>AREA_CODE;
#endif
}

void TCompanyInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%"PRId64"\t%s\t%[^\t]\t%s\t%[^\n]",
            &CO_ID, CO_ST_ID, CO_NAME, CO_IN_ID, CO_DESC);
    if (rc != 5) {
        std::ostringstream strm;
        strm << "TCompanyInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>CO_ID;
    file>>CO_ST_ID>>ws;
    file.get(CO_NAME, sizeof(CO_NAME), '\t');
    file>>CO_IN_ID>>ws;
    file.get(CO_DESC, sizeof(CO_DESC), '\n');
#endif
}

/*
 *   Function to read CompanyCompetitor row from the input stream.
 */
void TCompanyCompetitorInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%"PRId64"\t%"PRId64"\t%[^\n]",
            &CP_CO_ID, &CP_COMP_CO_ID, CP_IN_ID);
    if (rc != 3) {
        std::ostringstream strm;
        strm << "TCompanyCompetitorInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file>>CP_CO_ID;
    file>>ws;
    file>>CP_COMP_CO_ID;
    file>>ws;
    file.get(CP_IN_ID, sizeof(CP_IN_ID), '\n');
#endif
}

/*
 *   Function to read Company SP Rate row from the input stream.
 */
void TCompanySPRateInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            CO_SP_RATE);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TCompanySPRateInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file.get( CO_SP_RATE, sizeof( CO_SP_RATE ), '\n' );
#endif
}

/*
 *   Function to read first/last name row from the input stream.
 *   Needed to construct phones list.
 */
void TFirstNameInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            FIRST_NAME);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TFirstNameInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>FIRST_NAME;   //one field only
#endif
}

void TLastNameInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            LAST_NAME);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TLastNameInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>LAST_NAME;    //one field only
#endif
}

void TNewsInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            WORD);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TNewsInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>WORD; //one field only
#endif
}

void TSecurityInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%"PRId64"\t%s\t%s\t%s\t%s\t%"PRId64,
            &S_ID, S_ST_ID, S_SYMB, S_ISSUE, S_EX_ID, &S_CO_ID);
    if (rc != 6) {
        std::ostringstream strm;
        strm << "TSecurityInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>S_ID;
    file>>S_ST_ID>>ws;
    file>>S_SYMB;
    file>>S_ISSUE;
    file>>S_EX_ID;
    file>>S_CO_ID;
#endif
}

/*
 *   Function to read one row from the input stream.
 */
void TStreetNameInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            STREET);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TStreetNameInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file.get(STREET, sizeof(STREET)-1, '\n');   //read up to the delimiter
#endif
}

/*
 *   Function to read one row from the input stream.
 */
void TStreetSuffixInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\n]",
            SUFFIX);
    if (rc != 1) {
        std::ostringstream strm;
        strm << "TStreetSuffixInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file.get(SUFFIX, sizeof(SUFFIX)-1, '\n');   //read up to the delimiter
#endif
}

/*
 *   Function to read row from the input stream.
 */
void TTaxRateInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\t]\t%[^\t]\t%lf",
            TAX_ID, TAX_NAME, &TAX_RATE);
    if (rc != 3) {
        std::ostringstream strm;
        strm << "TTaxRateInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>TAX_ID;
    file>>ws;   //advance past whitespace to the next field
    file.get(TAX_NAME, sizeof(TAX_NAME), '\t');
    //now read the actual taxrate
    file>>TAX_RATE;
#endif
}

/*
 *   Function to read one row from the input stream.
 */
void TZipCodeInputRow::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%d\t%[^\t]\t%[^\t]\t%[^\n]",
            &iDivisionTaxKey, ZC_CODE, ZC_TOWN, ZC_DIV);
    if (rc != 4) {
        std::ostringstream strm;
        strm << "TZipCodeInputRow::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file>>iDivisionTaxKey;
    file>>ws;
    file.get( ZC_CODE, sizeof( ZC_CODE ), '\t' );
    file>>ws;
    file.get( ZC_TOWN, sizeof( ZC_TOWN ), '\t' );   //read up to the delimiter
    file>>ws;
    file.get( ZC_DIV, sizeof( ZC_DIV ), '\n' );
#endif
}

/***********************************************************************************
 *
 * Tables that are fully represented by flat files (no additional processing needed).
 *
 ************************************************************************************/

/*
 *   CHARGE
 */
void CHARGE_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\t]\t%d\t%lf",
            CH_TT_ID, &CH_C_TIER, &CH_CHRG);
    if (rc != 3) {
        std::ostringstream strm;
        strm << "CHARGE_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>CH_TT_ID;
    file>>CH_C_TIER;
    file>>CH_CHRG;
#endif
}

/*
 *   COMMISSION_RATE
 */
void COMMISSION_RATE_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%d\t%[^\t]\t%[^\t]\t%lf\t%lf\t%lf",
            &CR_C_TIER, CR_TT_ID, CR_EX_ID, &CR_FROM_QTY, &CR_TO_QTY, &CR_RATE);
    if (rc != 6) {
        std::ostringstream strm;
        strm << "COMMISSION_RATE_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>CR_C_TIER;
    file>>CR_TT_ID;
    file>>CR_EX_ID;
    file>>CR_FROM_QTY;
    file>>CR_TO_QTY;
    file>>CR_RATE;
#endif
}

/*
 *   EXCHANGE
 */
void EXCHANGE_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%[^\t]\t%[^\t]\t%d\t%d\t%[^\t]%"PRId64,
            EX_ID, EX_NAME, &EX_OPEN, &EX_CLOSE, EX_DESC, &EX_AD_ID);
    if (rc != 6) {
        std::ostringstream strm;
        strm << "EXCHANGE_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file.get(EX_ID, sizeof(EX_ID), '\t');   //read and skip past the next tab
    file>>ws;
    file.get(EX_NAME, sizeof(EX_NAME), '\t');   //read up to the delimiter
    file>>ws;
    file>>EX_OPEN;
    file>>ws;
    file>>EX_CLOSE;
    file>>ws;
    file.get(EX_DESC, sizeof(EX_DESC), '\t');   //read up to the delimiter
    file>>ws;
    file>>EX_AD_ID;
#endif
}

/*
 *   INDUSTRY
 */
void INDUSTRY_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%[^\t]\t%[^\t]\t%[^\n]",
            IN_ID, IN_NAME, IN_SC_ID);
    if (rc != 3) {
        std::ostringstream strm;
        strm << "INDUSTRY_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>IN_ID>>ws;
    file.get(IN_NAME, sizeof(IN_NAME), '\t');   //read up to the delimiter
    file>>ws;
    file>>IN_SC_ID;
#endif
}

/*
 *   SECTOR
 */
void SECTOR_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%[^\t]\t%[^\n]",
            SC_ID, SC_NAME);
    if (rc != 2) {
        std::ostringstream strm;
        strm << "SECTOR_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>SC_ID>>ws;    //read and skip past the next tab
    file.get(SC_NAME, sizeof(SC_NAME), '\n');   //read up to the delimiter
#endif
}

/*
 *   STATUS_TYPE
 */
void STATUS_TYPE_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%[^\t]\t%[^\n]",
            ST_ID, ST_NAME);
    if (rc != 2) {
        std::ostringstream strm;
        strm << "STATUS_TYPE_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>ws;
    file>>ST_ID;
    file>>ws;
    file.get(ST_NAME, sizeof(ST_NAME), '\n');
#endif
}

/*
 *   TRADE_TYPE
 */
void TRADE_TYPE_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "%[^\t]\t%[^\t]\t%d\t%d",
            TT_ID, TT_NAME, &TT_IS_SELL, &TT_IS_MRKT);
    if (rc != 4) {
        std::ostringstream strm;
        strm << "TRADE_TYPE_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file>>TT_ID>>ws;
    file.get(TT_NAME, sizeof(TT_NAME), '\t');
    file>>ws;
    file>>TT_IS_SELL;
    file>>TT_IS_MRKT;
#endif
}

/*
 *   ZIP_CODE
 */
void ZIP_CODE_ROW::Load(istream &file)
{
    char buf[1024];
    file.getline(buf, sizeof(buf));
    if (file.eof()) {
        return;
    }
    int rc = sscanf(buf, "\t%[^\t]\t%[^\t]\t%[^\n]",
            ZC_TOWN, ZC_DIV, ZC_CODE);
    if (rc != 3) {
        std::ostringstream strm;
        strm << "ZIP_CODE_ROW::Load only loaded " << rc << " values from line";
        throw std::runtime_error(strm.str());
    }
#if 0
    file.get(ZC_TOWN, sizeof(ZC_TOWN)-1, '\t'); //read up to the delimiter
    file>>ZC_DIV;
    file>>ZC_CODE;
    file>>ws;
#endif
}
