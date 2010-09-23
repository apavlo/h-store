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
*   Class performing the final modification on a row of data,
*   before the row is ready to be passed to a loader.
*   This class doesn't know anything about relationships between tables
*   or how the data in table rows is interdependent. Because of that,
*   this final transformation of data is separated into a new class,
*   instead of adding functionality to individual table classes.
*/
#ifndef FINAL_TRANSFORM_H
#define FINAL_TRANSFORM_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

class CFinalTransform
{
public:

    void Transform(PACCOUNT_PERMISSION_ROW  pRow)
    {
        pRow->AP_CA_ID += iTIdentShift;
    }

    void Transform(PADDRESS_ROW pRow)
    {
        pRow->AD_ID += iTIdentShift;
    }

    void Transform(PBROKER_ROW  pRow)
    {
        pRow->B_ID += iTIdentShift;
    }

    void Transform(PCASH_TRANSACTION_ROW    pRow)
    {
        pRow->CT_T_ID += iTTradeShift;
    }

    void Transform(PCHARGE_ROW  pRow)
    {
        // do nothing
    }

    void Transform(PCOMMISSION_RATE_ROW pRow)
    {
        // do nothing
    }

    void Transform(PCOMPANY_ROW pRow)
    {
        pRow->CO_ID += iTIdentShift;
        pRow->CO_AD_ID += iTIdentShift;
    }

    void Transform(PCOMPANY_COMPETITOR_ROW  pRow)
    {
        pRow->CP_CO_ID += iTIdentShift;
        pRow->CP_COMP_CO_ID += iTIdentShift;
    }

    void Transform(PCUSTOMER_ROW    pRow)
    {
        pRow->C_ID += iTIdentShift;
        pRow->C_AD_ID += iTIdentShift;
    }

    void Transform(PCUSTOMER_ACCOUNT_ROW    pRow)
    {
        pRow->CA_ID += iTIdentShift;
        pRow->CA_B_ID += iTIdentShift;
        pRow->CA_C_ID += iTIdentShift;
    }

    void Transform(PCUSTOMER_TAXRATE_ROW    pRow)
    {
        pRow->CX_C_ID += iTIdentShift;
    }

    void Transform(PDAILY_MARKET_ROW    pRow)
    {
        // do nothing
    }

    void Transform(PEXCHANGE_ROW    pRow)
    {
        pRow->EX_AD_ID += iTIdentShift;
    }

    void Transform(PFINANCIAL_ROW   pRow)
    {
        pRow->FI_CO_ID += iTIdentShift;
    }

    void Transform(PHOLDING_ROW pRow)
    {
        pRow->H_T_ID += iTTradeShift;
        pRow->H_CA_ID += iTIdentShift;
    }

    void Transform(PHOLDING_HISTORY_ROW pRow)
    {
        pRow->HH_H_T_ID += iTTradeShift;
        pRow->HH_T_ID += iTTradeShift;
    }

    void Transform(PHOLDING_SUMMARY_ROW pRow)
    {
        pRow->HS_CA_ID += iTIdentShift;
    }

    void Transform(PINDUSTRY_ROW    pRow)
    {
        // do nothing
    }

    void Transform(PLAST_TRADE_ROW  pRow)
    {
        // do nothing
    }

    void Transform(PNEWS_ITEM_ROW   pRow)
    {
        pRow->NI_ID += iTIdentShift;
    }

    void Transform(PNEWS_XREF_ROW   pRow)
    {
        pRow->NX_NI_ID += iTIdentShift;
        pRow->NX_CO_ID += iTIdentShift;
    }

    void Transform(PSECTOR_ROW  pRow)
    {
        // do nothing
    }

    void Transform(PSECURITY_ROW    pRow)
    {
        pRow->S_CO_ID += iTIdentShift;
    }

    void Transform(PSETTLEMENT_ROW  pRow)
    {
        pRow->SE_T_ID += iTTradeShift;
    }

    void Transform(PSTATUS_TYPE_ROW pRow)
    {
        // do nothing
    }

    void Transform(PTAXRATE_ROW pRow)
    {
        // do nothing
    }

    void Transform(PTRADE_ROW   pRow)
    {
        pRow->T_ID += iTTradeShift;
        pRow->T_CA_ID += iTIdentShift;
    }

    void Transform(PTRADE_HISTORY_ROW   pRow)
    {
        pRow->TH_T_ID += iTTradeShift;
    }

    void Transform(PTRADE_REQUEST_ROW   pRow)
    {
        pRow->TR_T_ID += iTTradeShift;
        pRow->TR_B_ID += iTIdentShift;
    }

    void Transform(PTRADE_TYPE_ROW  pRow)
    {
        // do nothing
    }

    void Transform(PWATCH_ITEM_ROW  pRow)
    {
        pRow->WI_WL_ID += iTIdentShift;
    }

    void Transform(PWATCH_LIST_ROW  pRow)
    {
        pRow->WL_ID += iTIdentShift;
        pRow->WL_C_ID += iTIdentShift;
    }

    void Transform(PZIP_CODE_ROW    pRow)
    {
        // do nothing
    }

    // These methods are for run-time parameter generation.
    //
    void TransformCustomerId(TIdent*    pC_ID)      // for customer id
    {
        *pC_ID += iTIdentShift;
    }

    void TransformCustomerAccountId(TIdent* pCA_ID) // for customer account id
    {
        *pCA_ID += iTIdentShift;
    }

    void TransformTradeId(TTrade*   pT_ID)          // for trade id
    {
        *pT_ID += iTTradeShift;
    }
};

}   // namespace TPCE

#endif //FINAL_TRANSFORM_H
