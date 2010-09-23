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

#ifndef TXN_HARNESS_SECURITY_DETAIL_H
#define TXN_HARNESS_SECURITY_DETAIL_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CSecurityDetail
{
    CSecurityDetailDBInterface* m_db;

public:
    CSecurityDetail(CSecurityDetailDBInterface *pDB)
        : m_db(pDB)
    {
    };

    void DoTxn( PSecurityDetailTxnInput pTxnInput, PSecurityDetailTxnOutput pTxnOutput )
    {
        TSecurityDetailFrame1Output Frame1Output;
        // We purposely do not memset the whole structure to 0
        // because of the large LOB members. So instead we just
        // 0 out the specific members that get returned out.
        //memset(&Frame1Output, 0, sizeof( Frame1Output ));
        pTxnOutput->last_vol = 0;
        pTxnOutput->news_len = 0;
        pTxnOutput->status = 0;

        m_db->DoSecurityDetailFrame1(pTxnInput, &Frame1Output);

        pTxnOutput->last_vol = Frame1Output.last_vol;
        pTxnOutput->news_len = Frame1Output.news_len;
        pTxnOutput->status = Frame1Output.status;
    }
};

}   // namespace TPCE

#endif //TXN_HARNESS_SECURITY_DETAIL_H
