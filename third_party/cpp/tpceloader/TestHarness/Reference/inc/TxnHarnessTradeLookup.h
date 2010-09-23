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

#ifndef TXN_HARNESS_TRADE_LOOKUP_H
#define TXN_HARNESS_TRADE_LOOKUP_H

#include <assert.h>
#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeLookup
{
    CTradeLookupDBInterface* m_db;

public:
    CTradeLookup(CTradeLookupDBInterface *pDB)
        : m_db(pDB)
    {
    };

    void DoTxn( PTradeLookupTxnInput pTxnInput, PTradeLookupTxnOutput pTxnOutput )
    {
        int i;

        memset( pTxnOutput, 0, sizeof( *pTxnOutput ));
        switch( pTxnInput->frame_to_execute )
        {
        case 1:
            TTradeLookupFrame1Input     Frame1Input;
            TTradeLookupFrame1Output    Frame1Output;
            memset(&Frame1Input, 0, sizeof( Frame1Input ));
            memset(&Frame1Output, 0, sizeof( Frame1Output ));

            Frame1Input.max_trades = pTxnInput->max_trades;
            memcpy( Frame1Input.trade_id, pTxnInput->trade_id, sizeof( Frame1Input.trade_id ));

            m_db->DoTradeLookupFrame1( &Frame1Input, &Frame1Output );

            pTxnOutput->frame_executed = 1;
            for (i = 0; i < Frame1Output.num_found; ++i)
            {
                pTxnOutput->is_cash[i] = Frame1Output.trade_info[i].is_cash;
                pTxnOutput->is_market[i] = Frame1Output.trade_info[i].is_market;
            }
            pTxnOutput->num_found = Frame1Output.num_found;
            pTxnOutput->status = Frame1Output.status;
            break;

        case 2:
            TTradeLookupFrame2Input     Frame2Input;
            TTradeLookupFrame2Output    Frame2Output;
            memset(&Frame2Input, 0, sizeof( Frame2Input ));
            memset(&Frame2Output, 0, sizeof( Frame2Output ));

            Frame2Input.acct_id = pTxnInput->acct_id;
            Frame2Input.max_trades = pTxnInput->max_trades;
            Frame2Input.start_trade_dts = pTxnInput->start_trade_dts;
            Frame2Input.end_trade_dts = pTxnInput->end_trade_dts;

            m_db->DoTradeLookupFrame2( &Frame2Input, &Frame2Output );

            pTxnOutput->frame_executed = 2;
            for (i = 0; i < Frame2Output.num_found; ++i)
            {
                pTxnOutput->is_cash[i] = Frame2Output.trade_info[i].is_cash;
                pTxnOutput->trade_list[i] = Frame2Output.trade_info[i].trade_id;
            }
            pTxnOutput->num_found = Frame2Output.num_found;
            pTxnOutput->status = Frame2Output.status;
            break;

        case 3:
            TTradeLookupFrame3Input     Frame3Input;
            TTradeLookupFrame3Output    Frame3Output;
            memset(&Frame3Input, 0, sizeof( Frame3Input ));
            memset(&Frame3Output, 0, sizeof( Frame3Output ));

            Frame3Input.max_trades = pTxnInput->max_trades;
            strncpy( Frame3Input.symbol, pTxnInput->symbol, sizeof( Frame3Input.symbol ) -1 );
            Frame3Input.start_trade_dts = pTxnInput->start_trade_dts;
            Frame3Input.end_trade_dts = pTxnInput->end_trade_dts;
            Frame3Input.max_acct_id = pTxnInput->max_acct_id;

            m_db->DoTradeLookupFrame3( &Frame3Input, &Frame3Output );

            pTxnOutput->frame_executed = 3;
            pTxnOutput->num_found = Frame3Output.num_found;
            pTxnOutput->status = Frame3Output.status;
            for (i = 0; i < Frame3Output.num_found; ++i)
            {
                pTxnOutput->trade_list[i] = Frame3Output.trade_info[i].trade_id;
            }
            break;

        case 4:
            TTradeLookupFrame4Input     Frame4Input;
            TTradeLookupFrame4Output    Frame4Output;
            memset(&Frame4Input, 0, sizeof( Frame4Input ));
            memset(&Frame4Output, 0, sizeof( Frame4Output ));

            Frame4Input.acct_id = pTxnInput->acct_id;
            Frame4Input.trade_dts = pTxnInput->start_trade_dts;

            m_db->DoTradeLookupFrame4( &Frame4Input, &Frame4Output );

            pTxnOutput->frame_executed = 4;
            pTxnOutput->num_found = Frame4Output.num_found;
            pTxnOutput->status = Frame4Output.status;
            pTxnOutput->trade_list[0] = Frame4Output.trade_id;
            break;

        default:
            // should never get here.
            assert( false );
            break;
        }
    }
};

}   // namespace TPCE

#endif //TXN_HARNESS_TRADE_LOOKUP_H
