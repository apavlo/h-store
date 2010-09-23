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
*   Trade Order transaction class.
*
*/
#ifndef TXN_HARNESS_TRADE_ORDER_H
#define TXN_HARNESS_TRADE_ORDER_H

#include "error.h"
#include <stdio.h>
#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeOrder
{
    CTradeOrderDBInterface* m_db;
    CSendToMarketInterface* m_pSendToMarket;

public:
    CTradeOrder(CTradeOrderDBInterface *pDB, CSendToMarketInterface *pSendToMarket)
        : m_db(pDB),
        m_pSendToMarket(pSendToMarket)
    {
    };

    void DoTxn( PTradeOrderTxnInput pTxnInput, PTradeOrderTxnOutput pTxnOutput )
    {
        TTradeOrderFrame1Input  Frame1Input;
        TTradeOrderFrame1Output Frame1Output;
        memset(&Frame1Input, 0, sizeof( Frame1Input ));
        memset(&Frame1Output, 0, sizeof( Frame1Output ));

        TTradeOrderFrame2Input  Frame2Input;
        TTradeOrderFrame2Output Frame2Output;
        memset(&Frame2Input, 0, sizeof( Frame2Input ));
        memset(&Frame2Output, 0, sizeof( Frame2Output ));

        TTradeOrderFrame3Input  Frame3Input;
        TTradeOrderFrame3Output Frame3Output;
        memset(&Frame3Input, 0, sizeof( Frame3Input ));
        memset(&Frame3Output, 0, sizeof( Frame3Output ));

        TTradeOrderFrame4Input  Frame4Input;
        TTradeOrderFrame4Output Frame4Output;
        memset(&Frame4Input, 0, sizeof( Frame4Input ));
        memset(&Frame4Output, 0, sizeof( Frame4Output ));

        TTradeOrderFrame5Output Frame5Output;
        memset(&Frame5Output, 0, sizeof( Frame5Output ));

        TTradeOrderFrame6Output Frame6Output;
        memset(&Frame6Output, 0, sizeof( Frame6Output ));

        TTradeRequest           TradeRequestForMEE; //sent to MEE

        //Init Frame1 input params
        Frame1Input.acct_id = pTxnInput->acct_id;
        m_db->DoTradeOrderFrame1(&Frame1Input, &Frame1Output );

        Frame2Output.ap_acl[0] = '\0';

        if (strcmp(pTxnInput->exec_l_name, Frame1Output.cust_l_name)
            || strcmp(pTxnInput->exec_f_name, Frame1Output.cust_f_name)
            || strcmp(pTxnInput->exec_tax_id, Frame1Output.tax_id))
        {
            Frame2Input.acct_id = pTxnInput->acct_id;
            strncpy(Frame2Input.exec_f_name, pTxnInput->exec_f_name, sizeof(Frame2Input.exec_f_name));
            strncpy(Frame2Input.exec_l_name, pTxnInput->exec_l_name, sizeof(Frame2Input.exec_l_name));
            strncpy(Frame2Input.exec_tax_id, pTxnInput->exec_tax_id, sizeof(Frame2Input.exec_tax_id));

            m_db->DoTradeOrderFrame2(&Frame2Input, &Frame2Output);

            if (Frame2Output.ap_acl[0] == '\0')
            {   //Frame 2 found unauthorized executor
                m_db->DoTradeOrderFrame5(&Frame5Output); //Rollback
                pTxnOutput->status = CBaseTxnErr::UNAUTHORIZED_EXECUTOR;    //return error code
                return;
            }

            pTxnOutput->status = Frame2Output.status;
        }

        //Init Frame 3 input params
        Frame3Input.acct_id = pTxnInput->acct_id;
        Frame3Input.cust_id = Frame1Output.cust_id;
        Frame3Input.cust_tier = Frame1Output.cust_tier;
        Frame3Input.is_lifo = pTxnInput->is_lifo;
        strncpy(Frame3Input.issue, pTxnInput->issue, sizeof(Frame3Input.issue));
        strncpy(Frame3Input.st_pending_id, pTxnInput->st_pending_id, sizeof(Frame3Input.st_pending_id));
        strncpy(Frame3Input.st_submitted_id, pTxnInput->st_submitted_id, sizeof(Frame3Input.st_submitted_id));
        Frame3Input.tax_status = Frame1Output.tax_status;
        Frame3Input.trade_qty = pTxnInput->trade_qty;
        strncpy(Frame3Input.trade_type_id, pTxnInput->trade_type_id, sizeof(Frame3Input.trade_type_id));
        Frame3Input.type_is_margin = pTxnInput->type_is_margin;
        strncpy(Frame3Input.co_name, pTxnInput->co_name, sizeof(Frame3Input.co_name));
        Frame3Input.requested_price = pTxnInput->requested_price;
        strncpy(Frame3Input.symbol, pTxnInput->symbol, sizeof(Frame3Input.symbol));

        m_db->DoTradeOrderFrame3(&Frame3Input, &Frame3Output);

        pTxnOutput->buy_value = Frame3Output.buy_value;   //output param
        pTxnOutput->sell_value = Frame3Output.sell_value;
        pTxnOutput->tax_amount = Frame3Output.tax_amount;

        //Frame 4 input params
        Frame4Input.acct_id = pTxnInput->acct_id;
        Frame4Input.broker_id = Frame1Output.broker_id;
        Frame4Input.charge_amount = Frame3Output.charge_amount;
        Frame4Input.comm_amount = Frame3Output.comm_rate / 100
                                          * pTxnInput->trade_qty
                                          * Frame3Output.requested_price;
        // round up for correct precision (cents only)
        Frame4Input.comm_amount = (double)((int)(100.00 * Frame4Input.comm_amount + 0.5)) / 100.00;

        sprintf(Frame4Input.exec_name, "%s %s", pTxnInput->exec_f_name, pTxnInput->exec_l_name);
        Frame4Input.is_cash = !Frame3Input.type_is_margin;
        Frame4Input.is_lifo = pTxnInput->is_lifo;
        Frame4Input.requested_price = Frame3Output.requested_price;
        strncpy(Frame4Input.status_id, Frame3Output.status_id, sizeof(Frame4Input.status_id));
        strncpy(Frame4Input.symbol, Frame3Output.symbol, sizeof(Frame4Input.symbol));
        Frame4Input.trade_qty = pTxnInput->trade_qty;
        strncpy(Frame4Input.trade_type_id, pTxnInput->trade_type_id, sizeof(Frame4Input.trade_type_id));
        Frame4Input.type_is_market = Frame3Output.type_is_market;

        m_db->DoTradeOrderFrame4(&Frame4Input, &Frame4Output);

        pTxnOutput->trade_id = Frame4Output.trade_id;   //output param
        pTxnOutput->status = Frame4Output.status;

        if (pTxnInput->roll_it_back)
        {
            m_db->DoTradeOrderFrame5(&Frame5Output);
            pTxnOutput->status = Frame5Output.status;
        }
        else
        {
            m_db->DoTradeOrderFrame6(&Frame6Output);
            pTxnOutput->status = Frame6Output.status;

            //
            //Send to Market Emulator here.
            //
            TradeRequestForMEE.price_quote = Frame4Input.requested_price;
            strncpy(TradeRequestForMEE.symbol, Frame4Input.symbol, sizeof(TradeRequestForMEE.symbol));
            TradeRequestForMEE.trade_id = Frame4Output.trade_id;
            TradeRequestForMEE.trade_qty = Frame4Input.trade_qty;
            strncpy( TradeRequestForMEE.trade_type_id, pTxnInput->trade_type_id, sizeof( TradeRequestForMEE.trade_type_id ));
            //TradeRequestForMEE.eTradeType = pTxnInput->eSTMTradeType;
            if( Frame4Input.type_is_market )
            {
                TradeRequestForMEE.eAction = eMEEProcessOrder;
            }
            else
            {
                TradeRequestForMEE.eAction = eMEESetLimitOrderTrigger;
            }

            m_pSendToMarket->SendToMarketFromHarness(TradeRequestForMEE); // maybe should check the return code here
        }

    }
};

}   // namespace TPCE

#endif //TXN_HARNESS_TRADE_ORDER_H
