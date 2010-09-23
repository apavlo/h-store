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
*   Trade Result transaction class.
*
*/
#ifndef TXN_HARNESS_TRADE_RESULT_H
#define TXN_HARNESS_TRADE_RESULT_H

#include "TxnHarnessDBInterface.h"

namespace TPCE
{

class CTradeResult
{
    CTradeResultDBInterface* m_db;

public:
    CTradeResult(CTradeResultDBInterface *pDB)
        : m_db(pDB)
    {
    };

    void DoTxn( PTradeResultTxnInput pTxnInput, PTradeResultTxnOutput pTxnOutput )
    {
        TTradeResultFrame1Input     Frame1Input;
        TTradeResultFrame1Output    Frame1Output;
        memset(&Frame1Input, 0, sizeof( Frame1Input ));
        memset(&Frame1Output, 0, sizeof( Frame1Output ));

        TTradeResultFrame2Input     Frame2Input;
        TTradeResultFrame2Output    Frame2Output;
        memset(&Frame2Input, 0, sizeof( Frame2Input ));
        memset(&Frame2Output, 0, sizeof( Frame2Output ));

        TTradeResultFrame3Input     Frame3Input;
        TTradeResultFrame3Output    Frame3Output;
        memset(&Frame3Input, 0, sizeof( Frame3Input ));
        memset(&Frame3Output, 0, sizeof( Frame3Output ));

        TTradeResultFrame4Input     Frame4Input;
        TTradeResultFrame4Output    Frame4Output;
        memset(&Frame4Input, 0, sizeof( Frame4Input ));
        memset(&Frame4Output, 0, sizeof( Frame4Output ));

        TTradeResultFrame5Input     Frame5Input;
        TTradeResultFrame5Output    Frame5Output;
        memset(&Frame5Input, 0, sizeof( Frame5Input ));
        memset(&Frame5Output, 0, sizeof( Frame5Output ));

        TTradeResultFrame6Input     Frame6Input;
        TTradeResultFrame6Output    Frame6Output;
        memset(&Frame6Input, 0, sizeof( Frame6Input ));
        memset(&Frame6Output, 0, sizeof( Frame6Output ));

        //Init Frame 1 input params
        Frame1Input.trade_id = pTxnInput->trade_id;

        m_db->DoTradeResultFrame1(&Frame1Input, &Frame1Output);
        pTxnOutput->status = Frame1Output.status;

        //Init Frame 2 input params
        Frame2Input.acct_id = Frame1Output.acct_id;
        Frame2Input.hs_qty = Frame1Output.hs_qty;
        Frame2Input.is_lifo = Frame1Output.is_lifo;
        strncpy(Frame2Input.symbol, Frame1Output.symbol, sizeof(Frame2Input.symbol));
        Frame2Input.trade_id = pTxnInput->trade_id;
        Frame2Input.trade_price = pTxnInput->trade_price;
        Frame2Input.trade_qty = Frame1Output.trade_qty;
        Frame2Input.type_is_sell = Frame1Output.type_is_sell;

        m_db->DoTradeResultFrame2(&Frame2Input, &Frame2Output);
        pTxnOutput->status = Frame2Output.status;

        Frame3Output.tax_amount = 0.0;
        if (( Frame2Output.tax_status == 1 || Frame2Output.tax_status == 2 ) &&
            ( Frame2Output.sell_value > Frame2Output.buy_value ))
        {
            //Init Frame 3 input params
            Frame3Input.buy_value = Frame2Output.buy_value;
            Frame3Input.cust_id = Frame2Output.cust_id;
            Frame3Input.sell_value = Frame2Output.sell_value;
            Frame3Input.trade_id = pTxnInput->trade_id;

            m_db->DoTradeResultFrame3(&Frame3Input, &Frame3Output);
            pTxnOutput->status = Frame3Output.status;
        }

        //Init Frame 4 input params
        Frame4Input.cust_id = Frame2Output.cust_id;
        strncpy(Frame4Input.symbol, Frame1Output.symbol, sizeof(Frame4Input.symbol));
        Frame4Input.trade_qty = Frame1Output.trade_qty;
        strncpy(Frame4Input.type_id, Frame1Output.type_id, sizeof(Frame4Input.type_id));

        m_db->DoTradeResultFrame4(&Frame4Input, &Frame4Output);
        pTxnOutput->status = Frame4Output.status;

        //Init Frame 5 input params
        Frame5Input.broker_id = Frame2Output.broker_id;
        Frame5Input.comm_amount = ( Frame4Output.comm_rate / 100.00 ) * ( Frame1Output.trade_qty * pTxnInput->trade_price );
        // round up for correct precision (cents only)
        Frame5Input.comm_amount = (double)((int)(100.00 * Frame5Input.comm_amount + 0.5)) / 100.00;
        // ToDo: Need to get completed ID from constant struct!!
        strncpy( Frame5Input.st_completed_id, "CMPT", sizeof( Frame5Input.st_completed_id ));
        Frame5Input.trade_dts = Frame2Output.trade_dts;
        Frame5Input.trade_id = pTxnInput->trade_id;
        Frame5Input.trade_price = pTxnInput->trade_price;

        m_db->DoTradeResultFrame5(&Frame5Input, &Frame5Output);
        pTxnOutput->status = Frame5Output.status;

        CDateTime   due_date_time( &Frame2Output.trade_dts );
        due_date_time.Add(2, 0);    //add 2 days

        if (Frame1Output.type_is_sell)
        {
            Frame6Input.se_amount = Frame1Output.trade_qty * pTxnInput->trade_price
                                        - Frame1Output.charge
                                        - Frame5Input.comm_amount;
        }
        else
        {
            Frame6Input.se_amount = -1 * ( Frame1Output.trade_qty * pTxnInput->trade_price
                                                + Frame1Output.charge
                                                + Frame5Input.comm_amount);
        }

        // withhold tax only for certain account tax status
        if (Frame2Output.tax_status == 1)
        {
            Frame6Input.se_amount -= Frame3Output.tax_amount;
        }

        //Init Frame 6 input params
        Frame6Input.acct_id = Frame1Output.acct_id;
        due_date_time.GetTimeStamp(&Frame6Input.due_date);
        strncpy(Frame6Input.s_name, Frame4Output.s_name, sizeof(Frame6Input.s_name));
        //Frame6Input.se_amount - set above
        Frame6Input.trade_dts = Frame2Output.trade_dts;
        Frame6Input.trade_id = pTxnInput->trade_id;
        Frame6Input.trade_is_cash = Frame1Output.trade_is_cash;
        Frame6Input.trade_qty = Frame1Output.trade_qty;
        strncpy(Frame6Input.type_name, Frame1Output.type_name, sizeof(Frame6Input.type_name));

        m_db->DoTradeResultFrame6(&Frame6Input, &Frame6Output);

        pTxnOutput->acct_id = Frame1Output.acct_id;
        pTxnOutput->acct_bal = Frame6Output.acct_bal;
        pTxnOutput->status = Frame6Output.status;
    }
};

}   // namespace TPCE

#endif //TXN_HARNESS_TRADE_RESULT_H
