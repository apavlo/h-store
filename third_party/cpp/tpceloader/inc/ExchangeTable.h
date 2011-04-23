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
 * - Doug Johnson
 */

/*
*   Class representing the Exchange table.
*/
#ifndef EXCHANGE_TABLE_H
#define EXCHANGE_TABLE_H

#include "EGenTables_common.h"

namespace TPCE
{

const int iSecurityCounts[4][11] = { { 0, 153, 307, 491, 688, 859, 1028, 1203, 1360, 1532, 1704 },
                                     { 0, 173, 344, 498, 658, 848, 1006, 1191, 1402, 1572, 1749 },
                                     { 0, 189, 360, 534, 714, 875, 1023, 1174, 1342, 1507, 1666 },
                                     { 0, 170, 359, 532, 680, 843, 1053, 1227, 1376, 1554, 1731 } };

class CExchangeTable : public TableTemplate<EXCHANGE_ROW>
{
    ifstream            InFile;
    TIdent              m_iCurExchange;
    INT32               m_iNumSecurities[4];

    /*
    *   Computes the number of securities in each exchange.
    *   Assumption is that exchanges are ordered in NYSE, NASDAQ, AMEX, PCX order.
    *   (This is the current ordering of exchanges in the flat_in/EXCHANGE.txt file.)
    */
    void ComputeNumSecurities( TIdent iCustomerCount )
    {
        INT32 numLU     = static_cast<INT32>(iCustomerCount / 1000);
        INT32 numLU_Tens = numLU / 10;
        INT32 numLU_Ones = numLU % 10;

        for (int i=0; i<4; i++)
        {
            m_iNumSecurities[i] = iSecurityCounts[i][10] * numLU_Tens + iSecurityCounts[i][numLU_Ones];
        }
    }

public:
    CExchangeTable( char *szDirName, TIdent iConfiguredCustomerCount )
        : TableTemplate<EXCHANGE_ROW>()
    {
        char szFileName[iMaxPath];

        strncpy(szFileName, szDirName, sizeof(szFileName));
        strncat(szFileName, "EXCHANGE.txt", sizeof(szFileName) - strlen(szDirName) - 1);

        InFile.open( szFileName );

        ComputeNumSecurities(iConfiguredCustomerCount);

        m_iCurExchange = 1;
    };

    ~CExchangeTable( )
    {
        InFile.close();
    };

    /*
    *   Generates all column values for the next row.
    */
    bool GenerateNextRecord()
    {
        if( InFile.good() )
        {
            m_row.Load(InFile);
            m_row.EX_AD_ID += iTIdentShift;
            m_row.EX_NUM_SYMB = m_iNumSecurities[m_iCurExchange-1];

            m_iCurExchange++;
        }

        return ( InFile.eof() );
    }
};

}   // namespace TPCE

#endif //EXCHANGE_TABLE_H
