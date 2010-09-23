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

#include "../inc/EGenTables_stdafx.h"

using namespace TPCE;

// Price change function period in seconds
//
const int   iSecPricePeriod = 15 * SecondsPerMinute;    // set to 15 minutes, in seconds

// Number of RNG calls for one simulated trade
const int iRNGSkipOneTrade = 11;    // average count for v3.5: 6.5

// Operator for priority queue
//
namespace TPCE
{

//Need const reference left argument for greater<TTradeInfo> comparison function
bool operator >(const TTradeInfo& l, const TTradeInfo& r)
{
    return l.CompletionTime > r.CompletionTime;
}

}   // namespace TPCE

/*
*   Constructor.
*   Creates priority queue.
*
*/
CTradeGen::CTradeGen(
                     CInputFiles    inputFiles,
                     TIdent         iCustomerCount,
                     TIdent         iStartFromCustomer,
                     TIdent         iTotalCustomers,
                     int            iLoadUnitSize,
                     int            iScaleFactor,
                     int            iHoursOfInitialTrades
                     )
: m_rnd(RNGSeedTradeGen)
, m_AddressTable(inputFiles, iCustomerCount, iStartFromCustomer, true)  // only customer addresses
, m_CustomerSelection(&m_rnd, 0, 0, 100, iStartFromCustomer, iLoadUnitSize) // only generate customer within partition
, m_CustomerTable(inputFiles, iCustomerCount, iStartFromCustomer)
, m_CustTaxrateTable(inputFiles, iCustomerCount, iStartFromCustomer)
, m_CustomerAccountTable(inputFiles, iLoadUnitSize, iCustomerCount, iStartFromCustomer)
, m_HoldingTable(inputFiles, iLoadUnitSize, iCustomerCount, iStartFromCustomer)
, m_BrokerTable(inputFiles, iCustomerCount, iStartFromCustomer)
, m_SecurityTable(inputFiles, iCustomerCount, iStartFromCustomer)
, m_Person(inputFiles)
, m_CompanyFile(inputFiles.Company)
, m_SecurityFile(inputFiles.Securities)
, m_ChargeFile(inputFiles.Charge)
, m_CommissionRateFile(inputFiles.CommissionRate)
, m_StatusTypeFile(inputFiles.StatusType)
, m_TradeTypeFile(inputFiles.TradeType)
, m_iStartFromCustomer(iStartFromCustomer + iTIdentShift)
, m_iCustomerCount(iCustomerCount)
, m_iTotalCustomers(iTotalCustomers)
, m_iLoadUnitSize(iLoadUnitSize)
, m_iLoadUnitAccountCount(iLoadUnitSize * iMaxAccountsPerCust)
, m_iScaleFactor(iScaleFactor)
, m_iHoursOfInitialTrades(iHoursOfInitialTrades)
, m_fMeanTimeBetweenTrades(100.0 / iAbortTrade * (double)iScaleFactor / iLoadUnitSize)
, m_fMeanInTheMoneySubmissionDelay( 1.0 )
, m_CurrentSimulatedTime(0)
, m_iCurrentCompletedTrades(0)
, m_iTotalTrades((TTrade)iHoursOfInitialTrades * SecondsPerHour * iLoadUnitSize / iScaleFactor)
, m_iCurrentInitiatedTrades(0)
, m_iTradesPerWorkDay(HoursPerWorkDay * SecondsPerHour * iLoadUnitSize / iScaleFactor * iAbortTrade / 100)
, m_MEESecurity()
, m_iCurrentAccountForHolding(0)
, m_iCurrentSecurityForHolding(0)
, m_pCurrentSecurityHolding()
, m_iCurrentAccountForHoldingSummary(0)
, m_iCurrentSecurityForHoldingSummary(-1)   //incremented in FindNextHoldingList()
, m_iCurrentLoadUnit(0)
{
    RNGSEED RNGSkipCount;

    //  Set the start time (time 0) to the base time
    m_StartTime.Set(
        InitialTradePopulationBaseYear,
        InitialTradePopulationBaseMonth,
        InitialTradePopulationBaseDay,
        InitialTradePopulationBaseHour,
        InitialTradePopulationBaseMinute,
        InitialTradePopulationBaseSecond,
        InitialTradePopulationBaseFraction );

    //  Get the first account number
    //
    m_iStartFromAccount = m_CustomerAccountTable.GetStartingCA_ID(m_iStartFromCustomer);

    // Create an array of customer holding lists
    //
    m_pCustomerHoldings = new THoldingList[ m_iLoadUnitAccountCount ]
                                          [ iMaxSecuritiesPerAccount ];

    // Clear row structures
    //
    memset(&m_NewTrade, 0, sizeof(m_NewTrade));
    memset(&m_TradeRow, 0, sizeof(m_TradeRow));
    memset(&m_HoldingRow, 0, sizeof(m_HoldingRow));
    memset(&m_HoldingSummaryRow, 0, sizeof(m_HoldingSummaryRow));

    // Position trade id at the proper start of the sequence
    //
    m_iCurrentTradeId = (TTrade)m_iHoursOfInitialTrades * SecondsPerHour
                        * (iStartFromCustomer - iDefaultStartFromCustomer)
                        / m_iScaleFactor    // divide after multiplication to avoid integer truncation
                        * iAbortTrade
                        / 100 + iTTradeShift;

    // Initialize BROKER table
    //
    m_BrokerTable.InitForGen(iLoadUnitSize, m_iStartFromCustomer - iTIdentShift);

    RNGSkipCount = m_iStartFromCustomer / m_iLoadUnitSize * m_iTotalTrades;

    m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTradeGen,
                                      RNGSkipCount * iRNGSkipOneTrade));

    m_HoldingTable.InitNextLoadUnit(RNGSkipCount);

    // Initialize security price emulation
    m_MEESecurity.Init( 0, NULL, NULL, m_fMeanInTheMoneySubmissionDelay );
}

/*
*   Destructor.
*   Frees any memory allocated in the constructor.
*/
CTradeGen::~CTradeGen()
{
    if (m_pCustomerHoldings != NULL)
    {
        delete[] m_pCustomerHoldings;
    }
}

/*
*   Initialize next load unit for a series of
*   GenerateNextTrade/GenerateNextHolding calls.
*
*   The first load unit doesn't have to be initalized.
*
*   RETURNS:
*           true    - if a new load unit could be found
*           false   - if all load units have been processed
*/
bool CTradeGen::InitNextLoadUnit()
{
    RNGSEED     RNGSkipCount;

    ++m_iCurrentLoadUnit;

    m_iCurrentCompletedTrades = 0;

    //  No need to empty holdings as they were emptied by
    //  GenerateNextHolding calls.
    //
    delete[] m_pCustomerHoldings;
    // Create an array of customer holding lists
    //
    m_pCustomerHoldings = new THoldingList[ m_iLoadUnitAccountCount ]
                                          [ iMaxSecuritiesPerAccount ];
    m_iCurrentAccountForHolding = 0;
    m_iCurrentSecurityForHolding = 0;

    m_iCurrentAccountForHoldingSummary = 0;
    m_iCurrentSecurityForHoldingSummary = -1;

    m_iStartFromCustomer += m_iLoadUnitSize;

    m_iStartFromAccount = m_CustomerAccountTable.GetStartingCA_ID(m_iStartFromCustomer);

    m_CurrentSimulatedTime = 0;

    m_iCurrentInitiatedTrades = 0;

    m_BrokerTable.InitForGen(m_iLoadUnitSize, m_iStartFromCustomer - iTIdentShift);

    RNGSkipCount = m_iStartFromCustomer / m_iLoadUnitSize * m_iTotalTrades;

    m_rnd.SetSeed(m_rnd.RndNthElement(RNGSeedTradeGen,
                                      RNGSkipCount * iRNGSkipOneTrade));

    // Move customer range for the next load unit.
    //
    m_CustomerSelection.SetPartitionRange(m_iStartFromCustomer, m_iLoadUnitSize);

    m_HoldingTable.InitNextLoadUnit(RNGSkipCount);  // there is only 1 call to HoldingTable per trade

    m_MEESecurity.Init( 0, NULL, NULL, m_fMeanInTheMoneySubmissionDelay );

    // Clear row structures.
    //
    memset(&m_TradeRow, 0, sizeof(m_TradeRow));
    memset(&m_HoldingRow, 0, sizeof(m_HoldingRow));
    memset(&m_HoldingSummaryRow, 0, sizeof(m_HoldingSummaryRow));

    return m_iCurrentLoadUnit < (m_iCustomerCount / m_iLoadUnitSize);
}

/*
*   Generate a new trade and all trade-related rows (except holdings).
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               true    - if there are more trades
*               false   - if there are no more trades to generate
*
*/
bool CTradeGen::GenerateNextTrade()
{
    bool bMoreTrades;

    if (m_iCurrentCompletedTrades < m_iTotalTrades)
    {
        // While the earliest completion time is before the current
        // simulated ('Trade Order') time, keep creating new
        // incomplete trades putting them on the queue and
        // incrementing the current simulated time.
        //
        while ( (m_iCurrentCompletedTrades + (int)m_CurrentTrades.size() < m_iTotalTrades) &&
               (m_CurrentTrades.empty() ||
               (m_CurrentSimulatedTime <
                m_CurrentTrades.top().CompletionTime)))
        {
            m_CurrentSimulatedTime = (m_iCurrentInitiatedTrades / m_iTradesPerWorkDay) // number of days
                                    * SecondsPerDay // number of seconds in a day
                                    // now add the offset in the day
                                    + (m_iCurrentInitiatedTrades % m_iTradesPerWorkDay)
                                    * m_fMeanTimeBetweenTrades
                                    + GenerateDelayBetweenTrades();

            // Generate new Trade Order; CMEESecurity
            // is used by this function.
            //
            GenerateNewTrade();

            if ( m_HoldingTable.IsAbortedTrade(m_iCurrentInitiatedTrades) )
            {
                //  Abort trade and not put it into the queue.
                //  Generate a new trade instead.
                //
                continue;
            }

            m_CurrentTrades.push(m_NewTrade);

        }

        // Get the earliest trade from the
        // front of the queue.
        //
        m_NewTrade = m_CurrentTrades.top();

        // Remove the top element.
        //
        m_CurrentTrades.pop();

        // Update HOLDING row for the customer
        //
        // Must be called before generating the complete trade
        // to calculate buy and sell values for T_TAX.
        //
        UpdateHoldings();

        // Generate all the remaining trade data.
        //
        GenerateCompleteTrade();

        bMoreTrades = m_iCurrentCompletedTrades < m_iTotalTrades;

    }
    else
    {
        bMoreTrades = false;
    }

    if (bMoreTrades)
    {
        return true;
    }
    else
    {
        // Before returning need to position
        // holding iterator for GenerateNextHolding()
        //
        m_pCurrentSecurityHolding = m_pCustomerHoldings[m_iCurrentAccountForHolding]
                                                       [m_iCurrentSecurityForHolding].begin();
        FindNextHolding();

        // Set up for GenerateNextHoldingSummary
        FindNextHoldingList();

        size_t iSize = m_CurrentTrades.size();  // info for debugging

        assert(iSize == 0);

        return false;
    }
}

/*
*   Generate a random delay in Submission (or Pending for limit trades)
*   time between two trades.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               seconds of delay before simulated time for the next trade
*
*/
inline double CTradeGen::GenerateDelayBetweenTrades()
{
    //  Return a random number between 0 and 1ms less than the mean.
    //
    return m_rnd.RndDoubleIncrRange(0.0, m_fMeanTimeBetweenTrades - 0.001, 0.001);
}

/*
*   Helper function to get the list of holdings
*   to modify after the last completed trade
*
*   RETURNS:
*           reference to the list of holdings
*/
inline THoldingList*    CTradeGen::GetHoldingListForCurrentTrade()
{
    return &m_pCustomerHoldings[GetCurrentAccID() - m_iStartFromAccount]
                                [GetCurrentSecurityAccountIndex() - 1];
}

/*
*   Helper function to get either the front or the end
*   of the holding list
*
*   RETURNS:
*           iterator positined at the first element or at the last element
*/
list<THoldingInfo>::iterator CTradeGen::PositionAtHoldingList(
                                                        THoldingList*   pHoldingList,
                                                        int             IsLifo)
{
    if (pHoldingList->empty())
    {
        return pHoldingList->end(); // iterator positioned after the last element
    }
    else
    {

        if (IsLifo)
        {
            return --(pHoldingList->end()); // position before the last element
        }
        else
        {
            return pHoldingList->begin();
        }
    }
}

/*
*   Update holding information based on the trade row
*   internal structures.
*   In other words, update holdings for the last trade
*   that was generated (by GenerateCompleteTrade()).
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::UpdateHoldings()
{
    THoldingList*                           pHoldingList;
    list<THoldingInfo>::iterator            pHolding;   // start of the holding list
    int                                     iNeededQty = GetCurrentTradeQty();
    int                                     iHoldQty;

    m_CompletedTradeInfo.fBuyValue = 0;
    m_CompletedTradeInfo.fSellValue = 0;

    // Start new series of HOLDING_HISTORY rows
    //
    m_iHoldingHistoryRowCount = 0;

    pHoldingList = GetHoldingListForCurrentTrade();

    pHolding = PositionAtHoldingList(pHoldingList, GetCurrentTradeIsLifo());

    if (   GetCurrentTradeType() == eMarketBuy
        || GetCurrentTradeType() == eLimitBuy)
    {
        // Buy trade

        // Liquidate negative (short) holdings
        //
        while (!pHoldingList->empty() && pHolding->iTradeQty < 0 && iNeededQty > 0)
        {
            iHoldQty = pHolding->iTradeQty;

            pHolding->iTradeQty += iNeededQty;

            if (pHolding->iTradeQty > 0)
            {
                //  Need to zero the qty for correct history row later
                //
                pHolding->iTradeQty = 0;    // holding fully closed

                m_CompletedTradeInfo.fSellValue += -iHoldQty * pHolding->fTradePrice;
                m_CompletedTradeInfo.fBuyValue += -iHoldQty * GetCurrentTradePrice();
            }
            else
            {
                m_CompletedTradeInfo.fSellValue += iNeededQty * pHolding->fTradePrice;
                m_CompletedTradeInfo.fBuyValue += iNeededQty * GetCurrentTradePrice();
            }

            GenerateHoldingHistoryRow(pHolding->iTradeId, GetCurrentTradeID(),
                                      iHoldQty, pHolding->iTradeQty);

            if (pHolding->iTradeQty == 0)
            {
                // There was enough new quantity to fully close the old holding
                //
                pHolding = pHoldingList->erase(pHolding);   // erase the old and return next holding
            }

            //  Reposition at proper end
            //
            pHolding = PositionAtHoldingList(pHoldingList, GetCurrentTradeIsLifo());

            iNeededQty += iHoldQty;
        }

        if (iNeededQty > 0)
        {
            // Still shares left after closing positions => create a new holding
            //
            THoldingInfo NewHolding = { GetCurrentTradeID(),
                            iNeededQty,
                            GetCurrentTradePrice(),
                            GetCurrentTradeCompletionTime(),
                            GetCurrentSecurityIndex()
                          };

            //  Note: insert should be at the same end all the time
            //  provided delete (PositionAtHoldingList()) is different
            //  depending on IsLifo.
            //  Vice versa also works - delete is at the
            //  same end and insert depends on IsLifo. However, TradeResult
            //  inserts at the end, so let loader insert in the same end.
            //
            pHoldingList->push_back(NewHolding);

            GenerateHoldingHistoryRow(GetCurrentTradeID(), GetCurrentTradeID(),
                                      0, iNeededQty);
        }
    }
    else
    {
        // Sell trade

        //iNeededQty *= (-1);   // make trade qty negative for convenience

        // Liquidate positive (long) holdings
        //
        while (!pHoldingList->empty() && pHolding->iTradeQty > 0 && iNeededQty > 0)
        {
            iHoldQty = pHolding->iTradeQty;

            pHolding->iTradeQty -= iNeededQty;

            if (pHolding->iTradeQty < 0)
            {
                //  Need to zero the qty for correct history row later
                //
                pHolding->iTradeQty = 0;    // holding fully closed

                m_CompletedTradeInfo.fSellValue += iHoldQty * GetCurrentTradePrice();
                m_CompletedTradeInfo.fBuyValue += iHoldQty * pHolding->fTradePrice;
            }
            else
            {
                m_CompletedTradeInfo.fSellValue += iNeededQty * GetCurrentTradePrice();
                m_CompletedTradeInfo.fBuyValue += iNeededQty * pHolding->fTradePrice;
            }

            GenerateHoldingHistoryRow(pHolding->iTradeId, GetCurrentTradeID(),
                                      iHoldQty, pHolding->iTradeQty);

            if (pHolding->iTradeQty == 0)
            {
                // There was enough new quantity to fully close the old holding
                //
                pHolding = pHoldingList->erase(pHolding);   // erase the old and return next holding
            }

            //  Reposition at proper end
            //
            pHolding = PositionAtHoldingList(pHoldingList, GetCurrentTradeIsLifo());

            iNeededQty -= iHoldQty;
        }

        if (iNeededQty > 0)
        {
            // Still shares left after closing positions => create a new holding
            //
            THoldingInfo NewHolding = { GetCurrentTradeID(),
                            -iNeededQty,
                            GetCurrentTradePrice(),
                            GetCurrentTradeCompletionTime(),
                            GetCurrentSecurityIndex()
                          };

            //  Note: insert should be at the same end all the time
            //  provided delete (PositionAtHoldingList()) is different
            //  depending on IsLifo.
            //  Vice versa also works - delete is at the
            //  same end and insert depends on IsLifo. However, TradeResult
            //  inserts at the end, so let loader insert in the same end.
            //
            pHoldingList->push_back(NewHolding);

            GenerateHoldingHistoryRow(GetCurrentTradeID(), GetCurrentTradeID(),
                                      0, -iNeededQty);
        }
    }

}

/*
*   Find next non-empty holding list.
*
*   RETURNS:
*           whether a non-empty holding list exists
*/
bool CTradeGen::FindNextHoldingList()
{
    THoldingList*   pHoldingList;

    //  Find the next non-empty holding list
    //
    do
    {
        m_iCurrentSecurityForHoldingSummary++;
        if (m_iCurrentSecurityForHoldingSummary == iMaxSecuritiesPerAccount)
        {
            // no more securities for the current account
            // so move on to next account.
            //
            m_iCurrentAccountForHoldingSummary++;
            m_iCurrentSecurityForHoldingSummary = 0;

            if (m_iCurrentAccountForHoldingSummary == m_iLoadUnitAccountCount)
            {
                // no more customers left, all holding lists have been processed
                //
                return false;
            }
        }

        //  Set list pointer
        //
        pHoldingList = &m_pCustomerHoldings[m_iCurrentAccountForHoldingSummary]
                                              [m_iCurrentSecurityForHoldingSummary];
    } while (pHoldingList->empty());    // test for empty HoldingList

    return true;
}

/*
*   Find non-empty holding and position internal
*   iterator at it.
*
*   RETURNS:
*           whether a non-empty holding exists
*/
bool CTradeGen::FindNextHolding()
{
    THoldingList*   pHoldingList;

    pHoldingList = &m_pCustomerHoldings[m_iCurrentAccountForHolding]
                                       [m_iCurrentSecurityForHolding];

    //  Make sure the holding iterator points to a valid holding
    //
    do
    {
        if (m_pCurrentSecurityHolding == pHoldingList->end())
        {
            // no more holding for the security => have to move to the next security
            //
            ++m_iCurrentSecurityForHolding;

            if (m_iCurrentSecurityForHolding == iMaxSecuritiesPerAccount)
            {

                // no more holding for the account => have to move to the next account
                //
                ++m_iCurrentAccountForHolding;

                m_iCurrentSecurityForHolding = 0;

                if (m_iCurrentAccountForHolding == m_iLoadUnitAccountCount)
                {
                    // no more customers left => all holdings have been returned
                    //
                    return false;
                }
            }

            //  Holding list has changed => reinitialize
            //
            pHoldingList = &m_pCustomerHoldings[m_iCurrentAccountForHolding]
                                               [m_iCurrentSecurityForHolding];
            //  Select the first holding in the new list
            //
            m_pCurrentSecurityHolding = pHoldingList->begin();
        }
    } while (m_pCurrentSecurityHolding == pHoldingList->end()); // test for empty HoldingList

    return true;
}

/*
*   Generate a new HOLDING_SUMMARY row.
*   This function uses the lists of holdings generated
*   during simulated trade generation and returns the
*   row for the current account/security pair.
*
*   PARAMETERS:
*               none
*   RETURNS:
*               true    - if there are more holding lists
*               false   - if there are no more holding lists
*/
bool CTradeGen::GenerateNextHoldingSummaryRow()
{
    TIdent  iSecurityFlatFileIndex; // index of the security in the input flat file

    if (m_iCurrentAccountForHoldingSummary < m_iLoadUnitAccountCount)
    {
        // There is always a valid holding list when this function
        // is called. The holding list to process is identified by
        // m_iCurrentAccountForHoldingSummary and
        // m_iCurrentSecurityForHoldingSummary.
        //
        m_HoldingSummaryRow.HS_CA_ID = m_iCurrentAccountForHoldingSummary + m_iStartFromAccount;
        iSecurityFlatFileIndex = m_HoldingTable.GetSecurityFlatFileIndex(
                                                    m_iCurrentAccountForHoldingSummary + m_iStartFromAccount,
                                                    m_iCurrentSecurityForHoldingSummary + 1);

        m_SecurityFile->CreateSymbol(iSecurityFlatFileIndex,
                                    m_HoldingSummaryRow.HS_S_SYMB,
                                    static_cast<int>(sizeof(m_HoldingSummaryRow.HS_S_SYMB)));

        // Sum up the quantities for the holding list
        THoldingList*                   pHoldingList;
        list<THoldingInfo>::iterator    pHolding;

        pHoldingList = &m_pCustomerHoldings[m_iCurrentAccountForHoldingSummary]
                                       [m_iCurrentSecurityForHoldingSummary];
        pHolding = pHoldingList->begin();
        m_HoldingSummaryRow.HS_QTY = 0;

        while( pHolding != pHoldingList->end() )
        {
            m_HoldingSummaryRow.HS_QTY += pHolding->iTradeQty;
            pHolding++;
        }

        return FindNextHoldingList();
    }
    else
    {
        return false;
    }
}

/*
*   Generate a new HOLDING_HISTORY row and update HOLDING_HISTORY row count.
*
*   RETURNS:
*               none
*/
void CTradeGen::GenerateHoldingHistoryRow(
                            TTrade iHoldingTradeID, // trade id of the original trade
                            TTrade iTradeTradeID,   // trade id of the modifying trade
                            int iBeforeQty,     // holding qty now
                            int iAfterQty)      // holding qty after modification
{
    if (m_iHoldingHistoryRowCount < iMaxHoldingHistoryRowsPerTrade)
    {
        m_TradeRow.m_HoldingHistory[m_iHoldingHistoryRowCount].HH_H_T_ID = iHoldingTradeID;
        m_TradeRow.m_HoldingHistory[m_iHoldingHistoryRowCount].HH_T_ID = iTradeTradeID;
        m_TradeRow.m_HoldingHistory[m_iHoldingHistoryRowCount].HH_BEFORE_QTY = iBeforeQty;
        m_TradeRow.m_HoldingHistory[m_iHoldingHistoryRowCount].HH_AFTER_QTY = iAfterQty;

        ++m_iHoldingHistoryRowCount;
    }
}

/*
*   Generate a new holding row.
*   This function uses already prepared holding list structure
*   and returns the next holding for the current customer.
*
*   The returned holding is deleted from the holding list
*   to clear the list for the next load unit.
*
*   It moves to the next customer if the current one doesn't
*   have any more holdings.
*
*   PARAMETERS:
*               none
*   RETURNS:
*               true    - if there are more holdings
*               false   - if there are no more holdings to return
*/
bool CTradeGen::GenerateNextHolding()
{
    TIdent      iSecurityFlatFileIndex; // index of the security in the input flat file

    if (m_iCurrentAccountForHolding < m_iLoadUnitAccountCount)
    {
        // There is always a valid holding when this function
        // is called. The holding to put into the HOLDING row
        // is pointed to by m_pCurrentSecurityHolding.
        //
        m_HoldingRow.H_CA_ID = m_iCurrentAccountForHolding + m_iStartFromAccount;
        //iSecurityFlatFileIndex = m_HoldingTable.GetSecurityFlatFileIndex(
        //                                          m_iCurrentAccountForHolding + m_iStartFromAccount,
        //                                          m_iCurrentSecurityForHolding + 1);

        iSecurityFlatFileIndex = m_pCurrentSecurityHolding->iSymbolIndex;

        m_SecurityFile->CreateSymbol(iSecurityFlatFileIndex,
                                    m_HoldingRow.H_S_SYMB,
                                    static_cast<int>(sizeof(m_HoldingRow.H_S_SYMB)));

        m_HoldingRow.H_T_ID = m_pCurrentSecurityHolding->iTradeId;
        m_HoldingRow.H_QTY = m_pCurrentSecurityHolding->iTradeQty;
        m_HoldingRow.H_PRICE = m_pCurrentSecurityHolding->fTradePrice.DollarAmount();
        m_HoldingRow.H_DTS = m_pCurrentSecurityHolding->BuyDTS;

        // Delete the holding and move to the next one in the account
        /*m_pCurrentSecurityHolding =
            m_pCustomerHoldings[m_iCurrentAccountForHolding]
                               [m_iCurrentSecurityForHolding].erase(m_pCurrentSecurityHolding);*/

        ++m_pCurrentSecurityHolding;

        // Move to the next valid holding for the next call.
        //
        return FindNextHolding();
    }
    else
    {
        return false;
    }
}

/*
*   Generate a trade id for the next trade.
*
*   PARAMETERS:
*               none
*   RETURNS:
*               a new unique trade id
*/
TTrade CTradeGen::GenerateNextTradeId()
{
    return ++m_iCurrentTradeId;
}

/*
*   Generates a random trade type according to a certain distribution
*
*   PARAMETERS:
*               none
*   RETURNS:
*               trade type id
*/
eTradeTypeID CTradeGen::GenerateTradeType()
{
    eTradeTypeID    eTradeType;
    // Generate Trade Type
    // NOTE:    The order of these "if" tests is significant!!
    //          Do not alter it unless you know what you are doing.
    //          :-)
    //
    int iLoadTradeTypePct = m_rnd.RndGenerateIntegerPercentage();

    if( iLoadTradeTypePct <= cMarketBuyLoadThreshold )          //  1% - 30%
    {
        eTradeType = eMarketBuy;
    }
    else if( iLoadTradeTypePct <= cMarketSellLoadThreshold )    // 31% - 60%
    {
        eTradeType = eMarketSell;
    }
    else if( iLoadTradeTypePct <= cLimitBuyLoadThreshold )      // 61% - 80%
    {
        eTradeType = eLimitBuy;
    }
    else if( iLoadTradeTypePct <= cLimitSellLoadThreshold )     // 81% - 90%
    {
        eTradeType = eLimitSell;
    }
    else if( iLoadTradeTypePct <= cStopLossLoadThreshold )      // 91% - 100%
    {
        eTradeType = eStopLoss;
    }
    else
    {
        assert(false);  // this should never happen
    }

    return eTradeType;
}

/*
*   Generate new incomplete trade (happens at Trade Order time)
*   with enough information to later generate a complete one.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*/
void CTradeGen::GenerateNewTrade()
{
    m_NewTrade.iTradeId = GenerateNextTradeId();

    // Select random customer
    //
    m_CustomerSelection.GenerateRandomCustomer(m_NewTrade.iCustomer, m_NewTrade.iCustomerTier);

    // Select random customer, account, and security within the account
    //
    m_HoldingTable.GenerateRandomAccountSecurity(
                                                m_NewTrade.iCustomer,
                                                m_NewTrade.iCustomerTier,
                                                &m_NewTrade.iCustomerAccount,
                                                &m_NewTrade.iSymbolIndex,
                                                &m_NewTrade.iSymbolIndexInAccount);

    m_NewTrade.eTradeType = GenerateTradeType();

    // Status is always 'Completed' for initial trades
    //
    m_NewTrade.eTradeStatus = eCompleted;

    m_NewTrade.fBidPrice = m_rnd.RndDoubleIncrRange(fMinSecPrice, fMaxSecPrice, 0.01);

    m_NewTrade.iTradeQty = cTRADE_QTY_SIZES[m_rnd.RndIntRange( 0, cNUM_TRADE_QTY_SIZES-1 )];

    if (m_NewTrade.eTradeType == eMarketBuy ||
        m_NewTrade.eTradeType == eMarketSell)
    {   // A Market order
        //
        m_NewTrade.SubmissionTime = m_CurrentSimulatedTime;

        // Update the bid price to the current market price (like runtime)
        //
        m_NewTrade.fBidPrice = m_MEESecurity.CalculatePrice( m_NewTrade.iSymbolIndex, m_CurrentSimulatedTime );
    }
    else
    {   // a Limit Order => need to calculate the Submission time
        //
        m_NewTrade.PendingTime = m_CurrentSimulatedTime;

        m_NewTrade.SubmissionTime = m_MEESecurity.
                                        GetSubmissionTime( m_NewTrade.iSymbolIndex,
                                            m_NewTrade.PendingTime,
                                            m_NewTrade.fBidPrice,
                                            m_NewTrade.eTradeType);

        // Move orders that would submit after market close (5pm)
        // to the beginning of the next day.
        //
        // Submission time here is kept from the beginning of the day, even though
        // it is later output to the database starting from 9am. So time 0h corresponds
        // to 9am, time 8hours corresponds to 5pm.
        //
        if ((((INT32)(m_NewTrade.SubmissionTime / SecondsPerHour)) % HoursPerDay == HoursPerWorkDay) &&   // >=5pm
            (
             (m_NewTrade.SubmissionTime / SecondsPerHour) -
             ((INT32)(m_NewTrade.SubmissionTime / SecondsPerHour)) > 0  // fractional seconds exist, e.g. not 5:00pm
            )
           )
        {
            m_NewTrade.SubmissionTime += 16 * SecondsPerHour;   //add 16 hours to move to 9am next day
        }
    }

    // Calculate Completion time and price
    //
    m_NewTrade.CompletionTime = m_MEESecurity.
                                        GetCompletionTime( m_NewTrade.iSymbolIndex,
                                            m_NewTrade.SubmissionTime,
                                            &m_NewTrade.fTradePrice);

    // Make sure the trade has the right price based on the type of trade.
    if(( m_NewTrade.eTradeType == eLimitBuy && m_NewTrade.fBidPrice < m_NewTrade.fTradePrice ) ||
        ( m_NewTrade.eTradeType == eLimitSell && m_NewTrade.fBidPrice > m_NewTrade.fTradePrice ))
    {
        m_NewTrade.fTradePrice = m_NewTrade.fBidPrice;
    }

    if (m_rnd.RndPercent( iPercentTradeIsLIFO ))
    {
        m_NewTrade.iIsLifo = 1;
    }
    else
    {
        m_NewTrade.iIsLifo = 0;
    }

    ++m_iCurrentInitiatedTrades;
}

/*
*   Generate a complete trade information
*   and fill all the internal row structures.
*
*   A valid incomplete trade must exist in m_NewTrade.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none.
*
*/
void CTradeGen::GenerateCompleteTrade()
{
    GenerateCompletedTradeInfo();

    GenerateTradeRow(); // TRADE row must be generated before all the others
    GenerateTradeHistoryRow();
    GenerateCashTransactionRow();
    GenerateSettlementRow();

    m_BrokerTable.UpdateTradeAndCommissionYTD(GetCurrentBrokerId(), 1, m_TradeRow.m_Trade.T_COMM);

    ++m_iCurrentCompletedTrades;
}

/*
*   Generate frequently used fields for the completed trade.
*   This function must be called before generating individual
*   table rows.
*
*   A valid incomplete trade must exist in m_NewTrade.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none.
*
*/
void CTradeGen::GenerateCompletedTradeInfo()
{
    m_CompletedTradeInfo.eAccountTaxStatus =
                    m_CustomerAccountTable.GetAccountTaxStatus(GetCurrentAccID());

    m_CompletedTradeInfo.iCurrentBrokerId =     // not needed anymore?
                        m_CustomerAccountTable.GenerateBrokerIdForAccount(GetCurrentAccID());

    GenerateTradeCharge();  // generate charge

    GenerateTradeCommission();  // generate commission

    GenerateTradeTax();

    GenerateSettlementAmount();
}

/*
*   Generate complete TRADE row information.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateTradeRow()
{
    m_TradeRow.m_Trade.T_ID = GetCurrentTradeID();

    m_TradeRow.m_Trade.T_CA_ID = GetCurrentAccID();

    strncpy(m_TradeRow.m_Trade.T_TT_ID,
            m_TradeTypeFile->GetRecord(GetCurrentTradeType())->TT_ID,
            sizeof(m_TradeRow.m_Trade.T_TT_ID)-1);

    strncpy(m_TradeRow.m_Trade.T_ST_ID,
            m_StatusTypeFile->GetRecord(GetCurrentTradeStatus())->ST_ID,
            sizeof(m_TradeRow.m_Trade.T_ST_ID)-1);

    // Generate whether the trade is cash trade. All sells are cash. 84% of buys are cash.
    //
    m_TradeRow.m_Trade.T_IS_CASH = 1;   // changed later if needed

    if((( GetCurrentTradeType() == eMarketBuy ) || ( GetCurrentTradeType() == eLimitBuy )) &&
        m_rnd.RndPercent( iPercentBuysOnMargin ))
    {
        m_TradeRow.m_Trade.T_IS_CASH = 0;
    }

    sprintf(m_TradeRow.m_Trade.T_EXEC_NAME, "%s %s",
        m_Person.GetFirstName(GetCurrentCustID()),
        m_Person.GetLastName(GetCurrentCustID()));

    m_SecurityFile->CreateSymbol(GetCurrentSecurityIndex(),
                                m_TradeRow.m_Trade.T_S_SYMB,
                                static_cast<int>(sizeof(m_TradeRow.m_Trade.T_S_SYMB)));

    m_TradeRow.m_Trade.T_BID_PRICE = GetCurrentBidPrice().DollarAmount();

    m_TradeRow.m_Trade.T_TRADE_PRICE = GetCurrentTradePrice().DollarAmount();

    m_TradeRow.m_Trade.T_QTY = GetCurrentTradeQty();

    m_TradeRow.m_Trade.T_CHRG = m_CompletedTradeInfo.Charge.DollarAmount(); // get charge

    m_TradeRow.m_Trade.T_COMM = m_CompletedTradeInfo.Commission.DollarAmount(); // get commission

    // Get the tax amount. The check for positive capital gain is
    // in GenerateTradeTax(). If there is no capital gain, tax amount
    // will be set to zero by this time.
    //
    switch( GetCurrentTaxStatus() )
    {
    case eNonTaxable: //no taxes
        m_TradeRow.m_Trade.T_TAX = 0;
        break;
    case eTaxableAndWithhold: //calculate and withhold
        m_TradeRow.m_Trade.T_TAX = GetCurrentTradeTax().DollarAmount();
        break;
    case eTaxableAndDontWithhold: //calculate and do not withhold
        m_TradeRow.m_Trade.T_TAX = GetCurrentTradeTax().DollarAmount();
        break;
    default: //should never happen
        assert(false);
    }

    // T_DTS contains trade completion time.
    //
    m_TradeRow.m_Trade.T_DTS = GetCurrentTradeCompletionTime();

    m_TradeRow.m_Trade.T_LIFO = GetCurrentTradeIsLifo();
}

/*
*   Select charge for the TRADE table from the input file.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateTradeCharge()
{
    unsigned int    i;
    PCHARGE_ROW     pChargeRow; // intermediate variable for convenience
    PTRADE_TYPE_ROW pTradeTypeRow; // intermediate variable for convenience

    //just scan sequentially for now
    for (i=0; i<m_ChargeFile->GetSize(); ++i)
    {
        pChargeRow = m_ChargeFile->GetRecord(i);
        // search for the customer tier
        if ( GetCurrentCustTier() == pChargeRow->CH_C_TIER )
        {
            pTradeTypeRow = m_TradeTypeFile->GetRecord(GetCurrentTradeType());
            // search for the trade type
            if (!strcmp(pTradeTypeRow->TT_ID, pChargeRow->CH_TT_ID))
            {
                //found the correct charge
                m_CompletedTradeInfo.Charge = pChargeRow->CH_CHRG;

                return;
            }
        }
    }
    //should never reach here
    assert(false);
}

/*
*   Select commission for the TRADE table from the input file.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateTradeCommission()
{
    unsigned int    i;
    int             iCustTier = GetCurrentCustTier();
    double          fTradeQty = GetCurrentTradeQty();
    CMoney          fComm;

    // Some extra logic to reduce looping in the CommissionRate file.
    // It is organized by tier, then trade type, then exchange.
    // Consider this extra knowledge to calculate the starting position for search.
    //
    // Number of rows in the CommissionRate file that have the same customer tier.
    //
    //
    int iCustomerTierRecords = m_CommissionRateFile->GetSize() / 3;

    // Number of rows in the CommissionRate file per trade type.
    //
    int iTradeTypeRecords = iCustomerTierRecords / m_TradeTypeFile->GetSize();

    for (i = (iCustTier - 1) * iCustomerTierRecords + GetCurrentTradeType() * iTradeTypeRecords;
        i < m_CommissionRateFile->GetSize();
        ++i)
    {
        if ( iCustTier == (m_CommissionRateFile->GetRecord(i))->CR_C_TIER  // match tier
            && fTradeQty >= (m_CommissionRateFile->GetRecord(i))->CR_FROM_QTY // match quantity
            && fTradeQty <= (m_CommissionRateFile->GetRecord(i))->CR_TO_QTY )
        {
            if ( !strcmp(m_TradeTypeFile->GetRecord(GetCurrentTradeType())->TT_ID, // match trade type
                (m_CommissionRateFile->GetRecord(i))->CR_TT_ID)
                && !strcmp(m_SecurityFile->GetRecord(GetCurrentSecurityIndex())->S_EX_ID, // match exchange
                (m_CommissionRateFile->GetRecord(i))->CR_EX_ID) )
            {
                //found the correct commission rate

                m_CompletedTradeInfo.Commission = (fTradeQty * GetCurrentTradePrice()) *
                                                (m_CommissionRateFile->GetRecord(i))->CR_RATE / 100.0;

                return;
            }
        }
    }

    //should never reach here
    assert(false);
}


/*
*   Generate tax based on the account tax status and the tax rates for the customer
*   that owns the account.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateTradeTax()
{
    TIdent  CustomerAD_ID;
    int     iDivCode, iCtryCode;
    CMoney  fProceeds;
    double  fCtryRate, fDivRate;

    // Check whether no capital gain exists and don't bother with calculations
    //
    if (GetCurrentTradeSellValue() <= GetCurrentTradeBuyValue())
    {
        m_CompletedTradeInfo.Tax = 0;
        return;
    }

    CustomerAD_ID = m_AddressTable.GetAD_IDForCustomer(GetCurrentCustID());

    m_AddressTable.GetDivisionAndCountryCodesForAddress(CustomerAD_ID, iDivCode, iCtryCode);

    fProceeds = GetCurrentTradeSellValue() - GetCurrentTradeBuyValue();

    fCtryRate = m_CustTaxrateTable.GetCountryTaxRow(GetCurrentCustID(), iCtryCode).TAX_RATE;
    fDivRate = m_CustTaxrateTable.GetDivisionTaxRow(GetCurrentCustID(), iDivCode).TAX_RATE;

    // Do a trick for proper rounding of resulting tax amount.
    // Txn rates (fCtryRate and fDivRate) have 4 digits after a floating point
    // so the existing CMoney class is not suitable to round them (because CMoney
    // only keeps 2 digits after the point). Therefore need to do the manual trick
    // of multiplying tax rates by 10000.0 (not 100.0), adding 0.5, and truncating
    // to int to get the proper rounding.
    //
    // This is all to match the database calculation of T_TAX done by runtime transactions.
    //
    m_CompletedTradeInfo.Tax = fProceeds *
        ((double)((int)(10000.0 * (fCtryRate + fDivRate) + 0.5) ) / 10000.0);
}

/*
*   Generate complete TRADE_HISTORY row(s) information.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateTradeHistoryRow()
{
    if (   GetCurrentTradeType() == eStopLoss
        || GetCurrentTradeType() == eLimitSell
        || GetCurrentTradeType() == eLimitBuy)
    {
        m_iTradeHistoryRowCount = 3;    //insert 3 rows

        // insert Pending record
        m_TradeRow.m_TradeHistory[0].TH_T_ID = GetCurrentTradeID();
        strncpy(m_TradeRow.m_TradeHistory[0].TH_ST_ID,
            m_StatusTypeFile->GetRecord(ePending)->ST_ID,
            sizeof(m_TradeRow.m_TradeHistory[0].TH_ST_ID)-1);
        m_TradeRow.m_TradeHistory[0].TH_DTS = GetCurrentTradePendingTime();

        // insert Submitted record
        m_TradeRow.m_TradeHistory[1].TH_T_ID = GetCurrentTradeID();
        strncpy(m_TradeRow.m_TradeHistory[1].TH_ST_ID,
            m_StatusTypeFile->GetRecord(eSubmitted)->ST_ID,
            sizeof(m_TradeRow.m_TradeHistory[1].TH_ST_ID)-1);
        m_TradeRow.m_TradeHistory[1].TH_DTS = GetCurrentTradeSubmissionTime();

        // insert Completed record
        m_TradeRow.m_TradeHistory[2].TH_T_ID = GetCurrentTradeID();
        strncpy(m_TradeRow.m_TradeHistory[2].TH_ST_ID,
            m_StatusTypeFile->GetRecord(eCompleted)->ST_ID,
            sizeof(m_TradeRow.m_TradeHistory[2].TH_ST_ID)-1);
        m_TradeRow.m_TradeHistory[2].TH_DTS = GetCurrentTradeCompletionTime();
    }
    else
    {
        m_iTradeHistoryRowCount = 2;    //insert 2 rows

        // insert Submitted record
        m_TradeRow.m_TradeHistory[0].TH_T_ID = GetCurrentTradeID();
        strncpy(m_TradeRow.m_TradeHistory[0].TH_ST_ID,
            m_StatusTypeFile->GetRecord(eSubmitted)->ST_ID,
            sizeof(m_TradeRow.m_TradeHistory[0].TH_ST_ID)-1);
        m_TradeRow.m_TradeHistory[0].TH_DTS = GetCurrentTradeSubmissionTime();

        // insert Completed record
        m_TradeRow.m_TradeHistory[1].TH_T_ID = GetCurrentTradeID();
        strncpy(m_TradeRow.m_TradeHistory[1].TH_ST_ID,
            m_StatusTypeFile->GetRecord(eCompleted)->ST_ID,
            sizeof(m_TradeRow.m_TradeHistory[1].TH_ST_ID)-1);
        m_TradeRow.m_TradeHistory[1].TH_DTS = GetCurrentTradeCompletionTime();
    }
}

/*
*   Generate settlement amount for the current trade (value to use for SE_AMT and CT_AMT).
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateSettlementAmount()
{
    //  Settlement amount calculation matching Trade Result Frame 6.
    //
    if (m_TradeTypeFile->GetRecord(GetCurrentTradeType())->TT_IS_SELL)
    {
        m_CompletedTradeInfo.SettlementAmount =
                                                GetCurrentTradeQty() * GetCurrentTradePrice() -
                                                m_CompletedTradeInfo.Charge -
                                                m_CompletedTradeInfo.Commission;
    }
    else
    {
        m_CompletedTradeInfo.SettlementAmount =
                                                -1 * (GetCurrentTradeQty() * GetCurrentTradePrice() +
                                                m_CompletedTradeInfo.Charge +
                                                m_CompletedTradeInfo.Commission);
    }

    switch( GetCurrentTaxStatus() )
    {
    case eNonTaxable: //no taxes
        break;
    case eTaxableAndWithhold: //calculate and withhold
        m_CompletedTradeInfo.SettlementAmount -= m_CompletedTradeInfo.Tax;
        break;
    case eTaxableAndDontWithhold: //calculate and do not withhold
        break;
    default: //should never happen
        assert(false);
    }
}

/*
*   Generate complete CASH_TRANSACTION row information.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateCashTransactionRow()
{
    char    S_NAME[cS_NAME_len + 1];

    if ( GetCurrentTradeIsCash() )
    {
        m_iCashTransactionRowCount = 1;

        m_TradeRow.m_CashTransaction.CT_DTS = GetCurrentTradeCompletionTime();
        m_TradeRow.m_CashTransaction.CT_T_ID = GetCurrentTradeID();
        m_TradeRow.m_CashTransaction.CT_AMT = GetCurrentSettlementAmount().DollarAmount();

        m_SecurityTable.CreateName(GetCurrentSecurityIndex(), S_NAME, static_cast<int>(sizeof(S_NAME)));

        sprintf(m_TradeRow.m_CashTransaction.CT_NAME, "%s %d shares of %s",
            (m_TradeTypeFile->GetRecord(GetCurrentTradeType()))->TT_NAME,
            GetCurrentTradeQty(),
            S_NAME);
    }
    else
    {
        m_iCashTransactionRowCount = 0; //no rows to insert
    }
}

/*
*   Generate complete SETTLEMENT row information.
*
*   PARAMETERS:
*               none
*
*   RETURNS:
*               none
*
*/
void CTradeGen::GenerateSettlementRow()
{
    m_iSettlementRowCount = 1;

    m_TradeRow.m_Settlement.SE_T_ID = GetCurrentTradeID();

    if (GetCurrentTradeIsCash())
    {
        strncpy(m_TradeRow.m_Settlement.SE_CASH_TYPE,
            "Cash Account",
            sizeof(m_TradeRow.m_Settlement.SE_CASH_TYPE)-1);
    }
    else
    {
        strncpy(m_TradeRow.m_Settlement.SE_CASH_TYPE,
            "Margin",
            sizeof(m_TradeRow.m_Settlement.SE_CASH_TYPE)-1);
    }

    m_TradeRow.m_Settlement.SE_CASH_DUE_DATE = GetCurrentTradeCompletionTime();
    m_TradeRow.m_Settlement.SE_CASH_DUE_DATE.Add(2, 0); // plus two days
    m_TradeRow.m_Settlement.SE_AMT = GetCurrentSettlementAmount().DollarAmount();
}


