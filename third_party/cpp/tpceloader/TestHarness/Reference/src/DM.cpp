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
 * - Doug Johnson, Matt Emmerton
 */

/******************************************************************************
*   Description:        Implementation of the DM class.
*                       See DM.h for a description.
******************************************************************************/

#include "../inc/DM.h"

using namespace TPCE;

const INT32     iDataMaintenanceTableCount = 12;
static const char*  DataMaintenanceTableName[iDataMaintenanceTableCount] =
                { "ACCOUNT_PERMISSION",
                  "ADDRESS",
                  "COMPANY",
                  "CUSTOMER",
                  "CUSTOMER_TAXRATE",
                  "DAILY_MARKET",
                  "EXCHANGE",
                  "FINANCIAL",
                  "NEWS_ITEM",
                  "SECURITY",
                  "TAXRATE",
                  "WATCH_ITEM" };

// Automatically generate unique RNG seeds.
// The CRandom class uses an unsigned 64-bit value for the seed.
// This routine automatically generates two unique seeds. One is used for
// the TxnInput generator RNG, and the other is for the TxnMixGenerator RNG.
// The 64 bits are used as follows.
//
//  Bits    0 - 31  Caller provided unique unsigned 32-bit id.
//  Bit     32      0
//  Bits    33 - 43 Number of days since the base time. The base time
//                  is set to be January 1 of the most recent year that is
//                  a multiple of 5. This allows enough space for the last
//                  field, and it makes the algorithm "timeless" by resetting
//                  the generated values every 5 years.
//  Bits    44 - 63 Current time of day measured in 1/10's of a second.
//
void CDM::AutoSetRNGSeeds( UINT32 UniqueId )
{
    CDateTime   Now;
    INT32       BaseYear;
    INT32       Tmp1, Tmp2;

    Now.GetYMD( &BaseYear, &Tmp1, &Tmp2 );

    // Set the base year to be the most recent year that was a multiple of 5.
    BaseYear -= ( BaseYear % 5 );
    CDateTime   Base( BaseYear, 1, 1 ); // January 1st in the BaseYear

    // Initialize the seed with the current time of day measured in 1/10's of a second.
    // This will use up to 20 bits.
    RNGSEED Seed;
    Seed = Now.MSec() / 100;

    // Now add in the number of days since the base time.
    // The number of days in the 5 year period requires 11 bits.
    // So shift up by that much to make room in the "lower" bits.
    Seed <<= 11;
    Seed += Now.DayNo() - Base.DayNo();

    // So far, we've used up 31 bits.
    // Save the "last" bit of the "upper" 32 for the RNG id. In
    // this case, it is always 0 since we don't have a second
    // RNG in this class.
    // In addition, make room for the caller's 32-bit unique id.
    // So shift a total of 33 bits.
    Seed <<= 33;

    // Now the "upper" 32-bits have been set with a value for RNG 0.
    // Add in the sponsor's unique id for the "lower" 32-bits.
    Seed += UniqueId;

    // Set the RNG to the unique seed.
    m_rnd.SetSeed( Seed );
    m_DriverDMSettings.cur.RNGSeed = Seed;
}

void CDM::Initialize( void )
{
    m_pLogger->SendToLogger(m_DriverGlobalSettings);

    m_iSecurityCount = m_pSecurities->GetActiveSecurityCount();
    m_iCompanyCount = m_pCompanies->GetActiveCompanyCount();
    m_iStartFromCompany = m_pCompanies->GetCompanyId(0);
    m_iDivisionTaxCount = m_pTaxRatesDivision->GetSize();
    m_iStartFromCustomer = iDefaultStartFromCustomer + iTIdentShift;
}

CDM::CDM( CDMSUTInterface *pSUT, CBaseLogger *pLogger, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, INT32 iDaysOfInitialTrades, INT32 iScaleFactor, UINT32 UniqueId )
    : m_DriverGlobalSettings ( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay )
    , m_DriverDMSettings( UniqueId, 0 )
    , m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount)
    , m_AccsAndPerms(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
    , m_pSecurities(inputFiles.Securities)
    , m_pCompanies(inputFiles.Company)
    , m_pTaxRatesDivision(inputFiles.TaxRatesDivision)
    , m_pStatusType(inputFiles.StatusType)
    , m_iDivisionTaxCount(0)
    , m_DataMaintenanceTableNum(0)
    , m_pSUT( pSUT )
    , m_pLogger ( pLogger )
{
    m_pLogger->SendToLogger("DM object constructed using constructor 1 (valid for publication: YES).");

    Initialize();
    AutoSetRNGSeeds( UniqueId );

    m_pLogger->SendToLogger(m_DriverDMSettings);    // log the RNG seeds
}

CDM::CDM( CDMSUTInterface *pSUT, CBaseLogger *pLogger, CInputFiles &inputFiles, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, INT32 iDaysOfInitialTrades, INT32 iScaleFactor, UINT32 UniqueId, RNGSEED RNGSeed )
    : m_DriverGlobalSettings ( iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay )
    , m_DriverDMSettings( UniqueId, RNGSeed )
    , m_rnd( RNGSeed )
    , m_CustomerSelection(&m_rnd, iDefaultStartFromCustomer, iActiveCustomerCount)
    , m_AccsAndPerms(inputFiles, iDefaultLoadUnitSize, iActiveCustomerCount, iDefaultStartFromCustomer)
    , m_pSecurities(inputFiles.Securities)
    , m_pCompanies(inputFiles.Company)
    , m_pTaxRatesDivision(inputFiles.TaxRatesDivision)
    , m_pStatusType(inputFiles.StatusType)
    , m_iDivisionTaxCount(0)
    , m_DataMaintenanceTableNum(0)
    , m_pSUT( pSUT )
    , m_pLogger( pLogger )
{
    m_pLogger->SendToLogger("DM object constructed using constructor 2 (valid for publication: NO).");

    Initialize();

    m_pLogger->SendToLogger(m_DriverDMSettings);    // log the RNG seeds
}

CDM::~CDM()
{
    m_pLogger->SendToLogger("DM object destroyed.");
}

TIdent CDM::GenerateRandomCustomerId()
{
    return m_rnd.RndInt64Range(m_iStartFromCustomer,
                             m_iStartFromCustomer + m_DriverGlobalSettings.cur.iActiveCustomerCount - 1);
}

TIdent CDM::GenerateRandomCustomerAccountId()
{
    TIdent iCustomerId;

    eCustomerTier iCustomerTier;

    m_CustomerSelection.GenerateRandomCustomer(iCustomerId, iCustomerTier);

    return( m_AccsAndPerms.GenerateRandomAccountId( m_rnd, iCustomerId, iCustomerTier));

}

TIdent CDM::GenerateRandomCompanyId()
{
    return m_rnd.RndInt64Range(m_iStartFromCompany, m_iStartFromCompany + m_iCompanyCount - 1);
}

TIdent CDM::GenerateRandomSecurityId()
{
    return m_rnd.RndInt64Range(0, m_iSecurityCount-1);
}

RNGSEED CDM::GetRNGSeed( void )
{
    return( m_rnd.GetSeed() );
}

void CDM::DoTxn( void )
{
    memset( &m_TxnInput, 0, sizeof( m_TxnInput ));
    strncpy( m_TxnInput.table_name,
            DataMaintenanceTableName[m_DataMaintenanceTableNum],
            sizeof(m_TxnInput.table_name ));

    switch( m_DataMaintenanceTableNum )
    {
    case 0: // ACCOUNT_PERMISSION
        m_TxnInput.acct_id = GenerateRandomCustomerAccountId();
        break;
    case 1: // ADDRESS
        if (m_rnd.RndPercent(67))
        {
            m_TxnInput.c_id = GenerateRandomCustomerId();
        }
        else
        {
            m_TxnInput.co_id = GenerateRandomCompanyId();
        }
        break;
    case 2: // COMPANY
        m_TxnInput.co_id = GenerateRandomCompanyId();
        break;
    case 3: // CUSTOMER
        m_TxnInput.c_id = GenerateRandomCustomerId();
        break;
    case 4: // CUSTOMER_TAXRATE
        m_TxnInput.c_id = GenerateRandomCustomerId();
        break;
    case 5: // DAILY_MARKET
        m_pSecurities->CreateSymbol( GenerateRandomSecurityId(), m_TxnInput.symbol, static_cast<int>(sizeof( m_TxnInput.symbol )));
        m_TxnInput.day_of_month = m_rnd.RndIntRange(1, 31);
        m_TxnInput.vol_incr = m_rnd.RndIntRange(-2, 3);
        if (m_TxnInput.vol_incr == 0)   // don't want 0 as increment
        {
            m_TxnInput.vol_incr = -3;
        }
        break;
    case 6: // EXCHANGE
        break;
    case 7: // FINANCIAL
        m_TxnInput.co_id = GenerateRandomCompanyId();
        break;
    case 8: // NEWS_ITEM
        m_TxnInput.co_id = GenerateRandomCompanyId();
        break;
    case 9: // SECURITY
        m_pSecurities->CreateSymbol( GenerateRandomSecurityId(), m_TxnInput.symbol, static_cast<int>(sizeof( m_TxnInput.symbol )));
        break;
    case 10: // TAXRATE
        const vector<TTaxRateInputRow>  *pRates;
        INT32                           iThreshold;

        pRates = m_pTaxRatesDivision->GetRecord(m_rnd.RndIntRange(0, m_iDivisionTaxCount - 1));
        iThreshold = m_rnd.RndIntRange(0, (INT32)pRates->size()-1);

        strncpy(m_TxnInput.tx_id,
                (*pRates)[iThreshold].TAX_ID,
                sizeof(m_TxnInput.tx_id));
        break;
    case 11: // WATCH_ITEM
        m_TxnInput.c_id = GenerateRandomCustomerId();
        break;

    default:
        assert(false);  // should never happen
    }

    m_pSUT->DataMaintenance( &m_TxnInput );

    m_DataMaintenanceTableNum = (m_DataMaintenanceTableNum + 1) % iDataMaintenanceTableCount;
}

void CDM::DoCleanupTxn( void )
{
    memset( &m_CleanupTxnInput, 0, sizeof( m_CleanupTxnInput ));

    // Compute Starting Trade ID (Copied from CETxnInputGenerator.cpp)
    m_CleanupTxnInput.start_trade_id = (TTrade)(( m_DriverGlobalSettings.cur.iDaysOfInitialTrades * HoursPerWorkDay * SecondsPerHour * ( m_DriverGlobalSettings.cur.iActiveCustomerCount / m_DriverGlobalSettings.cur.iScaleFactor )) * iAbortTrade / 100 ) + 1;  // 1.01 to account for rollbacks, +1 to get first runtime trade

        // Copy the status type id's from the flat file
        strncpy(m_CleanupTxnInput.st_pending_id,
                        (m_pStatusType->GetRecord(ePending))->ST_ID,
                        sizeof(m_CleanupTxnInput.st_pending_id));
        strncpy(m_CleanupTxnInput.st_submitted_id,
                        (m_pStatusType->GetRecord(eSubmitted))->ST_ID,
                        sizeof(m_CleanupTxnInput.st_submitted_id));
        strncpy(m_CleanupTxnInput.st_canceled_id,
                        (m_pStatusType->GetRecord(eCanceled))->ST_ID,
                        sizeof(m_CleanupTxnInput.st_canceled_id));

    // Execute Transaction
    m_pSUT->TradeCleanup( &m_CleanupTxnInput );
}
