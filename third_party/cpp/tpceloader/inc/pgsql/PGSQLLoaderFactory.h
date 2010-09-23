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

// 2006 Rilson Nascimento


/*
*	loader class factory for PostgreSQL.
*	This class instantiates particular table loader classes.
*/
#ifndef PGSQL_LOADER_FACTORY_H
#define PGSQL_LOADER_FACTORY_H

namespace TPCE
{

class CPGSQLLoaderFactory : public CBaseLoaderFactory
{
	char		m_szHost[iMaxPGHost];		// host name
	char		m_szDBName[iMaxPGDBName];	// database name
	char		m_szPostmasterPort[iMaxPGPort]; // Postmaster port
	
public:	
	// Constructor
	CPGSQLLoaderFactory(char *szHost, char *szDBName, char *szPostmasterPort)
	{
		assert(szHost);
		assert(szDBName);
		assert(szPostmasterPort);

		strncpy(m_szHost, szHost, sizeof(m_szHost));
		strncpy(m_szDBName, szDBName, sizeof(m_szDBName));
		strncpy(m_szPostmasterPort, szPostmasterPort, sizeof(m_szPostmasterPort));
	}

	// Functions to create loader classes for individual tables.	
	
	virtual CBaseLoader<ACCOUNT_PERMISSION_ROW>*	CreateAccountPermissionLoader() 
	{
		return new CPGSQLAccountPermissionLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<ADDRESS_ROW>*				CreateAddressLoader() 
	{
		return new CPGSQLAddressLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<BROKER_ROW>*				CreateBrokerLoader() 
	{
		return new CPGSQLBrokerLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CASH_TRANSACTION_ROW>*		CreateCashTransactionLoader() 
	{
		return new CPGSQLCashTransactionLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CHARGE_ROW>*				CreateChargeLoader() 
	{
		return new CPGSQLChargeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<COMMISSION_RATE_ROW>*		CreateCommissionRateLoader() 
	{
		return new CPGSQLCommissionRateLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<COMPANY_COMPETITOR_ROW>*	CreateCompanyCompetitorLoader() 
	{
		return new CPGSQLCompanyCompetitorLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<COMPANY_ROW>*				CreateCompanyLoader() 
	{
		return new CPGSQLCompanyLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CUSTOMER_ACCOUNT_ROW>*		CreateCustomerAccountLoader() 
	{
		return new CPGSQLCustomerAccountLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CUSTOMER_ROW>*				CreateCustomerLoader() 
	{
		return new CPGSQLCustomerLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<CUSTOMER_TAXRATE_ROW>*		CreateCustomerTaxrateLoader() 
	{
		return new CPGSQLCustomerTaxRateLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<DAILY_MARKET_ROW>*			CreateDailyMarketLoader() 
	{
		return new CPGSQLDailyMarketLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<EXCHANGE_ROW>*				CreateExchangeLoader() 
	{
		return new CPGSQLExchangeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<FINANCIAL_ROW>*				CreateFinancialLoader() 
	{
		return new CPGSQLFinancialLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<HOLDING_ROW>*				CreateHoldingLoader() 
	{
		return new CPGSQLHoldingLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<HOLDING_HISTORY_ROW>*		CreateHoldingHistoryLoader() 
	{
		return new CPGSQLHoldingHistoryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<HOLDING_SUMMARY_ROW>*			CreateHoldingSummaryLoader() 
	{
		return new CPGSQLHoldingSummaryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<INDUSTRY_ROW>*				CreateIndustryLoader() 
	{
		return new CPGSQLIndustryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<LAST_TRADE_ROW>*			CreateLastTradeLoader() 
	{
		return new CPGSQLLastTradeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<NEWS_ITEM_ROW>*				CreateNewsItemLoader() 
	{
		return new CPGSQLNewsItemLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<NEWS_XREF_ROW>*				CreateNewsXRefLoader() 
	{
		return new CPGSQLNewsXRefLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<SECTOR_ROW>*				CreateSectorLoader() 
	{
		return new CPGSQLSectorLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<SECURITY_ROW>*				CreateSecurityLoader() 
	{
		return new CPGSQLSecurityLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<SETTLEMENT_ROW>*			CreateSettlementLoader() 
	{
		return new CPGSQLSettlementLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<STATUS_TYPE_ROW>*			CreateStatusTypeLoader() 
	{
		return new CPGSQLStatusTypeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TAXRATE_ROW>*				CreateTaxrateLoader() 
	{
		return new CPGSQLTaxrateLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_HISTORY_ROW>*			CreateTradeHistoryLoader() 
	{
		return new CPGSQLTradeHistoryLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_ROW>*					CreateTradeLoader() 
	{
		return new CPGSQLTradeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_REQUEST_ROW>*			CreateTradeRequestLoader() 
	{
		return new CPGSQLTradeRequestLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<TRADE_TYPE_ROW>*			CreateTradeTypeLoader() 
	{
		return new CPGSQLTradeTypeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<WATCH_ITEM_ROW>*			CreateWatchItemLoader() 
	{
		return new CPGSQLWatchItemLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<WATCH_LIST_ROW>*			CreateWatchListLoader() 
	{
		return new CPGSQLWatchListLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
	virtual CBaseLoader<ZIP_CODE_ROW>*				CreateZipCodeLoader() 
	{
		return new CPGSQLZipCodeLoad(m_szHost, m_szDBName, m_szPostmasterPort);
	};
};

}	// namespace TPCE

#endif //PGSQL_LOADER_FACTORY_H
