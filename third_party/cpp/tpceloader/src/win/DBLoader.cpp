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

#include "../../inc/win/ODBCLoad_stdafx.h"

using namespace TPCE;

//Explicit Instantiation to generate code
template class CDBLoader<ACCOUNT_PERMISSION_ROW>;
template class CDBLoader<ADDRESS_ROW>;
template class CDBLoader<BROKER_ROW>;
template class CDBLoader<CASH_TRANSACTION_ROW>;
template class CDBLoader<CHARGE_ROW>;
template class CDBLoader<COMMISSION_RATE_ROW>;
template class CDBLoader<COMPANY_ROW>;
template class CDBLoader<COMPANY_COMPETITOR_ROW>;
template class CDBLoader<CUSTOMER_ROW>;
template class CDBLoader<CUSTOMER_ACCOUNT_ROW>;
template class CDBLoader<CUSTOMER_TAXRATE_ROW>;
template class CDBLoader<DAILY_MARKET_ROW>;
template class CDBLoader<EXCHANGE_ROW>;
template class CDBLoader<FINANCIAL_ROW>;
template class CDBLoader<HOLDING_ROW>;
template class CDBLoader<HOLDING_HISTORY_ROW>;
template class CDBLoader<HOLDING_SUMMARY_ROW>;
template class CDBLoader<INDUSTRY_ROW>;
template class CDBLoader<LAST_TRADE_ROW>;
template class CDBLoader<NEWS_ITEM_ROW>;
template class CDBLoader<NEWS_XREF_ROW>;
template class CDBLoader<SECTOR_ROW>;
template class CDBLoader<SECURITY_ROW>;
template class CDBLoader<SETTLEMENT_ROW>;
template class CDBLoader<STATUS_TYPE_ROW>;
template class CDBLoader<TAXRATE_ROW>;
template class CDBLoader<TRADE_ROW>;
template class CDBLoader<TRADE_HISTORY_ROW>;
template class CDBLoader<TRADE_REQUEST_ROW>;
template class CDBLoader<TRADE_TYPE_ROW>;
template class CDBLoader<WATCH_ITEM_ROW>;
template class CDBLoader<WATCH_LIST_ROW>;
template class CDBLoader<ZIP_CODE_ROW>;

/*
*   The constructor.
*/
template <typename T>
CDBLoader<T>::CDBLoader(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable)
: m_cnt(0)
, m_henv(SQL_NULL_HENV)
, m_hdbc(SQL_NULL_HDBC)
, m_hstmt(SQL_NULL_HSTMT)
{
    memset(m_szServer, 0, sizeof(m_szServer));
    strncpy(m_szServer, szServer, sizeof(m_szServer) - 1);

    memset(m_szDatabase, 0, sizeof(m_szDatabase));
    strncpy(m_szDatabase, szDatabase, sizeof(m_szDatabase) - 1);

    memset(m_szLoaderParams, 0, sizeof(m_szLoaderParams));
    strncpy(m_szLoaderParams, szLoaderParams, sizeof(m_szLoaderParams) - 1);

    memset(m_szTable, 0, sizeof(m_szTable));
    strncpy(m_szTable, szTable, sizeof(m_szTable) - 1);
}

/*
*   Destructor closes the connection.
*/
template <typename T>
CDBLoader<T>::~CDBLoader()
{
    Disconnect();
}

/*
*   Reset state e.g. close the connection, bind columns again, and reopen.
*   Needed after Commit() to continue loading.
*/
template <typename T>
void CDBLoader<T>::Init()
{
    Connect();

    BindColumns();
}

/*
*   Create connection handles and connect to SQL Server.
*/
template <typename T>
void CDBLoader<T>::Connect()
{
    RETCODE     rc;

    if ( SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_henv) != SQL_SUCCESS )
        ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_ENV, m_henv);

    if ( SQLSetEnvAttr(m_henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0 ) != SQL_SUCCESS )
        ThrowError(CODBCERR::eSetEnvAttr);

    if ( SQLAllocHandle(SQL_HANDLE_DBC, m_henv, &m_hdbc) != SQL_SUCCESS )
        ThrowError(CODBCERR::eAllocHandle,SQL_HANDLE_DBC, m_henv);

    if ( SQLSetConnectAttr(m_hdbc, SQL_COPT_SS_BCP, (void *)SQL_BCP_ON, SQL_IS_INTEGER ) != SQL_SUCCESS )
        ThrowError(CODBCERR::eSetConnectAttr);

    {
        char            szConnectStr[256];
        char            szOutStr[1024];
        SQLSMALLINT     iOutStrLen;

        if (m_szLoaderParams[0] == '\0')
        {
            sprintf( szConnectStr, "DRIVER=SQL Server;SERVER=%s;DATABASE=%s",
                m_szServer, m_szDatabase );

            rc = SQLDriverConnect(m_hdbc, NULL, (SQLCHAR*)szConnectStr, sizeof(szConnectStr),
                (SQLCHAR*)szOutStr, sizeof(szOutStr), &iOutStrLen, SQL_DRIVER_NOPROMPT );
        }
        else
        {
            rc = SQLDriverConnect(m_hdbc, NULL, (SQLCHAR*)m_szLoaderParams, sizeof(m_szLoaderParams),
                (SQLCHAR*)szOutStr, sizeof(szOutStr), &iOutStrLen, SQL_DRIVER_NOPROMPT );
        }

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eConnect);
    }

    if (SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc, &m_hstmt) != SQL_SUCCESS)
        ThrowError(CODBCERR::eAllocHandle, SQL_HANDLE_STMT, m_hdbc);

    if ( bcp_init(m_hdbc, m_szTable, NULL, NULL, DB_IN) != SUCCEED )
        ThrowError(CODBCERR::eBcpInit);

    //Now bind the columns to each particular value. This is table-specific and must be
    //defined in subclasses.
}

/*
*   Commit sent rows. This needs to be called every so often to avoid row-level lock accumulation.
*/
template <typename T>
void CDBLoader<T>::Commit()
{
    if (m_hdbc != SQL_NULL_HDBC)
    {
        if ( bcp_batch(m_hdbc) == -1 )
            ThrowError(CODBCERR::eBcpBatch);
    }
}

/*
*   Commit sent rows. This needs to be called after the last row has been sent
*   and before the object is destructed. Otherwise all rows will be discarded.
*/
template <typename T>
void CDBLoader<T>::FinishLoad()
{
    if (m_hdbc != SQL_NULL_HDBC)
    {
        if ( bcp_done(m_hdbc) == -1 )
            ThrowError(CODBCERR::eBcpDone);
    }
}

/*
*   Disconnect from the server. Should not throw any exceptions.
*/
template <typename T>
void CDBLoader<T>::Disconnect()
{
    // note: descriptors are automatically released when the connection is dropped
    if (m_hstmt != SQL_NULL_HSTMT)
    {
        SQLFreeHandle(SQL_HANDLE_STMT, m_hstmt);
        m_hstmt = SQL_NULL_HSTMT;
    }

    if (m_hdbc != SQL_NULL_HDBC)
    {
        SQLDisconnect(m_hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, m_hdbc);

        m_hdbc = SQL_NULL_HDBC;
    }

    if (m_henv != SQL_NULL_HENV)
    {
        SQLFreeHandle(SQL_HANDLE_ENV, m_henv);
        m_henv = SQL_NULL_HENV;
    }
}

/*
*   Loads a record into the database.
*/
template <typename T>
void CDBLoader<T>::WriteNextRecord(PT next_record)
{
    CopyRow(next_record);   //copy to the bound location inside this class first

    if ( bcp_sendrow(m_hdbc) != SUCCEED )
        ThrowError(CODBCERR::eBcpSendrow);
    m_cnt++;
    //Commit every so often
    //if (m_cnt % 10000 == 0)
    //  if ( bcp_batch(m_hdbc) == -1 )
    //      ThrowError(CODBCERR::eBcpBatch);
}

template <typename T>
void CDBLoader<T>::ThrowError( CODBCERR::ACTION eAction, SQLSMALLINT HandleType = 0, SQLHANDLE Handle = SQL_NULL_HANDLE )
{
    RETCODE     rc;
    SDWORD      lNativeError;
    char        szState[6];
    char        szMsg[SQL_MAX_MESSAGE_LENGTH];
    char        szTmp[6*SQL_MAX_MESSAGE_LENGTH];
    CODBCERR   *pODBCErr;           // not allocated until needed (maybe never)

    pODBCErr = new CODBCERR();

    pODBCErr->m_NativeError = 0;
    pODBCErr->m_eAction = eAction;
    pODBCErr->m_bDeadLock = FALSE;

    if (Handle == SQL_NULL_HANDLE)
    {
        switch (eAction)
        {
        case CODBCERR::eSetEnvAttr:
            HandleType = SQL_HANDLE_ENV;
            Handle = m_henv;
            break;

        case CODBCERR::eBcpBind:
        case CODBCERR::eBcpControl:
        case CODBCERR::eBcpBatch:
        case CODBCERR::eBcpDone:
        case CODBCERR::eBcpInit:
        case CODBCERR::eBcpSendrow:
        case CODBCERR::eConnect:
        case CODBCERR::eSetConnectAttr:
            HandleType = SQL_HANDLE_DBC;
            Handle = m_hdbc;
            break;

    /*
        case eAllocHandle:
        case eNone:
        case eUnknown:
        case eAllocConn:
        case eConnOption:
        case eAllocStmt:
        case eExecDirect:
        case eBindParam:
        case eBindCol:
        case eFetch:
        case eFetchScroll:
        case eMoreResults:
        case ePrepare:
        case eExecute:
        case eSetStmtAttr:
            HandleType = ;
            Handle = ;
    */
        }
    }

    szTmp[0] = 0;
    int i = 0;
    while (TRUE)
    {

        rc = SQLGetDiagRec( HandleType, Handle, ++i, (BYTE *)&szState, &lNativeError,
                      (BYTE *)&szMsg, sizeof(szMsg), NULL);
        if (rc == SQL_NO_DATA)
            break;

        // check for deadlock
//      if (lNativeError == 1205 || (lNativeError == iErrOleDbProvider &&
//          strstr(szMsg, sErrTimeoutExpired) != NULL))
//          pODBCErr->m_bDeadLock = TRUE;

        // capture the (first) database error
        if (pODBCErr->m_NativeError == 0 && lNativeError != 0)
            pODBCErr->m_NativeError = lNativeError;

        // quit if there isn't enough room to concatenate error text
        if ( (strlen(szMsg) + 2) > (sizeof(szTmp) - strlen(szTmp)) )
            break;

        // include line break after first error msg
        if (szTmp[0] != 0)
            strcat( szTmp, "\n");
        //sprintf(szTmp, "%sSQLState=%s %s", szTmp, szState, szMsg );
        strcat(szTmp, szMsg);
    }

    if (pODBCErr->m_odbcerrstr != NULL)
    {
        delete [] pODBCErr->m_odbcerrstr;
        pODBCErr->m_odbcerrstr = NULL;
    }

    if (szTmp[0] != 0)
    {
        pODBCErr->m_odbcerrstr = new char[ strlen(szTmp)+1 ];
        strcpy( pODBCErr->m_odbcerrstr, szTmp );
    }

    SQLFreeStmt(m_hstmt, SQL_CLOSE);

    throw pODBCErr;
}
