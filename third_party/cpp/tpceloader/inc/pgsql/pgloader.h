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
*	Class representing PostgreSQL database loader.
*/
#ifndef PG_LOADER_H
#define PG_LOADER_H

#include "PGSQLLoad.h"

namespace TPCE
{

/*
*	PGSQLLoader class.
*/
template <typename T> class CPGSQLLoader : public CBaseLoader<T>
{		
protected:
	T				m_row;
	connection*			m_Conn;			// libpqxx Connection
	work*				m_Txn;			// libpqxx Transaction
	tablewriter*			m_TW;			// libpqxx tablewriter
	vector<string>			buf;
	
	char			m_szHost[iMaxPGHost];		// host name
	char			m_szDBName[iMaxPGDBName];	// database name
	char 			m_szPostmasterPort[iMaxPGPort]; // PostgreSQL postmaster port
	char			m_szTable[iMaxPath];		// name of the table being loaded

public:
	typedef const T*	PT;		//pointer to the table row

protected:
	virtual inline void CopyRow(PT row) { memcpy(&m_row, row, sizeof(m_row)); };


public:	

	CPGSQLLoader(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable);
	virtual ~CPGSQLLoader(void);

	virtual void Init();			// resets to clean state; needed after FinishLoad to continue loading
	virtual void Commit();			// commit rows sent so far
	virtual void FinishLoad();		// finish load 
	void Connect();				// connect to PostgreSQL
	void Disconnect();			// disconnect - should not throw any exceptions (to put into the destructor)

	virtual void WriteNextRecord(PT next_record) = 0; // pure virtual function
};


/*
*	The constructor.
*/
template <typename T>
CPGSQLLoader<T>::CPGSQLLoader(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable)
: m_Conn(0)
, m_Txn(0)
, m_TW(0)
{	
	memset(m_szHost, 0, sizeof(m_szHost));
	strncpy(m_szHost, szHost, sizeof(m_szHost) - 1);

	memset(m_szDBName, 0, sizeof(m_szDBName));
	strncpy(m_szDBName, szDBName, sizeof(m_szDBName) - 1);

	memset(m_szPostmasterPort, 0, sizeof(m_szPostmasterPort));
	strncpy(m_szPostmasterPort, szPostmasterPort, sizeof(m_szPostmasterPort) - 1);

	memset(m_szTable, 0, sizeof(m_szTable));
	strncpy(m_szTable, szTable, sizeof(m_szTable) - 1);
}

/*
*	Destructor closes the connection.
*/
template <typename T>
CPGSQLLoader<T>::~CPGSQLLoader()
{
	Disconnect();
}

/*
*	Reset state e.g. close the connection, bind columns again, and reopen.
*	Needed after Commit() to continue loading.
*/
template <typename T>
void CPGSQLLoader<T>::Init()
{
	Connect();
}

/*
*	Create connection handles and connect to PostgreSQL
*/
template <typename T>
void CPGSQLLoader<T>::Connect()
{
	// Set up a connection to the backend

	char			szConnectStr[256];
	sprintf( szConnectStr, "host=%s dbname=%s port=%s", 
			m_szHost, m_szDBName, m_szPostmasterPort );
	
	m_Conn = new connection( szConnectStr );
	m_Txn = new work( *m_Conn, "txn" );
	m_TW = new tablewriter( *m_Txn, m_szTable );
 
	//Now prepare the insert statement. This is table-specific and must be
	//defined in subclasses.
}

/*
*	Commit sent rows. This needs to be called every so often to avoid row-level lock accumulation.
*/
template <typename T>
void CPGSQLLoader<T>::Commit()
{
	m_TW->complete();
	m_Txn->commit();

 	delete m_TW;
 	delete m_Txn;

	m_Txn = new work( *m_Conn, "txn" );
	m_TW = new tablewriter( *m_Txn, m_szTable );
}

/*
*	Commit sent rows. This needs to be called after the last row has been sent
*	and before the object is destructed. Otherwise all rows will be discarded.
*/
template <typename T>
void CPGSQLLoader<T>::FinishLoad()
{
	m_TW->complete();
	m_Txn->commit();
	delete m_TW;
	delete m_Txn;

	Disconnect();	// While destructor is not being called
}

/*
*	Disconnect from the server. Should not throw any exceptions.
*/
template <typename T>
void CPGSQLLoader<T>::Disconnect()
{
	// FIXME: It appears Disconnect() is called twice...  Hack around it by
	// checking setting m_Conn to NULL if we hit this code path.
	if (m_Conn != NULL) {
		m_Conn->disconnect();
		delete m_Conn;
		m_Conn = NULL;
	}
}

// Function that convert R type to string
template<typename R> inline string stringify(const R& x)	// convert to string
{
	ostringstream o;
	if (!(o << x))	throw runtime_error(string("stringify(") + typeid(x).name() + ")");
	return o.str();
}

}	// namespace TPCE

#endif //PG_LOADER_H
