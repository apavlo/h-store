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
*	Database loader class fot this table.
*/
#ifndef PGSQL_CUSTOMER_LOAD_H
#define PGSQL_CUSTOMER_LOAD_H

namespace TPCE
{

class CPGSQLCustomerLoad : public CPGSQLLoader <CUSTOMER_ROW>
{

public:
	CPGSQLCustomerLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "CUSTOMER")
		: CPGSQLLoader<CUSTOMER_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		buf.push_back(stringify(m_row.C_ID));			
		buf.push_back(m_row.C_TAX_ID);
		buf.push_back(m_row.C_ST_ID);
		buf.push_back(m_row.C_L_NAME);
		buf.push_back(m_row.C_F_NAME);
		buf.push_back(m_row.C_M_NAME);
		buf.push_back(stringify(m_row.C_GNDR));
		buf.push_back(stringify((int)m_row.C_TIER));
		buf.push_back(m_row.C_DOB.ToStr(iDateTimeFmt));
		buf.push_back(stringify(m_row.C_AD_ID));			
		buf.push_back(m_row.C_CTRY_1);
		buf.push_back(m_row.C_AREA_1);
		buf.push_back(m_row.C_LOCAL_1);
		buf.push_back(m_row.C_EXT_1);
		buf.push_back(m_row.C_CTRY_2);
		buf.push_back(m_row.C_AREA_2);
		buf.push_back(m_row.C_LOCAL_2);
		buf.push_back(m_row.C_EXT_2);
		buf.push_back(m_row.C_CTRY_3);
		buf.push_back(m_row.C_AREA_3);
		buf.push_back(m_row.C_LOCAL_3);
		buf.push_back(m_row.C_EXT_3);
		buf.push_back(m_row.C_EMAIL_1);
		buf.push_back(m_row.C_EMAIL_2);
			
		m_TW->insert(buf);
		buf.clear();
	}
};

}	// namespace TPCE

#endif //PGSQL_CUSTOMER_LOAD_H
