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
*   Class representing just straight flat input file.
*/
#ifndef FLAT_FILE_H
#define FLAT_FILE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

template <typename T, typename TKeyAndElementsLimits> class CFlatFile
{
protected:
    //Type of in-memory representation of input files
    typedef CFixedArray<T, TKeyAndElementsLimits>   CFileInMemoryList;  //array of arrays

    CFileInMemoryList       m_list;

    void ReadList(const char *szListFile)
    {
        ifstream    tmpFile;

        if (szListFile)
        {
            tmpFile.open(szListFile, ios_base::in);
            if (tmpFile)
            {
                ReadList(tmpFile);
                tmpFile.close();
            }
            else
            {   //Open failed
                tmpFile.close();
                throw CSystemErr(CSystemErr::eCreateFile, "CFlatFile::ReadList");
            }
        }
        else
        {
            throw CSystemErr(CSystemErr::eCreateFile, "CFlatFile::ReadList");
        }
    }

    void ReadList(const string &str)
    {
        istringstream tmpFile(str);
        ReadList(tmpFile);
    }

    void ReadList(istream &tmpFile)
    {
        T   row;
        memset(&row, 0, sizeof(row));

        while(tmpFile.good())
        {
        row.Load(tmpFile);  //read the row
        // We don't know if we've hit the end of the file
        // until after trying the read.
        if( ! tmpFile.eof() )
        {
            m_list.Add(&row);   //insert into the container
        }

        }
    }


public:

    //Constructor.
    CFlatFile(const char *szListFile)
    {
        ReadList(szListFile);
    }

    CFlatFile(const string &str)
    {
        ReadList(str);
    }
    virtual ~CFlatFile() {}

    //Returns the element at a specific index
    T*  GetRecord(int index) { return &m_list[index]; };

    //Returns the size of the file (number of rows)
    UINT    GetSize()   {return (UINT)m_list.size();}
};

}   // namespace TPCE

#endif //FLAT_FILE_H
