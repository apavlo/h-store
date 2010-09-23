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
*   A template that represents one input file in memory.
*
*/
#ifndef INPUT_FILE_H
#define INPUT_FILE_H

#include <sstream>
#include "EGenTables_stdafx.h"

namespace TPCE
{

template <typename T, typename TKeyAndElementsLimits> class CInputFile
{
    //Type of in-memory representation of input files
    typedef CFixedMap<T, TKeyAndElementsLimits> CFileInMemoryList;  //(key, data) pairs container

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
                throw CSystemErr(CSystemErr::eCreateFile, "CInputFile::ReadList");
            }
        }
        else
        {
            throw CSystemErr(CSystemErr::eCreateFile, "CInputFile::ReadList");
        }
    }

    void ReadList(const string &str)
    {
        istringstream tmpFile(str);
        ReadList(tmpFile);
    }

    void ReadList(istream &tmpFile) {
        T   row;
        memset(&row, 0, sizeof(row));
        int iThreshold = 0, iWeight;
        while(tmpFile.good())
        {
        tmpFile>>iWeight;   //first  the weight
        // We don't know if we've hit the end of the file
        // until after trying the first read.
        if( ! tmpFile.eof() )
        {
            row.Load(tmpFile);  //then the rest of the row
            iThreshold += iWeight;  //used as the key
            m_list.Add(iThreshold-1/*because weights start from 1*/, &row, iWeight);//add to the container
        }
        }
    }

public:

    //Constructor.

    //iTotalDataElements is the number of data elements in the file.
    //Right now it's the same as the number of lines since
    //there is only one entry per line.
    CInputFile(const char *szListFile)
    {
        ReadList(szListFile);
    }

    CInputFile(const string &str)
    {
        ReadList(str);
    }

    //return the whole record for a given key
    //returns the second member of the element pair
    T*  GetRecord(int key) { return m_list.GetElement(key); }

    //Get the next unique record in the set.
    T* GetRecordByPassKey( int iElementID )
    {
        return( m_list.GetElementByPassKey( iElementID ));
    }

    // Return current record count
    int RecordCount( )
    {
        return m_list.ElementCount();
    }

    //return the highest key number that exists in the list
    int GetGreatestKey() { return m_list.GetHighestKey(); }
};

}   // namespace TPCE

#endif //INPUT_FILE_H
