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
 * - Sergey Vasilevskiy
 */

/*
 * A template that represents one input file without the weight column in 
 * memory.
 */
#ifndef INPUT_FILE_NO_WEIGHT_H
#define INPUT_FILE_NO_WEIGHT_H

#include "EGenUtilities_stdafx.h"

namespace TPCE
{

template <typename T> class CInputFileNoWeight
{
    typedef vector<T>*  PVectorT;
    //Type of in-memory representation of input files
    typedef vector<PVectorT>    CFileInMemoryList;  //array of arrays

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
                throw CSystemErr(CSystemErr::eCreateFile, "CInputFileNoWeight::ReadList");
            }
        }
        else
        {
            throw CSystemErr(CSystemErr::eCreateFile, "CInputFileNoWeight::ReadList");
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
        int iIndex;
        int iLastIndex = -1; /* must be different from the 1st index in the input file */

                while(tmpFile.good())
                {
                    tmpFile>>iIndex;    //read the first column, which is the index
                    if( ! tmpFile.eof() )
                    {
                        row.Load(tmpFile);  //read the row
                        if (iIndex!=iLastIndex)
                        {
                            PVectorT parray_row = new vector<T>;
                            if (parray_row!=NULL)
                                m_list.push_back(parray_row);   //new array
                            else
                                throw CMemoryErr("CInputFileNoWeight::ReadFile");
                            iLastIndex = iIndex;
                        }
                        //Indices in the file start with 1 => substract 1.
                        m_list[(UINT)(iIndex-1)]->push_back(row);   //insert into the container
                    }
                }
    }

public:

    //Constructor.
    CInputFileNoWeight(const char *szListFile)
    {
        ReadList(szListFile);
    }

    CInputFileNoWeight(const string &str)
    {
        ReadList(str);
    }

    //Destructor
    ~CInputFileNoWeight()
    {
        for(size_t i=0; i<m_list.size(); ++i)
            delete m_list[i];
    }

    //Returns the element at a specific index
    PVectorT    GetRecord(UINT index) { return m_list[index]; };

    //Returns the number of records in the file (needed for TaxRates table
    UINT        GetSize() { return (UINT)m_list.size(); }
};

}   // namespace TPCE

#endif //INPUT_FILE_NO_WEIGHT_H
