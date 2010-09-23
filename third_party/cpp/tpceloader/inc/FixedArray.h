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
*   A fixed size array that provides random access by index.
*
*/
#ifndef FIXED_ARRAY_H
#define FIXED_ARRAY_H

#include "EGenUtilities_stdafx.h"

namespace TPCE
{

class CFixedArrayErr : public CBaseErr
{
public:
    enum eFixedArrayErrs
    {
        eNotEnoughMemory,
        eIndexOutOfRange,
        eOverflow
    };

    CFixedArrayErr( eFixedArrayErrs iErr, const char *szLoc) : CBaseErr(iErr, szLoc) {};
    int ErrorType() {return ERR_TYPE_FIXED_ARRAY;};

    const char *ErrorText() const
    {
        int i;
        static const char * szErrs[] = {
            "Not enough memory",
            "Index out of range.",
            "Cannot insert element - container is full.",
            ""
        };

        for(i = 0; szErrs[i][0]; i++)
        {
            // Confirm that an error message has been defined for the error code
            if ( i == m_idMsg )
                break;
        }

        return(szErrs[i][0] ? szErrs[i] : ERR_UNKNOWN);
    }
};

/*
*   Fixed-size array container
*
*   The first template parameter specifies data element type.
*   The second template parameter specifies a struct type that
*   contains the total number of data elements possible to store.
*   The struct in the second parameter must define TotalElements() public member function.
*/
template <typename TData, typename TElementsLimits>
class CFixedArray
{
    //Really using only the TotalElements() member function on this type
    TElementsLimits m_sLimits;
    int             m_iTotalElements;   // total elements from limits; taken once in the constructor for performance
    int             m_iCurrentElements; //current number of elements (cannot be greater than m_iTotalElements)
    TData           *m_pData;           //array of data elements

public:
    typedef TData*  PData;      //pointer to a data element

    //Constructor
    CFixedArray()
        : m_iCurrentElements(0) //no elements in the beginning
    {
        m_iTotalElements = m_sLimits.TotalElements();

        m_pData = new TData[m_iTotalElements];
    }
    //Destructor
    ~CFixedArray()
    {
        if (m_pData != NULL)
            delete [] m_pData;
    }

    //Add a (key, data) pair to the container.
    //Operation is performed in constant time for any (key, data) pair.
    void Add(TData *pData)
    {
        if (m_iCurrentElements < m_iTotalElements)
        {   //have place to insert new association

            m_pData[m_iCurrentElements] = *pData;   //copy the data value

            ++m_iCurrentElements;               //because just added one element
        }
        else
        {
            //container is full
            throw CFixedArrayErr(CFixedArrayErr::eOverflow, "CFixedArray::Add");
        }
    }

    //Element access by index
    TData& operator[](int iIndex)
    {
        assert(iIndex>=0 && iIndex < m_iTotalElements);
        //correct index value
        return m_pData[iIndex]; //return reference to the data value
    }

    //Return the total number of elements
    int size() {return m_iTotalElements;}
};

}   // namespace TPCE

#endif //FIXED_ARRAY_H
