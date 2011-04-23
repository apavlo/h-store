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

#ifndef EGEN_TEST_LOADER_H
#define EGEN_TEST_LOADER_H

#include <stddef.h>

/*
*   Test loader that compares two versions of the same row.
*/

namespace TPCETest
{

//  The first parameter is the type from Reference code.
//  The second parameter is the type from code being tested.
//
template <typename RefT, typename T> class CTestLoader
{
public:
    typedef const RefT* PRefT;  //pointer to the reference table row
    typedef const T*    PT;     //pointer to the table row

private:
    //  Row pointers for two versions.
    //  The actual data is stored externally in table classes.
    //
    PRefT           m_pRefRecord;
    PT              m_pRecord;

public:

    /*
    *   Constructor.
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           not applicable.
    */
    CTestLoader()
    : m_pRefRecord(NULL)
    , m_pRecord(NULL)
    {
    }

    /*
    *  Routine to store a new record from the Reference code (the one deemed correct).
    *
    *  PARAMETERS:
    *           IN  next_record
    *
    *  RETURNS:
    *           none.
    */
    void WriteNextRefRecord(PRefT next_record) { m_pRefRecord = next_record; }

    /*
    *  Routine to store a new record from the current code (the one being tested).
    *
    *  PARAMETERS:
    *           IN  next_record
    *
    *  RETURNS:
    *           none.
    */
    void WriteNextRecord(PT next_record) { m_pRecord = next_record; }

    /*
    *  Routine to compare two versions of the row.
    *
    *  PARAMETERS:
    *           none.
    *
    *  RETURNS:
    *           true    - if rows are different.
    *           false   - if rows are the same.
    */
    bool RowsDiffer() 
    { 
        if (sizeof(RefT) != sizeof(T))
        {
            return false;   // different sizes => cannot be the same
        }
        else
        {
            return (memcmp(m_pRefRecord, m_pRecord, sizeof(RefT)) != 0);    // explicit test in order to return bool (not int)
        }
    }
};

}   // namespace TPCETest

#endif // EGEN_TEST_LOADER_H
