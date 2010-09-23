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
 * - Doug Johnson
 */

/*
*   Class representing a flat file loader.
*/
#ifndef FLAT_FILE_LOADER_H
#define FLAT_FILE_LOADER_H

#include "FlatFileLoad_stdafx.h"
#include "unusedflag.h"

namespace TPCE
{
#ifdef DATETIME_FORMAT
const int   FlatFileDateTimeFormat = DATETIME_FORMAT;   // user-defined
#else
const int   FlatFileDateTimeFormat = 12;        // YYYY-MM-DD HH:MM:SS.mmm
#endif
#ifdef TIME_FORMAT
const int   FlatFileTimeFormat = TIME_FORMAT;   // user-defined
#else
const int   FlatFileTimeFormat = 01;        // hh:mm:ss
#endif
#ifdef DATE_FORMAT
const int   FlatFileDateFormat = DATE_FORMAT;   // user-defined
#else
const int   FlatFileDateFormat = 10;        // YYYY-MM-DD
#endif

// Overwrite vs. append functionality for output flat files.
enum FlatFileOutputModes {
    FLAT_FILE_OUTPUT_APPEND = 0,
    FLAT_FILE_OUTPUT_OVERWRITE
};

/*
*   FlatLoader class.
*/
template <typename T> class CFlatFileLoader : public CBaseLoader<T>
{
protected:
    FILE            *hOutFile;

public:

    CFlatFileLoader(char *szFileName, FlatFileOutputModes FlatFileOutputMode);
    ~CFlatFileLoader(void);

    virtual void WriteNextRecord(const T* next_record UNUSED) {};
    void FinishLoad();  //finish load

};

/*
*       The constructor.
*/
template <typename T>
CFlatFileLoader<T>::CFlatFileLoader(char *szFileName, FlatFileOutputModes flatFileOutputMode)
{
        if( FLAT_FILE_OUTPUT_APPEND == flatFileOutputMode )
        {
                hOutFile = fopen( szFileName, "a" );
        }
        else if( FLAT_FILE_OUTPUT_OVERWRITE == flatFileOutputMode )
        {
                hOutFile = fopen( szFileName, "w" );
        }

        if (!hOutFile)
        {
                throw CSystemErr(CSystemErr::eCreateFile, "CFlatFileLoader<T>::CFlatFileLoader");
        }
}

/*
*       Destructor.
*/
template <typename T>
CFlatFileLoader<T>::~CFlatFileLoader()
{
        fclose(hOutFile);
}

/*
*       Commit sent rows. This needs to be called after the last row has been sent
*       and before the object is destructed. Otherwise all rows will be discarded.
*/
template <typename T>
void CFlatFileLoader<T>::FinishLoad()
{
        fflush(hOutFile);
}

}   // namespace TPCE

#endif //FLAT_FILE_LOADER_H
