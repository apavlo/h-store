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

#ifndef EGEN_UTILITIES_STDAFX_H
#define EGEN_UTILITIES_STDAFX_H

#define WIN32_LEAN_AND_MEAN     // Exclude rarely-used stuff from Windows headers

#include <stddef.h>
#include <stdio.h>
#ifdef WIN32        //for Windows platform
#include <windows.h>
#else
#include <errno.h>  //for Unix
#include <sys/time.h>   //for gettimeofday() on Linux
#endif
#include <time.h>
#include <assert.h>
#include <list>

using namespace std;

// TODO: reference additional headers your program requires here
#include "EGenStandardTypes.h"
#include "DateTime.h"
#include "Random.h"
#include "error.h"
#include "TableConsts.h"
#include "MiscConsts.h"
#include "FixedMap.h"
#include "FixedArray.h"

// Include platform-dependent syncronization lock object
#ifdef WIN32
#include "win/SyncLock.h"   // Windows implementation
#include "win/error.h"
//#elif DEFINED( LINUX )
//#include "linux/SyncLock.h"   // Linux implementation
#else
#include "SyncLockInterface.h"  //empty stub
#endif

#include "RNGSeeds.h"
#include "EGenVersion.h"
#include "Money.h"

#endif  // #ifndef EGEN_UTILITIES_STDAFX_H
