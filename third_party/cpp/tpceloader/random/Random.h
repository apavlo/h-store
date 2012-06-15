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

#ifndef RANDOM_H
#define RANDOM_H

#include "EGenStandardTypes.h"

namespace TPCE
{

// Constants
#define UInt64Rand_A_MULTIPLIER UINT64_CONST(6364136223846793005)
#define UInt64Rand_C_INCREMENT  UINT64_CONST(1)
#define UInt64Rand_ONE      UINT64_CONST(1)

// Independent RNG seed type.
typedef UINT64  RNGSEED;

// For efficiency, use a constant for 1/2^64.
#define UInt64Rand_RECIPROCAL_2_POWER_64 (5.421010862427522e-20)

class CRandom
{
private:
    RNGSEED m_seed;
    inline RNGSEED UInt64Rand(void);

public:
    CRandom(void);
    CRandom(RNGSEED seed);
    ~CRandom(void) { };

    void SetSeed(RNGSEED seed);
    inline RNGSEED GetSeed(void) { return m_seed; };
    RNGSEED RndNthElement( RNGSEED nSeed, RNGSEED nCount);

    // returns a random value in the range [0 .. 0.99999999999999999994578989137572]
    // care should be taken in casting the result as a float because of the
    // potential loss of precision.
    double RndDouble(void);

    //return Nth element in the sequence converted to double
    double RndNthDouble(RNGSEED Seed, RNGSEED N);

    // returns a random integer value in the range [min .. max]
    int RndIntRange(int min, int max);

    // returns a random 64-bit integer value in the range [min .. max]
    INT64 RndInt64Range(INT64 min, INT64 max);

    // returns a random integer value in the range [low .. high] excluding the value (exclude)
    INT64 RndInt64RangeExclude(INT64 low, INT64 high, INT64 exclude);

    //return Nth element in the sequence over the integer range
    int RndNthIntRange(RNGSEED Seed, RNGSEED N, int min, int max);

    //return Nth element in the sequence over the integer range
    INT64 RndNthInt64Range(RNGSEED Seed, RNGSEED N, INT64 min, INT64 max);

    // returns a random integer value in the range [low .. high] excluding the value (exclude)
    int RndIntRangeExclude(int low, int high, int exclude);

    // returns a random float value in the range of [min .. max]
    float RndFloatRange(float min, float max);

    // returns a random double value in the range of [min .. max]
    double RndDoubleRange(double min, double max);

    // returns a random double value in the range of [min .. max] with incr precision
    double RndDoubleIncrRange(double min, double max, double incr);

    // returns TRUE or FALSE, with the chance of TRUE being as specified by (percent)
    inline bool RndPercent(int percent)
        { return (RndIntRange(1, 100) <= percent); };

    // Returns a random integer percentage (i.e. whole number between 1 and 100, inclusive)
    inline int RndGenerateIntegerPercentage( )
    {
        return( RndIntRange( 1, 100 ));
    }

    /* Returns a non-uniform random 64-bit integer in range of [P .. Q].
    *
    *  NURnd is used to create a skewed data access pattern.  The function is
    *  similar to NURand in TPC-C.  (The two functions are identical when C=0
    *  and s=0.)
    *
    *  The parameter A must be of the form 2^k - 1, so that Rnd[0..A] will
    *  produce a k-bit field with all bits having 50/50 probability of being 0
    *  or 1.
    *
    *  With a k-bit A value, the weights range from 3^k down to 1 with the
    *  number of equal probability values given by C(k,i) = k! /(i!(k-i)!) for
    *  0 <= i <= k.  So a bigger A value from a larger k has much more skew.
    *
    *  Left shifting of Rnd[0..A] by "s" bits gets a larger interval without
    *  getting huge amounts of skew.  For example, when applied to elapsed time
    *  in milliseconds, s=10 effectively ignores the milliseconds, while s=16
    *  effectively ignores seconds and milliseconds, giving a granularity of
    *  just over 1 minute (65.536 seconds).  A smaller A value can then give
    *  the desired amount of skew at effectively one-minute resolution.
    */
    INT64 NURnd( INT64 P, INT64 Q, INT32 A, INT32 s );

    //Returns random alphanumeric string obeying a specific format.
    //For the format: n - given character must be numeric
    //                a - given character must be alphabetical
    //Example: "nnnaannnnaannn"
    void RndAlphaNumFormatted(char *szReturnString, const char *szFormat);

};

}   // namespace TPCE

#endif // RANDOM_H
