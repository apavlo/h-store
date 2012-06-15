/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
 *                                                                         *
 *  Based DBT5 (2006):                                                     *
 *  Rilson Nascimento                                                      *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

#include "edu_brown_benchmark_tpce_util_EGenRandom.h"
#include "Random.h"
#include <assert.h>

using namespace std;
using namespace TPCE;

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    newEGenRandom
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_newEGenRandom
  (JNIEnv *env, jobject obj, jlong seed)
{

    CRandom *rnd = new CRandom(static_cast<RNGSEED>(seed));
    assert(rnd != NULL);

    return reinterpret_cast<jlong>(rnd);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    rndNthElement
 * Signature: (JJJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_rndNthElement
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong baseSeed, jlong count)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	RNGSEED new_seed = rnd->RndNthElement(static_cast<RNGSEED>(baseSeed), static_cast<RNGSEED>(count));

	return static_cast<jlong>(new_seed);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    getSeed
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_getSeed
  (JNIEnv *env, jobject obj, jlong rnd_ptr)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	RNGSEED seed = rnd->GetSeed();

	return static_cast<jlong>(seed);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    setSeed
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_setSeed
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong seed)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	rnd->SetSeed(static_cast<RNGSEED>(seed));
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    setSeedNth
 * Signature: (JJJ)V
 */
JNIEXPORT void JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_setSeedNth
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong seed, jlong count)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);
	rnd->SetSeed(rnd->RndNthElement(static_cast<RNGSEED>(seed), static_cast<RNGSEED>(count)));
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    intRange
 * Signature: (JII)I
 */
JNIEXPORT jint JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_intRange
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jint min, jint max)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	int n = rnd->RndIntRange(static_cast<int>(min), static_cast<int>(max));

	return static_cast<jint>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    int64Range
 * Signature: (JJJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_int64Range
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong min, jlong max)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	INT64 n = rnd->RndInt64Range(static_cast<INT64>(min), static_cast<INT64>(max));

	return static_cast<jlong>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    intRangeExclude
 * Signature: (JIII)I
 */
JNIEXPORT jint JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_intRangeExclude
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jint low, jint high, jint exclude)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	int n = rnd->RndIntRangeExclude(static_cast<int>(low), static_cast<int>(high), static_cast<int>(exclude));

	return static_cast<jint>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    int64RangeExclude
 * Signature: (JJJJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_int64RangeExclude
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong low, jlong high, jlong exclude)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	INT64 n = rnd->RndInt64RangeExclude(static_cast<INT64>(low), static_cast<INT64>(high), static_cast<INT64>(exclude));

	return static_cast<jlong>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    rndNthIntRange
 * Signature: (JJJII)I
 */
JNIEXPORT jint JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_rndNthIntRange
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong seed, jlong count, jint min, jint max)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	int n = rnd->RndNthIntRange(static_cast<INT64>(seed), static_cast<INT64>(count), static_cast<int>(min), static_cast<int>(max));

	return static_cast<jint>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    rndNthInt64Range
 * Signature: (JJJJJ)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_rndNthInt64Range
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong seed, jlong count, jlong min, jlong max)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	RNGSEED n = rnd->RndNthInt64Range(static_cast<INT64>(seed), static_cast<INT64>(count), static_cast<INT64>(min), static_cast<INT64>(max));

	return static_cast<jlong>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    doubleRange
 * Signature: (JDD)D
 */
JNIEXPORT jdouble JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_doubleRange
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jdouble min, jdouble max)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	double n = rnd->RndDoubleRange(static_cast<double>(min), static_cast<double>(max));

	return static_cast<jdouble>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    doubleIncrRange
 * Signature: (JDDD)D
 */
JNIEXPORT jdouble JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_doubleIncrRange
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jdouble min, jdouble max, jdouble incr)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	double n = rnd->RndDoubleIncrRange(static_cast<double>(min), static_cast<double>(max), static_cast<double>(incr));

	return static_cast<jdouble>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    rndNU
 * Signature: (JJJII)J
 */
JNIEXPORT jlong JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_rndNU
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jlong p, jlong q, jint a, jint s)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

	INT64 n = rnd->NURnd(static_cast<INT64>(p), static_cast<INT64>(q), static_cast<INT64>(a), static_cast<INT64>(s));

	return static_cast<jlong>(n);
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    rndAlphaNumFormatted
 * Signature: (JLjava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_rndAlphaNumFormatted
  (JNIEnv *env, jobject obj, jlong rnd_ptr, jstring format)
{
	CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

    const char* format_chars = env->GetStringUTFChars(format, NULL);
    char *result_chars = new char[strlen(format_chars) + 1];

    rnd->RndAlphaNumFormatted(result_chars, format_chars);
    jstring result = env->NewStringUTF(result_chars);

    env->ReleaseStringUTFChars(format, format_chars);
    delete[] result_chars;

    return result;
}

/*
 * Class:     edu_brown_benchmark_tpce_util_EGenRandom
 * Method:    rndDouble
 * Signature: (J)D
 */
JNIEXPORT jdouble JNICALL Java_edu_brown_benchmark_tpce_util_EGenRandom_rndDouble
  (JNIEnv *env, jobject obj, jlong rnd_ptr)
{
    CRandom *rnd = reinterpret_cast<CRandom *>(rnd_ptr);

    double res = rnd->RndDouble();

    return static_cast<jdouble>(res);
}
