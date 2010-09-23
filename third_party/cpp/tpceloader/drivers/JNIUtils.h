/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ---------------------------------------------------------------------
// COPIED ON 2009-11-20
// http://svn.apache.org/repos/asf/jakarta/bsf/trunk/src/org/apache/bsf/util/JNIUtils.c
// ---------------------------------------------------------------------

#ifndef TPCE_JNIUTILS_H
#define TPCE_JNIUTILS_H

#include <jni.h>
#include <inttypes.h>

namespace TPCE {
typedef struct tagTIMESTAMP_STRUCT TIMESTAMP_STRUCT;
}

#if defined(__cplusplus)
extern "C" {
#endif

/* make objects from primitives */
extern jobject makeString(JNIEnv *jenv, const char *val);
extern jobject makeDate(JNIEnv *jenv, TPCE::TIMESTAMP_STRUCT &val);
extern jobject makeBoolean(JNIEnv *jenv, int val);
extern jobject makeByte(JNIEnv *jenv, int val);
extern jobject makeShort(JNIEnv *jenv, int val);
extern jobject makeInteger(JNIEnv *jenv, int val);
extern jobject makeLong(JNIEnv *jenv, int64_t val);
extern jobject makeFloat(JNIEnv *jenv, float val);
extern jobject makeDouble(JNIEnv *jenv, double val);

extern void checkException(JNIEnv *env);
extern jobjectArray makeObjectArray(JNIEnv *env, const char *classname, int size);
#if defined(__cplusplus)
}
#endif
#endif
