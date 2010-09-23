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

#include "JNIUtils.h"

#include <jni.h>
#include <time.h>

#include "DateTime.h"

// ---------------------------------------------------------------------
// COPIED ON 2009-11-20
// http://svn.apache.org/repos/asf/jakarta/bsf/trunk/src/org/apache/bsf/util/JNIUtils.c
// ---------------------------------------------------------------------

jobject makeString(JNIEnv *jenv, const char *val) {
    return (jenv->NewStringUTF(val));
}

jobject makeDate(JNIEnv *jenv, TPCE::TIMESTAMP_STRUCT &val) {
    // Convert the TIMESTAMP_STRUCT to milliseconds
    struct tm tm;
    tm.tm_year = val.year - 1900;
    tm.tm_mon = val.month - 1;
    tm.tm_mday = val.day;
    tm.tm_hour = val.hour;
    tm.tm_min = val.minute;
    tm.tm_sec = val.second;
    
    time_t timestamp = mktime(&tm);
    
    if (timestamp == -1) {
        fprintf(stderr, "year:      %d [%d]\n", val.year, tm.tm_year);
        fprintf(stderr, "month:     %d [%d]\n", val.month, tm.tm_mon);
        fprintf(stderr, "day:       %d [%d]\n", val.day, tm.tm_mday);
        fprintf(stderr, "hour:      %d [%d]\n", val.hour, tm.tm_hour);
        fprintf(stderr, "minute:    %d [%d]\n", val.minute, tm.tm_min);
        fprintf(stderr, "second:    %d [%d]\n", val.second, tm.tm_sec);
        fprintf(stderr, "ERROR: Failed to convert TIMESTAMP_STRUCT to time_t\n");
        return (NULL);
    }
    jlong ms = static_cast<jlong>(timestamp) * 1000l;
//     fprintf(stderr, "timestamp: %lld\n", ms);

    jclass classobj = jenv->FindClass("java/util/Date");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(J)V");
    return jenv->NewObject(classobj, constructor, ms);
}

jobject makeBoolean(JNIEnv *jenv, int val) {
    jclass classobj = jenv->FindClass("java/lang/Boolean");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(Z)V");
    return jenv->NewObject(classobj, constructor, static_cast<jboolean>(val));
}

jobject makeByte(JNIEnv *jenv, int val) {
    jclass classobj = jenv->FindClass("java/lang/Byte");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(B)V");
    return jenv->NewObject(classobj, constructor, static_cast<jbyte>(val));
}

jobject makeShort(JNIEnv *jenv, int val) {
    jclass classobj = jenv->FindClass("java/lang/Short");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(S)V");
    return jenv->NewObject(classobj, constructor, static_cast<jshort>(val));
}

jobject makeInteger(JNIEnv *jenv, int val) {
    jclass classobj = jenv->FindClass("java/lang/Integer");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(I)V");
    return jenv->NewObject(classobj, constructor, static_cast<jint>(val));
}

jobject makeLong(JNIEnv *jenv, int64_t val) {
    jclass classobj = jenv->FindClass("java/lang/Long");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(J)V");
    return jenv->NewObject(classobj, constructor, static_cast<jlong>(val));
}

// jobject makeLong(JNIEnv *jenv, long val) {
//     fprintf(stderr, "makeLong: %d\n", val);
//     jclass classobj = jenv->FindClass("java/lang/Long");
//     jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(J)V");
//     return jenv->NewObject(classobj, constructor, static_cast<jlong>(val));
// }

jobject makeFloat(JNIEnv *jenv, float val) {
    jclass classobj = jenv->FindClass("java/lang/Float");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(F)V");
    return jenv->NewObject(classobj, constructor, static_cast<jfloat>(val));
}

jobject makeDouble(JNIEnv *jenv, double val) {
    jclass classobj = jenv->FindClass("java/lang/Double");
    jmethodID constructor = jenv->GetMethodID(classobj, "<init>", "(D)V");
    return jenv->NewObject(classobj, constructor, static_cast<jdouble>(val));
}


void checkException(JNIEnv *env) {
    jthrowable exc = env->ExceptionOccurred();
    if (exc) {
        jclass exccls(env->GetObjectClass(exc));
        jclass clscls(env->FindClass("java/lang/Class"));
        
        jmethodID getName(env->GetMethodID(clscls, "getName", "()Ljava/lang/String;"));
        jstring name(static_cast<jstring>(env->CallObjectMethod(exccls, getName)));
        char const* utfName(env->GetStringUTFChars(name, 0));
        
        char const* utfMessage = "Keep your knife close";
        // jmethodID getMessage(env->GetMethodID(exccls, "getMessage", "()Ljava/lang/String;"));
        // jstring message(static_cast<jstring>(env->CallObjectMethod(exc, getMessage)));
        // char const* utfMessage(env->GetStringUTFChars(message, 0));

        fprintf(stderr, "Exception: %s: %s\n", utfName, utfMessage); 
        env->ReleaseStringUTFChars(name, utfName);
        // env->ReleaseStringUTFChars(message, utfMessage);
        
        env->ExceptionDescribe();
        env->ExceptionClear();
        fprintf(stderr, "Caught and cleared Exception...\n");
    }
    return;
}

jobjectArray makeObjectArray(JNIEnv *env, const char *classname, int size) {
    jclass arrCls = env->FindClass(classname);
    if (arrCls == NULL) {
        fprintf(stderr, "Failed to find classname '%s'. Unable to construct object array\n", classname);
    }
    return (env->NewObjectArray(size, arrCls, NULL));
}