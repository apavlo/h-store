/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <jni.h>
#include <iostream>
#include <cstring>
#include "Bench.h"

using namespace std;

JNIEXPORT void JNICALL
Java_Bench_foo(JNIEnv *env, jclass cls)
{
}

JNIEXPORT void JNICALL
Java_Bench_bar(JNIEnv *env, jobject obj,
               jlong engine_ptr,
               jboolean need_undolog,
               jboolean need_readwriteset,
               jlong plan_fragment_id,
               jbyteArray serialized_parameterset)
{
}
JNIEXPORT void JNICALL
Java_Bench_bar2(JNIEnv *env, jobject obj,
               jlong engine_ptr,
               jlong plan_fragment_id,
               jobject serialized_parametersets,
               jint serialized_length,
               jobject output_buffer,
               jint output_capacity)
{
    const char* bytes = reinterpret_cast<const char*>(
            env->GetDirectBufferAddress(serialized_parametersets));
    char* data = reinterpret_cast<char*> (env->GetDirectBufferAddress(output_buffer));
    ::memcpy(data, bytes, serialized_length);
}
JNIEXPORT void JNICALL
Java_Bench_bar3(JNIEnv *env, jobject obj,
               jlong engine_ptr,
               jlong plan_fragment_id,
               jobject serialized_parametersets,
               jint serialized_length,
               jobject output_buffer,
               jint output_capacity)
{
    const char* bytes = reinterpret_cast<const char*>(
            env->GetDirectBufferAddress(serialized_parametersets));
    char* data = reinterpret_cast<char*> (env->GetDirectBufferAddress(output_buffer));
    ::memcpy(data, bytes, serialized_length);
    ::memcpy(data + (serialized_length * 1), bytes + (serialized_length * 1), serialized_length);
    ::memcpy(data + (serialized_length * 2), bytes + (serialized_length * 2), serialized_length);
    ::memcpy(data + (serialized_length * 3), bytes + (serialized_length * 3), serialized_length);
    ::memcpy(data + (serialized_length * 4), bytes + (serialized_length * 4), serialized_length);
}
JNIEXPORT jbyteArray JNICALL
Java_Bench_bar3a(JNIEnv *env, jobject obj,
               jlong engine_ptr,
               jlong plan_fragment_id,
               jbyteArray serialized_parametersets)
{
    jsize length = env->GetArrayLength(serialized_parametersets);
    jbyte *bytes = env->GetByteArrayElements(serialized_parametersets, NULL);
    env->ReleaseByteArrayElements(serialized_parametersets, bytes, JNI_ABORT);

    jbyteArray array = env->NewByteArray(length);
    env->SetByteArrayRegion(array, 0, length, bytes);
    return array;
}

JNIEXPORT void JNICALL
Java_Bench_bar4(JNIEnv *env, jobject obj,
               jlong engine_ptr,
               jlong plan_fragment_id,
               jobject serialized_parametersets,
               jint serialized_length,
               jobject output_buffer,
               jint output_capacity)
{
    env->GetDirectBufferAddress(serialized_parametersets);
    env->GetDirectBufferAddress(output_buffer);
}
