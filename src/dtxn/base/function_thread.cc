// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <assert.h>
#include <tr1/functional>

#include "base/assert.h"
#include "base/function_thread.h"

using std::tr1::function;

namespace {

void* thread_body(void* argument) {
    function<void ()>* func = reinterpret_cast<function<void ()>*>(argument);
    (*func)();
    delete func;
    return NULL;
}

}

FunctionThread::FunctionThread(const function<void ()>& func) {
    // Copy the function argument to the thread, so the caller can delete theirs.
    function<void ()>* func_ptr = new function<void ()>(func);
    //~ void* arg = const_cast<void*>(reinterpret_cast<void*>(&func));
    int success = pthread_create(&tid_, NULL, &thread_body, func_ptr);
    ASSERT(success == 0);
}

void FunctionThread::join() const {
    int success = pthread_join(tid_, NULL);
    ASSERT(success == 0);
}
