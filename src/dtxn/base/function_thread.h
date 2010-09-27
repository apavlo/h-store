// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

// Creates threads to execute a single function. Uses std::tr1::function.

#ifndef FUNCTION_THREAD_H__
#define FUNCTION_THREAD_H__

#include <pthread.h>

namespace std { namespace tr1 {
    template <typename T> class function;
} }

class FunctionThread {
public:
    // Execute func in a new thread.
    // TODO: This copies the function object into the thread. Maybe it should take a
    // function* instead, that is deleted by the thread?
    FunctionThread(const std::tr1::function<void ()>& func);

    // Join with this thread.
    void join() const;

private:
    // Thread id.
    pthread_t tid_;

    // Not copyable!
    FunctionThread(const FunctionThread& other);
    FunctionThread& operator=(const FunctionThread& other);
};

#endif
