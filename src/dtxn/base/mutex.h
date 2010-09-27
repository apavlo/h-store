// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

// Wrapper around pthread_mutex_t

#ifndef MUTEX_H__
#define MUTEX_H__

#include <errno.h>
#include <pthread.h>
#include <sys/time.h>

#include "base/assert.h"

class Mutex {
public:
    Mutex() {
        int status = pthread_mutex_init(&mutex_, NULL);
        ASSERT(status == 0);
    }

    ~Mutex() {
        int status = pthread_mutex_destroy(&mutex_);
        ASSERT(status == 0);
    }

    void lock() {
        int status = pthread_mutex_lock(&mutex_);
        ASSERT(status == 0);
    }

    // Returns true if the lock is acquired.
    bool trylock() {
        int status = pthread_mutex_trylock(&mutex_);
        if (status == 0) return true;
        ASSERT(status == EBUSY);
        return false;
    }

    void unlock() {
        int status = pthread_mutex_unlock(&mutex_);
        ASSERT(status == 0);
    }

    pthread_mutex_t* raw_mutex() { return &mutex_; }

private:
    pthread_mutex_t mutex_;
};

// Locks mutex in the constructor, unlocks it in the destructor.
class MutexLock {
public:
    MutexLock(Mutex* mutex) :
            mutex_(mutex) {
        mutex_->lock();
    }

    ~MutexLock() {
        mutex_->unlock();
    }

private:
    Mutex* mutex_;
};

static const long int ONE_S_IN_NS = 1000000000;

class Condition {
public:
    // mutex is the Mutex that must be locked when using the condition.
    Condition(Mutex* mutex) :
            mutex_(mutex) {
        int status = pthread_cond_init(&cond_, NULL);
        ASSERT(status == 0);
    }

    ~Condition() {
        int status = pthread_cond_destroy(&cond_);
        ASSERT(status == 0);
    }

    // Wait for the condition to be signalled. This must be called with the Mutex held.
    // This must be called within a loop. See man pthread_cond_wait for details.
    void wait() {
        int status = pthread_cond_wait(&cond_, mutex_->raw_mutex());
        ASSERT(status == 0);
    }

    // Calls timedwait with a relative, instead of an absolute, timeout.
    bool timedwait_relative(const struct timespec& relative_time) {
        ASSERT(0 <= relative_time.tv_nsec && relative_time.tv_nsec < ONE_S_IN_NS);

        struct timespec absolute;
        // clock_gettime would be more convenient, but that needs librt
        // int status = clock_gettime(CLOCK_REALTIME, &absolute);
        struct timeval tv;
        int status = gettimeofday(&tv, NULL);
        ASSERT(status == 0);
        absolute.tv_sec = tv.tv_sec + relative_time.tv_sec;
        absolute.tv_nsec = tv.tv_usec * 1000 + relative_time.tv_nsec;
        if (absolute.tv_nsec >= ONE_S_IN_NS) {
            absolute.tv_nsec -= ONE_S_IN_NS;
            absolute.tv_sec += 1;
        }
        ASSERT(0 <= absolute.tv_nsec && absolute.tv_nsec < ONE_S_IN_NS);

        return timedwait(absolute);
    }

    // Returns true if the lock is acquired, false otherwise. abstime is the *absolute* time.
    bool timedwait(const struct timespec& absolute_time) {
        ASSERT(0 <= absolute_time.tv_nsec && absolute_time.tv_nsec < ONE_S_IN_NS);

        int status = pthread_cond_timedwait(&cond_, mutex_->raw_mutex(), &absolute_time);
        if (status == ETIMEDOUT) {
            return false;
        }
        ASSERT(status == 0);
        return true;
    }

    void signal() {
        int status = pthread_cond_signal(&cond_);
        ASSERT(status == 0);
    }

    // Wake all threads that are waiting on this condition.
    void broadcast() {
        int status = pthread_cond_broadcast(&cond_);
        ASSERT(status == 0);
    }

private:
    pthread_cond_t cond_;
    Mutex* mutex_;
};

#endif
