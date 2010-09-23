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
 * - Christopher Chan-Nui
 */

#include "../inc/threading.h"

#include <iostream>
#include <sstream>
#include <stdexcept>
#ifndef WIN32
#include <sys/time.h>
#endif
#include <errno.h>
#include <stdlib.h>

#include "../inc/error.h"

namespace TPCE
{

#ifndef NO_THREADS
ThreadBase::~ThreadBase()
{
}

CCondition::CCondition(CMutex& pairedmutex)
    : mutex_(pairedmutex)
    , cond_()
{
    int rc = pthread_cond_init(&cond_, NULL);
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_init error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

void CCondition::lock()
{
    mutex_.lock();
}

void CCondition::unlock()
{
    mutex_.unlock();
}

void CCondition::wait() const
{
    int rc = pthread_cond_wait(&cond_, mutex_.mutex());
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_wait error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

void CCondition::signal()
{
    int rc = pthread_cond_signal(&cond_);
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_signal error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

void CCondition::broadcast()
{
    int rc = pthread_cond_broadcast(&cond_);
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_broadcast error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

bool CCondition::timedwait(const struct timespec& timeout) const
{
    int rc = pthread_cond_timedwait(&cond_, mutex_.mutex(), &timeout);
    if (rc == ETIMEDOUT) {
        return false;
    } else if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_timedwait error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
    return true;

}

bool CCondition::timedwait(long timeout /*us*/) const
{
    if (timeout < 0) {
        wait();
    }
    const int nsec_in_sec  = 1000000000;
    const int usec_in_sec  = 1000000;
    const int usec_in_nsec = 1000;
    struct timeval  tv;
    struct timespec ts;

    gettimeofday(&tv, NULL);

    ts.tv_sec  = tv.tv_sec + static_cast<long>(timeout / usec_in_sec);
    ts.tv_nsec = (tv.tv_usec + static_cast<long>(timeout % usec_in_sec) * usec_in_nsec);
    if (ts.tv_nsec > nsec_in_sec) {
        ts.tv_sec  += ts.tv_nsec / nsec_in_sec;
        ts.tv_nsec  = ts.tv_nsec % nsec_in_sec;
    }
    return timedwait(ts);
}


CMutex::CMutex()
    : mutex_()
{
    int rc = pthread_mutex_init(&mutex_, NULL);
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_mutex_init error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

pthread_mutex_t* CMutex::mutex()
{
    return &mutex_;
}

void CMutex::lock()
{
    int rc = pthread_mutex_lock(&mutex_);
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_wait error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

void CMutex::unlock()
{
    int rc = pthread_mutex_unlock(&mutex_);
    if (rc != 0) {
        std::ostringstream strm;
        strm << "pthread_cond_wait error: " << strerror(rc) << "(" << rc << ")";
        throw std::runtime_error(strm.str());
    }
}

extern "C"
void* start_thread(void *arg)
{
    ThreadBase* thrd = reinterpret_cast<ThreadBase*>(arg);
    // Catch exceptions here again because we're on a new stack
    // so any previous try/catch blocks won't catch exceptions
    // thrown in this thread.
    try {
        thrd->invoke();
        return NULL;
    } catch (std::exception& e) {
        std::cerr << "Caught Exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "Caught Exception: Unknown" << std::endl;
    }
    exit (1);
    return NULL; // Keep xlC happy with a return code...
}
#else // NO_THREADS
ThreadBase::~ThreadBase()
{
}

CCondition::CCondition(CMutex& mutex)
    : mutex_(mutex)
{
}

void CCondition::lock()
{
}

void CCondition::unlock()
{
}

void CCondition::wait() const
{
}

void CCondition::signal()
{
}

void CCondition::broadcast()
{
}

bool CCondition::timedwait(const struct timespec& timeout) const
{
    return true;
}

bool CCondition::timedwait(long timeout /*us*/) const
{
    return true;
}


CMutex::CMutex()
{
}

void CMutex::lock()
{
}

void CMutex::unlock()
{
}
#endif // NO_THREADS

}
