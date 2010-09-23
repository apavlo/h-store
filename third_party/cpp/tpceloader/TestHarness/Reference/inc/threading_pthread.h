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

/*
 * Thread abstraction clases
 *
 * This file should be included before any other include files for pthread.h to
 * do its magic.
 */

#ifndef THREADING_PTHREAD_H_INCLUDED
#define THREADING_PTHREAD_H_INCLUDED

#include <pthread.h>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace TPCE
{

class CCondition;

// Standard mutex
class CMutex
{
    private:
        pthread_mutex_t mutex_;
        pthread_mutex_t* mutex();
    public:
        CMutex();
        void lock();
        void unlock();

        friend class CCondition;
};

// Condition class, requires a separate mutex to pair with.
class CCondition
{
    private:
                CMutex&        mutex_;
        mutable pthread_cond_t cond_;
    protected:
        CMutex&        mutex();
    public:
        CCondition(CMutex& mutex);
        void wait() const;
        void lock();
        void unlock();
        bool timedwait(const struct timespec& timeout) const;
        bool timedwait(long timeout=-1 /*us*/) const;
        void signal();
        void broadcast();
};

// Provide a RAII style lock for any class which supports
// lock() and unlock()
template<typename T>
class Locker
{
    private:
        T& mutex_;

    public:
        explicit Locker<T>(T& mutex)
            : mutex_(mutex)
        {
            mutex_.lock();
        }

        ~Locker<T>() {
            mutex_.unlock();
        }
};

// Base class to provide a run() method for objects which can be threaded.
// This is required because under pthreads we have to provide an interface
// through a C ABI call, which we can't do with templated classes.
class ThreadBase
{
    public:
        virtual ~ThreadBase();
        virtual void invoke() = 0;
};

// Call the run() method of passed argument.  Always returns NULL.
extern "C" void* start_thread(void *arg);

// Template to wrap around a class that has a ThreadBase::run() method and
// spawn it in a thread of its own.
template<typename T>
class Thread : public ThreadBase
{
    private:
        std::auto_ptr<T> obj_;
        pthread_t tid_;
    public:
        Thread(std::auto_ptr<T> throbj)
            : obj_(throbj)
            , tid_()
        {
        }
        void start() {
            int rc = pthread_create(&tid_,  NULL, start_thread, this);
            if (rc != 0) {
                std::ostringstream strm;
                strm << "pthread_create error: " << strerror(rc) << "(" << rc << ")";
                throw std::runtime_error(strm.str());
            }
        }
        void stop() {
            int rc = pthread_join(tid_, NULL);
            if (rc != 0) {
                std::ostringstream strm;
                strm << "pthread_join error: " << strerror(rc) << "(" << rc << ")";
                throw std::runtime_error(strm.str());
            }
        }
        void invoke() {
            obj_->run(this);
        }
        T* obj() {
            return obj_.get();
        }
};

}

#endif // THREADING_PTHREAD_H_INCLUDED
