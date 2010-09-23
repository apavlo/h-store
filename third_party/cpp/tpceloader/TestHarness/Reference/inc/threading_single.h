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
 * Thread abstraction classes for machines that don't have a thread subsystem.
 *
 * These are pretty much only used by EGenValidate, as anything that really
 * needs threads isn't going to work correctly...
 */
#ifndef THREADING_SINGLE_H_INCLUDED
#define THREADING_SINGLE_H_INCLUDED

#include <memory>

namespace TPCE
{

// Thread abstraction classes these classes

class CCondition;
class CMutex
{
    public:
        CMutex();
        void lock();
        void unlock();

        friend class CCondition;
};

class CCondition
{
    private:
        CMutex&        mutex_;
    protected:
        CMutex&        mutex();
    public:
        CCondition(CMutex& mutex);
        void lock();
        void unlock();
        void wait() const;
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
        virtual void run() = 0;
};

// Template to wrap around a class that has a run() method and spawn it in a
// thread of its own.
template<typename T>
class Thread : public ThreadBase
{
    private:
        std::auto_ptr<T> obj_;
    public:
        Thread(std::auto_ptr<T> obj)
            : obj_(obj)
        {
        }
        void start() {
            obj_->run(this);
        }
        void stop() {
        }
        void run() {
        }
        T* obj() {
            return obj_.get();
        }
};

}

#endif // THREADING_SINGLE_H_INCLUDED
