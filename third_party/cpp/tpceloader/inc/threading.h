/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
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
 * - Chris Chan-nui, Matt Emmerton
 */

#ifndef THREADING_H_INCLUDED
#define THREADING_H_INCLUDED

#include <memory>
#include <sstream>
#include <stdexcept>

#include "EGenStandardTypes.h"

namespace TPCE
{

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
#ifdef WIN32
DWORD WINAPI start_thread(LPVOID arg);
#else
extern "C"
void* start_thread(void *arg);
#endif

// Template to wrap around a class that has a ThreadBase::run() method and
// spawn it in a thread of its own.
template<typename T>
class Thread : public ThreadBase
{
    private:
        std::auto_ptr<T> obj_;
        TThread tid_;
    public:
        Thread(std::auto_ptr<T> throbj)
        : obj_(throbj)
        , tid_()
        {
        }
        T* obj() {
            return obj_.get();
        }
        void invoke() {
            obj_->run(this);
        }
        void start();
        void stop();
};

//////////////////////////////////////////////////////////
// Windows Implementation
//////////////////////////////////////////////////////////

#ifdef WIN32


template<typename T>
void Thread<T>::start()
{
    tid_ = CreateThread(NULL, 0, start_thread, this, NULL, NULL);
    if (tid_ == NULL) {
       std::ostringstream strm;
       strm << "CreateThread error: " << GetLastError();
       throw std::runtime_error(strm.str());
   }
}

template<typename T>
void Thread<T>::stop()
{
   DWORD rc = WaitForSingleObject(tid_, INFINITE);
   if (rc != 0) {
       std::ostringstream strm;
       strm << "WaitForSingleObject error: " << GetLastError();
       throw std::runtime_error(strm.str());
   }
}

//////////////////////////////////////////////////////////
// Non-Windows (pthread) Implementation
//////////////////////////////////////////////////////////

#else

template<typename T>
void Thread<T>::start()
{
   int rc = pthread_create(&tid_, NULL, start_thread, this);
   if (rc != 0) {
       std::ostringstream strm;
       strm << "pthread_create error: " << strerror(rc) << "(" << rc << ")";
       throw std::runtime_error(strm.str());
   }
}

template<typename T>
void Thread<T>::stop()
{
    int rc = pthread_join(tid_, NULL);
    if (rc != 0) {
       std::ostringstream strm;
       strm << "pthread_join error: " << strerror(rc) << "(" << rc << ")";
       throw std::runtime_error(strm.str());
    }
}

#endif

}

#endif // THREADING_H_INCLUDED
