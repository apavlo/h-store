 /* Copyright (C) 2013 by H-Store Project
 * Brown University
 * Carnegie Mellon University
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#ifndef _STLMMAPALLOCATOR_H_
#define _STLMMAPALLOCATOR_H_

#include <iostream>
#include <cassert>
#include <cstdlib>
#include <climits>
#include <memory>

#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "common/executorcontext.hpp"

#include "MMAPMemoryManager.h"

using namespace std;

namespace voltdb{

    template <class T> 
        class STL_MMAP_Allocator
        {
            friend class ExecutorContext;

            public:
                typedef T          value_type;
                typedef size_t     size_type;
                typedef ptrdiff_t  difference_type;

                typedef T*         pointer;
                typedef const T*   const_pointer;

                typedef T&         reference;
                typedef const T&   const_reference;

                STL_MMAP_Allocator() :
                    m_fileName(""), m_persistent(true), m_manager(NULL){
                        init();
                    }

                STL_MMAP_Allocator(const std::string fileName, bool persistent) :
                    m_fileName(fileName), m_persistent(persistent), m_manager(NULL){
                        init();
                    }

                template <class Other>
                    STL_MMAP_Allocator(const STL_MMAP_Allocator<Other> &other) {                                              
                        m_fileName = other.m_fileName ;
                        m_persistent = other.m_persistent ;
                        m_manager = other.m_manager;
                        init();
                    }

                template <class U>
                    struct rebind{
                        typedef STL_MMAP_Allocator<U> other ;
                    };

                void init(){
                    m_block_cnt = 0;
                    m_index_size = 0;

                    VOLT_WARN("Persistent : %d", (int)m_persistent);

                    const unsigned int DEFAULT_MMAP_SIZE = 256 * 1024 * 1024;

                    if(!m_manager){
                        if(m_persistent)
                            m_manager = new MMAPMemoryManager(DEFAULT_MMAP_SIZE, m_fileName, true);
                        else
                            m_manager = new MMAPMemoryManager(DEFAULT_MMAP_SIZE, m_fileName, false);
                    }

                    VOLT_WARN("Init m_manager : %p", m_manager);

                    assert(m_manager);
                }


                pointer address( reference r ) const{
                    return &r;
                }

                const_pointer address( const_reference r ) const{
                    return &r;
                }

                pointer allocate( size_type n, const void* = 0 ){
                    pointer p;
                    // p = (pointer) malloc(nSize);
                    p = (pointer) m_manager->allocate(n*sizeof(T));

                    return p;
                }

                void deallocate( pointer p, size_type n){ 
                    m_manager->deallocate(p, n*sizeof(T));
                }

                void construct( pointer p, const T& val ){
                    new (p) T(val);
                }

                void destroy( pointer p ){
                    p->~T();
                }

                size_type max_size() const{
                    return std::numeric_limits<std::size_t>::max()/sizeof(T);
                }

                std::string m_fileName;
                bool m_persistent ;
                MMAPMemoryManager* m_manager;
 
                long m_block_cnt  ; 
                long m_index_size ;  
 
            private:
                void operator =(const STL_MMAP_Allocator &);

        };

    template <class T> 
        bool operator==( const STL_MMAP_Allocator<T>& left, const STL_MMAP_Allocator<T>& right ){
            if(left.m_manager == right.m_manager)
                return true;
            return false;
        }

    template <class T> 
        bool operator!=( const STL_MMAP_Allocator<T>& left, const STL_MMAP_Allocator<T>& right){
            if(left.m_manager != right.m_manager)
                return true;
            return false;
        }

}

#endif /* _STLMMAPALLOCATOR_H_ */
