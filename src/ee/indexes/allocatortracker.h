#ifndef TRACKERALLOCATOR_H_
#define TRACKERALLOCATOR_H_

#include <cstdio>
#include <typeinfo>
#include <vector>
#include <cstring>
#include <map>
#include <string>
#include <cstdlib>
#include <iostream>


namespace h_index {

template<typename ValueType> 
class AllocatorTracker : public std::allocator<ValueType> {
    public:
        typedef typename std::allocator<ValueType> BaseAllocator;
        typedef typename BaseAllocator::pointer pointer;
        typedef typename BaseAllocator::size_type size_type;

        int64_t *memory_size;

        // This shouldn't be called. We should call the constructor with pointer to the memory_size.
        AllocatorTracker() throw() : BaseAllocator() {}

        AllocatorTracker(int64_t* m_ptr) throw() : BaseAllocator() {
            memory_size = m_ptr;
        }
        AllocatorTracker(const AllocatorTracker& allocator) throw() : BaseAllocator(allocator) {
            memory_size = allocator.memory_size;
        }
        template <class U> AllocatorTracker(const AllocatorTracker<U>& allocator) throw(): BaseAllocator(allocator) {
            memory_size = allocator.memory_size;
        }

        ~AllocatorTracker() {}

        template<class U> struct rebind {
            typedef AllocatorTracker<U> other;
        };

        pointer allocate(size_type size) {
            pointer dataPtr = BaseAllocator::allocate(size);
            *memory_size += size * sizeof(ValueType);
            //printf("%d allocate +++++++ %p %lu.\n", *currentIndex, dataPtr, size * sizeof(ValueType));
            //printf("%s\n", typeid(ValueType).name());
            return dataPtr;
        }

        pointer allocate(size_type size, pointer ptr) {
            pointer dataPtr = BaseAllocator::allocate(size, ptr);
            *memory_size += size * sizeof(ValueType);
            //printf("%d allocate +++++++ %p %lu.\n", *currentIndex, dataPtr, size * sizeof(ValueType));
            //printf("%s\n", typeid(ValueType).name());
            return dataPtr;
        }

        void deallocate(pointer ptr, size_type size) throw() {
            BaseAllocator::deallocate(ptr, size);
            *memory_size -= size * sizeof(ValueType);
        }

        void construct(pointer __ptr, const ValueType& __val) {
            new(__ptr) ValueType(__val);
            //printf("%d construct +++++++ %p %lu.\n", *currentIndex, __ptr, sizeof(ValueType));
            //printf("%s\n", typeid(ValueType).name());
            *memory_size += sizeof(ValueType);
        }

        void destroy(pointer __ptr) {
            //printf("-+-+-+- %08x.\n", __p);
            __ptr->~ValueType();
            *memory_size -= sizeof(ValueType);
        }
};

} // namespace h_index

#endif
