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

extern int currentIndexID;
extern int indexCounter;
extern std::vector <int64_t> indexMemoryTable;

template<typename ValueType, int *currentIndex> 
class TrackerAllocator : public std::allocator<ValueType> {
    public:
        typedef typename std::allocator<ValueType> BaseAllocator;
        typedef typename BaseAllocator::pointer pointer;
        typedef typename BaseAllocator::size_type size_type;

        TrackerAllocator() throw() : BaseAllocator() {}
        TrackerAllocator(const TrackerAllocator& allocator) throw() : BaseAllocator(allocator) {}
        template <class U> TrackerAllocator(const TrackerAllocator<U, currentIndex>& allocator) throw(): BaseAllocator(allocator) {}

        ~TrackerAllocator() {}

        template<class U> struct rebind {
            typedef TrackerAllocator<U, currentIndex> other;
        };

        pointer allocate(size_type size) {
            pointer dataPtr = BaseAllocator::allocate(size);
            indexMemoryTable[*currentIndex] += size * sizeof(ValueType);
            //printf("%d allocate +++++++ %p %lu.\n", *currentIndex, dataPtr, size * sizeof(ValueType));
            //printf("%s\n", typeid(ValueType).name());
            return dataPtr;
        }

        pointer allocate(size_type size, pointer ptr) {
            pointer dataPtr = BaseAllocator::allocate(size, ptr);
            indexMemoryTable[*currentIndex] += size * sizeof(ValueType);
            //printf("%d allocate +++++++ %p %lu.\n", *currentIndex, dataPtr, size * sizeof(ValueType));
            //printf("%s\n", typeid(ValueType).name());
            return dataPtr;
        }

        void deallocate(pointer ptr, size_type size) throw() {
            BaseAllocator::deallocate(ptr, size);
            indexMemoryTable[*currentIndex] -= size * sizeof(ValueType);
        }

        void construct(pointer __ptr, const ValueType& __val) {
            new(__ptr) ValueType(__val);
            //printf("%d construct +++++++ %p %lu.\n", *currentIndex, __ptr, sizeof(ValueType));
            //printf("%s\n", typeid(ValueType).name());
            indexMemoryTable[*currentIndex] += sizeof(ValueType);
        }

        void destroy(pointer __ptr) {
            //printf("-+-+-+- %08x.\n", __p);
            __ptr->~ValueType();
            indexMemoryTable[*currentIndex] -= sizeof(ValueType);
        }
};

} // namespace index

#endif
