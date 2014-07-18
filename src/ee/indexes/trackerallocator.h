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

extern int currentIndexID;
extern int indexCounter;
// This might should be optimized
extern std::map <int32_t, int64_t> indexMemoryTable;


namespace hindex {

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
            indexMemoryTable[*currentIndex] += size * sizeof(size_type);
            //printf("allocate +++++++ %p %lu.\n", dataPtr, size * sizeof(size_type));
            //printf("%s\n", typeid(ValueType).name());
            return dataPtr;
        }

        pointer allocate(size_type size, pointer ptr) {
            pointer dataPtr = BaseAllocator::allocate(size, ptr);
            indexMemoryTable[*currentIndex] += size * sizeof(size_type);
            //printf("allocate +++++++ %p %lu.\n", dataPtr, size * sizeof(size_type));
            return dataPtr;
        }

        void deallocate(pointer ptr, size_type size) throw() {
            BaseAllocator::deallocate(ptr, size);
            indexMemoryTable[*currentIndex] -= size * sizeof(size_type);
        }

        void construct(pointer __ptr, const ValueType& __val) {
            new(__ptr) ValueType(__val);
            //printf("construct +++++++ %p %lu.\n", __ptr, sizeof(*__ptr));
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
/*
int64_t t;

int main() {
    map <int, int, less <int>, TrackerAllocator <pair <int, int>, &t > > m;
    m[1] = 2;
    printf("%ld\n", t);
}*/

#endif
