#include <cstdio>
#include <typeinfo>
#include <vector>
#include <cstring>
#include <map>
#include <string>
#include <cstdlib>
#include <iostream>
using namespace std;

template<class ValueType, int64_t *Counter, class BaseAllocator = std::allocator<ValueType> > 
class TrackerAllocator : public BaseAllocator {
    public:
        typedef typename BaseAllocator::pointer pointer;
        typedef typename BaseAllocator::size_type size_type;

        TrackerAllocator() throw() : BaseAllocator() {}
        TrackerAllocator(const TrackerAllocator& allocator) throw() : BaseAllocator(allocator) {}
        template <class U> TrackerAllocator(const TrackerAllocator<U, Counter>& allocator) throw(): BaseAllocator(allocator) {}

        ~TrackerAllocator() {}

        template<class U> struct rebind {
            typedef TrackerAllocator<U, Counter> other;
        };

        pointer allocate(size_type size) {
            pointer dataPtr = BaseAllocator::allocate(size);
            *Counter += size * sizeof(size_type);
            //printf("allocate +++++++ %p %lu.\n", dataPtr, size * sizeof(size_type));
            //printf("%s\n", typeid(ValueType).name());
            return dataPtr;
        }

        pointer allocate(size_type size, pointer ptr) {
            pointer dataPtr = BaseAllocator::allocate(size, ptr);
            *Counter += size * sizeof(size_type);
            //printf("allocate +++++++ %p %lu.\n", dataPtr, size * sizeof(size_type));
            return dataPtr;
        }

        void deallocate(pointer ptr, size_type size) throw() {
            BaseAllocator::deallocate(ptr, size);
            *Counter -= size * sizeof(size_type);
        }

        void construct(pointer __ptr, const ValueType& __val) {
            new(__ptr) ValueType(__val);
            //printf("construct +++++++ %p %lu.\n", __ptr, sizeof(*__ptr));
            //printf("%s\n", typeid(ValueType).name());
            *Counter += sizeof(ValueType);
        }

        void destroy(pointer __ptr) {
            //printf("-+-+-+- %08x.\n", __p);
            __ptr->~ValueType();
            *Counter -= sizeof(ValueType);
        }
};

/*
int64_t t;

int main() {
    map <int, int, less <int>, TrackerAllocator <pair <int, int>, &t > > m;
    m[1] = 2;
    printf("%ld\n", t);
}*/
