#ifndef LOGGING_CYCLETIMER_H__
#define LOGGING_CYCLETIMER_H__

#include <stdint.h>

namespace logging {

class CycleTimer {
public:
    void start() {
        cpuSync();
        start_ = rdtsc();
    }

    void end() {
        cpuSync();
        end_ = rdtsc();
    }

    uint32_t getCycles() {
        return end_ - start_;
    }

private:
    uint32_t rdtsc() {
#ifdef __x86_64__
        uint32_t low;
        uint32_t high;
        asm volatile("rdtsc" : "=a"(low), "=d" (high));
        return low;
#elif defined(__i386__)
        uint64_t tsc;
        asm volatile("rdtsc" : "=A" (tsc));
        return (uint32_t) tsc;
#else
#error unsupported
#endif
    }

    void cpuSync() {
        // Calls CPUID to force the pipeline to be flushed
        uint32_t eax;
        uint32_t ebx;
        uint32_t ecx;
        uint32_t edx;
#ifdef __PIC__
        // PIC: Need to save and restore ebx See:
        // http://sam.zoy.org/blog/2007-04-13-shlib-with-non-pic-code-have-inline-assembly-and-pic-mix-well
        asm("pushl %%ebx\n\t" /* save %ebx */
                "cpuid\n\t"
                "movl %%ebx, %[ebx]\n\t" /* save what cpuid just put in %ebx */
                "popl %%ebx" : "=a"(eax), [ebx] "=r"(ebx), "=c"(ecx), "=d"(edx) : "a" (0)
                : "cc");
#else
        asm("cpuid" : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx) : "a" (0));
#endif
    }

    uint32_t start_;
    uint32_t end_;
};

}
#endif
