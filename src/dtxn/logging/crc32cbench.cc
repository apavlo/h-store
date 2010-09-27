#include <cassert>
#include <cstdio>

#include "logging/crc32c.h"
#include "logging/cycletimer.h"

using namespace logging;

static const int TRIALS = 5;
static const int ITERATIONS = 10;

static const int BUFFER_MAX = 16384;
static const int ALIGNMENT = 8;

struct CRC32CFunctionInfo {
    CRC32CFunctionPtr crcfn;
    const char* name;
};

#define MAKE_FN_STRUCT(x) { x, # x }
static const CRC32CFunctionInfo FNINFO[] = {
    MAKE_FN_STRUCT(crc32cSarwate),
    MAKE_FN_STRUCT(crc32cSlicingBy4),
    MAKE_FN_STRUCT(crc32cSlicingBy8),
    MAKE_FN_STRUCT(crc32cHardware32),
#ifdef __LP64__
    MAKE_FN_STRUCT(crc32cHardware64),
#endif
};
#undef MAKE_FN_STRUCT

static size_t numValidFunctions() {
    size_t numFunctions = sizeof(FNINFO)/sizeof(*FNINFO);
    bool hasHardware = (detectBestCRC32C() != crc32cSlicingBy8);
    if (!hasHardware) {
        while (FNINFO[numFunctions-1].crcfn == crc32cHardware32 ||
                FNINFO[numFunctions-1].crcfn == crc32cHardware64) {
            numFunctions -= 1;
        }
    }
    return numFunctions;
}
static const size_t NUM_VALID_FUNCTIONS = numValidFunctions();


static const int DATA_LENGTHS[] = {
    16, 64, 256, 1024, 4096, 8192, 16384
};

void runTest(const CRC32CFunctionInfo& fninfo, const char* buffer, int length, bool aligned) {
    printf("%s,%s,%d", fninfo.name, aligned ? "true" : "false", length);

    for (int j = 0; j < TRIALS; ++j) {
        uint32_t crc = 0;
        CycleTimer timer;
        timer.start();
        for (int i = 0; i < ITERATIONS; ++i) {
            crc = fninfo.crcfn(crc32cInit(), buffer, length);
            crc = crc32cFinish(crc);
        }
        timer.end();

        uint32_t cycles = timer.getCycles();
        printf(",%d", cycles);
    }
    printf("\n");
}

int main() {
    char* buffer = new char[BUFFER_MAX + ALIGNMENT];
    char* aligned_buffer = (char*) (((intptr_t) buffer + (ALIGNMENT-1)) & ~(ALIGNMENT-1));
    assert(aligned_buffer + BUFFER_MAX <= buffer + BUFFER_MAX + ALIGNMENT);

    // fill the buffer with non-zero data
    for (int i = 0; i < BUFFER_MAX; ++i) {
        aligned_buffer[i] = (char) i;
    }

    printf("function,aligned,bytes,cycles,cycles,cycles,cycles,cycles\n");
    for (size_t fnIndex = 0; fnIndex < NUM_VALID_FUNCTIONS; ++fnIndex) {
        for (int aligned = 0; aligned < 2; ++aligned) {
            for (size_t lengthIndex = 0; lengthIndex < sizeof(DATA_LENGTHS)/sizeof(*DATA_LENGTHS);
                    ++lengthIndex) {
                int length = DATA_LENGTHS[lengthIndex];
                const char* data = aligned_buffer;
                // For mis-alignment, add one to the front and remove one from the back
                if (!aligned) {
                    data += 1;
                    length -= 1;
                }
                runTest(FNINFO[fnIndex], data, length, aligned);
            }
        }
    }

    delete[] buffer;
    return 0;
}
