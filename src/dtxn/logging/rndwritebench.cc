#include <cmath>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <errno.h>
#include <fcntl.h>

#include "base/array.h"
#include "base/assert.h"
#include "logging/logfile.h"
#include "logging/timer.h"
#include "randomgenerator.h"

using std::vector;

static const int REPETITIONS = 10000;
static const int PAGE_SIZE = 4096;
static const int ALIGNMENTS[] = { 1, 27, 512, 2048, 4096, };

static void* allocateAligned(int alignment, int size) {
#ifndef __APPLE__
    void* pointer = NULL;
    int error = posix_memalign(&pointer, alignment, size);
    ASSERT(error == 0);
#else
    // Pray!
    void* pointer = malloc(size);
    //~ CHECK(((intptr_t) pointer) % alignment == 0);
#endif
    return pointer;
}

static void freeAligned(void* aligned_pointer) {
    free(aligned_pointer);
}

double doRandomWrites(int fd, bool uncached, bool direct, int alignment) {
    char* buffer = (char*) allocateAligned(4096, PAGE_SIZE);

    // Randomly select some pages
    vector<int> page_ids(logging::LogWriter::LOG_SIZE / PAGE_SIZE);
    for (int i = 0; i < page_ids.size(); ++i) {
        page_ids[i] = i;
    }
    RandomGenerator rng;
    rng.shuffle(&page_ids, REPETITIONS);

    if (direct) {
        logging::FileWriter::setDirect(fd, true);
        // probe to see if the alignment is supported
        ssize_t bytes = pwrite(fd, buffer+alignment, alignment, alignment);
        if (bytes < 0) {
            printf("pwrite(fd, %p, %d, %d): %s\n", buffer+alignment, alignment, alignment, strerror(errno));
            assert(errno == EINVAL);
            freeAligned(buffer);
            logging::FileWriter::setDirect(fd, false);
            return NAN;
        }
        CHECK(bytes == alignment);
    }

    if (uncached) {
        // Attempt to drop the cache
#ifdef __linux__
        int fd2 = open("/proc/sys/vm/drop_caches", O_WRONLY, 0600);
        CHECK(fd2 >= 0);
        ssize_t fd2_bytes = write(fd2, "1", 1);
        CHECK(fd2_bytes == 1);
        fd2 = close(fd2);
        CHECK(fd2 == 0);
#elif defined(__APPLE__)
        int system_error = system("/usr/bin/purge");
        CHECK(system_error == 0);
#else
#warning Flushing cache not supported on this OS
#endif
        // Read a single page to hopefully prime the executable and the FS stuff.
        ssize_t bytes = pread(fd, buffer, PAGE_SIZE, 0);
        ASSERT(bytes == PAGE_SIZE);
    } else {
        // Read in the whole file to ensure it is cached
        for (int i = 0; i < logging::LogWriter::LOG_SIZE / PAGE_SIZE; ++i) {
            ssize_t bytes = pread(fd, buffer, PAGE_SIZE, i * PAGE_SIZE);
            ASSERT(bytes == PAGE_SIZE);
        }
    }
    int aligned_slots = PAGE_SIZE / alignment;

    logging::Timer timer;
    timer.start();
    for (int i = 0; i < page_ids.size(); ++i) {
        // Select an aligned slot
        int slot = rng.random() % aligned_slots;
        int offset = page_ids[i] * PAGE_SIZE + slot * alignment;
        off_t off = lseek(fd, offset, SEEK_SET);
        ASSERT(off == offset);
        ssize_t bytes = pwrite(fd, buffer, alignment, offset);
        ASSERT(bytes == alignment);
    }
    timer.stop();

    // fsync so all writes are flushed back
    int error = fsync(fd);
    ASSERT(error == 0);

    if (direct) {
        // Disable direct, if enabled
        logging::FileWriter::setDirect(fd, false);
    }

    freeAligned(buffer);

    double us_per_log = (double) timer.getMicroseconds() / (double) REPETITIONS;
    return us_per_log;
}

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "rndwritebench [temp file]\n");
        return 1;
    }
    const char* const path = argv[1];

    // Create a log file in order to preallocate a 64 MB chunk
    logging::LogWriter log(path, 0, false, false);
    log.close();

    // Open the pre-allocated file
    int fd = open(path, O_RDWR, 0600);
    CHECK(fd >= 0);

    // Let's do a bunch of random writes
    for (int uncached = 0; uncached < 2; ++uncached) {
        for (int direct = 0; direct < 2; ++direct) {
            for (int align_index = 0; align_index < base::arraySize(ALIGNMENTS); ++align_index) {
                double us_per_log = doRandomWrites(fd, uncached, direct, ALIGNMENTS[align_index]);
                printf("%scached %s align %d: %f us per log write\n",
                        uncached ? "un" : "", direct ? "direct" : "normal",
                        ALIGNMENTS[align_index], us_per_log);
            }
        }
    }

    return 0;
}
