#include <cstdio>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <unistd.h>

#include "base/assert.h"
#include "logging/timer.h"

#ifndef __APPLE__
#define HAVE_POSIX_FALLOCATE
#define HAVE_FDATASYNC
#endif

static inline int syncFileDescriptor(int fd) {
#ifdef HAVE_FDATASYNC
    return fdatasync(fd);
#else
    return fsync(fd);
#endif
}

static const int LOG_SIZE = 64 << 20;  // 64 M
//~ static const int LOG_SIZE = 256 << 20;  // 256 M
static const int LOG_BUFFER_SIZE = 4096;  // 4k
//~ static const int LOG_BUFFER_SIZE = 4095;

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "logbench [temp log file]\n");
        return 1;
    }
    const char* const log_path = argv[1];

    int mode = O_RDWR | O_CREAT | O_TRUNC;
#ifdef __linux__
    mode |= O_NOATIME;  // Linux-only
#endif
    int fh = open(log_path, mode, 0600);
    assert(fh > 0);

    char buffer[LOG_BUFFER_SIZE];
    int error = 0;

#ifdef HAVE_POSIX_FALLOCATE
    // Allocate disk space: this should hopefully mean the space is contiguous on ext4
    error = posix_fallocate(fh, 0, LOG_SIZE);
    ASSERT(error == 0);
#endif

    // zero fill the log file:
    // IMPORTANT: With fdatasync, zero filling the file is important for good performance.
    // Without this, it seems as though ext4 must do file metadata updates, as the performance
    // is as bad as with fsync.
    memset(buffer, 0, sizeof(buffer));
    for (int i = 0; i < LOG_SIZE; i += (int) sizeof(buffer)) {
        int write_size = sizeof(buffer);
        if (i + write_size > LOG_SIZE) {
            write_size = LOG_SIZE - i;
        }
        ssize_t bytes = write(fh, buffer, write_size);
        ASSERT(bytes == write_size);
    }

    // fsync and reposition at the beginning of the file
    error = fsync(fh);
    ASSERT(error == 0);
    off_t offset = lseek(fh, 0, SEEK_SET);
    ASSERT(offset == 0);

    // Let's do a bunch of logging!
    logging::Timer timer;
    timer.start();
    int count = 0;
    static const int REPETITIONS = 1;
    for (int j = 0; j < REPETITIONS; ++j) {
        // One pass through the log. This will not overwrite any "excess" if log size is not
        // divisible by buffer size
        for (int i = 0; i+sizeof(buffer) <= LOG_SIZE; i += (int) sizeof(buffer)) {
            ssize_t bytes = write(fh, buffer, sizeof(buffer));
            ASSERT(bytes == sizeof(buffer));
            // On Linux ext4 (2.6.31), fdatasync is way better than fsync
            error = syncFileDescriptor(fh);
            ASSERT(error == 0);
            count += 1;
        }
        off_t offset = lseek(fh, 0, SEEK_SET);
        ASSERT(offset == 0);
    }
    timer.stop();

    ASSERT(count == (LOG_SIZE / sizeof(buffer)) * REPETITIONS);

    double us_per_log = (double) timer.getMicroseconds() / (double) count;
    printf("%" PRIi64 " / %d = %f us per log\n", timer.getMicroseconds(), count, us_per_log);

    return 0;
}
