#ifndef LOGGING_TIMER_H__
#define LOGGING_TIMER_H__

#include <sys/time.h>
#include <time.h>

#include "base/assert.h"
#include "base/time.h"

namespace logging {

class Timer {
public:
    void start() {
        int error = gettimeofday(&start_, NULL);
        ASSERT(error == 0);
    }

    void stop() {
        int error = gettimeofday(&end_, NULL);
        ASSERT(error == 0);
    }

    int64_t getMicroseconds() {
        return base::timevalDiffMicroseconds(end_, start_);
    }

private:
    struct timeval start_;
    struct timeval end_;
};

}
#endif
