#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "base/array.h"
#include "logging/logfile.h"
#include "logging/timer.h"

static const int REPETITIONS = 10000;
static const char BUFFER[] = "data";
static const int ALIGNMENTS[] = { 0, 27, 512, 2048, 4096, };

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "logfilebench [temp log file]\n");
        return 1;
    }
    const char* const log_path = argv[1];

    // Let's do a bunch of logging!
    for (int direct = 0; direct < 2; ++direct) {
#ifdef __APPLE__
        // O_DSYNC Not supported on Mac OS X
        const int O_DSYNC_MAX = 1;
#else
        const int O_DSYNC_MAX = 2;
#endif
        for (int o_dsync = 0; o_dsync < O_DSYNC_MAX; ++o_dsync) {
            for (int align_index = 0; align_index < base::arraySize(ALIGNMENTS); ++align_index) {
                if (direct && ALIGNMENTS[align_index] < logging::LogWriter::DIRECT_ALIGNMENT) continue;

                logging::SyncLogWriter log(log_path, ALIGNMENTS[align_index], direct, o_dsync);

                logging::Timer timer;
                timer.start();
                for (int i = 0; i < REPETITIONS; ++i) {
                    log.bufferedWrite(BUFFER, sizeof(BUFFER));
                    log.synchronize();
                }
                timer.stop();

                double us_per_log = (double) timer.getMicroseconds() / (double) REPETITIONS;
                printf("%s %s align %d: %" PRIi64 " / %d = %f us per log\n",
                        direct ? "direct" : "cached", o_dsync ? "O_DSYNC" : "fdatasync",
                        ALIGNMENTS[align_index], timer.getMicroseconds(),
                        REPETITIONS, us_per_log);
            }
        }
    }

    return 0;
}
