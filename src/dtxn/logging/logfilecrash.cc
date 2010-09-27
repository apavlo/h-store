#include <cstdlib>
#include <cstring>

#include <netinet/in.h>
#include <sys/socket.h>

#include "base/assert.h"
#include "logging/logfile.h"
#include "networkaddress.h"

int main(int argc, const char* argv[]) {
    if (argc != 6) {
        fprintf(stderr,
                "logfilecrash [temp log file] [log server:port] [align] [O_DIRECT?] [O_SYNC?]\n");
        return 1;
    }
    const char* const log_path = argv[1];
    const char* const log_server = argv[2];
    int align = atoi(argv[3]);
    int direct = atoi(argv[4]);
    int o_dsync = atoi(argv[5]);
    CHECK(align >= 512 && align % 512 == 0);
    CHECK(direct == 0 || direct == 1);
    CHECK(o_dsync == 0 || o_dsync == 1);

    // Open a UDP sender
    NetworkAddress logger;
    CHECK(logger.parse(log_server));
    struct sockaddr_in si_other = logger.sockaddr();

    int sock_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    CHECK(sock_fd >= 0);

    // Let's do a bunch of logging!
    logging::LogWriter log(log_path, align, direct, o_dsync);

    // Fill a buffer with non-zero data
    logging::MinimalBuffer buffer;

    void* bufptr;
    int length;
    buffer.getWriteBuffer(&bufptr, &length);

    // Fill the buffer with non-zero data
    memset(bufptr, 0xee, align);
    buffer.advanceWrite(align);
    int32_t* counter = (int32_t*) bufptr;

    printf("%s %s align %d\n", 
            direct ? "O_DIRECT" : "cached", o_dsync ? "O_DSYNC" : "fdatasync", align);

    *counter = 0;
    while (true) {
        log.syncWriteBuffer(&buffer);
        ssize_t sent = sendto(sock_fd, counter, sizeof(*counter), 0,
                (const sockaddr*) &si_other, sizeof(si_other));
        ASSERT(sent == sizeof(*counter));
        //~ printf("%d\n", *counter);
        buffer.advanceWrite(align);
        *counter += 1;
    }

    return 0;
}
