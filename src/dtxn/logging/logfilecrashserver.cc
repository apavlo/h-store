/*
Server for reporting log records from logfilecrash or minlogcrash

NOTE: This compiles stand alone, as a C or C++ binary.
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <netinet/in.h>
#include <sys/socket.h>

/* Make assert *not* warn about unused variables when compiled with NDEBUG. */
#ifdef NDEBUG
#undef assert
#define assert(x) do { (void)sizeof(x); } while(0)
#endif

/* disable -Wconversion to work around a bug in htons in glibc
TODO: Remove this eventually */
#if defined(__OPTIMIZE__) && __GNUC__ == 4 && __GNUC_MINOR__ >= 3
#pragma GCC diagnostic ignored "-Wconversion"
#endif

int main(int argc, const char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "logfilecrashserver [port]\n");
        return 1;
    }
    int port = atoi(argv[1]);
    assert(0 < port && port < (1 << 16));

    // Open a UDP sender
    int sock_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    assert(sock_fd >= 0);

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = (uint16_t) htons((uint16_t) port);
    bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    int error = bind(sock_fd, (struct sockaddr*) &bind_addr, sizeof(bind_addr));
    assert(error == 0);

    char buffer[16];
    while (1) {
        struct sockaddr_in from_addr;
        socklen_t from_addr_len = sizeof(from_addr);
        ssize_t bytes_in = recvfrom(sock_fd, buffer, sizeof(buffer), 0,
                        (struct sockaddr*) &from_addr, &from_addr_len);
        assert(bytes_in == sizeof(int32_t));
        assert(from_addr_len == sizeof(from_addr));

        int32_t* value = (int32_t*) buffer;
        printf("received %d\n", *value);
    }

    return 0;
}
