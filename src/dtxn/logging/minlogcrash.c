/*
Crash testing for transactional log writes. Usage:


1. Start logfilecrashserver on a workstation:

./logfilecrashserver 12345

2. Start minlogcrash on the system under test:

./minlogcrash tmp workstation 12345 131072

3. Once the workstation starts receiving log records, pull the power from the
   back of the SSD.
4. Power off the system (my system doesn't support hotplug, so losing the
   power on the SSD makes it unhappy)
5. Reconnected power to the SSD.
6. Power the server back on.
7. Observe the output file using hexdump.

You should find that the file has *at least* the last record reported by
logfilecrashserver. It may have (part of) the next record. Error modes I have
observed:

* Missing the last reported record entirely.
* Truncated last record (doesn't have all block_size bytes)
* Media error in the kernel; the file is not completely readable.

*/

#ifdef __linux__
// Must be defined before other includes to get the fallocate prototype
#define _GNU_SOURCE
#include <features.h>

#if __GLIBC_PREREQ(2, 10)
// fallocate added in glibc 2.10
#define HAVE_FALLOCATE
#endif
#define HAVE_FDATASYNC
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define LOG_SIZE (128 << 20)

/* Make assert *not* warn about unused variables when compiled with NDEBUG. */
#ifdef NDEBUG
#undef assert
#define assert(x) do { (void)sizeof(x); } while(0)
#endif


int parseAddress(const char* address, int port, struct sockaddr_in* addr);

int main(int argc, const char* argv[]) {
    if (argc != 5) {
        fprintf(stderr, "minlogcrash [temp log file] [server address] [log port] [block size]\n");
        return 1;
    }
    const char* const log_path = argv[1];
    const char* const log_server = argv[2];
    int port = atoi(argv[3]);
    assert(0 < port && port < (16 << 10));
    int block_size = atoi(argv[4]);
    assert(block_size >= 512 && block_size % 512 == 0);
    assert(LOG_SIZE % block_size == 0);

    // Allocate the buffer
    char* buffer = malloc(block_size);
    assert(buffer != NULL);

    // Open a UDP sender    
    struct sockaddr_in server_addr;
    if (!parseAddress(log_server, port, &server_addr)) {
        fprintf(stderr, "Bad server address?\n");
        return 1;
    }
    int sock_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    assert(sock_fd >= 0);

    // Create the log file
    int log_fd = open(log_path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    assert(log_fd >= 0);
#ifdef HAVE_FALLOCATE
    // Pre-allocate disk space: this should hopefully mean the space is contiguous on ext4.
    static const int mode = 0;
    static const int fallocate_offset = 0;
    int fallocate_error= fallocate(log_fd, mode, fallocate_offset, LOG_SIZE);
    assert(fallocate_error == 0 || (fallocate_error == -1 &&
            (errno == EOPNOTSUPP || errno == ENOSYS)));
#endif

    // Zero fill the log file so we can use fdatasync
    memset(buffer, 0, block_size);
    int i = 0;
    for (i = 0; i < LOG_SIZE; i += block_size) {
        assert(i + block_size <= LOG_SIZE);
        ssize_t bytes = write(log_fd, buffer, block_size);
        assert(bytes == block_size);
    }

    // fsync() to ensure the file metadata is on disk before overwriting the zero filled file.
    int error = fsync(log_fd);
    assert(error == 0);
    off_t offset = lseek(log_fd, 0, SEEK_SET);
    assert(offset == 0);

    // Fill the buffer with non-zero data
    memset(buffer, 0xee, block_size);
    int32_t* counter = (int32_t*) buffer;

    printf("writing log records with block_size %d\n", block_size);

    *counter = 0;
    for (i = 0; i < LOG_SIZE; i += block_size) {
        assert(i + block_size <= LOG_SIZE);
        ssize_t bytes = write(log_fd, buffer, block_size);
        assert(bytes == block_size);
#ifdef HAVE_FDATASYNC
        error = fdatasync(log_fd);
#else
        error = fsync(log_fd);
#endif
        assert(error == 0);

        // Log record written: report to server
        bytes = sendto(sock_fd, counter, sizeof(*counter), 0,
                (const struct sockaddr*) &server_addr, sizeof(server_addr));
        assert(bytes == sizeof(*counter));
        //~ printf("%d\n", *counter);
        *counter += 1;
    }

    printf("wrote %d log records\n", *counter);
    free(buffer);
    return 0;
}

int parseAddress(const char* address, int port, struct sockaddr_in* addr) {
        // Convert the first part from text to an IP address

    struct addrinfo* node = NULL;
    int error = getaddrinfo(address, NULL, NULL, &node);
    if (error) return 0;

    int found = 0;
    struct addrinfo* ptr = node;
    while (ptr != NULL) {
        if (ptr->ai_family == AF_INET) {
            // Avoid alignment warning on Sparc
            assert(ptr->ai_addrlen == sizeof(*addr));
            memcpy(addr, ptr->ai_addr, sizeof(*addr));
            addr->sin_port = htons(port);
            found = 1;
            break;
        }
        ptr = ptr->ai_next;
    }
    freeaddrinfo(node);

    return found;
}
