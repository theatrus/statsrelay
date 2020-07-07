#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "rand.h"

size_t rand_gather(char* bytes, size_t len) {
    int fd = open("/dev/urandom", 0);
    if (fd == -1) {
        return -1;
    }
    size_t bread = 0;
    while(len - bread > 0) {
        ssize_t r = read(fd, &bytes[bread], len - bread);
        if (r == -1) {
            close(fd);
            return bread;
        }
        bread += r;
    }
    close(fd);
    return bread;
}